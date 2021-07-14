use super::check_balance::{check_my_coin_balance_for_swap, CheckBalanceError, CheckBalanceResult,
                           TakerFeeAdditionalInfo};
use super::pubkey_banning::ban_pubkey_on_failed_swap;
use super::swap_lock::{SwapLock, SwapLockOps};
use super::trade_preimage::{TradePreimageRequest, TradePreimageRpcError, TradePreimageRpcResult};
use super::{broadcast_my_swap_status, broadcast_swap_message_every, check_other_coin_balance_for_swap,
            dex_fee_amount_from_taker_coin, dex_fee_rate, dex_fee_threshold, get_locked_amount, my_swap_file_path,
            recv_swap_msg, swap_topic, AtomicSwap, LockedAmount, MySwapInfo, NegotiationDataMsg, NegotiationDataV2,
            RecoveredSwap, RecoveredSwapAction, SavedSwap, SavedTradeFee, SwapConfirmationsSettings, SwapError,
            SwapMsg, SwapsContext, TransactionIdentifier, WAIT_CONFIRM_INTERVAL};
use crate::mm2::lp_network::subscribe_to_topic;
use crate::mm2::lp_ordermatch::{MatchBy, OrderConfirmationsSettings, TakerAction, TakerOrderBuilder};
use crate::mm2::MM_VERSION;
use atomic::Atomic;
use bigdecimal::BigDecimal;
use coins::{lp_coinfind, CanRefundHtlc, FeeApproxStage, FoundSwapTxSpend, MmCoinEnum, TradeFee, TradePreimageValue};
use common::executor::Timer;
use common::log::{debug, error, warn};
use common::mm_ctx::MmArc;
use common::mm_error::prelude::*;
use common::mm_number::MmNumber;
use common::{bits256, now_ms, slurp, write, DEX_FEE_ADDR_RAW_PUBKEY};
use futures::{compat::Future01CompatExt, select, FutureExt};
use futures01::Future;
use http::Response;
use parking_lot::Mutex as PaMutex;
use primitives::hash::H264;
use rpc::v1::types::{Bytes as BytesJson, H160 as H160Json, H256 as H256Json, H264 as H264Json};
use serde_json::{self as json, Value as Json};
use std::path::PathBuf;
use std::sync::{atomic::Ordering, Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use uuid::Uuid;

pub fn stats_taker_swap_dir(ctx: &MmArc) -> PathBuf { ctx.dbdir().join("SWAPS").join("STATS").join("TAKER") }

pub fn stats_taker_swap_file_path(ctx: &MmArc, uuid: &Uuid) -> PathBuf {
    stats_taker_swap_dir(ctx).join(format!("{}.json", uuid))
}

fn save_my_taker_swap_event(ctx: &MmArc, swap: &TakerSwap, event: TakerSavedEvent) -> Result<(), String> {
    let path = my_swap_file_path(ctx, &swap.uuid);
    let content = try_s!(slurp(&path));
    let swap: SavedSwap = if content.is_empty() {
        SavedSwap::Taker(TakerSavedSwap {
            uuid: swap.uuid,
            my_order_uuid: swap.my_order_uuid,
            maker_amount: Some(swap.maker_amount.to_decimal()),
            maker_coin: Some(swap.maker_coin.ticker().to_owned()),
            taker_amount: Some(swap.taker_amount.to_decimal()),
            taker_coin: Some(swap.taker_coin.ticker().to_owned()),
            gui: ctx.gui().map(|g| g.to_owned()),
            mm_version: Some(MM_VERSION.to_owned()),
            events: vec![],
            success_events: vec![
                "Started".into(),
                "Negotiated".into(),
                "TakerFeeSent".into(),
                "MakerPaymentReceived".into(),
                "MakerPaymentWaitConfirmStarted".into(),
                "MakerPaymentValidatedAndConfirmed".into(),
                "TakerPaymentSent".into(),
                "TakerPaymentSpent".into(),
                "MakerPaymentSpent".into(),
                "Finished".into(),
            ],
            error_events: vec![
                "StartFailed".into(),
                "NegotiateFailed".into(),
                "TakerFeeSendFailed".into(),
                "MakerPaymentValidateFailed".into(),
                "MakerPaymentWaitConfirmFailed".into(),
                "TakerPaymentTransactionFailed".into(),
                "TakerPaymentWaitConfirmFailed".into(),
                "TakerPaymentDataSendFailed".into(),
                "TakerPaymentWaitForSpendFailed".into(),
                "MakerPaymentSpendFailed".into(),
                "TakerPaymentWaitRefundStarted".into(),
                "TakerPaymentRefunded".into(),
                "TakerPaymentRefundFailed".into(),
            ],
        })
    } else {
        try_s!(json::from_slice(&content))
    };

    if let SavedSwap::Taker(mut taker_swap) = swap {
        taker_swap.events.push(event);
        let new_swap = SavedSwap::Taker(taker_swap);
        let new_content = try_s!(json::to_vec(&new_swap));
        try_s!(write(&path, &new_content));
        Ok(())
    } else {
        ERR!("Expected SavedSwap::Taker at {}, got {:?}", path.display(), swap)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TakerSavedEvent {
    timestamp: u64,
    event: TakerSwapEvent,
}

impl TakerSavedEvent {
    /// get the next swap command that must be executed after swap restore
    fn get_command(&self) -> Option<TakerSwapCommand> {
        match self.event {
            TakerSwapEvent::Started(_) => Some(TakerSwapCommand::Negotiate),
            TakerSwapEvent::StartFailed(_) => Some(TakerSwapCommand::Finish),
            TakerSwapEvent::Negotiated(_) => Some(TakerSwapCommand::SendTakerFee),
            TakerSwapEvent::NegotiateFailed(_) => Some(TakerSwapCommand::Finish),
            TakerSwapEvent::TakerFeeSent(_) => Some(TakerSwapCommand::WaitForMakerPayment),
            TakerSwapEvent::TakerFeeSendFailed(_) => Some(TakerSwapCommand::Finish),
            TakerSwapEvent::MakerPaymentReceived(_) => Some(TakerSwapCommand::ValidateMakerPayment),
            TakerSwapEvent::MakerPaymentWaitConfirmStarted => Some(TakerSwapCommand::ValidateMakerPayment),
            TakerSwapEvent::MakerPaymentValidatedAndConfirmed => Some(TakerSwapCommand::SendTakerPayment),
            TakerSwapEvent::MakerPaymentValidateFailed(_) => Some(TakerSwapCommand::Finish),
            TakerSwapEvent::MakerPaymentWaitConfirmFailed(_) => Some(TakerSwapCommand::Finish),
            TakerSwapEvent::TakerPaymentSent(_) => Some(TakerSwapCommand::WaitForTakerPaymentSpend),
            TakerSwapEvent::TakerPaymentTransactionFailed(_) => Some(TakerSwapCommand::Finish),
            TakerSwapEvent::TakerPaymentDataSendFailed(_) => Some(TakerSwapCommand::RefundTakerPayment),
            TakerSwapEvent::TakerPaymentSpent(_) => Some(TakerSwapCommand::SpendMakerPayment),
            TakerSwapEvent::TakerPaymentWaitForSpendFailed(_) => Some(TakerSwapCommand::RefundTakerPayment),
            TakerSwapEvent::TakerPaymentWaitConfirmFailed(_) => Some(TakerSwapCommand::RefundTakerPayment),
            TakerSwapEvent::MakerPaymentSpent(_) => Some(TakerSwapCommand::Finish),
            TakerSwapEvent::MakerPaymentSpendFailed(_) => Some(TakerSwapCommand::RefundTakerPayment),
            TakerSwapEvent::TakerPaymentWaitRefundStarted { .. } => Some(TakerSwapCommand::RefundTakerPayment),
            TakerSwapEvent::TakerPaymentRefunded(_) => Some(TakerSwapCommand::Finish),
            TakerSwapEvent::TakerPaymentRefundFailed(_) => Some(TakerSwapCommand::Finish),
            TakerSwapEvent::Finished => None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TakerSavedSwap {
    pub uuid: Uuid,
    my_order_uuid: Option<Uuid>,
    pub events: Vec<TakerSavedEvent>,
    maker_amount: Option<BigDecimal>,
    maker_coin: Option<String>,
    taker_amount: Option<BigDecimal>,
    taker_coin: Option<String>,
    gui: Option<String>,
    mm_version: Option<String>,
    success_events: Vec<String>,
    error_events: Vec<String>,
}

impl TakerSavedSwap {
    pub fn maker_coin(&self) -> Result<String, String> {
        match self.events.first() {
            Some(event) => match &event.event {
                TakerSwapEvent::Started(data) => Ok(data.maker_coin.clone()),
                _ => ERR!("First swap event must be Started"),
            },
            None => ERR!("Can't get maker coin, events are empty"),
        }
    }

    pub fn taker_coin(&self) -> Result<String, String> {
        match self.events.first() {
            Some(event) => match &event.event {
                TakerSwapEvent::Started(data) => Ok(data.taker_coin.clone()),
                _ => ERR!("First swap event must be Started"),
            },
            None => ERR!("Can't get maker coin, events are empty"),
        }
    }

    pub fn is_finished(&self) -> bool {
        match self.events.last() {
            Some(event) => event.event == TakerSwapEvent::Finished,
            None => false,
        }
    }

    pub fn get_my_info(&self) -> Option<MySwapInfo> {
        match self.events.first() {
            Some(event) => match &event.event {
                TakerSwapEvent::Started(data) => Some(MySwapInfo {
                    my_coin: data.taker_coin.clone(),
                    other_coin: data.maker_coin.clone(),
                    my_amount: data.taker_amount.clone(),
                    other_amount: data.maker_amount.clone(),
                    started_at: data.started_at,
                }),
                _ => None,
            },
            None => None,
        }
    }

    pub fn is_recoverable(&self) -> bool {
        if !self.is_finished() {
            return false;
        };
        for event in self.events.iter() {
            match event.event {
                TakerSwapEvent::StartFailed(_)
                | TakerSwapEvent::NegotiateFailed(_)
                | TakerSwapEvent::TakerFeeSendFailed(_)
                | TakerSwapEvent::MakerPaymentValidateFailed(_)
                | TakerSwapEvent::TakerPaymentRefunded(_)
                | TakerSwapEvent::MakerPaymentSpent(_)
                | TakerSwapEvent::MakerPaymentWaitConfirmFailed(_) => {
                    return false;
                },
                _ => (),
            }
        }
        true
    }

    pub fn swap_data(&self) -> Result<&TakerSwapData, String> {
        match self.events.first() {
            Some(event) => match &event.event {
                TakerSwapEvent::Started(data) => Ok(data),
                _ => ERR!("First swap event must be Started"),
            },
            None => ERR!("Can't get swap_data, events are empty"),
        }
    }

    pub fn finished_at(&self) -> Result<u64, String> {
        match self.events.last() {
            Some(event) => match &event.event {
                TakerSwapEvent::Finished => Ok(event.timestamp / 1000),
                _ => ERR!("Last swap event must be Finished"),
            },
            None => ERR!("Can't get finished_at, events are empty"),
        }
    }

    pub fn is_success(&self) -> Result<bool, String> {
        if !self.is_finished() {
            return ERR!("Can not determine is_success state for not finished swap");
        }

        for event in self.events.iter() {
            if event.event.is_error() {
                return Ok(false);
            }
        }

        Ok(true)
    }
}

#[allow(clippy::large_enum_variant)]
pub enum RunTakerSwapInput {
    StartNew(TakerSwap),
    KickStart {
        maker_coin: MmCoinEnum,
        taker_coin: MmCoinEnum,
        swap_uuid: Uuid,
    },
}

impl RunTakerSwapInput {
    fn uuid(&self) -> &Uuid {
        match self {
            RunTakerSwapInput::StartNew(swap) => &swap.uuid,
            RunTakerSwapInput::KickStart { swap_uuid, .. } => &swap_uuid,
        }
    }
}

/// Starts the taker swap and drives it to completion (until None next command received).
/// Panics in case of command or event apply fails, not sure yet how to handle such situations
/// because it's usually means that swap is in invalid state which is possible only if there's developer error
/// Every produced event is saved to local DB. Swap status is broadcast to P2P network after completion.
pub async fn run_taker_swap(swap: RunTakerSwapInput, ctx: MmArc) {
    let uuid = swap.uuid().to_owned();
    let mut attempts = 0;
    let swap_lock = loop {
        match SwapLock::lock(&ctx, uuid, 40.).await {
            Ok(Some(l)) => break l,
            Ok(None) => {
                if attempts >= 1 {
                    warn!(
                        "Swap {} file lock is acquired by another process/thread, aborting",
                        uuid
                    );
                    return;
                } else {
                    attempts += 1;
                    Timer::sleep(40.).await;
                }
            },
            Err(e) => {
                error!("Swap {} file lock error: {}", uuid, e);
                return;
            },
        }
    };

    let (swap, mut command) = match swap {
        RunTakerSwapInput::StartNew(swap) => (swap, TakerSwapCommand::Start),
        RunTakerSwapInput::KickStart {
            maker_coin,
            taker_coin,
            swap_uuid,
        } => match TakerSwap::load_from_db_by_uuid(ctx, maker_coin, taker_coin, &swap_uuid) {
            Ok((swap, command)) => match command {
                Some(c) => {
                    log!("Swap " (uuid) " kick started.");
                    (swap, c)
                },
                None => {
                    log!("Swap " (uuid) " has been finished already, aborting.");
                    return;
                },
            },
            Err(e) => {
                log!("Error " (e) " loading swap " (uuid));
                return;
            },
        },
    };

    let mut touch_loop = Box::pin(
        async move {
            loop {
                match swap_lock.touch().await {
                    Ok(_) => (),
                    Err(e) => warn!("Swap {} file lock error: {}", uuid, e),
                };
                Timer::sleep(30.).await;
            }
        }
        .fuse(),
    );

    let ctx = swap.ctx.clone();
    subscribe_to_topic(&ctx, swap_topic(&swap.uuid)).await;
    let mut status = ctx.log.status_handle();
    let uuid = swap.uuid.to_string();
    let running_swap = Arc::new(swap);
    let weak_ref = Arc::downgrade(&running_swap);
    let swap_ctx = SwapsContext::from_ctx(&ctx).unwrap();
    swap_ctx.init_msg_store(running_swap.uuid, running_swap.maker);
    swap_ctx.running_swaps.lock().unwrap().push(weak_ref);
    let shutdown_rx = swap_ctx.shutdown_rx.clone();
    let swap_for_log = running_swap.clone();

    let mut swap_fut = Box::pin(
        async move {
            let mut events;
            loop {
                let res = running_swap.handle_command(command).await.expect("!handle_command");
                events = res.1;
                for event in events {
                    let to_save = TakerSavedEvent {
                        timestamp: now_ms(),
                        event: event.clone(),
                    };

                    save_my_taker_swap_event(&ctx, &running_swap, to_save).expect("!save_my_taker_swap_event");
                    if event.should_ban_maker() {
                        ban_pubkey_on_failed_swap(
                            &ctx,
                            running_swap.maker.bytes.into(),
                            &running_swap.uuid,
                            event.clone().into(),
                        )
                    }
                    status.status(&[&"swap", &("uuid", uuid.as_str())], &event.status_str());
                    running_swap.apply_event(event);
                }
                match res.0 {
                    Some(c) => {
                        command = c;
                    },
                    None => {
                        if let Err(e) = broadcast_my_swap_status(&running_swap.uuid, &ctx) {
                            log!("!broadcast_my_swap_status(" (uuid) "): " (e));
                        }
                        break;
                    },
                }
            }
        }
        .fuse(),
    );
    let mut shutdown_fut = Box::pin(shutdown_rx.recv().fuse());
    let do_nothing = (); // to fix https://rust-lang.github.io/rust-clippy/master/index.html#unused_unit
    select! {
        _swap = swap_fut => do_nothing, // swap finished normally
        _shutdown = shutdown_fut => log!("on_stop] swap " (swap_for_log.uuid) " stopped!"),
        _touch = touch_loop => unreachable!("Touch loop can not stop!"),
    };
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct TakerSwapData {
    pub taker_coin: String,
    pub maker_coin: String,
    maker: H256Json,
    my_persistent_pub: H264Json,
    lock_duration: u64,
    pub maker_amount: BigDecimal,
    pub taker_amount: BigDecimal,
    maker_payment_confirmations: u64,
    maker_payment_requires_nota: Option<bool>,
    taker_payment_confirmations: u64,
    taker_payment_requires_nota: Option<bool>,
    taker_payment_lock: u64,
    /// Allows to recognize one SWAP from the other in the logs. #274.
    uuid: Uuid,
    pub started_at: u64,
    maker_payment_wait: u64,
    maker_coin_start_block: u64,
    taker_coin_start_block: u64,
    /// A transaction fee that should be paid to send a `TakerFee`.
    /// Note this value is used to calculate locked amount only.
    fee_to_send_taker_fee: Option<SavedTradeFee>,
    /// A `TakerPayment` transaction fee.
    /// Note this value is used to calculate locked amount only.
    taker_payment_trade_fee: Option<SavedTradeFee>,
    /// A transaction fee that should be paid to spend a `MakerPayment`.
    /// Note this value is used to calculate locked amount only.
    maker_payment_spend_trade_fee: Option<SavedTradeFee>,
    #[serde(skip_serializing_if = "Option::is_none")]
    maker_coin_swap_contract_address: Option<BytesJson>,
    #[serde(skip_serializing_if = "Option::is_none")]
    taker_coin_swap_contract_address: Option<BytesJson>,
}

pub struct TakerSwapMut {
    data: TakerSwapData,
    other_persistent_pub: H264,
    taker_fee: Option<TransactionIdentifier>,
    maker_payment: Option<TransactionIdentifier>,
    taker_payment: Option<TransactionIdentifier>,
    maker_payment_spend: Option<TransactionIdentifier>,
    taker_payment_spend: Option<TransactionIdentifier>,
    taker_payment_refund: Option<TransactionIdentifier>,
    secret_hash: H160Json,
    secret: H256Json,
}

pub struct TakerSwap {
    ctx: MmArc,
    maker_coin: MmCoinEnum,
    taker_coin: MmCoinEnum,
    maker_amount: MmNumber,
    taker_amount: MmNumber,
    my_persistent_pub: H264,
    maker: bits256,
    uuid: Uuid,
    my_order_uuid: Option<Uuid>,
    maker_payment_lock: Atomic<u64>,
    maker_payment_confirmed: Atomic<bool>,
    errors: PaMutex<Vec<SwapError>>,
    finished_at: Atomic<u64>,
    mutable: RwLock<TakerSwapMut>,
    conf_settings: SwapConfirmationsSettings,
    payment_locktime: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct TakerPaymentSpentData {
    transaction: TransactionIdentifier,
    secret: H256Json,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct MakerNegotiationData {
    maker_payment_locktime: u64,
    maker_pubkey: H264Json,
    secret_hash: H160Json,
    maker_coin_swap_contract_addr: Option<BytesJson>,
    taker_coin_swap_contract_addr: Option<BytesJson>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", content = "data")]
#[allow(clippy::large_enum_variant)]
pub enum TakerSwapEvent {
    Started(TakerSwapData),
    StartFailed(SwapError),
    Negotiated(MakerNegotiationData),
    NegotiateFailed(SwapError),
    TakerFeeSent(TransactionIdentifier),
    TakerFeeSendFailed(SwapError),
    MakerPaymentReceived(TransactionIdentifier),
    MakerPaymentWaitConfirmStarted,
    MakerPaymentValidatedAndConfirmed,
    MakerPaymentValidateFailed(SwapError),
    MakerPaymentWaitConfirmFailed(SwapError),
    TakerPaymentSent(TransactionIdentifier),
    TakerPaymentTransactionFailed(SwapError),
    TakerPaymentDataSendFailed(SwapError),
    TakerPaymentWaitConfirmFailed(SwapError),
    TakerPaymentSpent(TakerPaymentSpentData),
    TakerPaymentWaitForSpendFailed(SwapError),
    MakerPaymentSpent(TransactionIdentifier),
    MakerPaymentSpendFailed(SwapError),
    TakerPaymentWaitRefundStarted { wait_until: u64 },
    TakerPaymentRefunded(TransactionIdentifier),
    TakerPaymentRefundFailed(SwapError),
    Finished,
}

impl TakerSwapEvent {
    fn status_str(&self) -> String {
        match self {
            TakerSwapEvent::Started(_) => "Started...".to_owned(),
            TakerSwapEvent::StartFailed(_) => "Start failed...".to_owned(),
            TakerSwapEvent::Negotiated(_) => "Negotiated...".to_owned(),
            TakerSwapEvent::NegotiateFailed(_) => "Negotiate failed...".to_owned(),
            TakerSwapEvent::TakerFeeSent(_) => "Taker fee sent...".to_owned(),
            TakerSwapEvent::TakerFeeSendFailed(_) => "Taker fee send failed...".to_owned(),
            TakerSwapEvent::MakerPaymentReceived(_) => "Maker payment received...".to_owned(),
            TakerSwapEvent::MakerPaymentWaitConfirmStarted => "Maker payment wait confirm started...".to_owned(),
            TakerSwapEvent::MakerPaymentValidatedAndConfirmed => "Maker payment validated and confirmed...".to_owned(),
            TakerSwapEvent::MakerPaymentValidateFailed(_) => "Maker payment validate failed...".to_owned(),
            TakerSwapEvent::MakerPaymentWaitConfirmFailed(_) => {
                "Maker payment wait for confirmation failed...".to_owned()
            },
            TakerSwapEvent::TakerPaymentSent(_) => "Taker payment sent...".to_owned(),
            TakerSwapEvent::TakerPaymentTransactionFailed(_) => "Taker payment transaction failed...".to_owned(),
            TakerSwapEvent::TakerPaymentDataSendFailed(_) => "Taker payment data send failed...".to_owned(),
            TakerSwapEvent::TakerPaymentWaitConfirmFailed(_) => {
                "Taker payment wait for confirmation failed...".to_owned()
            },
            TakerSwapEvent::TakerPaymentSpent(_) => "Taker payment spent...".to_owned(),
            TakerSwapEvent::TakerPaymentWaitForSpendFailed(_) => "Taker payment wait for spend failed...".to_owned(),
            TakerSwapEvent::MakerPaymentSpent(_) => "Maker payment spent...".to_owned(),
            TakerSwapEvent::MakerPaymentSpendFailed(_) => "Maker payment spend failed...".to_owned(),
            TakerSwapEvent::TakerPaymentWaitRefundStarted { wait_until } => {
                format!("Taker payment wait refund till {} started...", wait_until)
            },
            TakerSwapEvent::TakerPaymentRefunded(_) => "Taker payment refunded...".to_owned(),
            TakerSwapEvent::TakerPaymentRefundFailed(_) => "Taker payment refund failed...".to_owned(),
            TakerSwapEvent::Finished => "Finished".to_owned(),
        }
    }

    fn should_ban_maker(&self) -> bool {
        matches!(
            self,
            TakerSwapEvent::MakerPaymentValidateFailed(_) | TakerSwapEvent::TakerPaymentWaitForSpendFailed(_)
        )
    }

    fn is_success(&self) -> bool {
        matches!(
            self,
            TakerSwapEvent::Started(_)
                | TakerSwapEvent::Negotiated(_)
                | TakerSwapEvent::TakerFeeSent(_)
                | TakerSwapEvent::MakerPaymentReceived(_)
                | TakerSwapEvent::MakerPaymentWaitConfirmStarted
                | TakerSwapEvent::MakerPaymentValidatedAndConfirmed
                | TakerSwapEvent::TakerPaymentSent(_)
                | TakerSwapEvent::TakerPaymentSpent(_)
                | TakerSwapEvent::MakerPaymentSpent(_)
                | TakerSwapEvent::Finished
        )
    }

    fn is_error(&self) -> bool { !self.is_success() }
}

#[derive(Debug)]
pub enum TakerSwapCommand {
    Start,
    Negotiate,
    SendTakerFee,
    WaitForMakerPayment,
    ValidateMakerPayment,
    SendTakerPayment,
    WaitForTakerPaymentSpend,
    SpendMakerPayment,
    RefundTakerPayment,
    Finish,
}

impl TakerSwap {
    fn w(&self) -> RwLockWriteGuard<TakerSwapMut> { self.mutable.write().unwrap() }
    fn r(&self) -> RwLockReadGuard<TakerSwapMut> { self.mutable.read().unwrap() }

    fn wait_refund_until(&self) -> u64 { self.r().data.taker_payment_lock + 3700 }

    fn apply_event(&self, event: TakerSwapEvent) {
        match event {
            TakerSwapEvent::Started(data) => self.w().data = data,
            TakerSwapEvent::StartFailed(err) => self.errors.lock().push(err),
            TakerSwapEvent::Negotiated(data) => {
                self.maker_payment_lock
                    .store(data.maker_payment_locktime, Ordering::Relaxed);
                self.w().other_persistent_pub = data.maker_pubkey.into();
                self.w().secret_hash = data.secret_hash;

                if data.maker_coin_swap_contract_addr.is_some() {
                    self.w().data.maker_coin_swap_contract_address = data.maker_coin_swap_contract_addr;
                }

                if data.taker_coin_swap_contract_addr.is_some() {
                    self.w().data.taker_coin_swap_contract_address = data.taker_coin_swap_contract_addr;
                }
            },
            TakerSwapEvent::NegotiateFailed(err) => self.errors.lock().push(err),
            TakerSwapEvent::TakerFeeSent(tx) => self.w().taker_fee = Some(tx),
            TakerSwapEvent::TakerFeeSendFailed(err) => self.errors.lock().push(err),
            TakerSwapEvent::MakerPaymentReceived(tx) => self.w().maker_payment = Some(tx),
            TakerSwapEvent::MakerPaymentWaitConfirmStarted => (),
            TakerSwapEvent::MakerPaymentValidatedAndConfirmed => {
                self.maker_payment_confirmed.store(true, Ordering::Relaxed)
            },
            TakerSwapEvent::MakerPaymentValidateFailed(err) => self.errors.lock().push(err),
            TakerSwapEvent::MakerPaymentWaitConfirmFailed(err) => self.errors.lock().push(err),
            TakerSwapEvent::TakerPaymentSent(tx) => self.w().taker_payment = Some(tx),
            TakerSwapEvent::TakerPaymentTransactionFailed(err) => self.errors.lock().push(err),
            TakerSwapEvent::TakerPaymentDataSendFailed(err) => self.errors.lock().push(err),
            TakerSwapEvent::TakerPaymentWaitConfirmFailed(err) => self.errors.lock().push(err),
            TakerSwapEvent::TakerPaymentSpent(data) => {
                self.w().taker_payment_spend = Some(data.transaction);
                self.w().secret = data.secret;
            },
            TakerSwapEvent::TakerPaymentWaitForSpendFailed(err) => self.errors.lock().push(err),
            TakerSwapEvent::MakerPaymentSpent(tx) => self.w().maker_payment_spend = Some(tx),
            TakerSwapEvent::MakerPaymentSpendFailed(err) => self.errors.lock().push(err),
            TakerSwapEvent::TakerPaymentWaitRefundStarted { .. } => (),
            TakerSwapEvent::TakerPaymentRefunded(tx) => self.w().taker_payment_refund = Some(tx),
            TakerSwapEvent::TakerPaymentRefundFailed(err) => self.errors.lock().push(err),
            TakerSwapEvent::Finished => self.finished_at.store(now_ms() / 1000, Ordering::Relaxed),
        }
    }

    async fn handle_command(
        &self,
        command: TakerSwapCommand,
    ) -> Result<(Option<TakerSwapCommand>, Vec<TakerSwapEvent>), String> {
        match command {
            TakerSwapCommand::Start => self.start().await,
            TakerSwapCommand::Negotiate => self.negotiate().await,
            TakerSwapCommand::SendTakerFee => self.send_taker_fee().await,
            TakerSwapCommand::WaitForMakerPayment => self.wait_for_maker_payment().await,
            TakerSwapCommand::ValidateMakerPayment => self.validate_maker_payment().await,
            TakerSwapCommand::SendTakerPayment => self.send_taker_payment().await,
            TakerSwapCommand::WaitForTakerPaymentSpend => self.wait_for_taker_payment_spend().await,
            TakerSwapCommand::SpendMakerPayment => self.spend_maker_payment().await,
            TakerSwapCommand::RefundTakerPayment => self.refund_taker_payment().await,
            TakerSwapCommand::Finish => Ok((None, vec![TakerSwapEvent::Finished])),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ctx: MmArc,
        maker: bits256,
        maker_amount: MmNumber,
        taker_amount: MmNumber,
        my_persistent_pub: H264,
        uuid: Uuid,
        my_order_uuid: Option<Uuid>,
        conf_settings: SwapConfirmationsSettings,
        maker_coin: MmCoinEnum,
        taker_coin: MmCoinEnum,
        payment_locktime: u64,
    ) -> Self {
        TakerSwap {
            ctx,
            maker_coin,
            taker_coin,
            maker_amount,
            taker_amount,
            my_persistent_pub,
            maker,
            uuid,
            my_order_uuid,
            maker_payment_confirmed: Atomic::new(false),
            finished_at: Atomic::new(0),
            maker_payment_lock: Atomic::new(0),
            errors: PaMutex::new(Vec::new()),
            conf_settings,
            payment_locktime,
            mutable: RwLock::new(TakerSwapMut {
                data: TakerSwapData::default(),
                other_persistent_pub: H264::default(),
                taker_fee: None,
                maker_payment: None,
                taker_payment: None,
                taker_payment_spend: None,
                maker_payment_spend: None,
                taker_payment_refund: None,
                secret_hash: H160Json::default(),
                secret: H256Json::default(),
            }),
        }
    }

    async fn start(&self) -> Result<(Option<TakerSwapCommand>, Vec<TakerSwapEvent>), String> {
        // do not use self.r().data here as it is not initialized at this step yet
        let stage = FeeApproxStage::StartSwap;
        let dex_fee = dex_fee_amount_from_taker_coin(&self.taker_coin, self.maker_coin.ticker(), &self.taker_amount);
        let preimage_value = TradePreimageValue::Exact(self.taker_amount.to_decimal());

        let fee_to_send_dex_fee_fut = self
            .taker_coin
            .get_fee_to_send_taker_fee(dex_fee.to_decimal(), stage.clone());
        let fee_to_send_dex_fee = match fee_to_send_dex_fee_fut.compat().await {
            Ok(fee) => fee,
            Err(e) => {
                return Ok((Some(TakerSwapCommand::Finish), vec![TakerSwapEvent::StartFailed(
                    ERRL!("!taker_coin.get_fee_to_send_taker_fee {}", e).into(),
                )]))
            },
        };
        let get_sender_trade_fee_fut = self.taker_coin.get_sender_trade_fee(preimage_value, stage.clone());
        let taker_payment_trade_fee = match get_sender_trade_fee_fut.compat().await {
            Ok(fee) => fee,
            Err(e) => {
                return Ok((Some(TakerSwapCommand::Finish), vec![TakerSwapEvent::StartFailed(
                    ERRL!("!taker_coin.get_sender_trade_fee {}", e).into(),
                )]))
            },
        };
        let maker_payment_spend_trade_fee_fut = self.maker_coin.get_receiver_trade_fee(stage.clone());
        let maker_payment_spend_trade_fee = match maker_payment_spend_trade_fee_fut.compat().await {
            Ok(fee) => fee,
            Err(e) => {
                return Ok((Some(TakerSwapCommand::Finish), vec![TakerSwapEvent::StartFailed(
                    ERRL!("!maker_coin.get_receiver_trade_fee {}", e).into(),
                )]))
            },
        };

        let params = TakerSwapPreparedParams {
            dex_fee: dex_fee.clone(),
            fee_to_send_dex_fee: fee_to_send_dex_fee.clone(),
            taker_payment_trade_fee: taker_payment_trade_fee.clone(),
            maker_payment_spend_trade_fee: maker_payment_spend_trade_fee.clone(),
        };
        let check_balance_f = check_balance_for_taker_swap(
            &self.ctx,
            &self.taker_coin,
            &self.maker_coin,
            self.taker_amount.clone(),
            Some(&self.uuid),
            Some(params),
            stage,
        );
        if let Err(e) = check_balance_f.await {
            return Ok((Some(TakerSwapCommand::Finish), vec![TakerSwapEvent::StartFailed(
                ERRL!("!check_balance_for_taker_swap {}", e).into(),
            )]));
        }

        let started_at = now_ms() / 1000;

        let maker_coin_start_block = match self.maker_coin.current_block().compat().await {
            Ok(b) => b,
            Err(e) => {
                return Ok((Some(TakerSwapCommand::Finish), vec![TakerSwapEvent::StartFailed(
                    ERRL!("!maker_coin.current_block {}", e).into(),
                )]))
            },
        };

        let taker_coin_start_block = match self.taker_coin.current_block().compat().await {
            Ok(b) => b,
            Err(e) => {
                return Ok((Some(TakerSwapCommand::Finish), vec![TakerSwapEvent::StartFailed(
                    ERRL!("!taker_coin.current_block {}", e).into(),
                )]))
            },
        };

        let maker_coin_swap_contract_address = self.maker_coin.swap_contract_address();
        let taker_coin_swap_contract_address = self.taker_coin.swap_contract_address();

        let data = TakerSwapData {
            taker_coin: self.taker_coin.ticker().to_owned(),
            maker_coin: self.maker_coin.ticker().to_owned(),
            maker: self.maker.bytes.into(),
            started_at,
            lock_duration: self.payment_locktime,
            maker_amount: self.maker_amount.to_decimal(),
            taker_amount: self.taker_amount.to_decimal(),
            maker_payment_confirmations: self.conf_settings.maker_coin_confs,
            maker_payment_requires_nota: Some(self.conf_settings.maker_coin_nota),
            taker_payment_confirmations: self.conf_settings.taker_coin_confs,
            taker_payment_requires_nota: Some(self.conf_settings.taker_coin_nota),
            taker_payment_lock: started_at + self.payment_locktime,
            my_persistent_pub: self.my_persistent_pub.clone().into(),
            uuid: self.uuid,
            maker_payment_wait: started_at + (self.payment_locktime * 2) / 5,
            maker_coin_start_block,
            taker_coin_start_block,
            fee_to_send_taker_fee: Some(SavedTradeFee::from(fee_to_send_dex_fee)),
            taker_payment_trade_fee: Some(SavedTradeFee::from(taker_payment_trade_fee)),
            maker_payment_spend_trade_fee: Some(SavedTradeFee::from(maker_payment_spend_trade_fee)),
            maker_coin_swap_contract_address,
            taker_coin_swap_contract_address,
        };

        Ok((Some(TakerSwapCommand::Negotiate), vec![TakerSwapEvent::Started(data)]))
    }

    async fn negotiate(&self) -> Result<(Option<TakerSwapCommand>, Vec<TakerSwapEvent>), String> {
        const NEGOTIATE_TIMEOUT: u64 = 90;

        let recv_fut = recv_swap_msg(
            self.ctx.clone(),
            |store| store.negotiation.take(),
            &self.uuid,
            NEGOTIATE_TIMEOUT,
        );
        let maker_data = match recv_fut.await {
            Ok(d) => d,
            Err(e) => {
                return Ok((Some(TakerSwapCommand::Finish), vec![TakerSwapEvent::NegotiateFailed(
                    ERRL!("{:?}", e).into(),
                )]))
            },
        };

        let time_dif = (self.r().data.started_at as i64 - maker_data.started_at() as i64).abs();
        if time_dif > 60 {
            return Ok((Some(TakerSwapCommand::Finish), vec![TakerSwapEvent::NegotiateFailed(
                ERRL!("Started_at time_dif over 60 {}", time_dif).into(),
            )]));
        }

        let expected_lock_time = maker_data.started_at() + self.r().data.lock_duration * 2;
        if maker_data.payment_locktime() != expected_lock_time {
            return Ok((Some(TakerSwapCommand::Finish), vec![TakerSwapEvent::NegotiateFailed(
                ERRL!(
                    "maker_data.payment_locktime {} not equal to expected {}",
                    maker_data.payment_locktime(),
                    expected_lock_time
                )
                .into(),
            )]));
        }

        let maker_coin_swap_contract_addr = match self
            .maker_coin
            .negotiate_swap_contract_addr(maker_data.maker_coin_swap_contract())
        {
            Ok(addr) => addr,
            Err(e) => {
                return Ok((Some(TakerSwapCommand::Finish), vec![TakerSwapEvent::NegotiateFailed(
                    ERRL!("!maker_coin.negotiate_swap_contract_addr {}", e).into(),
                )]))
            },
        };

        let taker_coin_swap_contract_addr = match self
            .taker_coin
            .negotiate_swap_contract_addr(maker_data.taker_coin_swap_contract())
        {
            Ok(addr) => addr,
            Err(e) => {
                return Ok((Some(TakerSwapCommand::Finish), vec![TakerSwapEvent::NegotiateFailed(
                    ERRL!("!taker_coin.negotiate_swap_contract_addr {}", e).into(),
                )]))
            },
        };

        let taker_data = SwapMsg::NegotiationReply(NegotiationDataMsg::V2(NegotiationDataV2 {
            started_at: self.r().data.started_at,
            secret_hash: maker_data.secret_hash().to_vec(),
            payment_locktime: self.r().data.taker_payment_lock,
            persistent_pubkey: self.my_persistent_pub.to_vec(),
            maker_coin_swap_contract: maker_coin_swap_contract_addr.clone().map_or(vec![], |bytes| bytes.0),
            taker_coin_swap_contract: taker_coin_swap_contract_addr.clone().map_or(vec![], |bytes| bytes.0),
        }));
        let send_abort_handle = broadcast_swap_message_every(
            self.ctx.clone(),
            swap_topic(&self.uuid),
            taker_data,
            NEGOTIATE_TIMEOUT as f64 / 6.,
        );
        let recv_fut = recv_swap_msg(
            self.ctx.clone(),
            |store| store.negotiated.take(),
            &self.uuid,
            NEGOTIATE_TIMEOUT,
        );
        let negotiated = match recv_fut.await {
            Ok(d) => d,
            Err(e) => {
                return Ok((Some(TakerSwapCommand::Finish), vec![TakerSwapEvent::NegotiateFailed(
                    ERRL!("{:?}", e).into(),
                )]))
            },
        };
        drop(send_abort_handle);

        if !negotiated {
            return Ok((Some(TakerSwapCommand::Finish), vec![TakerSwapEvent::NegotiateFailed(
                ERRL!("Maker sent negotiated = false").into(),
            )]));
        }

        Ok((Some(TakerSwapCommand::SendTakerFee), vec![TakerSwapEvent::Negotiated(
            MakerNegotiationData {
                maker_payment_locktime: maker_data.payment_locktime(),
                maker_pubkey: maker_data.persistent_pubkey().into(),
                secret_hash: maker_data.secret_hash().into(),
                maker_coin_swap_contract_addr,
                taker_coin_swap_contract_addr,
            },
        )]))
    }

    async fn send_taker_fee(&self) -> Result<(Option<TakerSwapCommand>, Vec<TakerSwapEvent>), String> {
        let timeout = self.r().data.started_at + self.r().data.lock_duration / 3;
        let now = now_ms() / 1000;
        if now > timeout {
            return Ok((Some(TakerSwapCommand::Finish), vec![
                TakerSwapEvent::TakerFeeSendFailed(ERRL!("Timeout {} > {}", now, timeout).into()),
            ]));
        }

        let fee_amount =
            dex_fee_amount_from_taker_coin(&self.taker_coin, &self.r().data.maker_coin, &self.taker_amount);
        let fee_tx = self
            .taker_coin
            .send_taker_fee(&DEX_FEE_ADDR_RAW_PUBKEY, fee_amount.into())
            .compat()
            .await;
        let transaction = match fee_tx {
            Ok(t) => t,
            Err(err) => {
                return Ok((Some(TakerSwapCommand::Finish), vec![
                    TakerSwapEvent::TakerFeeSendFailed(ERRL!("{}", err).into()),
                ]))
            },
        };

        let tx_hash = transaction.tx_hash();
        log!({"Taker fee tx hash {:02x}", tx_hash});
        let tx_ident = TransactionIdentifier {
            tx_hex: transaction.tx_hex().into(),
            tx_hash,
        };

        Ok((Some(TakerSwapCommand::WaitForMakerPayment), vec![
            TakerSwapEvent::TakerFeeSent(tx_ident),
        ]))
    }

    async fn wait_for_maker_payment(&self) -> Result<(Option<TakerSwapCommand>, Vec<TakerSwapEvent>), String> {
        const MAKER_PAYMENT_WAIT_TIMEOUT: u64 = 180;
        let tx_hex = self.r().taker_fee.as_ref().unwrap().tx_hex.0.clone();
        let msg = SwapMsg::TakerFee(tx_hex);
        let abort_send_handle = broadcast_swap_message_every(
            self.ctx.clone(),
            swap_topic(&self.uuid),
            msg,
            MAKER_PAYMENT_WAIT_TIMEOUT as f64 / 6.,
        );

        let recv_fut = recv_swap_msg(
            self.ctx.clone(),
            |store| store.maker_payment.take(),
            &self.uuid,
            MAKER_PAYMENT_WAIT_TIMEOUT,
        );
        let payload = match recv_fut.await {
            Ok(p) => p,
            Err(e) => {
                return Ok((Some(TakerSwapCommand::Finish), vec![
                    TakerSwapEvent::MakerPaymentValidateFailed(
                        ERRL!("Error waiting for 'maker-payment' data: {}", e).into(),
                    ),
                ]))
            },
        };
        drop(abort_send_handle);
        let maker_payment = match self.maker_coin.tx_enum_from_bytes(&payload) {
            Ok(p) => p,
            Err(e) => {
                return Ok((Some(TakerSwapCommand::Finish), vec![
                    TakerSwapEvent::MakerPaymentValidateFailed(
                        ERRL!("Error parsing the 'maker-payment': {}", e).into(),
                    ),
                ]))
            },
        };

        let tx_hash = maker_payment.tx_hash();
        log!({"Got maker payment {:02x}", tx_hash});
        let tx_ident = TransactionIdentifier {
            tx_hex: maker_payment.tx_hex().into(),
            tx_hash,
        };

        Ok((Some(TakerSwapCommand::ValidateMakerPayment), vec![
            TakerSwapEvent::MakerPaymentReceived(tx_ident),
            TakerSwapEvent::MakerPaymentWaitConfirmStarted,
        ]))
    }

    async fn validate_maker_payment(&self) -> Result<(Option<TakerSwapCommand>, Vec<TakerSwapEvent>), String> {
        log!({ "Before wait confirm" });
        let f = self.maker_coin.wait_for_confirmations(
            &self.r().maker_payment.clone().unwrap().tx_hex,
            self.r().data.maker_payment_confirmations,
            self.r().data.maker_payment_requires_nota.unwrap_or(false),
            self.r().data.maker_payment_wait,
            WAIT_CONFIRM_INTERVAL,
        );
        if let Err(err) = f.compat().await {
            return Ok((Some(TakerSwapCommand::Finish), vec![
                TakerSwapEvent::MakerPaymentWaitConfirmFailed(
                    ERRL!("!wait for maker payment confirmations: {}", err).into(),
                ),
            ]));
        }
        log!({ "After wait confirm" });

        let validated_f = self.maker_coin.validate_maker_payment(
            &self.r().maker_payment.clone().unwrap().tx_hex,
            self.maker_payment_lock.load(Ordering::Relaxed) as u32,
            &*self.r().other_persistent_pub,
            &self.r().secret_hash.0,
            self.maker_amount.to_decimal(),
            &self.r().data.maker_coin_swap_contract_address,
        );
        let validated = validated_f.compat().await;

        if let Err(e) = validated {
            return Ok((Some(TakerSwapCommand::Finish), vec![
                TakerSwapEvent::MakerPaymentValidateFailed(ERRL!("!validate maker payment: {}", e).into()),
            ]));
        }

        Ok((Some(TakerSwapCommand::SendTakerPayment), vec![
            TakerSwapEvent::MakerPaymentValidatedAndConfirmed,
        ]))
    }

    async fn send_taker_payment(&self) -> Result<(Option<TakerSwapCommand>, Vec<TakerSwapEvent>), String> {
        let timeout = self.r().data.started_at + self.r().data.lock_duration / 3;
        let now = now_ms() / 1000;
        if now > timeout {
            return Ok((Some(TakerSwapCommand::Finish), vec![
                TakerSwapEvent::TakerPaymentTransactionFailed(ERRL!("Timeout {} > {}", now, timeout).into()),
            ]));
        }

        let f = self.taker_coin.check_if_my_payment_sent(
            self.r().data.taker_payment_lock as u32,
            &*self.r().other_persistent_pub,
            &self.r().secret_hash.0,
            self.r().data.taker_coin_start_block,
            &self.r().data.taker_coin_swap_contract_address,
        );
        let transaction = match f.compat().await {
            Ok(res) => match res {
                Some(tx) => tx,
                None => {
                    let payment_fut = self.taker_coin.send_taker_payment(
                        self.r().data.taker_payment_lock as u32,
                        &*self.r().other_persistent_pub,
                        &self.r().secret_hash.0,
                        self.taker_amount.to_decimal(),
                        &self.r().data.taker_coin_swap_contract_address,
                    );

                    match payment_fut.compat().await {
                        Ok(t) => t,
                        Err(e) => {
                            return Ok((Some(TakerSwapCommand::Finish), vec![
                                TakerSwapEvent::TakerPaymentTransactionFailed(ERRL!("{}", e).into()),
                            ]))
                        },
                    }
                },
            },
            Err(e) => {
                return Ok((Some(TakerSwapCommand::Finish), vec![
                    TakerSwapEvent::TakerPaymentTransactionFailed(ERRL!("{}", e).into()),
                ]))
            },
        };

        let tx_hash = transaction.tx_hash();
        log!({"Taker payment tx hash {:02x}", tx_hash });
        let tx_ident = TransactionIdentifier {
            tx_hex: transaction.tx_hex().into(),
            tx_hash,
        };

        Ok((Some(TakerSwapCommand::WaitForTakerPaymentSpend), vec![
            TakerSwapEvent::TakerPaymentSent(tx_ident),
        ]))
    }

    async fn wait_for_taker_payment_spend(&self) -> Result<(Option<TakerSwapCommand>, Vec<TakerSwapEvent>), String> {
        let tx_hex = self.r().taker_payment.as_ref().unwrap().tx_hex.0.clone();
        let msg = SwapMsg::TakerPayment(tx_hex);
        let send_abort_handle = broadcast_swap_message_every(self.ctx.clone(), swap_topic(&self.uuid), msg, 600.);

        let wait_duration = (self.r().data.lock_duration * 4) / 5;
        let wait_taker_payment = self.r().data.started_at + wait_duration;
        let wait_f = self
            .taker_coin
            .wait_for_confirmations(
                &self.r().taker_payment.clone().unwrap().tx_hex,
                self.r().data.taker_payment_confirmations,
                self.r().data.taker_payment_requires_nota.unwrap_or(false),
                wait_taker_payment,
                WAIT_CONFIRM_INTERVAL,
            )
            .compat();
        if let Err(err) = wait_f.await {
            return Ok((Some(TakerSwapCommand::RefundTakerPayment), vec![
                TakerSwapEvent::TakerPaymentWaitConfirmFailed(
                    ERRL!("!taker_coin.wait_for_confirmations: {}", err).into(),
                ),
                TakerSwapEvent::TakerPaymentWaitRefundStarted {
                    wait_until: self.wait_refund_until(),
                },
            ]));
        }

        let f = self.taker_coin.wait_for_tx_spend(
            &self.r().taker_payment.clone().unwrap().tx_hex,
            self.r().data.taker_payment_lock,
            self.r().data.taker_coin_start_block,
            &self.r().data.taker_coin_swap_contract_address,
        );
        let tx = match f.compat().await {
            Ok(t) => t,
            Err(e) => {
                return Ok((Some(TakerSwapCommand::RefundTakerPayment), vec![
                    TakerSwapEvent::TakerPaymentWaitForSpendFailed(e.into()),
                    TakerSwapEvent::TakerPaymentWaitRefundStarted {
                        wait_until: self.wait_refund_until(),
                    },
                ]))
            },
        };
        drop(send_abort_handle);
        let tx_hash = tx.tx_hash();
        log!({"Taker payment spend tx {:02x}", tx_hash });
        let tx_ident = TransactionIdentifier {
            tx_hex: tx.tx_hex().into(),
            tx_hash,
        };
        let secret = match self
            .taker_coin
            .extract_secret(&self.r().secret_hash.0, &tx_ident.tx_hex.0)
        {
            Ok(bytes) => H256Json::from(bytes.as_slice()),
            Err(e) => {
                return Ok((Some(TakerSwapCommand::Finish), vec![
                    TakerSwapEvent::TakerPaymentWaitForSpendFailed(ERRL!("{}", e).into()),
                ]))
            },
        };

        Ok((Some(TakerSwapCommand::SpendMakerPayment), vec![
            TakerSwapEvent::TakerPaymentSpent(TakerPaymentSpentData {
                transaction: tx_ident,
                secret,
            }),
        ]))
    }

    async fn spend_maker_payment(&self) -> Result<(Option<TakerSwapCommand>, Vec<TakerSwapEvent>), String> {
        let spend_fut = self.maker_coin.send_taker_spends_maker_payment(
            &self.r().maker_payment.clone().unwrap().tx_hex,
            self.maker_payment_lock.load(Ordering::Relaxed) as u32,
            &*self.r().other_persistent_pub,
            &self.r().secret.0,
            &self.r().data.maker_coin_swap_contract_address,
        );
        let transaction = match spend_fut.compat().await {
            Ok(t) => t,
            Err(err) => {
                return Ok((Some(TakerSwapCommand::Finish), vec![
                    TakerSwapEvent::MakerPaymentSpendFailed(ERRL!("{}", err).into()),
                ]))
            },
        };

        let tx_hash = transaction.tx_hash();
        log!({"Maker payment spend tx {:02x}", tx_hash });
        let tx_ident = TransactionIdentifier {
            tx_hex: transaction.tx_hex().into(),
            tx_hash,
        };

        Ok((Some(TakerSwapCommand::Finish), vec![TakerSwapEvent::MakerPaymentSpent(
            tx_ident,
        )]))
    }

    async fn refund_taker_payment(&self) -> Result<(Option<TakerSwapCommand>, Vec<TakerSwapEvent>), String> {
        let locktime = self.r().data.taker_payment_lock;
        loop {
            match self.taker_coin.can_refund_htlc(locktime).compat().await {
                Ok(CanRefundHtlc::CanRefundNow) => break,
                Ok(CanRefundHtlc::HaveToWait(to_sleep)) => Timer::sleep(to_sleep as f64).await,
                Err(e) => {
                    error!("Error {} on can_refund_htlc, retrying in 30 seconds", e);
                    Timer::sleep(30.).await;
                },
            }
        }

        let refund_fut = self.taker_coin.send_taker_refunds_payment(
            &self.r().taker_payment.clone().unwrap().tx_hex.0,
            self.r().data.taker_payment_lock as u32,
            &*self.r().other_persistent_pub,
            &self.r().secret_hash.0,
            &self.r().data.taker_coin_swap_contract_address,
        );

        let transaction = match refund_fut.compat().await {
            Ok(t) => t,
            Err(err) => {
                return Ok((Some(TakerSwapCommand::Finish), vec![
                    TakerSwapEvent::TakerPaymentRefundFailed(ERRL!("{}", err).into()),
                ]))
            },
        };

        let tx_hash = transaction.tx_hash();
        log!({"Taker refund tx hash {:02x}", tx_hash });
        let tx_ident = TransactionIdentifier {
            tx_hex: transaction.tx_hex().into(),
            tx_hash,
        };

        Ok((Some(TakerSwapCommand::Finish), vec![
            TakerSwapEvent::TakerPaymentRefunded(tx_ident),
        ]))
    }

    pub fn load_from_db_by_uuid(
        ctx: MmArc,
        maker_coin: MmCoinEnum,
        taker_coin: MmCoinEnum,
        swap_uuid: &Uuid,
    ) -> Result<(Self, Option<TakerSwapCommand>), String> {
        let path = my_swap_file_path(&ctx, swap_uuid);
        let saved: SavedSwap = try_s!(json::from_slice(&try_s!(slurp(&path))));
        let saved = match saved {
            SavedSwap::Taker(swap) => swap,
            SavedSwap::Maker(_) => return ERR!("Can not load TakerSwap from SavedSwap::Maker uuid: {}", swap_uuid),
        };
        Self::load_from_saved(ctx, maker_coin, taker_coin, saved)
    }

    pub fn load_from_saved(
        ctx: MmArc,
        maker_coin: MmCoinEnum,
        taker_coin: MmCoinEnum,
        mut saved: TakerSavedSwap,
    ) -> Result<(Self, Option<TakerSwapCommand>), String> {
        if saved.events.is_empty() {
            return ERR!("Can't restore swap from empty events set");
        };

        let data = match saved.events[0].event {
            TakerSwapEvent::Started(ref mut data) => data,
            _ => return ERR!("First swap event must be Started"),
        };

        // refresh swap contract addresses if the swap file is out-dated (doesn't contain the fields yet)
        if data.maker_coin_swap_contract_address.is_none() {
            data.maker_coin_swap_contract_address = maker_coin.swap_contract_address();
        }
        if data.taker_coin_swap_contract_address.is_none() {
            data.taker_coin_swap_contract_address = taker_coin.swap_contract_address();
        }

        let mut maker = bits256::from([0; 32]);
        maker.bytes = data.maker.0;
        let my_persistent_pub = H264::from(&**ctx.secp256k1_key_pair().public());
        let conf_settings = SwapConfirmationsSettings {
            maker_coin_confs: data.maker_payment_confirmations,
            maker_coin_nota: data
                .maker_payment_requires_nota
                .unwrap_or_else(|| maker_coin.requires_notarization()),
            taker_coin_confs: data.taker_payment_confirmations,
            taker_coin_nota: data
                .taker_payment_requires_nota
                .unwrap_or_else(|| taker_coin.requires_notarization()),
        };

        let swap = TakerSwap::new(
            ctx,
            maker,
            data.maker_amount.clone().into(),
            data.taker_amount.clone().into(),
            my_persistent_pub,
            saved.uuid,
            Some(saved.uuid),
            conf_settings,
            maker_coin,
            taker_coin,
            data.lock_duration,
        );
        let command = saved.events.last().unwrap().get_command();
        for saved_event in saved.events {
            swap.apply_event(saved_event.event);
        }
        Ok((swap, command))
    }

    pub fn recover_funds(&self) -> Result<RecoveredSwap, String> {
        if self.finished_at.load(Ordering::Relaxed) == 0 {
            return ERR!("Swap must be finished before recover funds attempt");
        }

        if self.r().taker_payment_refund.is_some() {
            return ERR!("Taker payment is refunded, swap is not recoverable");
        }

        if self.r().maker_payment_spend.is_some() {
            return ERR!("Maker payment is spent, swap is not recoverable");
        }

        let maker_payment = match &self.r().maker_payment {
            Some(tx) => tx.tx_hex.0.clone(),
            None => return ERR!("No info about maker payment, swap is not recoverable"),
        };

        macro_rules! check_maker_payment_is_not_spent {
            // validate that maker payment is not spent
            () => {
                match self.maker_coin.search_for_swap_tx_spend_other(
                    self.maker_payment_lock.load(Ordering::Relaxed) as u32,
                    &*self.r().other_persistent_pub,
                    &self.r().secret_hash.0,
                    &maker_payment,
                    self.r().data.maker_coin_start_block,
                    &self.r().data.maker_coin_swap_contract_address,
                ) {
                    Ok(Some(FoundSwapTxSpend::Spent(tx))) => {
                        return ERR!(
                            "Maker payment was already spent by {} tx {:02x}",
                            self.maker_coin.ticker(),
                            tx.tx_hash()
                        )
                    },
                    Ok(Some(FoundSwapTxSpend::Refunded(tx))) => {
                        return ERR!(
                            "Maker payment was already refunded by {} tx {:02x}",
                            self.maker_coin.ticker(),
                            tx.tx_hash()
                        )
                    },
                    Err(e) => return ERR!("Error {} when trying to find maker payment spend", e),
                    Ok(None) => (), // payment is not spent, continue
                }
            };
        }

        let taker_payment = match &self.r().taker_payment {
            Some(tx) => tx.tx_hex.0.clone(),
            None => {
                let maybe_sent = try_s!(self
                    .taker_coin
                    .check_if_my_payment_sent(
                        self.r().data.taker_payment_lock as u32,
                        &*self.r().other_persistent_pub,
                        &self.r().secret_hash.0,
                        self.r().data.taker_coin_start_block,
                        &self.r().data.taker_coin_swap_contract_address,
                    )
                    .wait());
                match maybe_sent {
                    Some(tx) => tx.tx_hex(),
                    None => return ERR!("Taker payment is not found, swap is not recoverable"),
                }
            },
        };

        if self.r().taker_payment_spend.is_some() {
            check_maker_payment_is_not_spent!();
            let transaction = try_s!(self
                .maker_coin
                .send_taker_spends_maker_payment(
                    &maker_payment,
                    self.maker_payment_lock.load(Ordering::Relaxed) as u32,
                    &*self.r().other_persistent_pub,
                    &self.r().secret.0,
                    &self.r().data.maker_coin_swap_contract_address,
                )
                .wait());

            return Ok(RecoveredSwap {
                action: RecoveredSwapAction::SpentOtherPayment,
                coin: self.maker_coin.ticker().to_string(),
                transaction,
            });
        }

        let taker_payment_spend = try_s!(self.taker_coin.search_for_swap_tx_spend_my(
            self.r().data.taker_payment_lock as u32,
            &*self.r().other_persistent_pub,
            &self.r().secret_hash.0,
            &taker_payment,
            self.r().data.taker_coin_start_block,
            &self.r().data.taker_coin_swap_contract_address,
        ));

        match taker_payment_spend {
            Some(spend) => match spend {
                FoundSwapTxSpend::Spent(tx) => {
                    check_maker_payment_is_not_spent!();
                    let secret = try_s!(self.taker_coin.extract_secret(&self.r().secret_hash.0, &tx.tx_hex()));
                    let transaction = try_s!(self
                        .maker_coin
                        .send_taker_spends_maker_payment(
                            &maker_payment,
                            self.maker_payment_lock.load(Ordering::Relaxed) as u32,
                            &*self.r().other_persistent_pub,
                            &secret,
                            &self.r().data.maker_coin_swap_contract_address,
                        )
                        .wait());

                    Ok(RecoveredSwap {
                        action: RecoveredSwapAction::SpentOtherPayment,
                        coin: self.maker_coin.ticker().to_string(),
                        transaction,
                    })
                },
                FoundSwapTxSpend::Refunded(tx) => ERR!(
                    "Taker payment has been refunded already by transaction {:02x}",
                    tx.tx_hash()
                ),
            },
            None => {
                if now_ms() / 1000 < self.r().data.taker_payment_lock + 3700 {
                    return ERR!(
                        "Too early to refund, wait until {}",
                        self.r().data.taker_payment_lock + 3700
                    );
                }

                let transaction = try_s!(self
                    .taker_coin
                    .send_taker_refunds_payment(
                        &taker_payment,
                        self.r().data.taker_payment_lock as u32,
                        &*self.r().other_persistent_pub,
                        &self.r().secret_hash.0,
                        &self.r().data.taker_coin_swap_contract_address,
                    )
                    .wait());

                Ok(RecoveredSwap {
                    action: RecoveredSwapAction::RefundedMyPayment,
                    coin: self.maker_coin.ticker().to_string(),
                    transaction,
                })
            },
        }
    }
}

impl AtomicSwap for TakerSwap {
    fn locked_amount(&self) -> Vec<LockedAmount> {
        let mut result = Vec::new();

        // if taker fee is not sent yet it must be virtually locked
        let taker_fee_amount =
            dex_fee_amount_from_taker_coin(&self.taker_coin, &self.r().data.maker_coin, &self.taker_amount);
        let trade_fee = self.r().data.fee_to_send_taker_fee.clone().map(TradeFee::from);
        if self.r().taker_fee.is_none() {
            result.push(LockedAmount {
                coin: self.taker_coin.ticker().to_owned(),
                amount: taker_fee_amount,
                trade_fee,
            });
        }

        // if taker payment is not sent yet it must be virtually locked
        if self.r().taker_payment.is_none() {
            let trade_fee = self.r().data.taker_payment_trade_fee.clone().map(TradeFee::from);
            result.push(LockedAmount {
                coin: self.taker_coin.ticker().to_owned(),
                amount: self.taker_amount.clone(),
                trade_fee,
            });
        }

        // if maker payment is not spent yet the `MakerPaymentSpend` tx fee must be virtually locked
        if self.r().maker_payment_spend.is_none() {
            let trade_fee = self.r().data.maker_payment_spend_trade_fee.clone().map(TradeFee::from);
            result.push(LockedAmount {
                coin: self.maker_coin.ticker().to_owned(),
                amount: 0.into(),
                trade_fee,
            });
        }

        result
    }

    fn uuid(&self) -> &Uuid { &self.uuid }

    fn maker_coin(&self) -> &str { self.maker_coin.ticker() }

    fn taker_coin(&self) -> &str { self.taker_coin.ticker() }
}

pub struct TakerSwapPreparedParams {
    dex_fee: MmNumber,
    fee_to_send_dex_fee: TradeFee,
    taker_payment_trade_fee: TradeFee,
    maker_payment_spend_trade_fee: TradeFee,
}

pub async fn check_balance_for_taker_swap(
    ctx: &MmArc,
    my_coin: &MmCoinEnum,
    other_coin: &MmCoinEnum,
    volume: MmNumber,
    swap_uuid: Option<&Uuid>,
    prepared_params: Option<TakerSwapPreparedParams>,
    stage: FeeApproxStage,
) -> CheckBalanceResult<()> {
    let params = match prepared_params {
        Some(params) => params,
        None => {
            let dex_fee = dex_fee_amount_from_taker_coin(my_coin, other_coin.ticker(), &volume);
            let fee_to_send_dex_fee = my_coin
                .get_fee_to_send_taker_fee(dex_fee.to_decimal(), stage.clone())
                .compat()
                .await
                .mm_err(|e| CheckBalanceError::from_trade_preimage_error(e, my_coin.ticker()))?;
            let preimage_value = TradePreimageValue::Exact(volume.to_decimal());
            let taker_payment_trade_fee = my_coin
                .get_sender_trade_fee(preimage_value, stage.clone())
                .compat()
                .await
                .mm_err(|e| CheckBalanceError::from_trade_preimage_error(e, my_coin.ticker()))?;
            let maker_payment_spend_trade_fee = other_coin
                .get_receiver_trade_fee(stage)
                .compat()
                .await
                .mm_err(|e| CheckBalanceError::from_trade_preimage_error(e, other_coin.ticker()))?;
            TakerSwapPreparedParams {
                dex_fee,
                fee_to_send_dex_fee,
                taker_payment_trade_fee,
                maker_payment_spend_trade_fee,
            }
        },
    };

    let taker_fee = TakerFeeAdditionalInfo {
        dex_fee: params.dex_fee,
        fee_to_send_dex_fee: params.fee_to_send_dex_fee,
    };

    check_my_coin_balance_for_swap(
        ctx,
        my_coin,
        swap_uuid,
        volume,
        params.taker_payment_trade_fee,
        Some(taker_fee),
    )
    .await?;
    if !params.maker_payment_spend_trade_fee.paid_from_trading_vol {
        check_other_coin_balance_for_swap(ctx, other_coin, swap_uuid, params.maker_payment_spend_trade_fee).await?;
    }
    Ok(())
}

pub struct TakerTradePreimage {
    /// The fee is paid per swap concerning the `base` coin.
    pub base_coin_fee: TradeFee,
    /// The fee is paid per swap concerning the `rel` coin.
    pub rel_coin_fee: TradeFee,
    /// The dex fee to be paid by taker coin.
    pub taker_fee: TradeFee,
    /// The miner fee is paid to send the dex fee.
    pub fee_to_send_taker_fee: TradeFee,
}

pub async fn taker_swap_trade_preimage(
    ctx: &MmArc,
    req: TradePreimageRequest,
    base_coin: MmCoinEnum,
    rel_coin: MmCoinEnum,
) -> TradePreimageRpcResult<TakerTradePreimage> {
    let action = req
        .swap_method
        .to_taker_action()
        .map_to_mm(TradePreimageRpcError::InternalError)?;
    let (my_coin, other_coin) = match action {
        TakerAction::Sell => (base_coin.clone(), rel_coin.clone()),
        TakerAction::Buy => (rel_coin.clone(), base_coin.clone()),
    };
    let my_coin_ticker = my_coin.ticker();
    let other_coin_ticker = other_coin.ticker();

    if req.max {
        return MmError::err(TradePreimageRpcError::InvalidParam {
            param: "max".to_owned(),
            reason: "'max' cannot be used with 'sell' or 'buy' method".to_owned(),
        });
    }

    let base_amount = req.volume.clone();
    let rel_amount = &req.price * &req.volume;

    let stage = FeeApproxStage::TradePreimage;
    let my_coin_volume = match action {
        TakerAction::Sell => base_amount.clone(),
        TakerAction::Buy => rel_amount.clone(),
    };

    let dex_amount = dex_fee_amount_from_taker_coin(&my_coin, other_coin_ticker, &my_coin_volume);
    let taker_fee = TradeFee {
        coin: my_coin_ticker.to_owned(),
        amount: dex_amount.clone(),
        paid_from_trading_vol: false,
    };

    let fee_to_send_taker_fee = my_coin
        .get_fee_to_send_taker_fee(dex_amount.to_decimal(), stage.clone())
        .compat()
        .await
        .mm_err(|e| TradePreimageRpcError::from_trade_preimage_error(e, my_coin_ticker))?;

    let preimage_value = TradePreimageValue::Exact(my_coin_volume.to_decimal());
    let my_coin_trade_fee = my_coin
        .get_sender_trade_fee(preimage_value, stage.clone())
        .compat()
        .await
        .mm_err(|e| TradePreimageRpcError::from_trade_preimage_error(e, my_coin_ticker))?;
    let other_coin_trade_fee = other_coin
        .get_receiver_trade_fee(stage.clone())
        .compat()
        .await
        .mm_err(|e| TradePreimageRpcError::from_trade_preimage_error(e, other_coin_ticker))?;

    let prepared_params = TakerSwapPreparedParams {
        dex_fee: dex_amount,
        fee_to_send_dex_fee: fee_to_send_taker_fee.clone(),
        taker_payment_trade_fee: my_coin_trade_fee.clone(),
        maker_payment_spend_trade_fee: other_coin_trade_fee.clone(),
    };
    check_balance_for_taker_swap(
        ctx,
        &my_coin,
        &other_coin,
        my_coin_volume.clone(),
        None,
        Some(prepared_params),
        stage,
    )
    .await?;

    let conf_settings = OrderConfirmationsSettings {
        base_confs: base_coin.required_confirmations(),
        base_nota: base_coin.requires_notarization(),
        rel_confs: rel_coin.required_confirmations(),
        rel_nota: rel_coin.requires_notarization(),
    };
    let our_public_id = ctx.public_id().expect("!ctx.public_id()");
    let order_builder = TakerOrderBuilder::new(&base_coin, &rel_coin)
        .with_base_amount(base_amount)
        .with_rel_amount(rel_amount)
        .with_action(action.clone())
        .with_match_by(MatchBy::Any)
        .with_conf_settings(conf_settings)
        .with_sender_pubkey(H256Json::from(our_public_id.bytes));
    let _ = order_builder
        .build()
        .map_to_mm(|e| TradePreimageRpcError::from_taker_order_build_error(e, &req.base, &req.rel))?;

    let (base_coin_fee, rel_coin_fee) = match action {
        TakerAction::Sell => (my_coin_trade_fee, other_coin_trade_fee),
        TakerAction::Buy => (other_coin_trade_fee, my_coin_trade_fee),
    };
    Ok(TakerTradePreimage {
        base_coin_fee,
        rel_coin_fee,
        taker_fee,
        fee_to_send_taker_fee,
    })
}

#[derive(Deserialize)]
struct MaxTakerVolRequest {
    coin: String,
    trade_with: Option<String>,
}

pub async fn max_taker_vol(ctx: MmArc, req: Json) -> Result<Response<Vec<u8>>, String> {
    let req: MaxTakerVolRequest = try_s!(json::from_value(req));
    let coin = match lp_coinfind(&ctx, &req.coin).await {
        Ok(Some(t)) => t,
        Ok(None) => return ERR!("No such coin: {}", req.coin),
        Err(err) => return ERR!("!lp_coinfind({}): {}", req.coin, err),
    };
    let other_coin = req.trade_with.as_ref().unwrap_or(&req.coin);
    let fut = calc_max_taker_vol(&ctx, &coin, other_coin, FeeApproxStage::TradePreimage);
    let max_vol = match fut.await {
        Ok(max_vol) => max_vol,
        Err(e) if e.get_inner().not_sufficient_balance() => {
            warn!("{}", e);
            MmNumber::from(0)
        },
        Err(err) => {
            return ERR!("{}", err);
        },
    };

    let res = try_s!(json::to_vec(&json!({
        "result": max_vol.to_fraction(),
        "coin": coin.ticker(),
    })));
    Ok(try_s!(Response::builder().body(res)))
}

/// If we want to calculate the maximum taker volume, we should solve the following equation:
/// `max_vol = balance - locked_amount - trade_fee(max_vol) - fee_to_send_taker_fee(dex_fee(max_vol)) - dex_fee(max_vol)`
///
/// 1) If the `trade_fee` and `fee_to_send_taker_fee` should be paid in base coin, the equation can be simplified:
/// `max_vol = balance - locked_amount - dex_fee(max_vol)`,
/// where we can calculate the exact `max_vol` since the function inverse to `dex_fee(x)` can be obtained.
///
/// 2) Otherwise we cannot express the `max_vol` from the equation above, but we can find smallest of the largest `max_vol`.
/// It means if we find the largest `trade_fee` and `fee_to_send_taker_fee` values and pass them into the equation, we will get:
/// `min_max_vol = balance - locked_amount - max_trade_fee - max_fee_to_send_taker_fee - dex_fee(max_vol)`
/// and then `min_max_vol` can be calculated as in the first case.
///
/// Please note the following condition is satisfied for any `x` and `y`:
/// `if x < y then trade_fee(x) <= trade_fee(y) and fee_to_send_taker_fee(x) <= fee_to_send_taker_fee(y) and dex_fee(x) <= dex_fee(y)`
/// Let `real_max_vol` is a real desired volume.
/// Performing the following steps one by one, we will get an approximate maximum volume:
/// - `max_possible = balance - locked_amount` is a largest possible max volume. Hint, we've replaced unknown subtracted `trade_fee`, `fee_to_send_taker_fee`, `dex_fee` variables with zeros.
/// - `max_trade_fee = trade_fee(max_possible)` is a largest possible `trade_fee` value.
/// - `max_possible_2 = balance - locked_amount - max_trade_fee` is more accurate max volume than `max_possible`. Please note `real_max_vol <= max_possible_2 <= max_possible`.
/// - `max_dex_fee = dex_fee(max_possible_2)` is an intermediate value that will be passed into the `fee_to_send_taker_fee`.
/// - `max_fee_to_send_taker_fee = fee_to_send_taker_fee(max_dex_fee)`
/// After that `min_max_vol = balance - locked_amount - max_trade_fee - max_fee_to_send_taker_fee - dex_fee(max_vol)` can be solved as in the first case.
pub async fn calc_max_taker_vol(
    ctx: &MmArc,
    coin: &MmCoinEnum,
    other_coin: &str,
    stage: FeeApproxStage,
) -> CheckBalanceResult<MmNumber> {
    let my_coin = coin.ticker();
    let balance: MmNumber = coin.my_spendable_balance().compat().await?.into();
    let locked = get_locked_amount(ctx, my_coin);
    let min_tx_amount = MmNumber::from(coin.min_tx_amount());

    let max_possible = &balance - &locked;
    let preimage_value = TradePreimageValue::UpperBound(max_possible.to_decimal());
    let max_trade_fee = coin
        .get_sender_trade_fee(preimage_value, stage.clone())
        .compat()
        .await
        .mm_err(|e| CheckBalanceError::from_trade_preimage_error(e, &my_coin))?;

    let max_vol = if my_coin == max_trade_fee.coin {
        // second case
        let max_possible_2 = &max_possible - &max_trade_fee.amount;
        let max_dex_fee = dex_fee_amount_from_taker_coin(coin, other_coin, &max_possible_2);
        let max_fee_to_send_taker_fee = coin
            .get_fee_to_send_taker_fee(max_dex_fee.to_decimal(), stage)
            .compat()
            .await
            .mm_err(|e| CheckBalanceError::from_trade_preimage_error(e, &my_coin))?;
        let min_max_possible = &max_possible_2 - &max_fee_to_send_taker_fee.amount;

        debug!(
            "max_taker_vol case 2: min_max_possible {:?}, balance {:?}, locked {:?}, max_trade_fee {:?}, max_dex_fee {:?}, max_fee_to_send_taker_fee {:?}",
            min_max_possible.to_fraction(),
            balance.to_fraction(),
            locked.to_fraction(),
            max_trade_fee.amount.to_fraction(),
            max_dex_fee.to_fraction(),
            max_fee_to_send_taker_fee.amount.to_fraction()
        );
        max_taker_vol_from_available(min_max_possible, my_coin, other_coin, &min_tx_amount)
            .mm_err(|e| CheckBalanceError::from_max_taker_vol_error(e, my_coin.to_owned(), locked.to_decimal()))?
    } else {
        // first case
        debug!(
            "max_taker_vol case 1: balance {:?}, locked {:?}",
            balance.to_fraction(),
            locked.to_fraction()
        );
        max_taker_vol_from_available(max_possible, my_coin, other_coin, &min_tx_amount)
            .mm_err(|e| CheckBalanceError::from_max_taker_vol_error(e, my_coin.to_owned(), locked.to_decimal()))?
    };
    // do not check if `max_vol < min_tx_amount`, because it is checked within `max_taker_vol_from_available` already
    Ok(max_vol)
}

#[derive(Debug)]
pub struct MaxTakerVolumeLessThanDust {
    pub max_vol: MmNumber,
    pub min_tx_amount: MmNumber,
}

pub fn max_taker_vol_from_available(
    available: MmNumber,
    base: &str,
    rel: &str,
    min_tx_amount: &MmNumber,
) -> Result<MmNumber, MmError<MaxTakerVolumeLessThanDust>> {
    let fee_threshold = dex_fee_threshold(min_tx_amount.clone());
    let dex_fee_rate = dex_fee_rate(base, rel);
    let threshold_coef = &(&MmNumber::from(1) + &dex_fee_rate) / &dex_fee_rate;
    let max_vol = if available > &fee_threshold * &threshold_coef {
        available / (MmNumber::from(1) + dex_fee_rate)
    } else {
        available - fee_threshold
    };

    if &max_vol <= min_tx_amount {
        return MmError::err(MaxTakerVolumeLessThanDust {
            max_vol,
            min_tx_amount: min_tx_amount.clone(),
        });
    }
    Ok(max_vol)
}

#[cfg(test)]
mod taker_swap_tests {
    use super::*;
    use crate::mm2::lp_swap::{dex_fee_amount, get_locked_amount_by_other_swaps};
    use coins::eth::{addr_from_str, signed_eth_tx_from_bytes, SignedEthTx};
    use coins::utxo::UtxoTx;
    use coins::{FoundSwapTxSpend, MarketCoinOps, MmCoin, SwapOps, TestCoin};
    use common::mm_ctx::MmCtxBuilder;
    use common::new_uuid;
    use common::privkey::key_pair_from_seed;
    use mocktopus::mocking::*;

    fn eth_tx_for_test() -> SignedEthTx {
        // raw transaction bytes of https://etherscan.io/tx/0x0869be3e5d4456a29d488a533ad6c118620fef450f36778aecf31d356ff8b41f
        let tx_bytes = [
            248, 240, 3, 133, 1, 42, 5, 242, 0, 131, 2, 73, 240, 148, 133, 0, 175, 192, 188, 82, 20, 114, 128, 130, 22,
            51, 38, 194, 255, 12, 115, 244, 168, 113, 135, 110, 205, 245, 24, 127, 34, 254, 184, 132, 21, 44, 243, 175,
            73, 33, 143, 82, 117, 16, 110, 27, 133, 82, 200, 114, 233, 42, 140, 198, 35, 21, 201, 249, 187, 180, 20,
            46, 148, 40, 9, 228, 193, 130, 71, 199, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 152, 41, 132, 9, 201, 73, 19,
            94, 237, 137, 35, 61, 4, 194, 207, 239, 152, 75, 175, 245, 157, 174, 10, 214, 161, 207, 67, 70, 87, 246,
            231, 212, 47, 216, 119, 68, 237, 197, 125, 141, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 93, 72, 125, 102, 28, 159, 180, 237, 198, 97,
            87, 80, 82, 200, 104, 40, 245, 221, 7, 28, 122, 104, 91, 99, 1, 159, 140, 25, 131, 101, 74, 87, 50, 168,
            146, 187, 90, 160, 51, 1, 123, 247, 6, 108, 165, 181, 188, 40, 56, 47, 211, 229, 221, 73, 5, 15, 89, 81,
            117, 225, 216, 108, 98, 226, 119, 232, 94, 184, 42, 106,
        ];
        signed_eth_tx_from_bytes(&tx_bytes).unwrap()
    }

    #[test]
    fn test_recover_funds_taker_swap_maker_payment_spend_errored() {
        let taker_saved_json = r#"{"error_events":["StartFailed","NegotiateFailed","TakerFeeSendFailed","MakerPaymentValidateFailed","TakerPaymentTransactionFailed","TakerPaymentDataSendFailed","TakerPaymentWaitForSpendFailed","MakerPaymentSpendFailed","TakerPaymentRefunded","TakerPaymentRefundFailed"],"events":[{"event":{"data":{"lock_duration":7800,"maker":"1bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8","maker_amount":"0.12596566232185483","maker_coin":"KMD","maker_coin_start_block":1458035,"maker_payment_confirmations":1,"maker_payment_wait":1564053079,"my_persistent_pub":"0326846707a52a233cfc49a61ef51b1698bbe6aa78fa8b8d411c02743c09688f0a","started_at":1564050479,"taker_amount":"50.000000000000001504212457800000","taker_coin":"DOGE","taker_coin_start_block":2823448,"taker_payment_confirmations":1,"taker_payment_lock":1564058279,"uuid":"41383f43-46a5-478c-9386-3b2cce0aca20"},"type":"Started"},"timestamp":1564050480269},{"event":{"data":{"maker_payment_locktime":1564066080,"maker_pubkey":"031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8","secret_hash":"3669eb83a007a3c507448d79f45a9f06ec2f36a8"},"type":"Negotiated"},"timestamp":1564050540991},{"event":{"data":{"tx_hash":"bdde828b492d6d1cc25cd2322fd592dafd722fcc7d8b0fedce4d3bb4a1a8c8ff","tx_hex":"0100000002c7efa995c8b7be0a8b6c2d526c6c444c1634d65584e9ee89904e9d8675eac88c010000006a473044022051f34d5e3b7d0b9098d5e35333f3550f9cb9e57df83d5e4635b7a8d2986d6d5602200288c98da05de6950e01229a637110a1800ba643e75cfec59d4eb1021ad9b40801210326846707a52a233cfc49a61ef51b1698bbe6aa78fa8b8d411c02743c09688f0affffffffae6c233989efa7c7d2aa6534adc96078917ff395b7f09f734a147b2f44ade164000000006a4730440220393a784c2da74d0e2a28ec4f7df6c8f9d8b2af6ae6957f1e68346d744223a8fd02201b7a96954ac06815a43a6c7668d829ae9cbb5de76fa77189ddfd9e3038df662c01210326846707a52a233cfc49a61ef51b1698bbe6aa78fa8b8d411c02743c09688f0affffffff02115f5800000000001976a914ca1e04745e8ca0c60d8c5881531d51bec470743f88ac41a84641020000001976a914444f0e1099709ba4d742454a7d98a5c9c162ceab88ac6d84395d"},"type":"TakerFeeSent"},"timestamp":1564050545296},{"event":{"data":{"tx_hash":"0a0f11fa82802c2c30862c50ab2162185dae8de7f7235f32c506f814c142b382","tx_hex":"0400008085202f8902ace337db2dd4c56b0697f58fb8cfb6bd1cd6f469d925fc0376d1dcfb7581bf82000000006b483045022100d1f95be235c5c8880f5d703ace287e2768548792c58c5dbd27f5578881b30ea70220030596106e21c7e0057ee0dab283f9a1fe273f15208cba80870c447bd559ef0d0121031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8ffffffff9f339752567c404427fd77f2b35cecdb4c21489edc64e25e729fdb281785e423000000006a47304402203179e95877dbc107123a417f1e648e3ff13d384890f1e4a67b6dd5087235152e0220102a8ab799fadb26b5d89ceb9c7bc721a7e0c2a0d0d7e46bbe0cf3d130010d430121031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8ffffffff025635c0000000000017a91480a95d366d65e34a465ab17b0c9eb1d5a33bae08876cbfce05000000001976a914c3f710deb7320b0efa6edb14e3ebeeb9155fa90d88ac8d7c395d000000000000000000000000000000"},"type":"MakerPaymentReceived"},"timestamp":1564050588176},{"event":{"type":"MakerPaymentWaitConfirmStarted"},"timestamp":1564050588178},{"event":{"type":"MakerPaymentValidatedAndConfirmed"},"timestamp":1564050693585},{"event":{"data":{"tx_hash":"539cb6dbdc25465bbccc575554f05d1bb04c70efce4316e41194e747375c3659","tx_hex":"0100000001ffc8a8a1b43b4dceed0f8b7dcc2f72fdda92d52f32d25cc21c6d2d498b82debd010000006a47304402203967b7f9f5532fa47116585c7d1bcba51861ea2059cca00409f34660db18e33a0220640991911852533a12fdfeb039fb9c8ca2c45482c6993bd84636af3670d49c1501210326846707a52a233cfc49a61ef51b1698bbe6aa78fa8b8d411c02743c09688f0affffffff0200f2052a0100000017a914f2fa08ae416b576779ae5da975e5442663215fce87415173f9000000001976a914444f0e1099709ba4d742454a7d98a5c9c162ceab88ac0585395d"},"type":"TakerPaymentSent"},"timestamp":1564050695611},{"event":{"data":{"secret":"1b8886b8a2cdb62505699400b694ac20f04d7bd4abd80e1ab154aa8d861fc093","transaction":{"tx_hash":"cc5af1cf68d246419fee49c3d74c0cd173599d115b86efe274368a614951bc47","tx_hex":"010000000159365c3747e79411e41643ceef704cb01b5df0545557ccbc5b4625dcdbb69c5300000000d747304402200e78e27d2f1c18676f98ca3dfa4e4a9eeaa8209b55f57b4dd5d9e1abdf034cfa0220623b5c22b62234cec230342aa306c497e43494b44ec2425b84e236b1bf01257001201b8886b8a2cdb62505699400b694ac20f04d7bd4abd80e1ab154aa8d861fc093004c6b6304a7a2395db175210326846707a52a233cfc49a61ef51b1698bbe6aa78fa8b8d411c02743c09688f0aac6782012088a9143669eb83a007a3c507448d79f45a9f06ec2f36a88821031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8ac68ffffffff01008d380c010000001976a914c3f710deb7320b0efa6edb14e3ebeeb9155fa90d88ac8c77395d"}},"type":"TakerPaymentSpent"},"timestamp":1564051092890},{"event":{"data":{"error":"lp_swap:1981] utxo:891] rpc_clients:738] JsonRpcError { request: JsonRpcRequest { jsonrpc: \"2.0\", id: \"67\", method: \"blockchain.transaction.broadcast\", params: [String(\"0400008085202f890182b342c114f806c5325f23f7e78dae5d186221ab502c86302c2c8082fa110f0a00000000d7473044022035791ea5548f87484065c9e1f0bdca9ebc699f2c7f51182c84f360102e32dc3d02200612ed53bca52d9c2568437f087598531534badf26229fe0f652ea72ddf03ca501201b8886b8a2cdb62505699400b694ac20f04d7bd4abd80e1ab154aa8d861fc093004c6b630420c1395db17521031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8ac6782012088a9143669eb83a007a3c507448d79f45a9f06ec2f36a888210326846707a52a233cfc49a61ef51b1698bbe6aa78fa8b8d411c02743c09688f0aac68ffffffff01460ec000000000001976a914444f0e1099709ba4d742454a7d98a5c9c162ceab88ac967e395d000000000000000000000000000000\")] }, error: Transport(\"rpc_clients:668] All electrums are currently disconnected\") }"},"type":"MakerPaymentSpendFailed"},"timestamp":1564051092897},{"event":{"type":"Finished"},"timestamp":1564051092900}],"success_events":["Started","Negotiated","TakerFeeSent","MakerPaymentReceived","MakerPaymentWaitConfirmStarted","MakerPaymentValidatedAndConfirmed","TakerPaymentSent","TakerPaymentSpent","MakerPaymentSpent","Finished"],"uuid":"41383f43-46a5-478c-9386-3b2cce0aca20"}"#;
        let taker_saved_swap: TakerSavedSwap = json::from_str(taker_saved_json).unwrap();
        let key_pair =
            key_pair_from_seed("spice describe gravity federal blast come thank unfair canal monkey style afraid")
                .unwrap();
        let ctx = MmCtxBuilder::default().with_secp256k1_key_pair(key_pair).into_mm_arc();

        TestCoin::ticker.mock_safe(|_| MockResult::Return("ticker"));
        TestCoin::swap_contract_address.mock_safe(|_| MockResult::Return(None));

        static mut MAKER_PAYMENT_SPEND_CALLED: bool = false;
        TestCoin::send_taker_spends_maker_payment.mock_safe(|_, _, _, _, _, _| {
            unsafe { MAKER_PAYMENT_SPEND_CALLED = true };
            MockResult::Return(Box::new(futures01::future::ok(eth_tx_for_test().into())))
        });
        TestCoin::search_for_swap_tx_spend_other.mock_safe(|_, _, _, _, _, _, _| MockResult::Return(Ok(None)));
        let maker_coin = MmCoinEnum::Test(TestCoin::default());
        let taker_coin = MmCoinEnum::Test(TestCoin::default());
        let (taker_swap, _) = TakerSwap::load_from_saved(ctx, maker_coin, taker_coin, taker_saved_swap).unwrap();
        let actual = taker_swap.recover_funds().unwrap();
        let expected = RecoveredSwap {
            action: RecoveredSwapAction::SpentOtherPayment,
            coin: "ticker".to_string(),
            transaction: eth_tx_for_test().into(),
        };
        assert_eq!(expected, actual);
        assert!(unsafe { MAKER_PAYMENT_SPEND_CALLED });
    }

    #[test]
    fn test_recover_funds_taker_swap_taker_payment_errored_but_sent_not_spent() {
        let taker_saved_json = r#"{"error_events":["StartFailed","NegotiateFailed","TakerFeeSendFailed","MakerPaymentValidateFailed","TakerPaymentTransactionFailed","TakerPaymentDataSendFailed","TakerPaymentWaitForSpendFailed","MakerPaymentSpendFailed","TakerPaymentRefunded","TakerPaymentRefundFailed"],"events":[{"event":{"data":{"lock_duration":7800,"maker":"1bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8","maker_amount":"3.54932734","maker_coin":"KMD","maker_coin_start_block":1452970,"maker_payment_confirmations":1,"maker_payment_wait":1563746537,"my_persistent_pub":"03101ace6b08605b9424b0582b5cce044b70a3c8d8d10cb2965e039b0967ae92b9","started_at":1563743937,"taker_amount":"0.02004833998671660000000000","taker_coin":"ETH","taker_coin_start_block":8196380,"taker_payment_confirmations":1,"taker_payment_lock":1563751737,"uuid":"3447b727-fe93-4357-8e5a-8cf2699b7e86"},"type":"Started"},"timestamp":1563743937741},{"event":{"data":{"maker_payment_locktime":1563759539,"maker_pubkey":"031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8","secret_hash":"432c8272ac59b47dea2d299b5cf1ee64ea1917b9"},"type":"Negotiated"},"timestamp":1563744003530},{"event":{"data":{"tx_hash":"a59203eb2328827de00bed699a29389792906e4f39fdea145eb40dc6b3821bd6","tx_hex":"f8690284ee6b280082520894d8997941dd1346e9231118d5685d866294f59e5b865af3107a4000801ca0743d2b7c9fad65805d882179062012261be328d7628ae12ee08eff8d7657d993a07eecbd051f49d35279416778faa4664962726d516ce65e18755c9b9406a9c2fd"},"type":"TakerFeeSent"},"timestamp":1563744020598},{"event":{"data":{"tx_hash":"0cf4acbcefde53645851c5c6053ea61fe0cbb5f828a906d69eb809e0b071a03b","tx_hex":"0400008085202f89025d5ae3e8c87418c9b735f8f2f7d29e26820c33c9f30d53f2d31f8b99ea9b1490010000006a47304402201185c06ca575261c539b287175751b7de642eb7466c59128639a19b4c2dd2f9b02201c8c4167d581864bedd4d1deb5596472e6e3ce29fe9e7996907a7b59c905d5490121031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8ffffffff06dbf9971c8dfd4a0c8c49f4f15c51de59ba13b2efa702682e26869843af9a87000000006a473044022012b47c12c7f6ad7d8b778fc4b5dcfd56a39325daf302f56e7b84753ba5216cfa022076bf571cf9e20facf70d2f134e8ed2de67aa08581a27ff3128bf93a9b594ac770121031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8ffffffff02fed727150000000017a914d5268b31131a652f9b6ddf57db62f02285cdfad1874e1d7835000000001976a914c3f710deb7320b0efa6edb14e3ebeeb9155fa90d88ac37cf345d000000000000000000000000000000"},"type":"MakerPaymentReceived"},"timestamp":1563744071778},{"event":{"type":"MakerPaymentWaitConfirmStarted"},"timestamp":1563744071781},{"event":{"type":"MakerPaymentValidatedAndConfirmed"},"timestamp":1563744118073},{"event":{"data":{"error":"lp_swap:1888] eth:654] RPC error: Error { code: ServerError(-32010), message: \"Transaction with the same hash was already imported.\", data: None }"},"type":"TakerPaymentTransactionFailed"},"timestamp":1563744118577},{"event":{"type":"Finished"},"timestamp":1563744118580}],"success_events":["Started","Negotiated","TakerFeeSent","MakerPaymentReceived","MakerPaymentWaitConfirmStarted","MakerPaymentValidatedAndConfirmed","TakerPaymentSent","TakerPaymentSpent","MakerPaymentSpent","Finished"],"uuid":"3447b727-fe93-4357-8e5a-8cf2699b7e86"}"#;
        let taker_saved_swap: TakerSavedSwap = json::from_str(taker_saved_json).unwrap();
        let key_pair =
            key_pair_from_seed("spice describe gravity federal blast come thank unfair canal monkey style afraid")
                .unwrap();
        let ctx = MmCtxBuilder::default().with_secp256k1_key_pair(key_pair).into_mm_arc();

        TestCoin::ticker.mock_safe(|_| MockResult::Return("ticker"));
        TestCoin::swap_contract_address.mock_safe(|_| MockResult::Return(None));

        static mut MY_PAYMENT_SENT_CALLED: bool = false;
        TestCoin::check_if_my_payment_sent.mock_safe(|_, _, _, _, _, _| {
            unsafe { MY_PAYMENT_SENT_CALLED = true };
            MockResult::Return(Box::new(futures01::future::ok(Some(eth_tx_for_test().into()))))
        });

        static mut TX_SPEND_CALLED: bool = false;
        TestCoin::search_for_swap_tx_spend_my.mock_safe(|_, _, _, _, _, _, _| {
            unsafe { TX_SPEND_CALLED = true };
            MockResult::Return(Ok(None))
        });

        static mut TAKER_PAYMENT_REFUND_CALLED: bool = false;
        TestCoin::send_taker_refunds_payment.mock_safe(|_, _, _, _, _, _| {
            unsafe { TAKER_PAYMENT_REFUND_CALLED = true };
            MockResult::Return(Box::new(futures01::future::ok(eth_tx_for_test().into())))
        });
        let maker_coin = MmCoinEnum::Test(TestCoin::default());
        let taker_coin = MmCoinEnum::Test(TestCoin::default());
        let (taker_swap, _) = TakerSwap::load_from_saved(ctx, maker_coin, taker_coin, taker_saved_swap).unwrap();
        let actual = taker_swap.recover_funds().unwrap();
        let expected = RecoveredSwap {
            action: RecoveredSwapAction::RefundedMyPayment,
            coin: "ticker".to_string(),
            transaction: eth_tx_for_test().into(),
        };
        assert_eq!(expected, actual);
        assert!(unsafe { MY_PAYMENT_SENT_CALLED });
        assert!(unsafe { TX_SPEND_CALLED });
        assert!(unsafe { TAKER_PAYMENT_REFUND_CALLED });
    }

    #[test]
    fn test_recover_funds_taker_swap_taker_payment_errored_but_sent_and_spent_by_maker() {
        let taker_saved_json = r#"{"error_events":["StartFailed","NegotiateFailed","TakerFeeSendFailed","MakerPaymentValidateFailed","TakerPaymentTransactionFailed","TakerPaymentDataSendFailed","TakerPaymentWaitForSpendFailed","MakerPaymentSpendFailed","TakerPaymentRefunded","TakerPaymentRefundFailed"],"events":[{"event":{"data":{"lock_duration":7800,"maker":"1bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8","maker_amount":"3.54932734","maker_coin":"KMD","maker_coin_start_block":1452970,"maker_payment_confirmations":1,"maker_payment_wait":1563746537,"my_persistent_pub":"03101ace6b08605b9424b0582b5cce044b70a3c8d8d10cb2965e039b0967ae92b9","started_at":1563743937,"taker_amount":"0.02004833998671660000000000","taker_coin":"ETH","taker_coin_start_block":8196380,"taker_payment_confirmations":1,"taker_payment_lock":1563751737,"uuid":"3447b727-fe93-4357-8e5a-8cf2699b7e86"},"type":"Started"},"timestamp":1563743937741},{"event":{"data":{"maker_payment_locktime":1563759539,"maker_pubkey":"031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8","secret_hash":"432c8272ac59b47dea2d299b5cf1ee64ea1917b9"},"type":"Negotiated"},"timestamp":1563744003530},{"event":{"data":{"tx_hash":"a59203eb2328827de00bed699a29389792906e4f39fdea145eb40dc6b3821bd6","tx_hex":"f8690284ee6b280082520894d8997941dd1346e9231118d5685d866294f59e5b865af3107a4000801ca0743d2b7c9fad65805d882179062012261be328d7628ae12ee08eff8d7657d993a07eecbd051f49d35279416778faa4664962726d516ce65e18755c9b9406a9c2fd"},"type":"TakerFeeSent"},"timestamp":1563744020598},{"event":{"data":{"tx_hash":"0cf4acbcefde53645851c5c6053ea61fe0cbb5f828a906d69eb809e0b071a03b","tx_hex":"0400008085202f89025d5ae3e8c87418c9b735f8f2f7d29e26820c33c9f30d53f2d31f8b99ea9b1490010000006a47304402201185c06ca575261c539b287175751b7de642eb7466c59128639a19b4c2dd2f9b02201c8c4167d581864bedd4d1deb5596472e6e3ce29fe9e7996907a7b59c905d5490121031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8ffffffff06dbf9971c8dfd4a0c8c49f4f15c51de59ba13b2efa702682e26869843af9a87000000006a473044022012b47c12c7f6ad7d8b778fc4b5dcfd56a39325daf302f56e7b84753ba5216cfa022076bf571cf9e20facf70d2f134e8ed2de67aa08581a27ff3128bf93a9b594ac770121031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8ffffffff02fed727150000000017a914d5268b31131a652f9b6ddf57db62f02285cdfad1874e1d7835000000001976a914c3f710deb7320b0efa6edb14e3ebeeb9155fa90d88ac37cf345d000000000000000000000000000000"},"type":"MakerPaymentReceived"},"timestamp":1563744071778},{"event":{"type":"MakerPaymentWaitConfirmStarted"},"timestamp":1563744071781},{"event":{"type":"MakerPaymentValidatedAndConfirmed"},"timestamp":1563744118073},{"event":{"data":{"error":"lp_swap:1888] eth:654] RPC error: Error { code: ServerError(-32010), message: \"Transaction with the same hash was already imported.\", data: None }"},"type":"TakerPaymentTransactionFailed"},"timestamp":1563744118577},{"event":{"type":"Finished"},"timestamp":1563744118580}],"success_events":["Started","Negotiated","TakerFeeSent","MakerPaymentReceived","MakerPaymentWaitConfirmStarted","MakerPaymentValidatedAndConfirmed","TakerPaymentSent","TakerPaymentSpent","MakerPaymentSpent","Finished"],"uuid":"3447b727-fe93-4357-8e5a-8cf2699b7e86"}"#;
        let taker_saved_swap: TakerSavedSwap = json::from_str(taker_saved_json).unwrap();
        let key_pair =
            key_pair_from_seed("spice describe gravity federal blast come thank unfair canal monkey style afraid")
                .unwrap();
        let ctx = MmCtxBuilder::default().with_secp256k1_key_pair(key_pair).into_mm_arc();

        TestCoin::ticker.mock_safe(|_| MockResult::Return("ticker"));
        TestCoin::swap_contract_address.mock_safe(|_| MockResult::Return(None));
        TestCoin::extract_secret.mock_safe(|_, _, _| MockResult::Return(Ok(vec![])));

        static mut MY_PAYMENT_SENT_CALLED: bool = false;
        TestCoin::check_if_my_payment_sent.mock_safe(|_, _, _, _, _, _| {
            unsafe { MY_PAYMENT_SENT_CALLED = true };
            MockResult::Return(Box::new(futures01::future::ok(Some(eth_tx_for_test().into()))))
        });

        static mut SEARCH_TX_SPEND_CALLED: bool = false;
        TestCoin::search_for_swap_tx_spend_my.mock_safe(|_, _, _, _, _, _, _| {
            unsafe { SEARCH_TX_SPEND_CALLED = true };
            let tx: UtxoTx = "0100000001de7aa8d29524906b2b54ee2e0281f3607f75662cbc9080df81d1047b78e21dbc00000000d7473044022079b6c50820040b1fbbe9251ced32ab334d33830f6f8d0bf0a40c7f1336b67d5b0220142ccf723ddabb34e542ed65c395abc1fbf5b6c3e730396f15d25c49b668a1a401209da937e5609680cb30bff4a7661364ca1d1851c2506fa80c443f00a3d3bf7365004c6b6304f62b0e5cb175210270e75970bb20029b3879ec76c4acd320a8d0589e003636264d01a7d566504bfbac6782012088a9142fb610d856c19fd57f2d0cffe8dff689074b3d8a882103f368228456c940ac113e53dad5c104cf209f2f102a409207269383b6ab9b03deac68ffffffff01d0dc9800000000001976a9146d9d2b554d768232320587df75c4338ecc8bf37d88ac40280e5c".into();
            MockResult::Return(Ok(Some(FoundSwapTxSpend::Spent(tx.into()))))
        });

        TestCoin::search_for_swap_tx_spend_other.mock_safe(|_, _, _, _, _, _, _| MockResult::Return(Ok(None)));

        static mut MAKER_PAYMENT_SPEND_CALLED: bool = false;
        TestCoin::send_taker_spends_maker_payment.mock_safe(|_, _, _, _, _, _| {
            unsafe { MAKER_PAYMENT_SPEND_CALLED = true };
            MockResult::Return(Box::new(futures01::future::ok(eth_tx_for_test().into())))
        });
        let maker_coin = MmCoinEnum::Test(TestCoin::default());
        let taker_coin = MmCoinEnum::Test(TestCoin::default());
        let (taker_swap, _) = TakerSwap::load_from_saved(ctx, maker_coin, taker_coin, taker_saved_swap).unwrap();
        let actual = taker_swap.recover_funds().unwrap();
        let expected = RecoveredSwap {
            action: RecoveredSwapAction::SpentOtherPayment,
            coin: "ticker".to_string(),
            transaction: eth_tx_for_test().into(),
        };
        assert_eq!(expected, actual);
        assert!(unsafe { MY_PAYMENT_SENT_CALLED });
        assert!(unsafe { SEARCH_TX_SPEND_CALLED });
        assert!(unsafe { MAKER_PAYMENT_SPEND_CALLED });
    }

    #[test]
    fn test_recover_funds_taker_swap_taker_payment_refund_failed_not_spent() {
        let taker_saved_json = r#"{"error_events":["StartFailed","NegotiateFailed","TakerFeeSendFailed","MakerPaymentValidateFailed","TakerPaymentTransactionFailed","TakerPaymentDataSendFailed","TakerPaymentWaitForSpendFailed","MakerPaymentSpendFailed","TakerPaymentRefunded","TakerPaymentRefundFailed"],"events":[{"event":{"data":{"lock_duration":7800,"maker":"1bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8","maker_amount":"0.58610590","maker_coin":"KMD","maker_coin_start_block":1450923,"maker_payment_confirmations":1,"maker_payment_wait":1563623475,"my_persistent_pub":"02713015d3fa4d30259e90be5f131beb593bf0131f3af2dcdb304e3322d8d52b91","started_at":1563620875,"taker_amount":"0.0077700000552410000000000","taker_coin":"LTC","taker_coin_start_block":1670837,"taker_payment_confirmations":1,"taker_payment_lock":1563628675,"uuid":"9db641f5-4300-4527-9fa6-f1c391d42c35"},"type":"Started"},"timestamp":1563620875766},{"event":{"data":{"maker_payment_locktime":1563636475,"maker_pubkey":"031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8","secret_hash":"7ed38daab6085c1a1e4426e61dc87a3c2c081a95"},"type":"Negotiated"},"timestamp":1563620955014},{"event":{"data":{"tx_hash":"6740136eaaa615d9d231969e3a9599d0fc59e53989237a8d31cd6fc86c160013","tx_hex":"0100000001a2586ea8294cedc55741bef625ba72c646399903391a7f6c604a58c6263135f2000000006b4830450221009c78c8ba4a7accab6b09f9a95da5bc59c81f4fc1e60b288ec3c5462b4d02ef01022056b63be1629cf17751d3cc5ffec51bcb1d7f9396e9ce9ca254d0f34104f7263a012102713015d3fa4d30259e90be5f131beb593bf0131f3af2dcdb304e3322d8d52b91ffffffff0210270000000000001976a914ca1e04745e8ca0c60d8c5881531d51bec470743f88ac78aa1900000000001976a91406ccabfd5f9075ecd5e8d0d31c0e973a54d51e8288ac5bf6325d"},"type":"TakerFeeSent"},"timestamp":1563620958220},{"event":{"data":{"tx_hash":"d0f6e664cea9d89fe7b5cf8005fdca070d1ab1d05a482aaef95c08cdaecddf0a","tx_hex":"0400008085202f89019f1cbda354342cdf982046b331bbd3791f53b692efc6e4becc36be495b2977d9000000006b483045022100fa9d4557394141f6a8b9bfb8cd594a521fd8bcd1965dbf8bc4e04abc849ac66e0220589f521814c10a7561abfd5e432f7a2ee60d4875fe4604618af3207dae531ac00121031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8ffffffff029e537e030000000017a9145534898009f1467191065f6890b96914b39a1c018791857702000000001976a914c3f710deb7320b0efa6edb14e3ebeeb9155fa90d88ac72ee325d000000000000000000000000000000"},"type":"MakerPaymentReceived"},"timestamp":1563620999307},{"event":{"type":"MakerPaymentWaitConfirmStarted"},"timestamp":1563620999310},{"event":{"type":"MakerPaymentValidatedAndConfirmed"},"timestamp":1563621244153},{"event":{"data":{"tx_hash":"1e883eb2f3991e84ba27f53651f89b7dda708678a5b9813d043577f222b9ca30","tx_hex":"01000000011300166cc86fcd318d7a238939e559fcd099953a9e9631d2d915a6aa6e134067010000006a47304402206781d5f2db2ff13d2ec7e266f774ea5630cc2dba4019e18e9716131b8b026051022006ebb33857b6d180f13aa6be2fc532f9734abde9d00ae14757e7d7ba3741c08c012102713015d3fa4d30259e90be5f131beb593bf0131f3af2dcdb304e3322d8d52b91ffffffff0228db0b000000000017a91483818667161bf94adda3964a81a231cbf6f5338187b0480c00000000001976a91406ccabfd5f9075ecd5e8d0d31c0e973a54d51e8288ac7cf7325d"},"type":"TakerPaymentSent"},"timestamp":1563621246370},{"event":{"data":{"error":"utxo:1145] rpc_clients:782] Waited too long until 1563628675 for output TransactionOutput { value: 777000, script_pubkey: a91483818667161bf94adda3964a81a231cbf6f5338187 } to be spent "},"type":"TakerPaymentWaitForSpendFailed"},"timestamp":1563638060370},{"event":{"data":{"error":"lp_swap:2025] utxo:938] rpc_clients:719] JsonRpcError { request: JsonRpcRequest { jsonrpc: \"2.0\", id: \"9\", method: \"blockchain.transaction.broadcast\", params: [String(\"010000000130cab922f27735043d81b9a5788670da7d9bf85136f527ba841e99f3b23e881e00000000b6473044022058a0c1da6bcf8c1418899ff8475f3ab6dddbff918528451c1fe71c2f7dad176302204c2e0bcf8f9b5f09e02ccfeb9256e9b34fb355ea655a5704a8a3fa920079b91501514c6b63048314335db1752102713015d3fa4d30259e90be5f131beb593bf0131f3af2dcdb304e3322d8d52b91ac6782012088a9147ed38daab6085c1a1e4426e61dc87a3c2c081a958821031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8ac68feffffff0188540a00000000001976a91406ccabfd5f9075ecd5e8d0d31c0e973a54d51e8288ac1c2b335d\")] }, error: Response(Object({\"code\": Number(1), \"message\": String(\"the transaction was rejected by network rules.\\n\\nMissing inputs\\n[010000000130cab922f27735043d81b9a5788670da7d9bf85136f527ba841e99f3b23e881e00000000b6473044022058a0c1da6bcf8c1418899ff8475f3ab6dddbff918528451c1fe71c2f7dad176302204c2e0bcf8f9b5f09e02ccfeb9256e9b34fb355ea655a5704a8a3fa920079b91501514c6b63048314335db1752102713015d3fa4d30259e90be5f131beb593bf0131f3af2dcdb304e3322d8d52b91ac6782012088a9147ed38daab6085c1a1e4426e61dc87a3c2c081a958821031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8ac68feffffff0188540a00000000001976a91406ccabfd5f9075ecd5e8d0d31c0e973a54d51e8288ac1c2b335d]\")})) }"},"type":"TakerPaymentRefundFailed"},"timestamp":1563638060583},{"event":{"type":"Finished"},"timestamp":1563638060585}],"success_events":["Started","Negotiated","TakerFeeSent","MakerPaymentReceived","MakerPaymentWaitConfirmStarted","MakerPaymentValidatedAndConfirmed","TakerPaymentSent","TakerPaymentSpent","MakerPaymentSpent","Finished"],"uuid":"9db641f5-4300-4527-9fa6-f1c391d42c35"}"#;
        let taker_saved_swap: TakerSavedSwap = json::from_str(taker_saved_json).unwrap();
        let key_pair =
            key_pair_from_seed("spice describe gravity federal blast come thank unfair canal monkey style afraid")
                .unwrap();
        let ctx = MmCtxBuilder::default().with_secp256k1_key_pair(key_pair).into_mm_arc();

        TestCoin::ticker.mock_safe(|_| MockResult::Return("ticker"));
        TestCoin::swap_contract_address.mock_safe(|_| MockResult::Return(None));

        static mut SEARCH_TX_SPEND_CALLED: bool = false;
        TestCoin::search_for_swap_tx_spend_my.mock_safe(|_, _, _, _, _, _, _| {
            unsafe { SEARCH_TX_SPEND_CALLED = true };
            MockResult::Return(Ok(None))
        });

        static mut REFUND_CALLED: bool = false;
        TestCoin::send_taker_refunds_payment.mock_safe(|_, _, _, _, _, _| {
            unsafe { REFUND_CALLED = true };
            MockResult::Return(Box::new(futures01::future::ok(eth_tx_for_test().into())))
        });
        let maker_coin = MmCoinEnum::Test(TestCoin::default());
        let taker_coin = MmCoinEnum::Test(TestCoin::default());
        let (taker_swap, _) = TakerSwap::load_from_saved(ctx, maker_coin, taker_coin, taker_saved_swap).unwrap();
        let actual = taker_swap.recover_funds().unwrap();
        let expected = RecoveredSwap {
            action: RecoveredSwapAction::RefundedMyPayment,
            coin: "ticker".to_string(),
            transaction: eth_tx_for_test().into(),
        };
        assert_eq!(expected, actual);
        assert!(unsafe { SEARCH_TX_SPEND_CALLED });
        assert!(unsafe { REFUND_CALLED });
    }

    #[test]
    fn test_recover_funds_taker_swap_taker_payment_refund_failed_not_spent_too_early_to_refund() {
        let taker_saved_json = r#"{"error_events":["StartFailed","NegotiateFailed","TakerFeeSendFailed","MakerPaymentValidateFailed","TakerPaymentTransactionFailed","TakerPaymentDataSendFailed","TakerPaymentWaitForSpendFailed","MakerPaymentSpendFailed","TakerPaymentRefunded","TakerPaymentRefundFailed"],"events":[{"event":{"data":{"lock_duration":7800,"maker":"1bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8","maker_amount":"0.58610590","maker_coin":"KMD","maker_coin_start_block":1450923,"maker_payment_confirmations":1,"maker_payment_wait":1563623475,"my_persistent_pub":"02713015d3fa4d30259e90be5f131beb593bf0131f3af2dcdb304e3322d8d52b91","started_at":1563620875,"taker_amount":"0.0077700000552410000000000","taker_coin":"LTC","taker_coin_start_block":1670837,"taker_payment_confirmations":1,"taker_payment_lock":1563628675,"uuid":"9db641f5-4300-4527-9fa6-f1c391d42c35"},"type":"Started"},"timestamp":1563620875766},{"event":{"data":{"maker_payment_locktime":1563636475,"maker_pubkey":"031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8","secret_hash":"7ed38daab6085c1a1e4426e61dc87a3c2c081a95"},"type":"Negotiated"},"timestamp":1563620955014},{"event":{"data":{"tx_hash":"6740136eaaa615d9d231969e3a9599d0fc59e53989237a8d31cd6fc86c160013","tx_hex":"0100000001a2586ea8294cedc55741bef625ba72c646399903391a7f6c604a58c6263135f2000000006b4830450221009c78c8ba4a7accab6b09f9a95da5bc59c81f4fc1e60b288ec3c5462b4d02ef01022056b63be1629cf17751d3cc5ffec51bcb1d7f9396e9ce9ca254d0f34104f7263a012102713015d3fa4d30259e90be5f131beb593bf0131f3af2dcdb304e3322d8d52b91ffffffff0210270000000000001976a914ca1e04745e8ca0c60d8c5881531d51bec470743f88ac78aa1900000000001976a91406ccabfd5f9075ecd5e8d0d31c0e973a54d51e8288ac5bf6325d"},"type":"TakerFeeSent"},"timestamp":1563620958220},{"event":{"data":{"tx_hash":"d0f6e664cea9d89fe7b5cf8005fdca070d1ab1d05a482aaef95c08cdaecddf0a","tx_hex":"0400008085202f89019f1cbda354342cdf982046b331bbd3791f53b692efc6e4becc36be495b2977d9000000006b483045022100fa9d4557394141f6a8b9bfb8cd594a521fd8bcd1965dbf8bc4e04abc849ac66e0220589f521814c10a7561abfd5e432f7a2ee60d4875fe4604618af3207dae531ac00121031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8ffffffff029e537e030000000017a9145534898009f1467191065f6890b96914b39a1c018791857702000000001976a914c3f710deb7320b0efa6edb14e3ebeeb9155fa90d88ac72ee325d000000000000000000000000000000"},"type":"MakerPaymentReceived"},"timestamp":1563620999307},{"event":{"type":"MakerPaymentWaitConfirmStarted"},"timestamp":1563620999310},{"event":{"type":"MakerPaymentValidatedAndConfirmed"},"timestamp":1563621244153},{"event":{"data":{"tx_hash":"1e883eb2f3991e84ba27f53651f89b7dda708678a5b9813d043577f222b9ca30","tx_hex":"01000000011300166cc86fcd318d7a238939e559fcd099953a9e9631d2d915a6aa6e134067010000006a47304402206781d5f2db2ff13d2ec7e266f774ea5630cc2dba4019e18e9716131b8b026051022006ebb33857b6d180f13aa6be2fc532f9734abde9d00ae14757e7d7ba3741c08c012102713015d3fa4d30259e90be5f131beb593bf0131f3af2dcdb304e3322d8d52b91ffffffff0228db0b000000000017a91483818667161bf94adda3964a81a231cbf6f5338187b0480c00000000001976a91406ccabfd5f9075ecd5e8d0d31c0e973a54d51e8288ac7cf7325d"},"type":"TakerPaymentSent"},"timestamp":1563621246370},{"event":{"data":{"error":"utxo:1145] rpc_clients:782] Waited too long until 1563628675 for output TransactionOutput { value: 777000, script_pubkey: a91483818667161bf94adda3964a81a231cbf6f5338187 } to be spent "},"type":"TakerPaymentWaitForSpendFailed"},"timestamp":1563638060370},{"event":{"data":{"error":"lp_swap:2025] utxo:938] rpc_clients:719] JsonRpcError { request: JsonRpcRequest { jsonrpc: \"2.0\", id: \"9\", method: \"blockchain.transaction.broadcast\", params: [String(\"010000000130cab922f27735043d81b9a5788670da7d9bf85136f527ba841e99f3b23e881e00000000b6473044022058a0c1da6bcf8c1418899ff8475f3ab6dddbff918528451c1fe71c2f7dad176302204c2e0bcf8f9b5f09e02ccfeb9256e9b34fb355ea655a5704a8a3fa920079b91501514c6b63048314335db1752102713015d3fa4d30259e90be5f131beb593bf0131f3af2dcdb304e3322d8d52b91ac6782012088a9147ed38daab6085c1a1e4426e61dc87a3c2c081a958821031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8ac68feffffff0188540a00000000001976a91406ccabfd5f9075ecd5e8d0d31c0e973a54d51e8288ac1c2b335d\")] }, error: Response(Object({\"code\": Number(1), \"message\": String(\"the transaction was rejected by network rules.\\n\\nMissing inputs\\n[010000000130cab922f27735043d81b9a5788670da7d9bf85136f527ba841e99f3b23e881e00000000b6473044022058a0c1da6bcf8c1418899ff8475f3ab6dddbff918528451c1fe71c2f7dad176302204c2e0bcf8f9b5f09e02ccfeb9256e9b34fb355ea655a5704a8a3fa920079b91501514c6b63048314335db1752102713015d3fa4d30259e90be5f131beb593bf0131f3af2dcdb304e3322d8d52b91ac6782012088a9147ed38daab6085c1a1e4426e61dc87a3c2c081a958821031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8ac68feffffff0188540a00000000001976a91406ccabfd5f9075ecd5e8d0d31c0e973a54d51e8288ac1c2b335d]\")})) }"},"type":"TakerPaymentRefundFailed"},"timestamp":1563638060583},{"event":{"type":"Finished"},"timestamp":1563638060585}],"success_events":["Started","Negotiated","TakerFeeSent","MakerPaymentReceived","MakerPaymentWaitConfirmStarted","MakerPaymentValidatedAndConfirmed","TakerPaymentSent","TakerPaymentSpent","MakerPaymentSpent","Finished"],"uuid":"9db641f5-4300-4527-9fa6-f1c391d42c35"}"#;
        let taker_saved_swap: TakerSavedSwap = json::from_str(taker_saved_json).unwrap();
        let key_pair =
            key_pair_from_seed("spice describe gravity federal blast come thank unfair canal monkey style afraid")
                .unwrap();
        let ctx = MmCtxBuilder::default().with_secp256k1_key_pair(key_pair).into_mm_arc();

        TestCoin::ticker.mock_safe(|_| MockResult::Return("ticker"));
        TestCoin::swap_contract_address.mock_safe(|_| MockResult::Return(None));

        static mut SEARCH_TX_SPEND_CALLED: bool = false;
        TestCoin::search_for_swap_tx_spend_my.mock_safe(|_, _, _, _, _, _, _| {
            unsafe { SEARCH_TX_SPEND_CALLED = true };
            MockResult::Return(Ok(None))
        });
        let maker_coin = MmCoinEnum::Test(TestCoin::default());
        let taker_coin = MmCoinEnum::Test(TestCoin::default());
        let (taker_swap, _) = TakerSwap::load_from_saved(ctx, maker_coin, taker_coin, taker_saved_swap).unwrap();
        taker_swap.w().data.taker_payment_lock = (now_ms() / 1000) - 3690;
        assert!(taker_swap.recover_funds().is_err());
        assert!(unsafe { SEARCH_TX_SPEND_CALLED });
    }

    #[test]
    fn test_recover_funds_taker_swap_taker_payment_refund_failed_spent_by_maker() {
        let taker_saved_json = r#"{"error_events":["StartFailed","NegotiateFailed","TakerFeeSendFailed","MakerPaymentValidateFailed","TakerPaymentTransactionFailed","TakerPaymentDataSendFailed","TakerPaymentWaitForSpendFailed","MakerPaymentSpendFailed","TakerPaymentRefunded","TakerPaymentRefundFailed"],"events":[{"event":{"data":{"lock_duration":7800,"maker":"1bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8","maker_amount":"0.58610590","maker_coin":"KMD","maker_coin_start_block":1450923,"maker_payment_confirmations":1,"maker_payment_wait":1563623475,"my_persistent_pub":"02713015d3fa4d30259e90be5f131beb593bf0131f3af2dcdb304e3322d8d52b91","started_at":1563620875,"taker_amount":"0.0077700000552410000000000","taker_coin":"LTC","taker_coin_start_block":1670837,"taker_payment_confirmations":1,"taker_payment_lock":1563628675,"uuid":"9db641f5-4300-4527-9fa6-f1c391d42c35"},"type":"Started"},"timestamp":1563620875766},{"event":{"data":{"maker_payment_locktime":1563636475,"maker_pubkey":"031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8","secret_hash":"7ed38daab6085c1a1e4426e61dc87a3c2c081a95"},"type":"Negotiated"},"timestamp":1563620955014},{"event":{"data":{"tx_hash":"6740136eaaa615d9d231969e3a9599d0fc59e53989237a8d31cd6fc86c160013","tx_hex":"0100000001a2586ea8294cedc55741bef625ba72c646399903391a7f6c604a58c6263135f2000000006b4830450221009c78c8ba4a7accab6b09f9a95da5bc59c81f4fc1e60b288ec3c5462b4d02ef01022056b63be1629cf17751d3cc5ffec51bcb1d7f9396e9ce9ca254d0f34104f7263a012102713015d3fa4d30259e90be5f131beb593bf0131f3af2dcdb304e3322d8d52b91ffffffff0210270000000000001976a914ca1e04745e8ca0c60d8c5881531d51bec470743f88ac78aa1900000000001976a91406ccabfd5f9075ecd5e8d0d31c0e973a54d51e8288ac5bf6325d"},"type":"TakerFeeSent"},"timestamp":1563620958220},{"event":{"data":{"tx_hash":"d0f6e664cea9d89fe7b5cf8005fdca070d1ab1d05a482aaef95c08cdaecddf0a","tx_hex":"0400008085202f89019f1cbda354342cdf982046b331bbd3791f53b692efc6e4becc36be495b2977d9000000006b483045022100fa9d4557394141f6a8b9bfb8cd594a521fd8bcd1965dbf8bc4e04abc849ac66e0220589f521814c10a7561abfd5e432f7a2ee60d4875fe4604618af3207dae531ac00121031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8ffffffff029e537e030000000017a9145534898009f1467191065f6890b96914b39a1c018791857702000000001976a914c3f710deb7320b0efa6edb14e3ebeeb9155fa90d88ac72ee325d000000000000000000000000000000"},"type":"MakerPaymentReceived"},"timestamp":1563620999307},{"event":{"type":"MakerPaymentWaitConfirmStarted"},"timestamp":1563620999310},{"event":{"type":"MakerPaymentValidatedAndConfirmed"},"timestamp":1563621244153},{"event":{"data":{"tx_hash":"1e883eb2f3991e84ba27f53651f89b7dda708678a5b9813d043577f222b9ca30","tx_hex":"01000000011300166cc86fcd318d7a238939e559fcd099953a9e9631d2d915a6aa6e134067010000006a47304402206781d5f2db2ff13d2ec7e266f774ea5630cc2dba4019e18e9716131b8b026051022006ebb33857b6d180f13aa6be2fc532f9734abde9d00ae14757e7d7ba3741c08c012102713015d3fa4d30259e90be5f131beb593bf0131f3af2dcdb304e3322d8d52b91ffffffff0228db0b000000000017a91483818667161bf94adda3964a81a231cbf6f5338187b0480c00000000001976a91406ccabfd5f9075ecd5e8d0d31c0e973a54d51e8288ac7cf7325d"},"type":"TakerPaymentSent"},"timestamp":1563621246370},{"event":{"data":{"error":"utxo:1145] rpc_clients:782] Waited too long until 1563628675 for output TransactionOutput { value: 777000, script_pubkey: a91483818667161bf94adda3964a81a231cbf6f5338187 } to be spent "},"type":"TakerPaymentWaitForSpendFailed"},"timestamp":1563638060370},{"event":{"data":{"error":"lp_swap:2025] utxo:938] rpc_clients:719] JsonRpcError { request: JsonRpcRequest { jsonrpc: \"2.0\", id: \"9\", method: \"blockchain.transaction.broadcast\", params: [String(\"010000000130cab922f27735043d81b9a5788670da7d9bf85136f527ba841e99f3b23e881e00000000b6473044022058a0c1da6bcf8c1418899ff8475f3ab6dddbff918528451c1fe71c2f7dad176302204c2e0bcf8f9b5f09e02ccfeb9256e9b34fb355ea655a5704a8a3fa920079b91501514c6b63048314335db1752102713015d3fa4d30259e90be5f131beb593bf0131f3af2dcdb304e3322d8d52b91ac6782012088a9147ed38daab6085c1a1e4426e61dc87a3c2c081a958821031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8ac68feffffff0188540a00000000001976a91406ccabfd5f9075ecd5e8d0d31c0e973a54d51e8288ac1c2b335d\")] }, error: Response(Object({\"code\": Number(1), \"message\": String(\"the transaction was rejected by network rules.\\n\\nMissing inputs\\n[010000000130cab922f27735043d81b9a5788670da7d9bf85136f527ba841e99f3b23e881e00000000b6473044022058a0c1da6bcf8c1418899ff8475f3ab6dddbff918528451c1fe71c2f7dad176302204c2e0bcf8f9b5f09e02ccfeb9256e9b34fb355ea655a5704a8a3fa920079b91501514c6b63048314335db1752102713015d3fa4d30259e90be5f131beb593bf0131f3af2dcdb304e3322d8d52b91ac6782012088a9147ed38daab6085c1a1e4426e61dc87a3c2c081a958821031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8ac68feffffff0188540a00000000001976a91406ccabfd5f9075ecd5e8d0d31c0e973a54d51e8288ac1c2b335d]\")})) }"},"type":"TakerPaymentRefundFailed"},"timestamp":1563638060583},{"event":{"type":"Finished"},"timestamp":1563638060585}],"success_events":["Started","Negotiated","TakerFeeSent","MakerPaymentReceived","MakerPaymentWaitConfirmStarted","MakerPaymentValidatedAndConfirmed","TakerPaymentSent","TakerPaymentSpent","MakerPaymentSpent","Finished"],"uuid":"9db641f5-4300-4527-9fa6-f1c391d42c35"}"#;
        let taker_saved_swap: TakerSavedSwap = json::from_str(taker_saved_json).unwrap();
        let key_pair =
            key_pair_from_seed("spice describe gravity federal blast come thank unfair canal monkey style afraid")
                .unwrap();
        let ctx = MmCtxBuilder::default().with_secp256k1_key_pair(key_pair).into_mm_arc();

        TestCoin::ticker.mock_safe(|_| MockResult::Return("ticker"));
        TestCoin::swap_contract_address.mock_safe(|_| MockResult::Return(None));
        TestCoin::extract_secret.mock_safe(|_, _, _| MockResult::Return(Ok(vec![])));

        static mut SEARCH_TX_SPEND_CALLED: bool = false;
        TestCoin::search_for_swap_tx_spend_my.mock_safe(|_, _, _, _, _, _, _| {
            unsafe { SEARCH_TX_SPEND_CALLED = true };
            let tx: UtxoTx = "0100000001de7aa8d29524906b2b54ee2e0281f3607f75662cbc9080df81d1047b78e21dbc00000000d7473044022079b6c50820040b1fbbe9251ced32ab334d33830f6f8d0bf0a40c7f1336b67d5b0220142ccf723ddabb34e542ed65c395abc1fbf5b6c3e730396f15d25c49b668a1a401209da937e5609680cb30bff4a7661364ca1d1851c2506fa80c443f00a3d3bf7365004c6b6304f62b0e5cb175210270e75970bb20029b3879ec76c4acd320a8d0589e003636264d01a7d566504bfbac6782012088a9142fb610d856c19fd57f2d0cffe8dff689074b3d8a882103f368228456c940ac113e53dad5c104cf209f2f102a409207269383b6ab9b03deac68ffffffff01d0dc9800000000001976a9146d9d2b554d768232320587df75c4338ecc8bf37d88ac40280e5c".into();
            MockResult::Return(Ok(Some(FoundSwapTxSpend::Spent(tx.into()))))
        });

        TestCoin::search_for_swap_tx_spend_other.mock_safe(|_, _, _, _, _, _, _| MockResult::Return(Ok(None)));

        static mut MAKER_PAYMENT_SPEND_CALLED: bool = false;
        TestCoin::send_taker_spends_maker_payment.mock_safe(|_, _, _, _, _, _| {
            unsafe { MAKER_PAYMENT_SPEND_CALLED = true };
            MockResult::Return(Box::new(futures01::future::ok(eth_tx_for_test().into())))
        });
        let maker_coin = MmCoinEnum::Test(TestCoin::default());
        let taker_coin = MmCoinEnum::Test(TestCoin::default());
        let (taker_swap, _) = TakerSwap::load_from_saved(ctx, maker_coin, taker_coin, taker_saved_swap).unwrap();
        let actual = taker_swap.recover_funds().unwrap();
        let expected = RecoveredSwap {
            action: RecoveredSwapAction::SpentOtherPayment,
            coin: "ticker".to_string(),
            transaction: eth_tx_for_test().into(),
        };
        assert_eq!(expected, actual);
        assert!(unsafe { SEARCH_TX_SPEND_CALLED });
        assert!(unsafe { MAKER_PAYMENT_SPEND_CALLED });
    }

    #[test]
    fn test_recover_funds_taker_swap_not_finished() {
        // the json doesn't have Finished event at the end
        let taker_saved_json = r#"{"error_events":["StartFailed","NegotiateFailed","TakerFeeSendFailed","MakerPaymentValidateFailed","TakerPaymentTransactionFailed","TakerPaymentDataSendFailed","TakerPaymentWaitForSpendFailed","MakerPaymentSpendFailed","TakerPaymentRefunded","TakerPaymentRefundFailed"],"events":[{"event":{"data":{"lock_duration":7800,"maker":"1bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8","maker_amount":"0.12596566232185483","maker_coin":"KMD","maker_coin_start_block":1458035,"maker_payment_confirmations":1,"maker_payment_wait":1564053079,"my_persistent_pub":"0326846707a52a233cfc49a61ef51b1698bbe6aa78fa8b8d411c02743c09688f0a","started_at":1564050479,"taker_amount":"50.000000000000001504212457800000","taker_coin":"DOGE","taker_coin_start_block":2823448,"taker_payment_confirmations":1,"taker_payment_lock":1564058279,"uuid":"41383f43-46a5-478c-9386-3b2cce0aca20"},"type":"Started"},"timestamp":1564050480269},{"event":{"data":{"maker_payment_locktime":1564066080,"maker_pubkey":"031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8","secret_hash":"3669eb83a007a3c507448d79f45a9f06ec2f36a8"},"type":"Negotiated"},"timestamp":1564050540991},{"event":{"data":{"tx_hash":"bdde828b492d6d1cc25cd2322fd592dafd722fcc7d8b0fedce4d3bb4a1a8c8ff","tx_hex":"0100000002c7efa995c8b7be0a8b6c2d526c6c444c1634d65584e9ee89904e9d8675eac88c010000006a473044022051f34d5e3b7d0b9098d5e35333f3550f9cb9e57df83d5e4635b7a8d2986d6d5602200288c98da05de6950e01229a637110a1800ba643e75cfec59d4eb1021ad9b40801210326846707a52a233cfc49a61ef51b1698bbe6aa78fa8b8d411c02743c09688f0affffffffae6c233989efa7c7d2aa6534adc96078917ff395b7f09f734a147b2f44ade164000000006a4730440220393a784c2da74d0e2a28ec4f7df6c8f9d8b2af6ae6957f1e68346d744223a8fd02201b7a96954ac06815a43a6c7668d829ae9cbb5de76fa77189ddfd9e3038df662c01210326846707a52a233cfc49a61ef51b1698bbe6aa78fa8b8d411c02743c09688f0affffffff02115f5800000000001976a914ca1e04745e8ca0c60d8c5881531d51bec470743f88ac41a84641020000001976a914444f0e1099709ba4d742454a7d98a5c9c162ceab88ac6d84395d"},"type":"TakerFeeSent"},"timestamp":1564050545296},{"event":{"data":{"tx_hash":"0a0f11fa82802c2c30862c50ab2162185dae8de7f7235f32c506f814c142b382","tx_hex":"0400008085202f8902ace337db2dd4c56b0697f58fb8cfb6bd1cd6f469d925fc0376d1dcfb7581bf82000000006b483045022100d1f95be235c5c8880f5d703ace287e2768548792c58c5dbd27f5578881b30ea70220030596106e21c7e0057ee0dab283f9a1fe273f15208cba80870c447bd559ef0d0121031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8ffffffff9f339752567c404427fd77f2b35cecdb4c21489edc64e25e729fdb281785e423000000006a47304402203179e95877dbc107123a417f1e648e3ff13d384890f1e4a67b6dd5087235152e0220102a8ab799fadb26b5d89ceb9c7bc721a7e0c2a0d0d7e46bbe0cf3d130010d430121031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8ffffffff025635c0000000000017a91480a95d366d65e34a465ab17b0c9eb1d5a33bae08876cbfce05000000001976a914c3f710deb7320b0efa6edb14e3ebeeb9155fa90d88ac8d7c395d000000000000000000000000000000"},"type":"MakerPaymentReceived"},"timestamp":1564050588176},{"event":{"type":"MakerPaymentWaitConfirmStarted"},"timestamp":1564050588178},{"event":{"type":"MakerPaymentValidatedAndConfirmed"},"timestamp":1564050693585},{"event":{"data":{"tx_hash":"539cb6dbdc25465bbccc575554f05d1bb04c70efce4316e41194e747375c3659","tx_hex":"0100000001ffc8a8a1b43b4dceed0f8b7dcc2f72fdda92d52f32d25cc21c6d2d498b82debd010000006a47304402203967b7f9f5532fa47116585c7d1bcba51861ea2059cca00409f34660db18e33a0220640991911852533a12fdfeb039fb9c8ca2c45482c6993bd84636af3670d49c1501210326846707a52a233cfc49a61ef51b1698bbe6aa78fa8b8d411c02743c09688f0affffffff0200f2052a0100000017a914f2fa08ae416b576779ae5da975e5442663215fce87415173f9000000001976a914444f0e1099709ba4d742454a7d98a5c9c162ceab88ac0585395d"},"type":"TakerPaymentSent"},"timestamp":1564050695611},{"event":{"data":{"secret":"1b8886b8a2cdb62505699400b694ac20f04d7bd4abd80e1ab154aa8d861fc093","transaction":{"tx_hash":"cc5af1cf68d246419fee49c3d74c0cd173599d115b86efe274368a614951bc47","tx_hex":"010000000159365c3747e79411e41643ceef704cb01b5df0545557ccbc5b4625dcdbb69c5300000000d747304402200e78e27d2f1c18676f98ca3dfa4e4a9eeaa8209b55f57b4dd5d9e1abdf034cfa0220623b5c22b62234cec230342aa306c497e43494b44ec2425b84e236b1bf01257001201b8886b8a2cdb62505699400b694ac20f04d7bd4abd80e1ab154aa8d861fc093004c6b6304a7a2395db175210326846707a52a233cfc49a61ef51b1698bbe6aa78fa8b8d411c02743c09688f0aac6782012088a9143669eb83a007a3c507448d79f45a9f06ec2f36a88821031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8ac68ffffffff01008d380c010000001976a914c3f710deb7320b0efa6edb14e3ebeeb9155fa90d88ac8c77395d"}},"type":"TakerPaymentSpent"},"timestamp":1564051092890},{"event":{"data":{"error":"lp_swap:1981] utxo:891] rpc_clients:738] JsonRpcError { request: JsonRpcRequest { jsonrpc: \"2.0\", id: \"67\", method: \"blockchain.transaction.broadcast\", params: [String(\"0400008085202f890182b342c114f806c5325f23f7e78dae5d186221ab502c86302c2c8082fa110f0a00000000d7473044022035791ea5548f87484065c9e1f0bdca9ebc699f2c7f51182c84f360102e32dc3d02200612ed53bca52d9c2568437f087598531534badf26229fe0f652ea72ddf03ca501201b8886b8a2cdb62505699400b694ac20f04d7bd4abd80e1ab154aa8d861fc093004c6b630420c1395db17521031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8ac6782012088a9143669eb83a007a3c507448d79f45a9f06ec2f36a888210326846707a52a233cfc49a61ef51b1698bbe6aa78fa8b8d411c02743c09688f0aac68ffffffff01460ec000000000001976a914444f0e1099709ba4d742454a7d98a5c9c162ceab88ac967e395d000000000000000000000000000000\")] }, error: Transport(\"rpc_clients:668] All electrums are currently disconnected\") }"},"type":"MakerPaymentSpendFailed"},"timestamp":1564051092897}],"success_events":["Started","Negotiated","TakerFeeSent","MakerPaymentReceived","MakerPaymentWaitConfirmStarted","MakerPaymentValidatedAndConfirmed","TakerPaymentSent","TakerPaymentSpent","MakerPaymentSpent","Finished"],"uuid":"41383f43-46a5-478c-9386-3b2cce0aca20"}"#;
        let taker_saved_swap: TakerSavedSwap = json::from_str(taker_saved_json).unwrap();
        let key_pair =
            key_pair_from_seed("spice describe gravity federal blast come thank unfair canal monkey style afraid")
                .unwrap();
        let ctx = MmCtxBuilder::default().with_secp256k1_key_pair(key_pair).into_mm_arc();

        TestCoin::ticker.mock_safe(|_| MockResult::Return("ticker"));
        TestCoin::swap_contract_address.mock_safe(|_| MockResult::Return(None));
        let maker_coin = MmCoinEnum::Test(TestCoin::default());
        let taker_coin = MmCoinEnum::Test(TestCoin::default());
        let (taker_swap, _) = TakerSwap::load_from_saved(ctx, maker_coin, taker_coin, taker_saved_swap).unwrap();
        assert!(taker_swap.recover_funds().is_err());
    }

    #[test]
    fn test_taker_swap_event_should_ban() {
        let event = TakerSwapEvent::TakerPaymentWaitConfirmFailed("err".into());
        assert!(!event.should_ban_maker());

        let event = TakerSwapEvent::MakerPaymentWaitConfirmFailed("err".into());
        assert!(!event.should_ban_maker());

        let event = TakerSwapEvent::MakerPaymentValidateFailed("err".into());
        assert!(event.should_ban_maker());

        let event = TakerSwapEvent::TakerPaymentWaitForSpendFailed("err".into());
        assert!(event.should_ban_maker());
    }

    #[test]
    fn test_recheck_swap_contract_address_if_none() {
        // swap file contains neither maker_coin_swap_contract_address nor taker_coin_swap_contract_address
        let taker_saved_json = r#"{"error_events":["StartFailed","NegotiateFailed","TakerFeeSendFailed","MakerPaymentValidateFailed","TakerPaymentTransactionFailed","TakerPaymentDataSendFailed","TakerPaymentWaitForSpendFailed","MakerPaymentSpendFailed","TakerPaymentRefunded","TakerPaymentRefundFailed"],"events":[{"event":{"data":{"lock_duration":7800,"maker":"1bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8","maker_amount":"0.58610590","maker_coin":"KMD","maker_coin_start_block":1450923,"maker_payment_confirmations":1,"maker_payment_wait":1563623475,"my_persistent_pub":"02713015d3fa4d30259e90be5f131beb593bf0131f3af2dcdb304e3322d8d52b91","started_at":1563620875,"taker_amount":"0.0077700000552410000000000","taker_coin":"LTC","taker_coin_start_block":1670837,"taker_payment_confirmations":1,"taker_payment_lock":1563628675,"uuid":"9db641f5-4300-4527-9fa6-f1c391d42c35"},"type":"Started"},"timestamp":1563620875766},{"event":{"data":{"maker_payment_locktime":1563636475,"maker_pubkey":"031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8","secret_hash":"7ed38daab6085c1a1e4426e61dc87a3c2c081a95"},"type":"Negotiated"},"timestamp":1563620955014},{"event":{"data":{"tx_hash":"6740136eaaa615d9d231969e3a9599d0fc59e53989237a8d31cd6fc86c160013","tx_hex":"0100000001a2586ea8294cedc55741bef625ba72c646399903391a7f6c604a58c6263135f2000000006b4830450221009c78c8ba4a7accab6b09f9a95da5bc59c81f4fc1e60b288ec3c5462b4d02ef01022056b63be1629cf17751d3cc5ffec51bcb1d7f9396e9ce9ca254d0f34104f7263a012102713015d3fa4d30259e90be5f131beb593bf0131f3af2dcdb304e3322d8d52b91ffffffff0210270000000000001976a914ca1e04745e8ca0c60d8c5881531d51bec470743f88ac78aa1900000000001976a91406ccabfd5f9075ecd5e8d0d31c0e973a54d51e8288ac5bf6325d"},"type":"TakerFeeSent"},"timestamp":1563620958220},{"event":{"data":{"tx_hash":"d0f6e664cea9d89fe7b5cf8005fdca070d1ab1d05a482aaef95c08cdaecddf0a","tx_hex":"0400008085202f89019f1cbda354342cdf982046b331bbd3791f53b692efc6e4becc36be495b2977d9000000006b483045022100fa9d4557394141f6a8b9bfb8cd594a521fd8bcd1965dbf8bc4e04abc849ac66e0220589f521814c10a7561abfd5e432f7a2ee60d4875fe4604618af3207dae531ac00121031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8ffffffff029e537e030000000017a9145534898009f1467191065f6890b96914b39a1c018791857702000000001976a914c3f710deb7320b0efa6edb14e3ebeeb9155fa90d88ac72ee325d000000000000000000000000000000"},"type":"MakerPaymentReceived"},"timestamp":1563620999307},{"event":{"type":"MakerPaymentWaitConfirmStarted"},"timestamp":1563620999310},{"event":{"type":"MakerPaymentValidatedAndConfirmed"},"timestamp":1563621244153},{"event":{"data":{"tx_hash":"1e883eb2f3991e84ba27f53651f89b7dda708678a5b9813d043577f222b9ca30","tx_hex":"01000000011300166cc86fcd318d7a238939e559fcd099953a9e9631d2d915a6aa6e134067010000006a47304402206781d5f2db2ff13d2ec7e266f774ea5630cc2dba4019e18e9716131b8b026051022006ebb33857b6d180f13aa6be2fc532f9734abde9d00ae14757e7d7ba3741c08c012102713015d3fa4d30259e90be5f131beb593bf0131f3af2dcdb304e3322d8d52b91ffffffff0228db0b000000000017a91483818667161bf94adda3964a81a231cbf6f5338187b0480c00000000001976a91406ccabfd5f9075ecd5e8d0d31c0e973a54d51e8288ac7cf7325d"},"type":"TakerPaymentSent"},"timestamp":1563621246370},{"event":{"data":{"error":"utxo:1145] rpc_clients:782] Waited too long until 1563628675 for output TransactionOutput { value: 777000, script_pubkey: a91483818667161bf94adda3964a81a231cbf6f5338187 } to be spent "},"type":"TakerPaymentWaitForSpendFailed"},"timestamp":1563638060370},{"event":{"data":{"error":"lp_swap:2025] utxo:938] rpc_clients:719] JsonRpcError { request: JsonRpcRequest { jsonrpc: \"2.0\", id: \"9\", method: \"blockchain.transaction.broadcast\", params: [String(\"010000000130cab922f27735043d81b9a5788670da7d9bf85136f527ba841e99f3b23e881e00000000b6473044022058a0c1da6bcf8c1418899ff8475f3ab6dddbff918528451c1fe71c2f7dad176302204c2e0bcf8f9b5f09e02ccfeb9256e9b34fb355ea655a5704a8a3fa920079b91501514c6b63048314335db1752102713015d3fa4d30259e90be5f131beb593bf0131f3af2dcdb304e3322d8d52b91ac6782012088a9147ed38daab6085c1a1e4426e61dc87a3c2c081a958821031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8ac68feffffff0188540a00000000001976a91406ccabfd5f9075ecd5e8d0d31c0e973a54d51e8288ac1c2b335d\")] }, error: Response(Object({\"code\": Number(1), \"message\": String(\"the transaction was rejected by network rules.\\n\\nMissing inputs\\n[010000000130cab922f27735043d81b9a5788670da7d9bf85136f527ba841e99f3b23e881e00000000b6473044022058a0c1da6bcf8c1418899ff8475f3ab6dddbff918528451c1fe71c2f7dad176302204c2e0bcf8f9b5f09e02ccfeb9256e9b34fb355ea655a5704a8a3fa920079b91501514c6b63048314335db1752102713015d3fa4d30259e90be5f131beb593bf0131f3af2dcdb304e3322d8d52b91ac6782012088a9147ed38daab6085c1a1e4426e61dc87a3c2c081a958821031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8ac68feffffff0188540a00000000001976a91406ccabfd5f9075ecd5e8d0d31c0e973a54d51e8288ac1c2b335d]\")})) }"},"type":"TakerPaymentRefundFailed"},"timestamp":1563638060583},{"event":{"type":"Finished"},"timestamp":1563638060585}],"success_events":["Started","Negotiated","TakerFeeSent","MakerPaymentReceived","MakerPaymentWaitConfirmStarted","MakerPaymentValidatedAndConfirmed","TakerPaymentSent","TakerPaymentSpent","MakerPaymentSpent","Finished"],"uuid":"9db641f5-4300-4527-9fa6-f1c391d42c35"}"#;
        let taker_saved_swap: TakerSavedSwap = json::from_str(taker_saved_json).unwrap();
        let key_pair =
            key_pair_from_seed("spice describe gravity federal blast come thank unfair canal monkey style afraid")
                .unwrap();
        let ctx = MmCtxBuilder::default().with_secp256k1_key_pair(key_pair).into_mm_arc();

        TestCoin::ticker.mock_safe(|_| MockResult::Return("ticker"));
        static mut SWAP_CONTRACT_ADDRESS_CALLED: usize = 0;
        TestCoin::swap_contract_address.mock_safe(|_| {
            unsafe { SWAP_CONTRACT_ADDRESS_CALLED += 1 };
            MockResult::Return(Some(BytesJson::default()))
        });
        let maker_coin = MmCoinEnum::Test(TestCoin::default());
        let taker_coin = MmCoinEnum::Test(TestCoin::default());
        let (taker_swap, _) =
            TakerSwap::load_from_saved(ctx.clone(), maker_coin, taker_coin, taker_saved_swap).unwrap();

        assert_eq!(unsafe { SWAP_CONTRACT_ADDRESS_CALLED }, 2);
        assert_eq!(
            taker_swap.r().data.maker_coin_swap_contract_address,
            Some(BytesJson::default())
        );
        assert_eq!(
            taker_swap.r().data.taker_coin_swap_contract_address,
            Some(BytesJson::default())
        );
    }

    #[test]
    fn test_recheck_only_one_swap_contract_address() {
        // swap file contains only maker_coin_swap_contract_address
        let taker_saved_json = r#"{"type":"Taker","uuid":"49c79ea4-e1eb-4fb2-a0ef-265bded0b77f","events":[{"timestamp":1608542326909,"event":{"type":"Started","data":{"taker_coin":"RICK","maker_coin":"ETH","maker":"c6a78589e18b482aea046975e6d0acbdea7bf7dbf04d9d5bd67fda917815e3ed","my_persistent_pub":"02031d4256c4bc9f99ac88bf3dba21773132281f65f9bf23a59928bce08961e2f3","lock_duration":7800,"maker_amount":"0.1","taker_amount":"0.1","maker_payment_confirmations":1,"maker_payment_requires_nota":false,"taker_payment_confirmations":0,"taker_payment_requires_nota":false,"taker_payment_lock":1608550126,"uuid":"49c79ea4-e1eb-4fb2-a0ef-265bded0b77f","started_at":1608542326,"maker_payment_wait":1608545446,"maker_coin_start_block":14360,"taker_coin_start_block":723123,"maker_coin_swap_contract_address":"a09ad3cd7e96586ebd05a2607ee56b56fb2db8fd"}}},{"timestamp":1608542327416,"event":{"type":"Negotiated","data":{"maker_payment_locktime":1608557926,"maker_pubkey":"03c6a78589e18b482aea046975e6d0acbdea7bf7dbf04d9d5bd67fda917815e3ed","secret_hash":"8b0221f3b977c1c65dddf17c1c28e2bbced9e7b4"}}},{"timestamp":1608542332604,"event":{"type":"TakerFeeSent","data":{"tx_hex":"0400008085202f89011ca964f77200b73d64b481f47de84098041d3470d6256e44f2741f080e2b11cf020000006b4830450221008a064f5e51ef8281d43eb7bcd016fed7e560ea1eb7b0713ec977602c96d8f79b02205bfaa6655b849b9922c03276b938273f2edb8fb9ffcaa2a9212d7220560f6060012102031d4256c4bc9f99ac88bf3dba21773132281f65f9bf23a59928bce08961e2f3ffffffff0246320000000000001976a914ca1e04745e8ca0c60d8c5881531d51bec470743f88ac62752e27000000001976a91405aab5342166f8594baf17a7d9bef5d56744332788ac7768e05f000000000000000000000000000000","tx_hash":"3793df28ed2aac6188d2c48ec65eff12eea301089d60da655fc96f598326d708"}}},{"timestamp":1608542334018,"event":{"type":"MakerPaymentReceived","data":{"tx_hex":"f8ef82021c80830249f094a09ad3cd7e96586ebd05a2607ee56b56fb2db8fd88016345785d8a0000b884152cf3af50aebafeaf827c62c2eed09e265fa5aa9e013c0f27f0a88259f1aaa1279f0c32000000000000000000000000bab36286672fbdc7b250804bf6d14be0df69fa298b0221f3b977c1c65dddf17c1c28e2bbced9e7b4000000000000000000000000000000000000000000000000000000000000000000000000000000005fe0a5661ba0f18a0c5c349462b51dacd1a0761e4997d4572a01e48480c4e310d69a40308ad3a04510513f01a79c59f22c9cb79952547c8dfc4c74785b630f512d64369323e0c1","tx_hash":"6782323490584a2bc768cd5199506bfa1ed91e7515b35bb72fa269604b7dc0aa"}}},{"timestamp":1608542334019,"event":{"type":"MakerPaymentWaitConfirmStarted"}},{"timestamp":1608542334825,"event":{"type":"MakerPaymentValidatedAndConfirmed"}},{"timestamp":1608542337671,"event":{"type":"TakerPaymentSent","data":{"tx_hex":"0400008085202f890108d72683596fc95f65da609d0801a3ee12ff5ec68ec4d28861ac2aed28df9337010000006b48304502210086a03db599438b243bee2b02af56e23447f85d09854416b51305536b9ca5890e02204b288acdea4cdc7ab1ffbd9766a7bdf95f5bd02d2917dfb7089dbf29032591b0012102031d4256c4bc9f99ac88bf3dba21773132281f65f9bf23a59928bce08961e2f3ffffffff03809698000000000017a914888e9e1816214c3960eac7b55e35521ca4426b0c870000000000000000166a148b0221f3b977c1c65dddf17c1c28e2bbced9e7b4fada9526000000001976a91405aab5342166f8594baf17a7d9bef5d56744332788ac7f68e05f000000000000000000000000000000","tx_hash":"44fa493757df5fdca823bbac05a8b8feb5862d799d4947fd544abcd129feceea"}}},{"timestamp":1608542348271,"event":{"type":"TakerPaymentSpent","data":{"transaction":{"tx_hex":"0400008085202f8901eacefe29d1bc4a54fd47499d792d86b5feb8a805acbb23a8dc5fdf573749fa4400000000d74730440220508c853cc4f1fcb9e6aa00e704eef99adaee9a4ea63a1fd6393bb7ff18da02c802200396bb5d52157bd77ff26ac521ed75aca388d3ec1e5e3ebb7b3aed73c3d33ec50120df871242dcbcc4fe9ed4d3413e21b2f8ce606a3ee7128c9b2d2e31fcedc1848e004c6b6304ee86e05fb1752102031d4256c4bc9f99ac88bf3dba21773132281f65f9bf23a59928bce08961e2f3ac6782012088a9148b0221f3b977c1c65dddf17c1c28e2bbced9e7b4882103c6a78589e18b482aea046975e6d0acbdea7bf7dbf04d9d5bd67fda917815e3edac68ffffffff0198929800000000001976a9146d9d2b554d768232320587df75c4338ecc8bf37d88ac725ae05f000000000000000000000000000000","tx_hash":"9376dde62249802a0aba8259f51def9bb2e509af85a5ec7df04b479a9da28a29"},"secret":"df871242dcbcc4fe9ed4d3413e21b2f8ce606a3ee7128c9b2d2e31fcedc1848e"}}},{"timestamp":1608542349372,"event":{"type":"MakerPaymentSpent","data":{"tx_hex":"f90107821fb980830249f094a09ad3cd7e96586ebd05a2607ee56b56fb2db8fd80b8a402ed292b50aebafeaf827c62c2eed09e265fa5aa9e013c0f27f0a88259f1aaa1279f0c32000000000000000000000000000000000000000000000000016345785d8a0000df871242dcbcc4fe9ed4d3413e21b2f8ce606a3ee7128c9b2d2e31fcedc1848e00000000000000000000000000000000000000000000000000000000000000000000000000000000000000004b2d0d6c2c785217457b69b922a2a9cea98f71e91ca0ed6a4942a78c7ae6eb3c9dec496459a9ef68b34cb389acd939d13d3ecaf7e4aca021bb77e80fc60acf25a7a01cc1272b1b76594a521fb1abe1322d650e58a672c2","tx_hash":"c2d206e665aee159a5ab9aff60f76444e97bdad8f9152eccb6ca07d9204974ca"}}},{"timestamp":1608542349373,"event":{"type":"Finished"}}],"maker_amount":"0.1","maker_coin":"ETH","taker_amount":"0.1","taker_coin":"RICK","gui":"nogui","mm_version":"1a6082121","success_events":["Started","Negotiated","TakerFeeSent","MakerPaymentReceived","MakerPaymentWaitConfirmStarted","MakerPaymentValidatedAndConfirmed","TakerPaymentSent","TakerPaymentSpent","MakerPaymentSpent","Finished"],"error_events":["StartFailed","NegotiateFailed","TakerFeeSendFailed","MakerPaymentValidateFailed","MakerPaymentWaitConfirmFailed","TakerPaymentTransactionFailed","TakerPaymentWaitConfirmFailed","TakerPaymentDataSendFailed","TakerPaymentWaitForSpendFailed","MakerPaymentSpendFailed","TakerPaymentWaitRefundStarted","TakerPaymentRefunded","TakerPaymentRefundFailed"]}"#;
        let taker_saved_swap: TakerSavedSwap = json::from_str(taker_saved_json).unwrap();
        let key_pair =
            key_pair_from_seed("spice describe gravity federal blast come thank unfair canal monkey style afraid")
                .unwrap();
        let ctx = MmCtxBuilder::default().with_secp256k1_key_pair(key_pair).into_mm_arc();

        TestCoin::ticker.mock_safe(|_| MockResult::Return("ticker"));
        static mut SWAP_CONTRACT_ADDRESS_CALLED: usize = 0;
        TestCoin::swap_contract_address.mock_safe(|_| {
            unsafe { SWAP_CONTRACT_ADDRESS_CALLED += 1 };
            MockResult::Return(Some(BytesJson::default()))
        });
        let maker_coin = MmCoinEnum::Test(TestCoin::default());
        let taker_coin = MmCoinEnum::Test(TestCoin::default());
        let (taker_swap, _) =
            TakerSwap::load_from_saved(ctx.clone(), maker_coin, taker_coin, taker_saved_swap).unwrap();

        assert_eq!(unsafe { SWAP_CONTRACT_ADDRESS_CALLED }, 1);
        let expected_addr = addr_from_str("0xa09ad3cd7e96586ebd05a2607ee56b56fb2db8fd").unwrap();
        let expected = BytesJson::from(expected_addr.0.as_ref());
        assert_eq!(taker_swap.r().data.maker_coin_swap_contract_address, Some(expected));
        assert_eq!(
            taker_swap.r().data.taker_coin_swap_contract_address,
            Some(BytesJson::default())
        );
    }

    #[test]
    // https://github.com/KomodoPlatform/atomicDEX-API/issues/647
    fn test_recoverable() {
        // Swap ended with MakerPaymentWaitConfirmFailed event.
        // MM2 did not attempt to send the payment in this case so swap is not recoverable.
        let swap: TakerSavedSwap = json::from_str(r#"{"error_events":["StartFailed","NegotiateFailed","TakerFeeSendFailed","MakerPaymentValidateFailed","MakerPaymentWaitConfirmFailed","TakerPaymentTransactionFailed","TakerPaymentWaitConfirmFailed","TakerPaymentDataSendFailed","TakerPaymentWaitForSpendFailed","MakerPaymentSpendFailed","TakerPaymentWaitRefundStarted","TakerPaymentRefunded","TakerPaymentRefundFailed"],"events":[{"event":{"data":{"lock_duration":7800,"maker":"1bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8","maker_amount":"0.12596566232185483","maker_coin":"KMD","maker_coin_start_block":1458035,"maker_payment_confirmations":1,"maker_payment_wait":1564053079,"my_persistent_pub":"0326846707a52a233cfc49a61ef51b1698bbe6aa78fa8b8d411c02743c09688f0a","started_at":1564050479,"taker_amount":"50.000000000000001504212457800000","taker_coin":"DOGE","taker_coin_start_block":2823448,"taker_payment_confirmations":1,"taker_payment_lock":1564058279,"uuid":"41383f43-46a5-478c-9386-3b2cce0aca20"},"type":"Started"},"timestamp":1564050480269},{"event":{"data":{"maker_payment_locktime":1564066080,"maker_pubkey":"031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8","secret_hash":"3669eb83a007a3c507448d79f45a9f06ec2f36a8"},"type":"Negotiated"},"timestamp":1564050540991},{"event":{"data":{"tx_hash":"bdde828b492d6d1cc25cd2322fd592dafd722fcc7d8b0fedce4d3bb4a1a8c8ff","tx_hex":"0100000002c7efa995c8b7be0a8b6c2d526c6c444c1634d65584e9ee89904e9d8675eac88c010000006a473044022051f34d5e3b7d0b9098d5e35333f3550f9cb9e57df83d5e4635b7a8d2986d6d5602200288c98da05de6950e01229a637110a1800ba643e75cfec59d4eb1021ad9b40801210326846707a52a233cfc49a61ef51b1698bbe6aa78fa8b8d411c02743c09688f0affffffffae6c233989efa7c7d2aa6534adc96078917ff395b7f09f734a147b2f44ade164000000006a4730440220393a784c2da74d0e2a28ec4f7df6c8f9d8b2af6ae6957f1e68346d744223a8fd02201b7a96954ac06815a43a6c7668d829ae9cbb5de76fa77189ddfd9e3038df662c01210326846707a52a233cfc49a61ef51b1698bbe6aa78fa8b8d411c02743c09688f0affffffff02115f5800000000001976a914ca1e04745e8ca0c60d8c5881531d51bec470743f88ac41a84641020000001976a914444f0e1099709ba4d742454a7d98a5c9c162ceab88ac6d84395d"},"type":"TakerFeeSent"},"timestamp":1564050545296},{"event":{"data":{"tx_hash":"0a0f11fa82802c2c30862c50ab2162185dae8de7f7235f32c506f814c142b382","tx_hex":"0400008085202f8902ace337db2dd4c56b0697f58fb8cfb6bd1cd6f469d925fc0376d1dcfb7581bf82000000006b483045022100d1f95be235c5c8880f5d703ace287e2768548792c58c5dbd27f5578881b30ea70220030596106e21c7e0057ee0dab283f9a1fe273f15208cba80870c447bd559ef0d0121031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8ffffffff9f339752567c404427fd77f2b35cecdb4c21489edc64e25e729fdb281785e423000000006a47304402203179e95877dbc107123a417f1e648e3ff13d384890f1e4a67b6dd5087235152e0220102a8ab799fadb26b5d89ceb9c7bc721a7e0c2a0d0d7e46bbe0cf3d130010d430121031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8ffffffff025635c0000000000017a91480a95d366d65e34a465ab17b0c9eb1d5a33bae08876cbfce05000000001976a914c3f710deb7320b0efa6edb14e3ebeeb9155fa90d88ac8d7c395d000000000000000000000000000000"},"type":"MakerPaymentReceived"},"timestamp":1564050588176},{"event":{"type":"MakerPaymentWaitConfirmStarted"},"timestamp":1564050588178},{"event":{"data":{"error":"error"},"type":"MakerPaymentWaitConfirmFailed"},"timestamp":1564051092897},{"event":{"type":"Finished"},"timestamp":1564051092900}],"success_events":["Started","Negotiated","TakerFeeSent","MakerPaymentReceived","MakerPaymentWaitConfirmStarted","MakerPaymentValidatedAndConfirmed","TakerPaymentSent","TakerPaymentSpent","MakerPaymentSpent","Finished"],"uuid":"41383f43-46a5-478c-9386-3b2cce0aca20"}"#).unwrap();
        assert!(!swap.is_recoverable());
    }

    #[test]
    fn test_max_taker_vol_from_available() {
        let dex_fee_threshold = MmNumber::from("0.0001");
        let min_tx_amount = MmNumber::from("0.00001");

        // For these `availables` the dex_fee must be greater than threshold
        let source = vec![
            ("0.0779", false),
            ("0.1", false),
            ("0.135", false),
            ("12.000001", false),
            ("999999999999999999999999999999999999999999999999999999", false),
            ("0.0778000000000000000000000000000000000000000000000002", false),
            ("0.0779", false),
            ("0.0778000000000000000000000000000000000000000000000001", false),
            ("0.0863333333333333333333333333333333333333333333333334", true),
            ("0.0863333333333333333333333333333333333333333333333333", true),
        ];
        for (available, is_kmd) in source {
            let available = MmNumber::from(available);
            // no matter base or rel is KMD
            let base = if is_kmd { "RICK" } else { "MORTY" };
            let max_taker_vol = max_taker_vol_from_available(available.clone(), "RICK", "MORTY", &min_tx_amount)
                .expect("!max_taker_vol_from_available");

            let dex_fee = dex_fee_amount(base, "MORTY", &max_taker_vol, &dex_fee_threshold);
            assert!(dex_fee_threshold < dex_fee);
            assert!(min_tx_amount <= max_taker_vol);
            assert_eq!(max_taker_vol + dex_fee, available);
        }

        // for these `availables` the dex_fee must be the same as `threshold`
        let source = vec![
            ("0.0863333333333333333333333333333333333333333333333332", true),
            ("0.0863333333333333333333333333333333333333333333333331", true),
            ("0.0777999999999999999999999999999999999999999999999999", false),
            ("0.0777", false),
            ("0.0002", false),
        ];
        for (available, is_kmd) in source {
            let available = MmNumber::from(available);
            // no matter base or rel is KMD
            let base = if is_kmd { "KMD" } else { "RICK" };
            let max_taker_vol = max_taker_vol_from_available(available.clone(), base, "MORTY", &min_tx_amount)
                .expect("!max_taker_vol_from_available");
            let dex_fee = dex_fee_amount(base, "MORTY", &max_taker_vol, &dex_fee_threshold);
            log!("available "[available.to_decimal()]" max_taker_vol "[max_taker_vol.to_decimal()]", dex_fee "[dex_fee.to_decimal()]);
            assert_eq!(dex_fee_threshold, dex_fee);
            assert!(min_tx_amount <= max_taker_vol);
            assert_eq!(max_taker_vol + dex_fee, available);
        }

        // these `availables` must return an error
        let availables = vec![
            "0.0001999",
            "0.00011",
            "0.0001000000000000000000000000000000000000000000000001",
            "0.0001",
            "0.0000999999999999999999999999999999999999999999999999",
            "0.0000000000000000000000000000000000000000000000000001",
            "0",
            "-2",
        ];
        for available in availables {
            let available = MmNumber::from(available);
            max_taker_vol_from_available(available.clone(), "KMD", "MORTY", &dex_fee_threshold)
                .expect_err("!max_taker_vol_from_available success but should be error");
        }
    }

    #[test]
    fn locked_amount_should_not_use_paid_from_trading_vol_fee() {
        use crate::mm2::lp_swap::get_locked_amount;

        let taker_saved_json = r#"{
            "type": "Taker",
            "uuid": "af5e0383-97f6-4408-8c03-a8eb8d17e46d",
            "my_order_uuid": "af5e0383-97f6-4408-8c03-a8eb8d17e46d",
            "events": [
                {
                    "timestamp": 1617096259172,
                    "event": {
                        "type": "Started",
                        "data": {
                            "taker_coin": "MORTY",
                            "maker_coin": "RICK",
                            "maker": "15d9c51c657ab1be4ae9d3ab6e76a619d3bccfe830d5363fa168424c0d044732",
                            "my_persistent_pub": "03ad6f89abc2e5beaa8a3ac28e22170659b3209fe2ddf439681b4b8f31508c36fa",
                            "lock_duration": 7800,
                            "maker_amount": "0.1",
                            "taker_amount": "0.11",
                            "maker_payment_confirmations": 1,
                            "maker_payment_requires_nota": false,
                            "taker_payment_confirmations": 1,
                            "taker_payment_requires_nota": false,
                            "taker_payment_lock": 1617104058,
                            "uuid": "af5e0383-97f6-4408-8c03-a8eb8d17e46d",
                            "started_at": 1617096258,
                            "maker_payment_wait": 1617099378,
                            "maker_coin_start_block": 865240,
                            "taker_coin_start_block": 869167,
                            "fee_to_send_taker_fee": {
                                "coin": "MORTY",
                                "amount": "0.00001",
                                "paid_from_trading_vol": false
                            },
                            "taker_payment_trade_fee": {
                                "coin": "MORTY",
                                "amount": "0.00001",
                                "paid_from_trading_vol": false
                            },
                            "maker_payment_spend_trade_fee": {
                                "coin": "RICK",
                                "amount": "0.00001",
                                "paid_from_trading_vol": true
                            }
                        }
                    }
                }
            ],
            "maker_amount": "0.1",
            "maker_coin": "RICK",
            "taker_amount": "0.11",
            "taker_coin": "MORTY",
            "gui": null,
            "mm_version": "21867da64",
            "success_events": [],
            "error_events": []
        }"#;
        let taker_saved_swap: TakerSavedSwap = json::from_str(taker_saved_json).unwrap();
        let key_pair =
            key_pair_from_seed("spice describe gravity federal blast come thank unfair canal monkey style afraid")
                .unwrap();
        let ctx = MmCtxBuilder::default().with_secp256k1_key_pair(key_pair).into_mm_arc();

        let maker_coin = MmCoinEnum::Test(TestCoin::new("RICK"));
        let taker_coin = MmCoinEnum::Test(TestCoin::new("MORTY"));

        TestCoin::swap_contract_address.mock_safe(|_| MockResult::Return(None));
        TestCoin::min_tx_amount.mock_safe(|_| MockResult::Return(BigDecimal::from(0)));

        let (swap, _) = TakerSwap::load_from_saved(ctx.clone(), maker_coin, taker_coin, taker_saved_swap).unwrap();
        let swaps_ctx = SwapsContext::from_ctx(&ctx).unwrap();
        let arc = Arc::new(swap);
        let weak_ref = Arc::downgrade(&arc);
        swaps_ctx.running_swaps.lock().unwrap().push(weak_ref);

        let actual = get_locked_amount(&ctx, "RICK");
        assert_eq!(actual, MmNumber::from(0));

        let actual = get_locked_amount_by_other_swaps(&ctx, &new_uuid(), "RICK");
        assert_eq!(actual, MmNumber::from(0));
    }
}
