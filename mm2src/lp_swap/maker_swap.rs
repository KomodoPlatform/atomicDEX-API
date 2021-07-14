use super::check_balance::{check_base_coin_balance_for_swap, check_my_coin_balance_for_swap, CheckBalanceError,
                           CheckBalanceResult};
use super::pubkey_banning::ban_pubkey_on_failed_swap;
use super::swap_lock::{SwapLock, SwapLockOps};
use super::trade_preimage::{TradePreimageRequest, TradePreimageRpcError, TradePreimageRpcResult};
use super::{broadcast_my_swap_status, broadcast_swap_message_every, check_other_coin_balance_for_swap,
            dex_fee_amount_from_taker_coin, get_locked_amount, my_swap_file_path, recv_swap_msg, swap_topic,
            AtomicSwap, LockedAmount, MySwapInfo, NegotiationDataMsg, NegotiationDataV2, RecoveredSwap,
            RecoveredSwapAction, SavedSwap, SavedTradeFee, SwapConfirmationsSettings, SwapError, SwapMsg,
            SwapsContext, TransactionIdentifier, WAIT_CONFIRM_INTERVAL};

use crate::mm2::lp_network::subscribe_to_topic;
use crate::mm2::lp_ordermatch::{MakerOrderBuilder, OrderConfirmationsSettings};
use crate::mm2::MM_VERSION;
use atomic::Atomic;
use bigdecimal::BigDecimal;
use bitcrypto::dhash160;
use coins::{CanRefundHtlc, FeeApproxStage, FoundSwapTxSpend, MmCoinEnum, TradeFee, TradePreimageValue, TransactionEnum};
use common::log::{error, warn};
use common::mm_error::prelude::*;
use common::{bits256, executor::Timer, mm_ctx::MmArc, mm_number::MmNumber, now_ms, slurp, write,
             DEX_FEE_ADDR_RAW_PUBKEY};
use futures::{compat::Future01CompatExt, select, FutureExt};
use futures01::Future;
use parking_lot::Mutex as PaMutex;
use primitives::hash::H264;
use rand::Rng;
use rpc::v1::types::{Bytes as BytesJson, H160 as H160Json, H256 as H256Json, H264 as H264Json};
use serde_json as json;
use std::path::PathBuf;
use std::sync::{atomic::Ordering, Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use uuid::Uuid;

pub fn stats_maker_swap_dir(ctx: &MmArc) -> PathBuf { ctx.dbdir().join("SWAPS").join("STATS").join("MAKER") }

pub fn stats_maker_swap_file_path(ctx: &MmArc, uuid: &Uuid) -> PathBuf {
    stats_maker_swap_dir(ctx).join(format!("{}.json", uuid))
}

fn save_my_maker_swap_event(ctx: &MmArc, swap: &MakerSwap, event: MakerSavedEvent) -> Result<(), String> {
    let path = my_swap_file_path(ctx, &swap.uuid);
    let content = try_s!(slurp(&path));
    let swap: SavedSwap = if content.is_empty() {
        SavedSwap::Maker(MakerSavedSwap {
            uuid: swap.uuid,
            my_order_uuid: swap.my_order_uuid,
            maker_amount: Some(swap.maker_amount.clone()),
            maker_coin: Some(swap.maker_coin.ticker().to_owned()),
            taker_amount: Some(swap.taker_amount.clone()),
            taker_coin: Some(swap.taker_coin.ticker().to_owned()),
            gui: ctx.gui().map(|g| g.to_owned()),
            mm_version: Some(MM_VERSION.to_owned()),
            events: vec![],
            success_events: vec![
                "Started".into(),
                "Negotiated".into(),
                "TakerFeeValidated".into(),
                "MakerPaymentSent".into(),
                "TakerPaymentReceived".into(),
                "TakerPaymentWaitConfirmStarted".into(),
                "TakerPaymentValidatedAndConfirmed".into(),
                "TakerPaymentSpent".into(),
                "TakerPaymentSpendConfirmStarted".into(),
                "TakerPaymentSpendConfirmed".into(),
                "Finished".into(),
            ],
            error_events: vec![
                "StartFailed".into(),
                "NegotiateFailed".into(),
                "TakerFeeValidateFailed".into(),
                "MakerPaymentTransactionFailed".into(),
                "MakerPaymentDataSendFailed".into(),
                "MakerPaymentWaitConfirmFailed".into(),
                "TakerPaymentValidateFailed".into(),
                "TakerPaymentWaitConfirmFailed".into(),
                "TakerPaymentSpendFailed".into(),
                "TakerPaymentSpendConfirmFailed".into(),
                "MakerPaymentWaitRefundStarted".into(),
                "MakerPaymentRefunded".into(),
                "MakerPaymentRefundFailed".into(),
            ],
        })
    } else {
        try_s!(json::from_slice(&content))
    };

    if let SavedSwap::Maker(mut maker_swap) = swap {
        maker_swap.events.push(event);
        let new_swap = SavedSwap::Maker(maker_swap);
        let new_content = try_s!(json::to_vec(&new_swap));
        try_s!(write(&path, &new_content));
        Ok(())
    } else {
        ERR!("Expected SavedSwap::Maker at {}, got {:?}", path.display(), swap)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TakerNegotiationData {
    pub taker_payment_locktime: u64,
    pub taker_pubkey: H264Json,
    pub maker_coin_swap_contract_addr: Option<BytesJson>,
    pub taker_coin_swap_contract_addr: Option<BytesJson>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
pub struct MakerSwapData {
    pub taker_coin: String,
    pub maker_coin: String,
    taker: H256Json,
    secret: H256Json,
    secret_hash: Option<H160Json>,
    my_persistent_pub: H264Json,
    lock_duration: u64,
    pub maker_amount: BigDecimal,
    pub taker_amount: BigDecimal,
    maker_payment_confirmations: u64,
    maker_payment_requires_nota: Option<bool>,
    taker_payment_confirmations: u64,
    taker_payment_requires_nota: Option<bool>,
    maker_payment_lock: u64,
    /// Allows to recognize one SWAP from the other in the logs. #274.
    uuid: Uuid,
    pub started_at: u64,
    maker_coin_start_block: u64,
    taker_coin_start_block: u64,
    /// A `MakerPayment` transaction fee.
    /// Note this value is used to calculate locked amount only.
    maker_payment_trade_fee: Option<SavedTradeFee>,
    /// A transaction fee that should be paid to spend a `TakerPayment`.
    /// Note this value is used to calculate locked amount only.
    taker_payment_spend_trade_fee: Option<SavedTradeFee>,
    #[serde(skip_serializing_if = "Option::is_none")]
    maker_coin_swap_contract_address: Option<BytesJson>,
    #[serde(skip_serializing_if = "Option::is_none")]
    taker_coin_swap_contract_address: Option<BytesJson>,
}

pub struct MakerSwapMut {
    data: MakerSwapData,
    other_persistent_pub: H264,
    #[allow(dead_code)]
    taker_fee: Option<TransactionIdentifier>,
    maker_payment: Option<TransactionIdentifier>,
    taker_payment: Option<TransactionIdentifier>,
    taker_payment_spend: Option<TransactionIdentifier>,
    maker_payment_refund: Option<TransactionIdentifier>,
}

pub struct MakerSwap {
    ctx: MmArc,
    maker_coin: MmCoinEnum,
    taker_coin: MmCoinEnum,
    maker_amount: BigDecimal,
    taker_amount: BigDecimal,
    my_persistent_pub: H264,
    taker: bits256,
    uuid: Uuid,
    my_order_uuid: Option<Uuid>,
    taker_payment_lock: Atomic<u64>,
    taker_payment_confirmed: Atomic<bool>,
    errors: PaMutex<Vec<SwapError>>,
    finished_at: Atomic<u64>,
    mutable: RwLock<MakerSwapMut>,
    conf_settings: SwapConfirmationsSettings,
    payment_locktime: u64,
}

impl MakerSwap {
    fn w(&self) -> RwLockWriteGuard<MakerSwapMut> { self.mutable.write().unwrap() }
    fn r(&self) -> RwLockReadGuard<MakerSwapMut> { self.mutable.read().unwrap() }

    fn generate_secret(&self) -> [u8; 32] {
        let mut rng = rand::thread_rng();
        rng.gen()
    }

    fn wait_refund_until(&self) -> u64 { self.r().data.maker_payment_lock + 3700 }

    fn apply_event(&self, event: MakerSwapEvent) {
        match event {
            MakerSwapEvent::Started(data) => self.w().data = data,
            MakerSwapEvent::StartFailed(err) => self.errors.lock().push(err),
            MakerSwapEvent::Negotiated(data) => {
                self.taker_payment_lock
                    .store(data.taker_payment_locktime, Ordering::Relaxed);
                self.w().other_persistent_pub = data.taker_pubkey.into();
                if data.maker_coin_swap_contract_addr.is_some() {
                    self.w().data.maker_coin_swap_contract_address = data.maker_coin_swap_contract_addr;
                }

                if data.taker_coin_swap_contract_addr.is_some() {
                    self.w().data.taker_coin_swap_contract_address = data.taker_coin_swap_contract_addr;
                }
            },
            MakerSwapEvent::NegotiateFailed(err) => self.errors.lock().push(err),
            MakerSwapEvent::TakerFeeValidated(tx) => self.w().taker_fee = Some(tx),
            MakerSwapEvent::TakerFeeValidateFailed(err) => self.errors.lock().push(err),
            MakerSwapEvent::MakerPaymentSent(tx) => self.w().maker_payment = Some(tx),
            MakerSwapEvent::MakerPaymentTransactionFailed(err) => self.errors.lock().push(err),
            MakerSwapEvent::MakerPaymentDataSendFailed(err) => self.errors.lock().push(err),
            MakerSwapEvent::MakerPaymentWaitConfirmFailed(err) => self.errors.lock().push(err),
            MakerSwapEvent::TakerPaymentReceived(tx) => self.w().taker_payment = Some(tx),
            MakerSwapEvent::TakerPaymentWaitConfirmStarted => (),
            MakerSwapEvent::TakerPaymentValidatedAndConfirmed => {
                self.taker_payment_confirmed.store(true, Ordering::Relaxed)
            },
            MakerSwapEvent::TakerPaymentValidateFailed(err) => self.errors.lock().push(err),
            MakerSwapEvent::TakerPaymentWaitConfirmFailed(err) => self.errors.lock().push(err),
            MakerSwapEvent::TakerPaymentSpent(tx) => self.w().taker_payment_spend = Some(tx),
            MakerSwapEvent::TakerPaymentSpendFailed(err) => self.errors.lock().push(err),
            MakerSwapEvent::TakerPaymentSpendConfirmStarted => (),
            MakerSwapEvent::TakerPaymentSpendConfirmed => (),
            MakerSwapEvent::TakerPaymentSpendConfirmFailed(err) => self.errors.lock().push(err),
            MakerSwapEvent::MakerPaymentWaitRefundStarted { .. } => (),
            MakerSwapEvent::MakerPaymentRefunded(tx) => self.w().maker_payment_refund = Some(tx),
            MakerSwapEvent::MakerPaymentRefundFailed(err) => self.errors.lock().push(err),
            MakerSwapEvent::Finished => self.finished_at.store(now_ms() / 1000, Ordering::Relaxed),
        }
    }

    async fn handle_command(
        &self,
        command: MakerSwapCommand,
    ) -> Result<(Option<MakerSwapCommand>, Vec<MakerSwapEvent>), String> {
        match command {
            MakerSwapCommand::Start => self.start().await,
            MakerSwapCommand::Negotiate => self.negotiate().await,
            MakerSwapCommand::WaitForTakerFee => self.wait_taker_fee().await,
            MakerSwapCommand::SendPayment => self.maker_payment().await,
            MakerSwapCommand::WaitForTakerPayment => self.wait_for_taker_payment().await,
            MakerSwapCommand::ValidateTakerPayment => self.validate_taker_payment().await,
            MakerSwapCommand::SpendTakerPayment => self.spend_taker_payment().await,
            MakerSwapCommand::ConfirmTakerPaymentSpend => self.confirm_taker_payment_spend().await,
            MakerSwapCommand::RefundMakerPayment => self.refund_maker_payment().await,
            MakerSwapCommand::Finish => Ok((None, vec![MakerSwapEvent::Finished])),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ctx: MmArc,
        taker: bits256,
        maker_amount: BigDecimal,
        taker_amount: BigDecimal,
        my_persistent_pub: H264,
        uuid: Uuid,
        my_order_uuid: Option<Uuid>,
        conf_settings: SwapConfirmationsSettings,
        maker_coin: MmCoinEnum,
        taker_coin: MmCoinEnum,
        payment_locktime: u64,
    ) -> Self {
        MakerSwap {
            ctx,
            maker_coin,
            taker_coin,
            maker_amount,
            taker_amount,
            my_persistent_pub,
            taker,
            uuid,
            my_order_uuid,
            taker_payment_lock: Atomic::new(0),
            errors: PaMutex::new(Vec::new()),
            finished_at: Atomic::new(0),
            taker_payment_confirmed: Atomic::new(false),
            conf_settings,
            payment_locktime,
            mutable: RwLock::new(MakerSwapMut {
                data: MakerSwapData::default(),
                other_persistent_pub: H264::default(),
                taker_fee: None,
                maker_payment: None,
                taker_payment: None,
                taker_payment_spend: None,
                maker_payment_refund: None,
            }),
        }
    }

    async fn start(&self) -> Result<(Option<MakerSwapCommand>, Vec<MakerSwapEvent>), String> {
        // do not use self.r().data here as it is not initialized at this step yet
        let preimage_value = TradePreimageValue::Exact(self.maker_amount.clone());
        let stage = FeeApproxStage::StartSwap;
        let get_sender_trade_fee_fut = self.maker_coin.get_sender_trade_fee(preimage_value, stage.clone());
        let maker_payment_trade_fee = match get_sender_trade_fee_fut.compat().await {
            Ok(fee) => fee,
            Err(e) => {
                return Ok((Some(MakerSwapCommand::Finish), vec![MakerSwapEvent::StartFailed(
                    ERRL!("!maker_coin.get_sender_trade_fee {}", e).into(),
                )]))
            },
        };
        let taker_payment_spend_trade_fee_fut = self.taker_coin.get_receiver_trade_fee(stage.clone());
        let taker_payment_spend_trade_fee = match taker_payment_spend_trade_fee_fut.compat().await {
            Ok(fee) => fee,
            Err(e) => {
                return Ok((Some(MakerSwapCommand::Finish), vec![MakerSwapEvent::StartFailed(
                    ERRL!("!taker_coin.get_receiver_trade_fee {}", e).into(),
                )]))
            },
        };

        let params = MakerSwapPreparedParams {
            maker_payment_trade_fee: maker_payment_trade_fee.clone(),
            taker_payment_spend_trade_fee: taker_payment_spend_trade_fee.clone(),
        };
        match check_balance_for_maker_swap(
            &self.ctx,
            &self.maker_coin,
            &self.taker_coin,
            self.maker_amount.clone().into(),
            Some(&self.uuid),
            Some(params),
            stage,
        )
        .await
        {
            Ok(_) => (),
            Err(e) => {
                return Ok((Some(MakerSwapCommand::Finish), vec![MakerSwapEvent::StartFailed(
                    ERRL!("!check_balance_for_maker_swap {}", e).into(),
                )]))
            },
        };

        let secret = self.generate_secret();
        let started_at = now_ms() / 1000;
        let maker_coin_start_block = match self.maker_coin.current_block().compat().await {
            Ok(b) => b,
            Err(e) => {
                return Ok((Some(MakerSwapCommand::Finish), vec![MakerSwapEvent::StartFailed(
                    ERRL!("!maker_coin.current_block {}", e).into(),
                )]))
            },
        };

        let taker_coin_start_block = match self.taker_coin.current_block().compat().await {
            Ok(b) => b,
            Err(e) => {
                return Ok((Some(MakerSwapCommand::Finish), vec![MakerSwapEvent::StartFailed(
                    ERRL!("!taker_coin.current_block {}", e).into(),
                )]))
            },
        };

        let maker_coin_swap_contract_address = self.maker_coin.swap_contract_address();
        let taker_coin_swap_contract_address = self.taker_coin.swap_contract_address();

        let data = MakerSwapData {
            taker_coin: self.taker_coin.ticker().to_owned(),
            maker_coin: self.maker_coin.ticker().to_owned(),
            taker: self.taker.bytes.into(),
            secret_hash: Some(dhash160(&secret).into()),
            secret: secret.into(),
            started_at,
            lock_duration: self.payment_locktime,
            maker_amount: self.maker_amount.clone(),
            taker_amount: self.taker_amount.clone(),
            maker_payment_confirmations: self.conf_settings.maker_coin_confs,
            maker_payment_requires_nota: Some(self.conf_settings.maker_coin_nota),
            taker_payment_confirmations: self.conf_settings.taker_coin_confs,
            taker_payment_requires_nota: Some(self.conf_settings.taker_coin_nota),
            maker_payment_lock: started_at + self.payment_locktime * 2,
            my_persistent_pub: self.my_persistent_pub.clone().into(),
            uuid: self.uuid,
            maker_coin_start_block,
            taker_coin_start_block,
            maker_payment_trade_fee: Some(SavedTradeFee::from(maker_payment_trade_fee)),
            taker_payment_spend_trade_fee: Some(SavedTradeFee::from(taker_payment_spend_trade_fee)),
            maker_coin_swap_contract_address,
            taker_coin_swap_contract_address,
        };

        Ok((Some(MakerSwapCommand::Negotiate), vec![MakerSwapEvent::Started(data)]))
    }

    async fn negotiate(&self) -> Result<(Option<MakerSwapCommand>, Vec<MakerSwapEvent>), String> {
        let maker_negotiation_data = SwapMsg::Negotiation(NegotiationDataMsg::V2(NegotiationDataV2 {
            started_at: self.r().data.started_at,
            payment_locktime: self.r().data.maker_payment_lock,
            secret_hash: dhash160(&self.r().data.secret.0).take().to_vec(),
            persistent_pubkey: self.my_persistent_pub.to_vec(),
            maker_coin_swap_contract: self.maker_coin.swap_contract_address().map_or(vec![], |addr| addr.0),
            taker_coin_swap_contract: self.taker_coin.swap_contract_address().map_or(vec![], |addr| addr.0),
        }));
        const NEGOTIATION_TIMEOUT: u64 = 90;

        let send_abort_handle = broadcast_swap_message_every(
            self.ctx.clone(),
            swap_topic(&self.uuid),
            maker_negotiation_data,
            NEGOTIATION_TIMEOUT as f64 / 6.,
        );
        let recv_fut = recv_swap_msg(
            self.ctx.clone(),
            |store| store.negotiation_reply.take(),
            &self.uuid,
            NEGOTIATION_TIMEOUT,
        );
        let taker_data = match recv_fut.await {
            Ok(d) => d,
            Err(e) => {
                return Ok((Some(MakerSwapCommand::Finish), vec![MakerSwapEvent::NegotiateFailed(
                    ERRL!("{:?}", e).into(),
                )]))
            },
        };
        drop(send_abort_handle);
        let time_dif = (self.r().data.started_at as i64 - taker_data.started_at() as i64).abs();
        if time_dif > 60 {
            return Ok((Some(MakerSwapCommand::Finish), vec![MakerSwapEvent::NegotiateFailed(
                ERRL!("Started_at time_dif over 60 {}", time_dif).into(),
            )]));
        }

        let expected_lock_time = taker_data.started_at() + self.r().data.lock_duration;
        if taker_data.payment_locktime() != expected_lock_time {
            return Ok((Some(MakerSwapCommand::Finish), vec![MakerSwapEvent::NegotiateFailed(
                ERRL!(
                    "taker_data.payment_locktime {} not equal to expected {}",
                    taker_data.payment_locktime(),
                    expected_lock_time
                )
                .into(),
            )]));
        }

        let maker_coin_swap_contract_addr = match self
            .maker_coin
            .negotiate_swap_contract_addr(taker_data.maker_coin_swap_contract())
        {
            Ok(addr) => addr,
            Err(e) => {
                return Ok((Some(MakerSwapCommand::Finish), vec![MakerSwapEvent::NegotiateFailed(
                    ERRL!("!maker_coin.negotiate_swap_contract_addr {}", e).into(),
                )]))
            },
        };

        let taker_coin_swap_contract_addr = match self
            .taker_coin
            .negotiate_swap_contract_addr(taker_data.taker_coin_swap_contract())
        {
            Ok(addr) => addr,
            Err(e) => {
                return Ok((Some(MakerSwapCommand::Finish), vec![MakerSwapEvent::NegotiateFailed(
                    ERRL!("!taker_coin.negotiate_swap_contract_addr {}", e).into(),
                )]))
            },
        };

        Ok((Some(MakerSwapCommand::WaitForTakerFee), vec![
            MakerSwapEvent::Negotiated(TakerNegotiationData {
                taker_payment_locktime: taker_data.payment_locktime(),
                taker_pubkey: taker_data.persistent_pubkey().into(),
                maker_coin_swap_contract_addr,
                taker_coin_swap_contract_addr,
            }),
        ]))
    }

    async fn wait_taker_fee(&self) -> Result<(Option<MakerSwapCommand>, Vec<MakerSwapEvent>), String> {
        const TAKER_FEE_RECV_TIMEOUT: u64 = 180;
        let negotiated = SwapMsg::Negotiated(true);
        let send_abort_handle = broadcast_swap_message_every(
            self.ctx.clone(),
            swap_topic(&self.uuid),
            negotiated,
            TAKER_FEE_RECV_TIMEOUT as f64 / 6.,
        );

        let recv_fut = recv_swap_msg(
            self.ctx.clone(),
            |store| store.taker_fee.take(),
            &self.uuid,
            TAKER_FEE_RECV_TIMEOUT,
        );
        let payload = match recv_fut.await {
            Ok(d) => d,
            Err(e) => {
                return Ok((Some(MakerSwapCommand::Finish), vec![
                    MakerSwapEvent::TakerFeeValidateFailed(ERRL!("{}", e).into()),
                ]))
            },
        };
        drop(send_abort_handle);
        let taker_fee = match self.taker_coin.tx_enum_from_bytes(&payload) {
            Ok(tx) => tx,
            Err(e) => {
                return Ok((Some(MakerSwapCommand::Finish), vec![
                    MakerSwapEvent::TakerFeeValidateFailed(ERRL!("{}", e).into()),
                ]))
            },
        };

        let hash = taker_fee.tx_hash();
        log!({ "Taker fee tx {:02x}", hash });

        let taker_amount = MmNumber::from(self.taker_amount.clone());
        let fee_amount = dex_fee_amount_from_taker_coin(&self.taker_coin, &self.r().data.maker_coin, &taker_amount);
        let other_pub = self.r().other_persistent_pub.clone();
        let taker_coin_start_block = self.r().data.taker_coin_start_block;

        let mut attempts = 0;
        loop {
            match self
                .taker_coin
                .validate_fee(
                    &taker_fee,
                    &*other_pub,
                    &DEX_FEE_ADDR_RAW_PUBKEY,
                    &fee_amount.clone().into(),
                    taker_coin_start_block,
                )
                .compat()
                .await
            {
                Ok(_) => break,
                Err(err) => {
                    if attempts >= 3 {
                        return Ok((Some(MakerSwapCommand::Finish), vec![
                            MakerSwapEvent::TakerFeeValidateFailed(ERRL!("{}", err).into()),
                        ]));
                    } else {
                        attempts += 1;
                        Timer::sleep(10.).await;
                    }
                },
            };
        }

        let fee_ident = TransactionIdentifier {
            tx_hex: taker_fee.tx_hex().into(),
            tx_hash: hash,
        };

        Ok((Some(MakerSwapCommand::SendPayment), vec![
            MakerSwapEvent::TakerFeeValidated(fee_ident),
        ]))
    }

    async fn maker_payment(&self) -> Result<(Option<MakerSwapCommand>, Vec<MakerSwapEvent>), String> {
        let timeout = self.r().data.started_at + self.r().data.lock_duration / 3;
        let now = now_ms() / 1000;
        if now > timeout {
            return Ok((Some(MakerSwapCommand::Finish), vec![
                MakerSwapEvent::MakerPaymentTransactionFailed(ERRL!("Timeout {} > {}", now, timeout).into()),
            ]));
        }

        let before_check_my_payment = now_ms();
        let transaction_f = self
            .maker_coin
            .check_if_my_payment_sent(
                self.r().data.maker_payment_lock as u32,
                &*self.r().other_persistent_pub,
                &*dhash160(&self.r().data.secret.0),
                self.r().data.maker_coin_start_block,
                &self.r().data.maker_coin_swap_contract_address,
            )
            .compat();
        let transaction = match transaction_f.await {
            Ok(res) => match res {
                Some(tx) => tx,
                None => {
                    let after_check_my_payment = now_ms();
                    log!("check_if_my_payment_sent took "(
                        after_check_my_payment - before_check_my_payment
                    ));

                    let before_send_maker_payment = now_ms();
                    let payment_fut = self.maker_coin.send_maker_payment(
                        self.r().data.maker_payment_lock as u32,
                        &*self.r().other_persistent_pub,
                        &*dhash160(&self.r().data.secret.0),
                        self.maker_amount.clone(),
                        &self.r().data.maker_coin_swap_contract_address,
                    );

                    match payment_fut.compat().await {
                        Ok(t) => {
                            let after_send_maker_payment = now_ms();
                            log!("send_maker_payment took "(
                                after_send_maker_payment - before_send_maker_payment
                            ));
                            t
                        },
                        Err(err) => {
                            return Ok((Some(MakerSwapCommand::Finish), vec![
                                MakerSwapEvent::MakerPaymentTransactionFailed(ERRL!("{}", err).into()),
                            ]))
                        },
                    }
                },
            },
            Err(e) => {
                return Ok((Some(MakerSwapCommand::Finish), vec![
                    MakerSwapEvent::MakerPaymentTransactionFailed(ERRL!("{}", e).into()),
                ]))
            },
        };

        let tx_hash = transaction.tx_hash();
        log!({ "Maker payment tx {:02x}", tx_hash });

        let tx_ident = TransactionIdentifier {
            tx_hex: transaction.tx_hex().into(),
            tx_hash,
        };

        Ok((Some(MakerSwapCommand::WaitForTakerPayment), vec![
            MakerSwapEvent::MakerPaymentSent(tx_ident),
        ]))
    }

    async fn wait_for_taker_payment(&self) -> Result<(Option<MakerSwapCommand>, Vec<MakerSwapEvent>), String> {
        let maker_payment_hex = self.r().maker_payment.as_ref().unwrap().tx_hex.0.clone();
        let msg = SwapMsg::MakerPayment(maker_payment_hex);
        let abort_send_handle = broadcast_swap_message_every(self.ctx.clone(), swap_topic(&self.uuid), msg, 600.);

        let maker_payment_wait_confirm = self.r().data.started_at + (self.r().data.lock_duration * 2) / 5;
        let f = self.maker_coin.wait_for_confirmations(
            &self.r().maker_payment.clone().unwrap().tx_hex,
            self.r().data.maker_payment_confirmations,
            self.r().data.maker_payment_requires_nota.unwrap_or(false),
            maker_payment_wait_confirm,
            WAIT_CONFIRM_INTERVAL,
        );
        if let Err(err) = f.compat().await {
            return Ok((Some(MakerSwapCommand::RefundMakerPayment), vec![
                MakerSwapEvent::MakerPaymentWaitConfirmFailed(
                    ERRL!("!wait for maker payment confirmations: {}", err).into(),
                ),
                MakerSwapEvent::MakerPaymentWaitRefundStarted {
                    wait_until: self.wait_refund_until(),
                },
            ]));
        }

        // wait for 3/5, we need to leave some time space for transaction to be confirmed
        let wait_duration = (self.r().data.lock_duration * 3) / 5;
        let recv_fut = recv_swap_msg(
            self.ctx.clone(),
            |store| store.taker_payment.take(),
            &self.uuid,
            wait_duration,
        );
        let payload = match recv_fut.await {
            Ok(p) => p,
            Err(e) => {
                return Ok((Some(MakerSwapCommand::RefundMakerPayment), vec![
                    MakerSwapEvent::TakerPaymentValidateFailed(e.into()),
                    MakerSwapEvent::MakerPaymentWaitRefundStarted {
                        wait_until: self.wait_refund_until(),
                    },
                ]))
            },
        };
        drop(abort_send_handle);

        let taker_payment = match self.taker_coin.tx_enum_from_bytes(&payload) {
            Ok(tx) => tx,
            Err(err) => {
                return Ok((Some(MakerSwapCommand::RefundMakerPayment), vec![
                    MakerSwapEvent::TakerPaymentValidateFailed(ERRL!("!taker_coin.tx_enum_from_bytes: {}", err).into()),
                    MakerSwapEvent::MakerPaymentWaitRefundStarted {
                        wait_until: self.wait_refund_until(),
                    },
                ]))
            },
        };

        let tx_hash = taker_payment.tx_hash();
        log!({ "Taker payment tx {:02x}", tx_hash });
        let tx_ident = TransactionIdentifier {
            tx_hex: taker_payment.tx_hex().into(),
            tx_hash,
        };

        Ok((Some(MakerSwapCommand::ValidateTakerPayment), vec![
            MakerSwapEvent::TakerPaymentReceived(tx_ident),
            MakerSwapEvent::TakerPaymentWaitConfirmStarted,
        ]))
    }

    async fn validate_taker_payment(&self) -> Result<(Option<MakerSwapCommand>, Vec<MakerSwapEvent>), String> {
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
            return Ok((Some(MakerSwapCommand::RefundMakerPayment), vec![
                MakerSwapEvent::TakerPaymentWaitConfirmFailed(
                    ERRL!("!taker_coin.wait_for_confirmations: {}", err).into(),
                ),
                MakerSwapEvent::MakerPaymentWaitRefundStarted {
                    wait_until: self.wait_refund_until(),
                },
            ]));
        }

        let validated_f = self
            .taker_coin
            .validate_taker_payment(
                &self.r().taker_payment.clone().unwrap().tx_hex,
                self.taker_payment_lock.load(Ordering::Relaxed) as u32,
                &*self.r().other_persistent_pub,
                &*dhash160(&self.r().data.secret.0),
                self.taker_amount.clone(),
                &self.r().data.taker_coin_swap_contract_address,
            )
            .compat();

        if let Err(e) = validated_f.await {
            return Ok((Some(MakerSwapCommand::RefundMakerPayment), vec![
                MakerSwapEvent::TakerPaymentValidateFailed(ERRL!("!taker_coin.validate_taker_payment: {}", e).into()),
                MakerSwapEvent::MakerPaymentWaitRefundStarted {
                    wait_until: self.wait_refund_until(),
                },
            ]));
        }

        Ok((Some(MakerSwapCommand::SpendTakerPayment), vec![
            MakerSwapEvent::TakerPaymentValidatedAndConfirmed,
        ]))
    }

    async fn spend_taker_payment(&self) -> Result<(Option<MakerSwapCommand>, Vec<MakerSwapEvent>), String> {
        let duration = (self.r().data.lock_duration * 4) / 5;
        let timeout = self.r().data.started_at + duration;

        let now = now_ms() / 1000;
        if now > timeout {
            return Ok((Some(MakerSwapCommand::RefundMakerPayment), vec![
                MakerSwapEvent::TakerPaymentSpendFailed(ERRL!("Timeout {} > {}", now, timeout).into()),
                MakerSwapEvent::MakerPaymentWaitRefundStarted {
                    wait_until: self.wait_refund_until(),
                },
            ]));
        }

        let spend_fut = self.taker_coin.send_maker_spends_taker_payment(
            &self.r().taker_payment.clone().unwrap().tx_hex,
            self.taker_payment_lock.load(Ordering::Relaxed) as u32,
            &*self.r().other_persistent_pub,
            &self.r().data.secret.0,
            &self.r().data.taker_coin_swap_contract_address,
        );

        let transaction = match spend_fut.compat().await {
            Ok(t) => t,
            Err(err) => {
                return Ok((Some(MakerSwapCommand::RefundMakerPayment), vec![
                    MakerSwapEvent::TakerPaymentSpendFailed(
                        ERRL!("!taker_coin.send_maker_spends_taker_payment: {}", err).into(),
                    ),
                    MakerSwapEvent::MakerPaymentWaitRefundStarted {
                        wait_until: self.wait_refund_until(),
                    },
                ]))
            },
        };

        let tx_hash = transaction.tx_hash();
        log!({ "Taker payment spend tx {:02x}", tx_hash });
        let tx_ident = TransactionIdentifier {
            tx_hex: transaction.tx_hex().into(),
            tx_hash,
        };

        Ok((Some(MakerSwapCommand::ConfirmTakerPaymentSpend), vec![
            MakerSwapEvent::TakerPaymentSpent(tx_ident),
            MakerSwapEvent::TakerPaymentSpendConfirmStarted,
        ]))
    }

    async fn confirm_taker_payment_spend(&self) -> Result<(Option<MakerSwapCommand>, Vec<MakerSwapEvent>), String> {
        // we should wait for only one confirmation to make sure our spend transaction is not failed
        let confirmations = std::cmp::min(1, self.r().data.taker_payment_confirmations);
        let requires_nota = false;
        let wait_fut = self.taker_coin.wait_for_confirmations(
            &self.r().taker_payment_spend.clone().unwrap().tx_hex,
            confirmations,
            requires_nota,
            self.wait_refund_until(),
            WAIT_CONFIRM_INTERVAL,
        );
        if let Err(err) = wait_fut.compat().await {
            return Ok((Some(MakerSwapCommand::RefundMakerPayment), vec![
                MakerSwapEvent::TakerPaymentSpendConfirmFailed(
                    ERRL!("!wait for taker payment spend confirmations: {}", err).into(),
                ),
                MakerSwapEvent::MakerPaymentWaitRefundStarted {
                    wait_until: self.wait_refund_until(),
                },
            ]));
        }

        Ok((Some(MakerSwapCommand::Finish), vec![
            MakerSwapEvent::TakerPaymentSpendConfirmed,
        ]))
    }

    async fn refund_maker_payment(&self) -> Result<(Option<MakerSwapCommand>, Vec<MakerSwapEvent>), String> {
        let locktime = self.r().data.maker_payment_lock;
        loop {
            match self.maker_coin.can_refund_htlc(locktime).compat().await {
                Ok(CanRefundHtlc::CanRefundNow) => break,
                Ok(CanRefundHtlc::HaveToWait(to_sleep)) => Timer::sleep(to_sleep as f64).await,
                Err(e) => {
                    error!("Error {} on can_refund_htlc, retrying in 30 seconds", e);
                    Timer::sleep(30.).await;
                },
            }
        }

        let spend_fut = self.maker_coin.send_maker_refunds_payment(
            &self.r().maker_payment.clone().unwrap().tx_hex,
            self.r().data.maker_payment_lock as u32,
            &*self.r().other_persistent_pub,
            &*dhash160(&self.r().data.secret.0),
            &self.r().data.maker_coin_swap_contract_address,
        );

        let transaction = match spend_fut.compat().await {
            Ok(t) => t,
            Err(err) => {
                return Ok((Some(MakerSwapCommand::Finish), vec![
                    MakerSwapEvent::MakerPaymentRefundFailed(
                        ERRL!("!maker_coin.send_maker_refunds_payment: {}", err).into(),
                    ),
                ]))
            },
        };
        let tx_hash = transaction.tx_hash();
        log!({ "Maker payment refund tx {:02x}", tx_hash });
        let tx_ident = TransactionIdentifier {
            tx_hex: transaction.tx_hex().into(),
            tx_hash,
        };

        Ok((Some(MakerSwapCommand::Finish), vec![
            MakerSwapEvent::MakerPaymentRefunded(tx_ident),
        ]))
    }

    pub fn load_from_db_by_uuid(
        ctx: MmArc,
        maker_coin: MmCoinEnum,
        taker_coin: MmCoinEnum,
        swap_uuid: &Uuid,
    ) -> Result<(Self, Option<MakerSwapCommand>), String> {
        let path = my_swap_file_path(&ctx, swap_uuid);
        let saved: SavedSwap = try_s!(json::from_slice(&try_s!(slurp(&path))));
        let saved = match saved {
            SavedSwap::Maker(swap) => swap,
            SavedSwap::Taker(_) => return ERR!("Can not load MakerSwap from SavedSwap::Taker uuid: {}", swap_uuid),
        };
        Self::load_from_saved(ctx, maker_coin, taker_coin, saved)
    }

    pub fn load_from_saved(
        ctx: MmArc,
        maker_coin: MmCoinEnum,
        taker_coin: MmCoinEnum,
        mut saved: MakerSavedSwap,
    ) -> Result<(Self, Option<MakerSwapCommand>), String> {
        if saved.events.is_empty() {
            return ERR!("Can't restore swap from empty events set");
        }

        let data = match saved.events[0].event {
            MakerSwapEvent::Started(ref mut data) => data,
            _ => return ERR!("First swap event must be Started"),
        };

        // refresh swap contract addresses if the swap file is out-dated (doesn't contain the fields yet)
        if data.maker_coin_swap_contract_address.is_none() {
            data.maker_coin_swap_contract_address = maker_coin.swap_contract_address();
        }
        if data.taker_coin_swap_contract_address.is_none() {
            data.taker_coin_swap_contract_address = taker_coin.swap_contract_address();
        }

        let mut taker = bits256::from([0; 32]);
        taker.bytes = data.taker.0;
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
        let swap = MakerSwap::new(
            ctx,
            taker,
            data.maker_amount.clone(),
            data.taker_amount.clone(),
            my_persistent_pub,
            saved.uuid,
            saved.my_order_uuid,
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
        fn try_spend_taker_payment(selfi: &MakerSwap, secret_hash: &[u8]) -> Result<TransactionEnum, String> {
            let taker_payment_hex = &selfi
                .r()
                .taker_payment
                .clone()
                .ok_or(ERRL!("No info about taker payment, swap is not recoverable"))?
                .tx_hex;

            let timelock = selfi.taker_payment_lock.load(Ordering::Relaxed) as u32;
            let other_pub = &*selfi.r().other_persistent_pub;
            // check if the taker payment is not spent yet
            match selfi.taker_coin.search_for_swap_tx_spend_other(
                timelock,
                other_pub,
                secret_hash,
                taker_payment_hex,
                selfi.r().data.taker_coin_start_block,
                &selfi.r().data.taker_coin_swap_contract_address,
            ) {
                Ok(Some(FoundSwapTxSpend::Spent(tx))) => {
                    return ERR!(
                        "Taker payment was already spent by {} tx {:02x}",
                        selfi.taker_coin.ticker(),
                        tx.tx_hash()
                    )
                },
                Ok(Some(FoundSwapTxSpend::Refunded(tx))) => {
                    return ERR!(
                        "Taker payment was already refunded by {} tx {:02x}",
                        selfi.taker_coin.ticker(),
                        tx.tx_hash()
                    )
                },
                Err(e) => return ERR!("Error {} when trying to find taker payment spend", e),
                Ok(None) => (), // payment is not spent, continue
            }

            selfi
                .taker_coin
                .send_maker_spends_taker_payment(
                    taker_payment_hex,
                    timelock,
                    other_pub,
                    &selfi.r().data.secret.0,
                    &selfi.r().data.taker_coin_swap_contract_address,
                )
                .wait()
                .map_err(|e| ERRL!("{}", e))
        }

        if self.finished_at.load(Ordering::Relaxed) == 0 {
            return ERR!("Swap must be finished before recover funds attempt");
        }

        if self.r().maker_payment_refund.is_some() {
            return ERR!("Maker payment is refunded, swap is not recoverable");
        }

        let secret_hash = self
            .r()
            .data
            .secret_hash
            .clone()
            .unwrap_or_else(|| dhash160(&self.r().data.secret.0).into());

        let maker_payment = match &self.r().maker_payment {
            Some(tx) => tx.tx_hex.0.clone(),
            None => {
                let maybe_maker_payment = try_s!(self
                    .maker_coin
                    .check_if_my_payment_sent(
                        self.r().data.maker_payment_lock as u32,
                        &*self.r().other_persistent_pub,
                        &secret_hash.0,
                        self.r().data.maker_coin_start_block,
                        &self.r().data.maker_coin_swap_contract_address,
                    )
                    .wait());
                match maybe_maker_payment {
                    Some(tx) => tx.tx_hex(),
                    None => return ERR!("Maker payment transaction was not found"),
                }
            },
        };
        // validate that maker payment is not spent
        match self.maker_coin.search_for_swap_tx_spend_my(
            self.r().data.maker_payment_lock as u32,
            &*self.r().other_persistent_pub,
            &secret_hash.0,
            &maker_payment,
            self.r().data.maker_coin_start_block,
            &self.r().data.maker_coin_swap_contract_address,
        ) {
            Ok(Some(FoundSwapTxSpend::Spent(_))) => {
                log!("Warning: MakerPayment spent, but TakerPayment is not yet. Trying to spend TakerPayment");
                let transaction = try_s!(try_spend_taker_payment(self, &secret_hash.0));
                Ok(RecoveredSwap {
                    action: RecoveredSwapAction::SpentOtherPayment,
                    coin: self.taker_coin.ticker().to_string(),
                    transaction,
                })
            },
            Ok(Some(FoundSwapTxSpend::Refunded(tx))) => ERR!(
                "Maker payment was already refunded by {} tx {:02x}",
                self.maker_coin.ticker(),
                tx.tx_hash()
            ),
            Err(e) => ERR!("Error {} when trying to find maker payment spend", e),
            Ok(None) => {
                // our payment is not spent, try to refund
                log!("Trying to refund MakerPayment");
                if now_ms() / 1000 < self.r().data.maker_payment_lock + 3700 {
                    return ERR!(
                        "Too early to refund, wait until {}",
                        self.r().data.maker_payment_lock + 3700
                    );
                }
                let transaction = try_s!(self
                    .maker_coin
                    .send_maker_refunds_payment(
                        &maker_payment,
                        self.r().data.maker_payment_lock as u32,
                        &*self.r().other_persistent_pub,
                        &secret_hash.0,
                        &self.r().data.maker_coin_swap_contract_address,
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

impl AtomicSwap for MakerSwap {
    fn locked_amount(&self) -> Vec<LockedAmount> {
        let mut result = Vec::new();

        // if maker payment is not sent yet it must be virtually locked
        if self.r().maker_payment.is_none() {
            let trade_fee = self.r().data.maker_payment_trade_fee.clone().map(TradeFee::from);
            result.push(LockedAmount {
                coin: self.maker_coin.ticker().to_owned(),
                amount: self.maker_amount.clone().into(),
                trade_fee,
            });
        }

        // if taker payment is not spent yet the `TakerPaymentSpend` tx fee must be virtually locked
        if self.r().taker_payment_spend.is_none() {
            let trade_fee = self.r().data.taker_payment_spend_trade_fee.clone().map(TradeFee::from);
            result.push(LockedAmount {
                coin: self.taker_coin.ticker().to_owned(),
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

#[derive(Debug)]
pub enum MakerSwapCommand {
    Start,
    Negotiate,
    WaitForTakerFee,
    SendPayment,
    WaitForTakerPayment,
    ValidateTakerPayment,
    SpendTakerPayment,
    ConfirmTakerPaymentSpend,
    RefundMakerPayment,
    Finish,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", content = "data")]
#[allow(clippy::large_enum_variant)]
pub enum MakerSwapEvent {
    Started(MakerSwapData),
    StartFailed(SwapError),
    Negotiated(TakerNegotiationData),
    NegotiateFailed(SwapError),
    TakerFeeValidated(TransactionIdentifier),
    TakerFeeValidateFailed(SwapError),
    MakerPaymentSent(TransactionIdentifier),
    MakerPaymentTransactionFailed(SwapError),
    MakerPaymentDataSendFailed(SwapError),
    MakerPaymentWaitConfirmFailed(SwapError),
    TakerPaymentReceived(TransactionIdentifier),
    TakerPaymentWaitConfirmStarted,
    TakerPaymentValidatedAndConfirmed,
    TakerPaymentValidateFailed(SwapError),
    TakerPaymentWaitConfirmFailed(SwapError),
    TakerPaymentSpent(TransactionIdentifier),
    TakerPaymentSpendFailed(SwapError),
    TakerPaymentSpendConfirmStarted,
    TakerPaymentSpendConfirmed,
    TakerPaymentSpendConfirmFailed(SwapError),
    MakerPaymentWaitRefundStarted { wait_until: u64 },
    MakerPaymentRefunded(TransactionIdentifier),
    MakerPaymentRefundFailed(SwapError),
    Finished,
}

impl MakerSwapEvent {
    fn status_str(&self) -> String {
        match self {
            MakerSwapEvent::Started(_) => "Started...".to_owned(),
            MakerSwapEvent::StartFailed(_) => "Start failed...".to_owned(),
            MakerSwapEvent::Negotiated(_) => "Negotiated...".to_owned(),
            MakerSwapEvent::NegotiateFailed(_) => "Negotiate failed...".to_owned(),
            MakerSwapEvent::TakerFeeValidated(_) => "Taker fee validated...".to_owned(),
            MakerSwapEvent::TakerFeeValidateFailed(_) => "Taker fee validate failed...".to_owned(),
            MakerSwapEvent::MakerPaymentSent(_) => "Maker payment sent...".to_owned(),
            MakerSwapEvent::MakerPaymentTransactionFailed(_) => "Maker payment failed...".to_owned(),
            MakerSwapEvent::MakerPaymentDataSendFailed(_) => "Maker payment failed...".to_owned(),
            MakerSwapEvent::MakerPaymentWaitConfirmFailed(_) => {
                "Maker payment wait for confirmation failed...".to_owned()
            },
            MakerSwapEvent::TakerPaymentReceived(_) => "Taker payment received...".to_owned(),
            MakerSwapEvent::TakerPaymentWaitConfirmStarted => "Taker payment wait confirm started...".to_owned(),
            MakerSwapEvent::TakerPaymentValidatedAndConfirmed => "Taker payment validated and confirmed...".to_owned(),
            MakerSwapEvent::TakerPaymentValidateFailed(_) => "Taker payment validate failed...".to_owned(),
            MakerSwapEvent::TakerPaymentWaitConfirmFailed(_) => {
                "Taker payment wait for confirmation failed...".to_owned()
            },
            MakerSwapEvent::TakerPaymentSpent(_) => "Taker payment spent...".to_owned(),
            MakerSwapEvent::TakerPaymentSpendFailed(_) => "Taker payment spend failed...".to_owned(),
            MakerSwapEvent::TakerPaymentSpendConfirmStarted => "Taker payment send wait confirm started...".to_owned(),
            MakerSwapEvent::TakerPaymentSpendConfirmed => "Taker payment spend confirmed...".to_owned(),
            MakerSwapEvent::TakerPaymentSpendConfirmFailed(_) => "Taker payment spend confirm failed...".to_owned(),
            MakerSwapEvent::MakerPaymentWaitRefundStarted { wait_until } => {
                format!("Maker payment wait refund till {} started...", wait_until)
            },
            MakerSwapEvent::MakerPaymentRefunded(_) => "Maker payment refunded...".to_owned(),
            MakerSwapEvent::MakerPaymentRefundFailed(_) => "Maker payment refund failed...".to_owned(),
            MakerSwapEvent::Finished => "Finished".to_owned(),
        }
    }

    fn should_ban_taker(&self) -> bool {
        matches!(
            self,
            MakerSwapEvent::TakerFeeValidateFailed(_) | MakerSwapEvent::TakerPaymentValidateFailed(_)
        )
    }

    fn is_success(&self) -> bool {
        matches!(
            self,
            MakerSwapEvent::Started(_)
                | MakerSwapEvent::Negotiated(_)
                | MakerSwapEvent::TakerFeeValidated(_)
                | MakerSwapEvent::MakerPaymentSent(_)
                | MakerSwapEvent::TakerPaymentReceived(_)
                | MakerSwapEvent::TakerPaymentWaitConfirmStarted
                | MakerSwapEvent::TakerPaymentValidatedAndConfirmed
                | MakerSwapEvent::TakerPaymentSpent(_)
                | MakerSwapEvent::TakerPaymentSpendConfirmStarted
                | MakerSwapEvent::TakerPaymentSpendConfirmed
                | MakerSwapEvent::Finished
        )
    }

    fn is_error(&self) -> bool { !self.is_success() }
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
struct MakerSavedEvent {
    timestamp: u64,
    event: MakerSwapEvent,
}

impl MakerSavedEvent {
    /// next command that must be executed after swap is restored
    fn get_command(&self) -> Option<MakerSwapCommand> {
        match self.event {
            MakerSwapEvent::Started(_) => Some(MakerSwapCommand::Negotiate),
            MakerSwapEvent::StartFailed(_) => Some(MakerSwapCommand::Finish),
            MakerSwapEvent::Negotiated(_) => Some(MakerSwapCommand::WaitForTakerFee),
            MakerSwapEvent::NegotiateFailed(_) => Some(MakerSwapCommand::Finish),
            MakerSwapEvent::TakerFeeValidated(_) => Some(MakerSwapCommand::SendPayment),
            MakerSwapEvent::TakerFeeValidateFailed(_) => Some(MakerSwapCommand::Finish),
            MakerSwapEvent::MakerPaymentSent(_) => Some(MakerSwapCommand::WaitForTakerPayment),
            MakerSwapEvent::MakerPaymentTransactionFailed(_) => Some(MakerSwapCommand::Finish),
            MakerSwapEvent::MakerPaymentDataSendFailed(_) => Some(MakerSwapCommand::RefundMakerPayment),
            MakerSwapEvent::MakerPaymentWaitConfirmFailed(_) => Some(MakerSwapCommand::RefundMakerPayment),
            MakerSwapEvent::TakerPaymentReceived(_) => Some(MakerSwapCommand::ValidateTakerPayment),
            MakerSwapEvent::TakerPaymentWaitConfirmStarted => Some(MakerSwapCommand::ValidateTakerPayment),
            MakerSwapEvent::TakerPaymentValidatedAndConfirmed => Some(MakerSwapCommand::SpendTakerPayment),
            MakerSwapEvent::TakerPaymentValidateFailed(_) => Some(MakerSwapCommand::RefundMakerPayment),
            MakerSwapEvent::TakerPaymentWaitConfirmFailed(_) => Some(MakerSwapCommand::RefundMakerPayment),
            MakerSwapEvent::TakerPaymentSpent(_) => Some(MakerSwapCommand::ConfirmTakerPaymentSpend),
            MakerSwapEvent::TakerPaymentSpendFailed(_) => Some(MakerSwapCommand::RefundMakerPayment),
            MakerSwapEvent::TakerPaymentSpendConfirmStarted => Some(MakerSwapCommand::ConfirmTakerPaymentSpend),
            MakerSwapEvent::TakerPaymentSpendConfirmed => Some(MakerSwapCommand::Finish),
            MakerSwapEvent::TakerPaymentSpendConfirmFailed(_) => Some(MakerSwapCommand::RefundMakerPayment),
            MakerSwapEvent::MakerPaymentWaitRefundStarted { .. } => Some(MakerSwapCommand::RefundMakerPayment),
            MakerSwapEvent::MakerPaymentRefunded(_) => Some(MakerSwapCommand::Finish),
            MakerSwapEvent::MakerPaymentRefundFailed(_) => Some(MakerSwapCommand::Finish),
            MakerSwapEvent::Finished => None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MakerSavedSwap {
    pub uuid: Uuid,
    my_order_uuid: Option<Uuid>,
    events: Vec<MakerSavedEvent>,
    maker_amount: Option<BigDecimal>,
    maker_coin: Option<String>,
    taker_amount: Option<BigDecimal>,
    taker_coin: Option<String>,
    gui: Option<String>,
    mm_version: Option<String>,
    success_events: Vec<String>,
    error_events: Vec<String>,
}

impl MakerSavedSwap {
    pub fn maker_coin(&self) -> Result<String, String> {
        match self.events.first() {
            Some(event) => match &event.event {
                MakerSwapEvent::Started(data) => Ok(data.maker_coin.clone()),
                _ => ERR!("First swap event must be Started"),
            },
            None => ERR!("Can't get maker coin, events are empty"),
        }
    }

    pub fn taker_coin(&self) -> Result<String, String> {
        match self.events.first() {
            Some(event) => match &event.event {
                MakerSwapEvent::Started(data) => Ok(data.taker_coin.clone()),
                _ => ERR!("First swap event must be Started"),
            },
            None => ERR!("Can't get maker coin, events are empty"),
        }
    }

    pub fn is_finished(&self) -> bool {
        match self.events.last() {
            Some(event) => event.event == MakerSwapEvent::Finished,
            None => false,
        }
    }

    pub fn get_my_info(&self) -> Option<MySwapInfo> {
        match self.events.first() {
            Some(event) => match &event.event {
                MakerSwapEvent::Started(data) => Some(MySwapInfo {
                    my_coin: data.maker_coin.clone(),
                    other_coin: data.taker_coin.clone(),
                    my_amount: data.maker_amount.clone(),
                    other_amount: data.taker_amount.clone(),
                    started_at: data.started_at,
                }),
                _ => None,
            },
            None => None,
        }
    }

    pub fn hide_secret(&mut self) {
        if let Some(ref mut event) = self.events.first_mut() {
            if let MakerSwapEvent::Started(ref mut data) = event.event {
                data.secret = H256Json::default();
            }
        }
    }

    pub fn is_recoverable(&self) -> bool {
        if !self.is_finished() {
            return false;
        };
        for event in self.events.iter() {
            match event.event {
                MakerSwapEvent::StartFailed(_)
                | MakerSwapEvent::NegotiateFailed(_)
                | MakerSwapEvent::TakerFeeValidateFailed(_)
                | MakerSwapEvent::TakerPaymentSpendConfirmed
                | MakerSwapEvent::MakerPaymentRefunded(_) => {
                    return false;
                },
                _ => (),
            }
        }
        true
    }

    pub fn swap_data(&self) -> Result<&MakerSwapData, String> {
        match self.events.first() {
            Some(event) => match &event.event {
                MakerSwapEvent::Started(data) => Ok(data),
                _ => ERR!("First swap event must be Started"),
            },
            None => ERR!("Can't get swap_data, events are empty"),
        }
    }

    pub fn finished_at(&self) -> Result<u64, String> {
        match self.events.last() {
            Some(event) => match &event.event {
                MakerSwapEvent::Finished => Ok(event.timestamp / 1000),
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
pub enum RunMakerSwapInput {
    StartNew(MakerSwap),
    KickStart {
        maker_coin: MmCoinEnum,
        taker_coin: MmCoinEnum,
        swap_uuid: Uuid,
    },
}

impl RunMakerSwapInput {
    fn uuid(&self) -> &Uuid {
        match self {
            RunMakerSwapInput::StartNew(swap) => &swap.uuid,
            RunMakerSwapInput::KickStart { swap_uuid, .. } => &swap_uuid,
        }
    }
}

/// Starts the maker swap and drives it to completion (until None next command received).
/// Panics in case of command or event apply fails, not sure yet how to handle such situations
/// because it's usually means that swap is in invalid state which is possible only if there's developer error.
/// Every produced event is saved to local DB. Swap status is broadcasted to P2P network after completion.
pub async fn run_maker_swap(swap: RunMakerSwapInput, ctx: MmArc) {
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
        };
    };

    let (swap, mut command) = match swap {
        RunMakerSwapInput::StartNew(swap) => (swap, MakerSwapCommand::Start),
        RunMakerSwapInput::KickStart {
            maker_coin,
            taker_coin,
            swap_uuid,
        } => match MakerSwap::load_from_db_by_uuid(ctx, maker_coin, taker_coin, &swap_uuid) {
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
    let uuid_str = swap.uuid.to_string();
    macro_rules! swap_tags {
        () => {
            &[&"swap", &("uuid", uuid_str.as_str())]
        };
    }
    let running_swap = Arc::new(swap);
    let weak_ref = Arc::downgrade(&running_swap);
    let swap_ctx = SwapsContext::from_ctx(&ctx).unwrap();
    swap_ctx.init_msg_store(running_swap.uuid, running_swap.taker);
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
                    let to_save = MakerSavedEvent {
                        timestamp: now_ms(),
                        event: event.clone(),
                    };

                    save_my_maker_swap_event(&ctx, &running_swap, to_save).expect("!save_my_maker_swap_event");
                    if event.should_ban_taker() {
                        ban_pubkey_on_failed_swap(
                            &ctx,
                            running_swap.taker.bytes.into(),
                            &running_swap.uuid,
                            event.clone().into(),
                        )
                    }
                    status.status(swap_tags!(), &event.status_str());
                    running_swap.apply_event(event);
                }
                match res.0 {
                    Some(c) => {
                        command = c;
                    },
                    None => {
                        if let Err(e) = broadcast_my_swap_status(&uuid, &ctx) {
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

pub struct MakerSwapPreparedParams {
    maker_payment_trade_fee: TradeFee,
    taker_payment_spend_trade_fee: TradeFee,
}

pub async fn check_balance_for_maker_swap(
    ctx: &MmArc,
    my_coin: &MmCoinEnum,
    other_coin: &MmCoinEnum,
    volume: MmNumber,
    swap_uuid: Option<&Uuid>,
    prepared_params: Option<MakerSwapPreparedParams>,
    stage: FeeApproxStage,
) -> CheckBalanceResult<()> {
    let (maker_payment_trade_fee, taker_payment_spend_trade_fee) = match prepared_params {
        Some(MakerSwapPreparedParams {
            maker_payment_trade_fee,
            taker_payment_spend_trade_fee,
        }) => (maker_payment_trade_fee, taker_payment_spend_trade_fee),
        None => {
            let preimage_value = TradePreimageValue::Exact(volume.to_decimal());
            let maker_payment_trade_fee = my_coin
                .get_sender_trade_fee(preimage_value, stage.clone())
                .compat()
                .await
                .mm_err(|e| CheckBalanceError::from_trade_preimage_error(e, my_coin.ticker()))?;
            let taker_payment_spend_trade_fee = other_coin
                .get_receiver_trade_fee(stage)
                .compat()
                .await
                .mm_err(|e| CheckBalanceError::from_trade_preimage_error(e, other_coin.ticker()))?;
            (maker_payment_trade_fee, taker_payment_spend_trade_fee)
        },
    };

    check_my_coin_balance_for_swap(ctx, my_coin, swap_uuid, volume, maker_payment_trade_fee, None).await?;
    check_other_coin_balance_for_swap(ctx, other_coin, swap_uuid, taker_payment_spend_trade_fee).await?;
    Ok(())
}

pub struct MakerTradePreimage {
    /// The fee is paid per swap concerning the `base` coin.
    pub base_coin_fee: TradeFee,
    /// The fee is paid per swap concerning the `rel` coin.
    pub rel_coin_fee: TradeFee,
    /// The max available volume that can be traded (in decimal representation). Empty if the `max` argument is missing or false.
    pub volume: Option<MmNumber>,
}

pub async fn maker_swap_trade_preimage(
    ctx: &MmArc,
    req: TradePreimageRequest,
    base_coin: MmCoinEnum,
    rel_coin: MmCoinEnum,
) -> TradePreimageRpcResult<MakerTradePreimage> {
    let base_coin_ticker = base_coin.ticker();
    let rel_coin_ticker = rel_coin.ticker();
    let volume = if req.max {
        let balance = base_coin.my_spendable_balance().compat().await?;
        calc_max_maker_vol(&ctx, &base_coin, &balance, FeeApproxStage::TradePreimage).await?
    } else {
        let threshold = base_coin.min_trading_vol().to_decimal();
        if req.volume.is_zero() {
            return MmError::err(TradePreimageRpcError::VolumeTooLow {
                coin: base_coin_ticker.to_owned(),
                volume: req.volume.to_decimal(),
                threshold,
            });
        }
        req.volume
    };

    let preimage_value = TradePreimageValue::Exact(volume.to_decimal());
    let base_coin_fee = base_coin
        .get_sender_trade_fee(preimage_value, FeeApproxStage::TradePreimage)
        .compat()
        .await
        .mm_err(|e| TradePreimageRpcError::from_trade_preimage_error(e, base_coin_ticker))?;
    let rel_coin_fee = rel_coin
        .get_receiver_trade_fee(FeeApproxStage::TradePreimage)
        .compat()
        .await
        .mm_err(|e| TradePreimageRpcError::from_trade_preimage_error(e, rel_coin_ticker))?;

    if req.max {
        // Note the `calc_max_maker_vol` returns [`CheckBalanceError::NotSufficientBalance`] error if the balance of `base_coin` is not sufficient.
        // So we have to check the balance of the other coin only.
        check_other_coin_balance_for_swap(ctx, &rel_coin, None, rel_coin_fee.clone()).await?
    } else {
        let prepared_params = MakerSwapPreparedParams {
            maker_payment_trade_fee: base_coin_fee.clone(),
            taker_payment_spend_trade_fee: rel_coin_fee.clone(),
        };
        check_balance_for_maker_swap(
            ctx,
            &base_coin,
            &rel_coin,
            volume.clone(),
            None,
            Some(prepared_params),
            FeeApproxStage::TradePreimage,
        )
        .await?
    }

    let conf_settings = OrderConfirmationsSettings {
        base_confs: base_coin.required_confirmations(),
        base_nota: base_coin.requires_notarization(),
        rel_confs: rel_coin.required_confirmations(),
        rel_nota: rel_coin.requires_notarization(),
    };
    let builder = MakerOrderBuilder::new(&base_coin, &rel_coin)
        .with_max_base_vol(volume.clone())
        .with_price(req.price)
        .with_conf_settings(conf_settings);
    // perform an additional validation
    let _order = builder
        .build()
        .map_to_mm(|e| TradePreimageRpcError::from_maker_order_build_error(e, base_coin_ticker, rel_coin_ticker))?;

    let volume = if req.max { Some(volume) } else { None };
    Ok(MakerTradePreimage {
        base_coin_fee,
        rel_coin_fee,
        volume,
    })
}

/// Calculate max Maker volume.
/// Returns [`CheckBalanceError::NotSufficientBalance`] if the balance is not sufficient.
/// Note the function checks base coin balance if the trade fee should be paid in base coin.
pub async fn calc_max_maker_vol(
    ctx: &MmArc,
    coin: &MmCoinEnum,
    balance: &BigDecimal,
    stage: FeeApproxStage,
) -> CheckBalanceResult<MmNumber> {
    let ticker = coin.ticker();
    let locked = get_locked_amount(ctx, ticker);
    let available = &MmNumber::from(balance.clone()) - &locked;
    let mut vol = available.clone();

    let preimage_value = TradePreimageValue::UpperBound(vol.to_decimal());
    let trade_fee = coin
        .get_sender_trade_fee(preimage_value, stage)
        .compat()
        .await
        .mm_err(|e| CheckBalanceError::from_trade_preimage_error(e, ticker))?;

    let mut required_to_pay_fee = MmNumber::from(0);
    if trade_fee.coin == ticker {
        vol = &vol - &trade_fee.amount;
        required_to_pay_fee = trade_fee.amount;
    } else {
        let base_coin_balance = coin.base_coin_balance().compat().await?;
        check_base_coin_balance_for_swap(ctx, &MmNumber::from(base_coin_balance), trade_fee.clone(), None).await?;
    }
    let min_tx_amount = MmNumber::from(coin.min_tx_amount());
    if vol < min_tx_amount {
        let required = min_tx_amount + required_to_pay_fee;
        return MmError::err(CheckBalanceError::NotSufficientBalance {
            coin: ticker.to_owned(),
            available: available.to_decimal(),
            required: required.to_decimal(),
            locked_by_swaps: Some(locked.to_decimal()),
        });
    }
    Ok(vol)
}

#[cfg(test)]
mod maker_swap_tests {
    use super::*;
    use coins::eth::{addr_from_str, signed_eth_tx_from_bytes, SignedEthTx};
    use coins::{MarketCoinOps, MmCoin, SwapOps, TestCoin};
    use common::mm_ctx::MmCtxBuilder;
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
    fn test_recover_funds_maker_swap_payment_errored_but_sent() {
        // the swap ends up with MakerPaymentTransactionFailed error but the transaction is actually
        // sent, need to find it and refund
        // TODO remove TransactionDetails from json
        let maker_saved_json = r#"{"error_events":["StartFailed","NegotiateFailed","TakerFeeValidateFailed","MakerPaymentTransactionFailed","MakerPaymentDataSendFailed","TakerPaymentValidateFailed","TakerPaymentSpendFailed","TakerPaymentSpendConfirmFailed","MakerPaymentRefunded","MakerPaymentRefundFailed"],"events":[{"event":{"data":{"lock_duration":7800,"maker_amount":"3.54932734","maker_coin":"KMD","maker_coin_start_block":1452970,"maker_payment_confirmations":1,"maker_payment_lock":1563759539,"my_persistent_pub":"031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8","secret":"0000000000000000000000000000000000000000000000000000000000000000","started_at":1563743939,"taker":"101ace6b08605b9424b0582b5cce044b70a3c8d8d10cb2965e039b0967ae92b9","taker_amount":"0.02004833998671660000000000","taker_coin":"ETH","taker_coin_start_block":8196380,"taker_payment_confirmations":1,"uuid":"3447b727-fe93-4357-8e5a-8cf2699b7e86"},"type":"Started"},"timestamp":1563743939211},{"event":{"data":{"taker_payment_locktime":1563751737,"taker_pubkey":"03101ace6b08605b9424b0582b5cce044b70a3c8d8d10cb2965e039b0967ae92b9"},"type":"Negotiated"},"timestamp":1563743979835},{"event":{"data":{"tx_hash":"a59203eb2328827de00bed699a29389792906e4f39fdea145eb40dc6b3821bd6","tx_hex":"f8690284ee6b280082520894d8997941dd1346e9231118d5685d866294f59e5b865af3107a4000801ca0743d2b7c9fad65805d882179062012261be328d7628ae12ee08eff8d7657d993a07eecbd051f49d35279416778faa4664962726d516ce65e18755c9b9406a9c2fd"},"type":"TakerFeeValidated"},"timestamp":1563744052878},{"event":{"data":{"error":"lp_swap:1888] eth:654] RPC error: Error { code: ServerError(-32010), message: \"Transaction with the same hash was already imported.\", data: None }"},"type":"MakerPaymentTransactionFailed"},"timestamp":1563744118577},{"event":{"type":"Finished"},"timestamp":1563763243350}],"success_events":["Started","Negotiated","TakerFeeValidated","MakerPaymentSent","TakerPaymentReceived","TakerPaymentWaitConfirmStarted","TakerPaymentValidatedAndConfirmed","TakerPaymentSpent","TakerPaymentSpendConfirmStarted","TakerPaymentSpendConfirmed","TakerPaymentSpendConfirmStarted","TakerPaymentSpendConfirmed","Finished"],"uuid":"3447b727-fe93-4357-8e5a-8cf2699b7e86"}"#;
        let maker_saved_swap: MakerSavedSwap = json::from_str(maker_saved_json).unwrap();
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

        static mut MAKER_REFUND_CALLED: bool = false;
        TestCoin::send_maker_refunds_payment.mock_safe(|_, _, _, _, _, _| {
            unsafe { MAKER_REFUND_CALLED = true };
            MockResult::Return(Box::new(futures01::future::ok(eth_tx_for_test().into())))
        });
        TestCoin::search_for_swap_tx_spend_my.mock_safe(|_, _, _, _, _, _, _| MockResult::Return(Ok(None)));
        let maker_coin = MmCoinEnum::Test(TestCoin::default());
        let taker_coin = MmCoinEnum::Test(TestCoin::default());
        let (maker_swap, _) = MakerSwap::load_from_saved(ctx, maker_coin, taker_coin, maker_saved_swap).unwrap();
        let actual = maker_swap.recover_funds().unwrap();
        let expected = RecoveredSwap {
            action: RecoveredSwapAction::RefundedMyPayment,
            coin: "ticker".to_string(),
            transaction: eth_tx_for_test().into(),
        };
        assert_eq!(expected, actual);
        assert!(unsafe { MY_PAYMENT_SENT_CALLED });
        assert!(unsafe { MAKER_REFUND_CALLED });
    }

    #[test]
    fn test_recover_funds_maker_payment_refund_errored() {
        // the swap ends up with MakerPaymentRefundFailed error
        let maker_saved_json = r#"{"error_events":["StartFailed","NegotiateFailed","TakerFeeValidateFailed","MakerPaymentTransactionFailed","MakerPaymentDataSendFailed","TakerPaymentValidateFailed","TakerPaymentSpendFailed","TakerPaymentSpendConfirmFailed","MakerPaymentRefunded","MakerPaymentRefundFailed"],"events":[{"event":{"data":{"lock_duration":7800,"maker_amount":"0.58610590","maker_coin":"KMD","maker_coin_start_block":1450923,"maker_payment_confirmations":1,"maker_payment_lock":1563636475,"my_persistent_pub":"031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8","secret":"0000000000000000000000000000000000000000000000000000000000000000","started_at":1563620875,"taker":"14a96292bfcd7762ece8eb08ead915da927c2619277363853572f30880d5155e","taker_amount":"0.0077700000552410000000000","taker_coin":"LTC","taker_coin_start_block":1670837,"taker_payment_confirmations":1,"uuid":"9db641f5-4300-4527-9fa6-f1c391d42c35"},"type":"Started"},"timestamp":1563620875062},{"event":{"data":{"taker_payment_locktime":1563628675,"taker_pubkey":"02713015d3fa4d30259e90be5f131beb593bf0131f3af2dcdb304e3322d8d52b91"},"type":"Negotiated"},"timestamp":1563620915497},{"event":{"data":{"tx_hash":"6740136eaaa615d9d231969e3a9599d0fc59e53989237a8d31cd6fc86c160013","tx_hex":"0100000001a2586ea8294cedc55741bef625ba72c646399903391a7f6c604a58c6263135f2000000006b4830450221009c78c8ba4a7accab6b09f9a95da5bc59c81f4fc1e60b288ec3c5462b4d02ef01022056b63be1629cf17751d3cc5ffec51bcb1d7f9396e9ce9ca254d0f34104f7263a012102713015d3fa4d30259e90be5f131beb593bf0131f3af2dcdb304e3322d8d52b91ffffffff0210270000000000001976a914ca1e04745e8ca0c60d8c5881531d51bec470743f88ac78aa1900000000001976a91406ccabfd5f9075ecd5e8d0d31c0e973a54d51e8288ac5bf6325d"},"type":"TakerFeeValidated"},"timestamp":1563620976060},{"event":{"data":{"tx_hash":"d0f6e664cea9d89fe7b5cf8005fdca070d1ab1d05a482aaef95c08cdaecddf0a","tx_hex":"0400008085202f89019f1cbda354342cdf982046b331bbd3791f53b692efc6e4becc36be495b2977d9000000006b483045022100fa9d4557394141f6a8b9bfb8cd594a521fd8bcd1965dbf8bc4e04abc849ac66e0220589f521814c10a7561abfd5e432f7a2ee60d4875fe4604618af3207dae531ac00121031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8ffffffff029e537e030000000017a9145534898009f1467191065f6890b96914b39a1c018791857702000000001976a914c3f710deb7320b0efa6edb14e3ebeeb9155fa90d88ac72ee325d000000000000000000000000000000"},"type":"MakerPaymentSent"},"timestamp":1563620976189},{"event":{"data":{"tx_hash":"1e883eb2f3991e84ba27f53651f89b7dda708678a5b9813d043577f222b9ca30","tx_hex":"01000000011300166cc86fcd318d7a238939e559fcd099953a9e9631d2d915a6aa6e134067010000006a47304402206781d5f2db2ff13d2ec7e266f774ea5630cc2dba4019e18e9716131b8b026051022006ebb33857b6d180f13aa6be2fc532f9734abde9d00ae14757e7d7ba3741c08c012102713015d3fa4d30259e90be5f131beb593bf0131f3af2dcdb304e3322d8d52b91ffffffff0228db0b000000000017a91483818667161bf94adda3964a81a231cbf6f5338187b0480c00000000001976a91406ccabfd5f9075ecd5e8d0d31c0e973a54d51e8288ac7cf7325d"},"type":"TakerPaymentReceived"},"timestamp":1563621268320},{"event":{"type":"TakerPaymentWaitConfirmStarted"},"timestamp":1563621268321},{"event":{"type":"TakerPaymentValidatedAndConfirmed"},"timestamp":1563621778471},{"event":{"data":{"error":"lp_swap:2025] utxo:938] rpc_clients:719] JsonRpcError { request: JsonRpcRequest { jsonrpc: \"2.0\", id: \"9\", method: \"blockchain.transaction.broadcast\", params: [String(\"010000000130cab922f27735043d81b9a5788670da7d9bf85136f527ba841e99f3b23e881e00000000b6473044022058a0c1da6bcf8c1418899ff8475f3ab6dddbff918528451c1fe71c2f7dad176302204c2e0bcf8f9b5f09e02ccfeb9256e9b34fb355ea655a5704a8a3fa920079b91501514c6b63048314335db1752102713015d3fa4d30259e90be5f131beb593bf0131f3af2dcdb304e3322d8d52b91ac6782012088a9147ed38daab6085c1a1e4426e61dc87a3c2c081a958821031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8ac68feffffff0188540a00000000001976a91406ccabfd5f9075ecd5e8d0d31c0e973a54d51e8288ac1c2b335d\")] }, error: Response(Object({\"code\": Number(1), \"message\": String(\"the transaction was rejected by network rules.\\n\\nMissing inputs\\n[010000000130cab922f27735043d81b9a5788670da7d9bf85136f527ba841e99f3b23e881e00000000b6473044022058a0c1da6bcf8c1418899ff8475f3ab6dddbff918528451c1fe71c2f7dad176302204c2e0bcf8f9b5f09e02ccfeb9256e9b34fb355ea655a5704a8a3fa920079b91501514c6b63048314335db1752102713015d3fa4d30259e90be5f131beb593bf0131f3af2dcdb304e3322d8d52b91ac6782012088a9147ed38daab6085c1a1e4426e61dc87a3c2c081a958821031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8ac68feffffff0188540a00000000001976a91406ccabfd5f9075ecd5e8d0d31c0e973a54d51e8288ac1c2b335d]\")})) }"},"type":"TakerPaymentSpendFailed"},"timestamp":1563638060583},{"event":{"data":{"error":"lp_swap:2025] utxo:938] rpc_clients:719] JsonRpcError { request: JsonRpcRequest { jsonrpc: \"2.0\", id: \"9\", method: \"blockchain.transaction.broadcast\", params: [String(\"010000000130cab922f27735043d81b9a5788670da7d9bf85136f527ba841e99f3b23e881e00000000b6473044022058a0c1da6bcf8c1418899ff8475f3ab6dddbff918528451c1fe71c2f7dad176302204c2e0bcf8f9b5f09e02ccfeb9256e9b34fb355ea655a5704a8a3fa920079b91501514c6b63048314335db1752102713015d3fa4d30259e90be5f131beb593bf0131f3af2dcdb304e3322d8d52b91ac6782012088a9147ed38daab6085c1a1e4426e61dc87a3c2c081a958821031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8ac68feffffff0188540a00000000001976a91406ccabfd5f9075ecd5e8d0d31c0e973a54d51e8288ac1c2b335d\")] }, error: Response(Object({\"code\": Number(1), \"message\": String(\"the transaction was rejected by network rules.\\n\\nMissing inputs\\n[010000000130cab922f27735043d81b9a5788670da7d9bf85136f527ba841e99f3b23e881e00000000b6473044022058a0c1da6bcf8c1418899ff8475f3ab6dddbff918528451c1fe71c2f7dad176302204c2e0bcf8f9b5f09e02ccfeb9256e9b34fb355ea655a5704a8a3fa920079b91501514c6b63048314335db1752102713015d3fa4d30259e90be5f131beb593bf0131f3af2dcdb304e3322d8d52b91ac6782012088a9147ed38daab6085c1a1e4426e61dc87a3c2c081a958821031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8ac68feffffff0188540a00000000001976a91406ccabfd5f9075ecd5e8d0d31c0e973a54d51e8288ac1c2b335d]\")})) }"},"type":"MakerPaymentRefundFailed"},"timestamp":1563638060583},{"event":{"type":"Finished"},"timestamp":1563621778483}],"success_events":["Started","Negotiated","TakerFeeValidated","MakerPaymentSent","TakerPaymentReceived","TakerPaymentWaitConfirmStarted","TakerPaymentValidatedAndConfirmed","TakerPaymentSpent","TakerPaymentSpendConfirmStarted","TakerPaymentSpendConfirmed","Finished"],"uuid":"9db641f5-4300-4527-9fa6-f1c391d42c35"}"#;
        let maker_saved_swap: MakerSavedSwap = json::from_str(maker_saved_json).unwrap();
        let key_pair =
            key_pair_from_seed("spice describe gravity federal blast come thank unfair canal monkey style afraid")
                .unwrap();
        let ctx = MmCtxBuilder::default().with_secp256k1_key_pair(key_pair).into_mm_arc();

        TestCoin::ticker.mock_safe(|_| MockResult::Return("ticker"));
        TestCoin::swap_contract_address.mock_safe(|_| MockResult::Return(None));
        static mut MAKER_REFUND_CALLED: bool = false;

        TestCoin::send_maker_refunds_payment.mock_safe(|_, _, _, _, _, _| {
            unsafe { MAKER_REFUND_CALLED = true };
            MockResult::Return(Box::new(futures01::future::ok(eth_tx_for_test().into())))
        });

        TestCoin::search_for_swap_tx_spend_my.mock_safe(|_, _, _, _, _, _, _| MockResult::Return(Ok(None)));
        let maker_coin = MmCoinEnum::Test(TestCoin::default());
        let taker_coin = MmCoinEnum::Test(TestCoin::default());
        let (maker_swap, _) = MakerSwap::load_from_saved(ctx, maker_coin, taker_coin, maker_saved_swap).unwrap();
        let actual = maker_swap.recover_funds().unwrap();
        let expected = RecoveredSwap {
            action: RecoveredSwapAction::RefundedMyPayment,
            coin: "ticker".to_string(),
            transaction: eth_tx_for_test().into(),
        };
        assert_eq!(expected, actual);
        assert!(unsafe { MAKER_REFUND_CALLED });
    }

    #[test]
    fn test_recover_funds_maker_payment_refund_errored_already_refunded() {
        // the swap ends up with MakerPaymentRefundFailed error
        let maker_saved_json = r#"{"error_events":["StartFailed","NegotiateFailed","TakerFeeValidateFailed","MakerPaymentTransactionFailed","MakerPaymentDataSendFailed","TakerPaymentValidateFailed","TakerPaymentSpendFailed","TakerPaymentSpendConfirmFailed","MakerPaymentRefunded","MakerPaymentRefundFailed"],"events":[{"event":{"data":{"lock_duration":7800,"maker_amount":"0.58610590","maker_coin":"KMD","maker_coin_start_block":1450923,"maker_payment_confirmations":1,"maker_payment_lock":1563636475,"my_persistent_pub":"031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8","secret":"0000000000000000000000000000000000000000000000000000000000000000","started_at":1563620875,"taker":"14a96292bfcd7762ece8eb08ead915da927c2619277363853572f30880d5155e","taker_amount":"0.0077700000552410000000000","taker_coin":"LTC","taker_coin_start_block":1670837,"taker_payment_confirmations":1,"uuid":"9db641f5-4300-4527-9fa6-f1c391d42c35"},"type":"Started"},"timestamp":1563620875062},{"event":{"data":{"taker_payment_locktime":1563628675,"taker_pubkey":"02713015d3fa4d30259e90be5f131beb593bf0131f3af2dcdb304e3322d8d52b91"},"type":"Negotiated"},"timestamp":1563620915497},{"event":{"data":{"tx_hash":"6740136eaaa615d9d231969e3a9599d0fc59e53989237a8d31cd6fc86c160013","tx_hex":"0100000001a2586ea8294cedc55741bef625ba72c646399903391a7f6c604a58c6263135f2000000006b4830450221009c78c8ba4a7accab6b09f9a95da5bc59c81f4fc1e60b288ec3c5462b4d02ef01022056b63be1629cf17751d3cc5ffec51bcb1d7f9396e9ce9ca254d0f34104f7263a012102713015d3fa4d30259e90be5f131beb593bf0131f3af2dcdb304e3322d8d52b91ffffffff0210270000000000001976a914ca1e04745e8ca0c60d8c5881531d51bec470743f88ac78aa1900000000001976a91406ccabfd5f9075ecd5e8d0d31c0e973a54d51e8288ac5bf6325d"},"type":"TakerFeeValidated"},"timestamp":1563620976060},{"event":{"data":{"tx_hash":"d0f6e664cea9d89fe7b5cf8005fdca070d1ab1d05a482aaef95c08cdaecddf0a","tx_hex":"0400008085202f89019f1cbda354342cdf982046b331bbd3791f53b692efc6e4becc36be495b2977d9000000006b483045022100fa9d4557394141f6a8b9bfb8cd594a521fd8bcd1965dbf8bc4e04abc849ac66e0220589f521814c10a7561abfd5e432f7a2ee60d4875fe4604618af3207dae531ac00121031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8ffffffff029e537e030000000017a9145534898009f1467191065f6890b96914b39a1c018791857702000000001976a914c3f710deb7320b0efa6edb14e3ebeeb9155fa90d88ac72ee325d000000000000000000000000000000"},"type":"MakerPaymentSent"},"timestamp":1563620976189},{"event":{"data":{"tx_hash":"1e883eb2f3991e84ba27f53651f89b7dda708678a5b9813d043577f222b9ca30","tx_hex":"01000000011300166cc86fcd318d7a238939e559fcd099953a9e9631d2d915a6aa6e134067010000006a47304402206781d5f2db2ff13d2ec7e266f774ea5630cc2dba4019e18e9716131b8b026051022006ebb33857b6d180f13aa6be2fc532f9734abde9d00ae14757e7d7ba3741c08c012102713015d3fa4d30259e90be5f131beb593bf0131f3af2dcdb304e3322d8d52b91ffffffff0228db0b000000000017a91483818667161bf94adda3964a81a231cbf6f5338187b0480c00000000001976a91406ccabfd5f9075ecd5e8d0d31c0e973a54d51e8288ac7cf7325d"},"type":"TakerPaymentReceived"},"timestamp":1563621268320},{"event":{"type":"TakerPaymentWaitConfirmStarted"},"timestamp":1563621268321},{"event":{"type":"TakerPaymentValidatedAndConfirmed"},"timestamp":1563621778471},{"event":{"data":{"error":"lp_swap:2025] utxo:938] rpc_clients:719] JsonRpcError { request: JsonRpcRequest { jsonrpc: \"2.0\", id: \"9\", method: \"blockchain.transaction.broadcast\", params: [String(\"010000000130cab922f27735043d81b9a5788670da7d9bf85136f527ba841e99f3b23e881e00000000b6473044022058a0c1da6bcf8c1418899ff8475f3ab6dddbff918528451c1fe71c2f7dad176302204c2e0bcf8f9b5f09e02ccfeb9256e9b34fb355ea655a5704a8a3fa920079b91501514c6b63048314335db1752102713015d3fa4d30259e90be5f131beb593bf0131f3af2dcdb304e3322d8d52b91ac6782012088a9147ed38daab6085c1a1e4426e61dc87a3c2c081a958821031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8ac68feffffff0188540a00000000001976a91406ccabfd5f9075ecd5e8d0d31c0e973a54d51e8288ac1c2b335d\")] }, error: Response(Object({\"code\": Number(1), \"message\": String(\"the transaction was rejected by network rules.\\n\\nMissing inputs\\n[010000000130cab922f27735043d81b9a5788670da7d9bf85136f527ba841e99f3b23e881e00000000b6473044022058a0c1da6bcf8c1418899ff8475f3ab6dddbff918528451c1fe71c2f7dad176302204c2e0bcf8f9b5f09e02ccfeb9256e9b34fb355ea655a5704a8a3fa920079b91501514c6b63048314335db1752102713015d3fa4d30259e90be5f131beb593bf0131f3af2dcdb304e3322d8d52b91ac6782012088a9147ed38daab6085c1a1e4426e61dc87a3c2c081a958821031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8ac68feffffff0188540a00000000001976a91406ccabfd5f9075ecd5e8d0d31c0e973a54d51e8288ac1c2b335d]\")})) }"},"type":"TakerPaymentSpendFailed"},"timestamp":1563638060583},{"event":{"data":{"error":"lp_swap:2025] utxo:938] rpc_clients:719] JsonRpcError { request: JsonRpcRequest { jsonrpc: \"2.0\", id: \"9\", method: \"blockchain.transaction.broadcast\", params: [String(\"010000000130cab922f27735043d81b9a5788670da7d9bf85136f527ba841e99f3b23e881e00000000b6473044022058a0c1da6bcf8c1418899ff8475f3ab6dddbff918528451c1fe71c2f7dad176302204c2e0bcf8f9b5f09e02ccfeb9256e9b34fb355ea655a5704a8a3fa920079b91501514c6b63048314335db1752102713015d3fa4d30259e90be5f131beb593bf0131f3af2dcdb304e3322d8d52b91ac6782012088a9147ed38daab6085c1a1e4426e61dc87a3c2c081a958821031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8ac68feffffff0188540a00000000001976a91406ccabfd5f9075ecd5e8d0d31c0e973a54d51e8288ac1c2b335d\")] }, error: Response(Object({\"code\": Number(1), \"message\": String(\"the transaction was rejected by network rules.\\n\\nMissing inputs\\n[010000000130cab922f27735043d81b9a5788670da7d9bf85136f527ba841e99f3b23e881e00000000b6473044022058a0c1da6bcf8c1418899ff8475f3ab6dddbff918528451c1fe71c2f7dad176302204c2e0bcf8f9b5f09e02ccfeb9256e9b34fb355ea655a5704a8a3fa920079b91501514c6b63048314335db1752102713015d3fa4d30259e90be5f131beb593bf0131f3af2dcdb304e3322d8d52b91ac6782012088a9147ed38daab6085c1a1e4426e61dc87a3c2c081a958821031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8ac68feffffff0188540a00000000001976a91406ccabfd5f9075ecd5e8d0d31c0e973a54d51e8288ac1c2b335d]\")})) }"},"type":"MakerPaymentRefundFailed"},"timestamp":1563638060583},{"event":{"type":"Finished"},"timestamp":1563621778483}],"success_events":["Started","Negotiated","TakerFeeValidated","MakerPaymentSent","TakerPaymentReceived","TakerPaymentWaitConfirmStarted","TakerPaymentValidatedAndConfirmed","TakerPaymentSpent","TakerPaymentSpendConfirmStarted","TakerPaymentSpendConfirmed","Finished"],"uuid":"9db641f5-4300-4527-9fa6-f1c391d42c35"}"#;
        let maker_saved_swap: MakerSavedSwap = json::from_str(maker_saved_json).unwrap();
        let key_pair =
            key_pair_from_seed("spice describe gravity federal blast come thank unfair canal monkey style afraid")
                .unwrap();
        let ctx = MmCtxBuilder::default().with_secp256k1_key_pair(key_pair).into_mm_arc();

        TestCoin::ticker.mock_safe(|_| MockResult::Return("ticker"));
        TestCoin::swap_contract_address.mock_safe(|_| MockResult::Return(None));

        TestCoin::search_for_swap_tx_spend_my.mock_safe(|_, _, _, _, _, _, _| {
            MockResult::Return(Ok(Some(FoundSwapTxSpend::Refunded(eth_tx_for_test().into()))))
        });
        let maker_coin = MmCoinEnum::Test(TestCoin::default());
        let taker_coin = MmCoinEnum::Test(TestCoin::default());
        let (maker_swap, _) = MakerSwap::load_from_saved(ctx, maker_coin, taker_coin, maker_saved_swap).unwrap();
        assert!(maker_swap.recover_funds().is_err());
    }

    #[test]
    fn test_recover_funds_maker_payment_refund_errored_already_spent() {
        // the swap ends up with MakerPaymentRefundFailed error
        let maker_saved_json = r#"{"error_events":["StartFailed","NegotiateFailed","TakerFeeValidateFailed","MakerPaymentTransactionFailed","MakerPaymentDataSendFailed","TakerPaymentValidateFailed","TakerPaymentSpendFailed","TakerPaymentSpendConfirmFailed","MakerPaymentRefunded","MakerPaymentRefundFailed"],"events":[{"event":{"data":{"lock_duration":7800,"maker_amount":"0.58610590","maker_coin":"KMD","maker_coin_start_block":1450923,"maker_payment_confirmations":1,"maker_payment_lock":1563636475,"my_persistent_pub":"031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8","secret":"0000000000000000000000000000000000000000000000000000000000000000","started_at":1563620875,"taker":"14a96292bfcd7762ece8eb08ead915da927c2619277363853572f30880d5155e","taker_amount":"0.0077700000552410000000000","taker_coin":"LTC","taker_coin_start_block":1670837,"taker_payment_confirmations":1,"uuid":"9db641f5-4300-4527-9fa6-f1c391d42c35"},"type":"Started"},"timestamp":1563620875062},{"event":{"data":{"taker_payment_locktime":1563628675,"taker_pubkey":"02713015d3fa4d30259e90be5f131beb593bf0131f3af2dcdb304e3322d8d52b91"},"type":"Negotiated"},"timestamp":1563620915497},{"event":{"data":{"tx_hash":"6740136eaaa615d9d231969e3a9599d0fc59e53989237a8d31cd6fc86c160013","tx_hex":"0100000001a2586ea8294cedc55741bef625ba72c646399903391a7f6c604a58c6263135f2000000006b4830450221009c78c8ba4a7accab6b09f9a95da5bc59c81f4fc1e60b288ec3c5462b4d02ef01022056b63be1629cf17751d3cc5ffec51bcb1d7f9396e9ce9ca254d0f34104f7263a012102713015d3fa4d30259e90be5f131beb593bf0131f3af2dcdb304e3322d8d52b91ffffffff0210270000000000001976a914ca1e04745e8ca0c60d8c5881531d51bec470743f88ac78aa1900000000001976a91406ccabfd5f9075ecd5e8d0d31c0e973a54d51e8288ac5bf6325d"},"type":"TakerFeeValidated"},"timestamp":1563620976060},{"event":{"data":{"tx_hash":"d0f6e664cea9d89fe7b5cf8005fdca070d1ab1d05a482aaef95c08cdaecddf0a","tx_hex":"0400008085202f89019f1cbda354342cdf982046b331bbd3791f53b692efc6e4becc36be495b2977d9000000006b483045022100fa9d4557394141f6a8b9bfb8cd594a521fd8bcd1965dbf8bc4e04abc849ac66e0220589f521814c10a7561abfd5e432f7a2ee60d4875fe4604618af3207dae531ac00121031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8ffffffff029e537e030000000017a9145534898009f1467191065f6890b96914b39a1c018791857702000000001976a914c3f710deb7320b0efa6edb14e3ebeeb9155fa90d88ac72ee325d000000000000000000000000000000"},"type":"MakerPaymentSent"},"timestamp":1563620976189},{"event":{"data":{"tx_hash":"1e883eb2f3991e84ba27f53651f89b7dda708678a5b9813d043577f222b9ca30","tx_hex":"01000000011300166cc86fcd318d7a238939e559fcd099953a9e9631d2d915a6aa6e134067010000006a47304402206781d5f2db2ff13d2ec7e266f774ea5630cc2dba4019e18e9716131b8b026051022006ebb33857b6d180f13aa6be2fc532f9734abde9d00ae14757e7d7ba3741c08c012102713015d3fa4d30259e90be5f131beb593bf0131f3af2dcdb304e3322d8d52b91ffffffff0228db0b000000000017a91483818667161bf94adda3964a81a231cbf6f5338187b0480c00000000001976a91406ccabfd5f9075ecd5e8d0d31c0e973a54d51e8288ac7cf7325d"},"type":"TakerPaymentReceived"},"timestamp":1563621268320},{"event":{"type":"TakerPaymentWaitConfirmStarted"},"timestamp":1563621268321},{"event":{"type":"TakerPaymentValidatedAndConfirmed"},"timestamp":1563621778471},{"event":{"data":{"error":"lp_swap:2025] utxo:938] rpc_clients:719] JsonRpcError { request: JsonRpcRequest { jsonrpc: \"2.0\", id: \"9\", method: \"blockchain.transaction.broadcast\", params: [String(\"010000000130cab922f27735043d81b9a5788670da7d9bf85136f527ba841e99f3b23e881e00000000b6473044022058a0c1da6bcf8c1418899ff8475f3ab6dddbff918528451c1fe71c2f7dad176302204c2e0bcf8f9b5f09e02ccfeb9256e9b34fb355ea655a5704a8a3fa920079b91501514c6b63048314335db1752102713015d3fa4d30259e90be5f131beb593bf0131f3af2dcdb304e3322d8d52b91ac6782012088a9147ed38daab6085c1a1e4426e61dc87a3c2c081a958821031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8ac68feffffff0188540a00000000001976a91406ccabfd5f9075ecd5e8d0d31c0e973a54d51e8288ac1c2b335d\")] }, error: Response(Object({\"code\": Number(1), \"message\": String(\"the transaction was rejected by network rules.\\n\\nMissing inputs\\n[010000000130cab922f27735043d81b9a5788670da7d9bf85136f527ba841e99f3b23e881e00000000b6473044022058a0c1da6bcf8c1418899ff8475f3ab6dddbff918528451c1fe71c2f7dad176302204c2e0bcf8f9b5f09e02ccfeb9256e9b34fb355ea655a5704a8a3fa920079b91501514c6b63048314335db1752102713015d3fa4d30259e90be5f131beb593bf0131f3af2dcdb304e3322d8d52b91ac6782012088a9147ed38daab6085c1a1e4426e61dc87a3c2c081a958821031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8ac68feffffff0188540a00000000001976a91406ccabfd5f9075ecd5e8d0d31c0e973a54d51e8288ac1c2b335d]\")})) }"},"type":"TakerPaymentSpendFailed"},"timestamp":1563638060583},{"event":{"data":{"error":"lp_swap:2025] utxo:938] rpc_clients:719] JsonRpcError { request: JsonRpcRequest { jsonrpc: \"2.0\", id: \"9\", method: \"blockchain.transaction.broadcast\", params: [String(\"010000000130cab922f27735043d81b9a5788670da7d9bf85136f527ba841e99f3b23e881e00000000b6473044022058a0c1da6bcf8c1418899ff8475f3ab6dddbff918528451c1fe71c2f7dad176302204c2e0bcf8f9b5f09e02ccfeb9256e9b34fb355ea655a5704a8a3fa920079b91501514c6b63048314335db1752102713015d3fa4d30259e90be5f131beb593bf0131f3af2dcdb304e3322d8d52b91ac6782012088a9147ed38daab6085c1a1e4426e61dc87a3c2c081a958821031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8ac68feffffff0188540a00000000001976a91406ccabfd5f9075ecd5e8d0d31c0e973a54d51e8288ac1c2b335d\")] }, error: Response(Object({\"code\": Number(1), \"message\": String(\"the transaction was rejected by network rules.\\n\\nMissing inputs\\n[010000000130cab922f27735043d81b9a5788670da7d9bf85136f527ba841e99f3b23e881e00000000b6473044022058a0c1da6bcf8c1418899ff8475f3ab6dddbff918528451c1fe71c2f7dad176302204c2e0bcf8f9b5f09e02ccfeb9256e9b34fb355ea655a5704a8a3fa920079b91501514c6b63048314335db1752102713015d3fa4d30259e90be5f131beb593bf0131f3af2dcdb304e3322d8d52b91ac6782012088a9147ed38daab6085c1a1e4426e61dc87a3c2c081a958821031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8ac68feffffff0188540a00000000001976a91406ccabfd5f9075ecd5e8d0d31c0e973a54d51e8288ac1c2b335d]\")})) }"},"type":"MakerPaymentRefundFailed"},"timestamp":1563638060583},{"event":{"type":"Finished"},"timestamp":1563621778483}],"success_events":["Started","Negotiated","TakerFeeValidated","MakerPaymentSent","TakerPaymentReceived","TakerPaymentWaitConfirmStarted","TakerPaymentValidatedAndConfirmed","TakerPaymentSpent","TakerPaymentSpendConfirmStarted","TakerPaymentSpendConfirmed","Finished"],"uuid":"9db641f5-4300-4527-9fa6-f1c391d42c35"}"#;
        let maker_saved_swap: MakerSavedSwap = json::from_str(maker_saved_json).unwrap();
        let key_pair =
            key_pair_from_seed("spice describe gravity federal blast come thank unfair canal monkey style afraid")
                .unwrap();
        let ctx = MmCtxBuilder::default().with_secp256k1_key_pair(key_pair).into_mm_arc();

        TestCoin::ticker.mock_safe(|_| MockResult::Return("ticker"));
        TestCoin::swap_contract_address.mock_safe(|_| MockResult::Return(None));

        static mut SEARCH_FOR_SWAP_TX_SPEND_MY_CALLED: bool = true;
        TestCoin::search_for_swap_tx_spend_my.mock_safe(|_, _, _, _, _, _, _| {
            unsafe { SEARCH_FOR_SWAP_TX_SPEND_MY_CALLED = true }
            MockResult::Return(Ok(Some(FoundSwapTxSpend::Spent(eth_tx_for_test().into()))))
        });

        static mut SEARCH_FOR_SWAP_TX_SPEND_OTHER_CALLED: bool = true;
        TestCoin::search_for_swap_tx_spend_other.mock_safe(|_, _, _, _, _, _, _| {
            unsafe { SEARCH_FOR_SWAP_TX_SPEND_OTHER_CALLED = true }
            MockResult::Return(Ok(Some(FoundSwapTxSpend::Refunded(eth_tx_for_test().into()))))
        });
        let maker_coin = MmCoinEnum::Test(TestCoin::default());
        let taker_coin = MmCoinEnum::Test(TestCoin::default());
        let (maker_swap, _) = MakerSwap::load_from_saved(ctx, maker_coin, taker_coin, maker_saved_swap).unwrap();
        let err = maker_swap.recover_funds().expect_err("Expected an error");
        log!("Error: "(err));
        assert!(err.contains("Taker payment was already refunded"));
        assert!(unsafe { SEARCH_FOR_SWAP_TX_SPEND_MY_CALLED });
        assert!(unsafe { SEARCH_FOR_SWAP_TX_SPEND_OTHER_CALLED });
    }

    #[test]
    fn test_recover_funds_maker_swap_payment_errored_but_too_early_to_refund() {
        // the swap ends up with MakerPaymentTransactionFailed error but the transaction is actually
        // sent, need to find it and refund, prevent refund if payment is not spendable due to locktime restrictions
        let maker_saved_json = r#"{"error_events":["StartFailed","NegotiateFailed","TakerFeeValidateFailed","MakerPaymentTransactionFailed","MakerPaymentDataSendFailed","TakerPaymentValidateFailed","TakerPaymentSpendFailed","TakerPaymentSpendConfirmFailed","MakerPaymentRefunded","MakerPaymentRefundFailed"],"events":[{"event":{"data":{"lock_duration":7800,"maker_amount":"3.54932734","maker_coin":"KMD","maker_coin_start_block":1452970,"maker_payment_confirmations":1,"maker_payment_lock":1563759539,"my_persistent_pub":"031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8","secret":"0000000000000000000000000000000000000000000000000000000000000000","started_at":1563743939,"taker":"101ace6b08605b9424b0582b5cce044b70a3c8d8d10cb2965e039b0967ae92b9","taker_amount":"0.02004833998671660000000000","taker_coin":"ETH","taker_coin_start_block":8196380,"taker_payment_confirmations":1,"uuid":"3447b727-fe93-4357-8e5a-8cf2699b7e86"},"type":"Started"},"timestamp":1563743939211},{"event":{"data":{"taker_payment_locktime":1563751737,"taker_pubkey":"03101ace6b08605b9424b0582b5cce044b70a3c8d8d10cb2965e039b0967ae92b9"},"type":"Negotiated"},"timestamp":1563743979835},{"event":{"data":{"tx_hash":"a59203eb2328827de00bed699a29389792906e4f39fdea145eb40dc6b3821bd6","tx_hex":"f8690284ee6b280082520894d8997941dd1346e9231118d5685d866294f59e5b865af3107a4000801ca0743d2b7c9fad65805d882179062012261be328d7628ae12ee08eff8d7657d993a07eecbd051f49d35279416778faa4664962726d516ce65e18755c9b9406a9c2fd"},"type":"TakerFeeValidated"},"timestamp":1563744052878},{"event":{"data":{"error":"lp_swap:1888] eth:654] RPC error: Error { code: ServerError(-32010), message: \"Transaction with the same hash was already imported.\", data: None }"},"type":"MakerPaymentTransactionFailed"},"timestamp":1563744118577},{"event":{"type":"Finished"},"timestamp":1563763243350}],"success_events":["Started","Negotiated","TakerFeeValidated","MakerPaymentSent","TakerPaymentReceived","TakerPaymentWaitConfirmStarted","TakerPaymentValidatedAndConfirmed","TakerPaymentSpent","TakerPaymentSpendConfirmStarted","TakerPaymentSpendConfirmed","Finished"],"uuid":"3447b727-fe93-4357-8e5a-8cf2699b7e86"}"#;
        let maker_saved_swap: MakerSavedSwap = json::from_str(maker_saved_json).unwrap();
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
        TestCoin::search_for_swap_tx_spend_my.mock_safe(|_, _, _, _, _, _, _| MockResult::Return(Ok(None)));
        let maker_coin = MmCoinEnum::Test(TestCoin::default());
        let taker_coin = MmCoinEnum::Test(TestCoin::default());
        let (maker_swap, _) = MakerSwap::load_from_saved(ctx, maker_coin, taker_coin, maker_saved_swap).unwrap();
        maker_swap.w().data.maker_payment_lock = (now_ms() / 1000) - 3690;
        assert!(maker_swap.recover_funds().is_err());
        assert!(unsafe { MY_PAYMENT_SENT_CALLED });
    }

    #[test]
    fn test_recover_funds_maker_swap_payment_errored_and_not_sent() {
        // the swap ends up with MakerPaymentTransactionFailed error and transaction is not sent,
        // recover must return error in this case
        let maker_saved_json = r#"{"error_events":["StartFailed","NegotiateFailed","TakerFeeValidateFailed","MakerPaymentTransactionFailed","MakerPaymentDataSendFailed","TakerPaymentValidateFailed","TakerPaymentSpendFailed","TakerPaymentSpendConfirmFailed","MakerPaymentRefunded","MakerPaymentRefundFailed"],"events":[{"event":{"data":{"lock_duration":7800,"maker_amount":"3.54932734","maker_coin":"KMD","maker_coin_start_block":1452970,"maker_payment_confirmations":1,"maker_payment_lock":1563759539,"my_persistent_pub":"031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8","secret":"0000000000000000000000000000000000000000000000000000000000000000","started_at":1563743939,"taker":"101ace6b08605b9424b0582b5cce044b70a3c8d8d10cb2965e039b0967ae92b9","taker_amount":"0.02004833998671660000000000","taker_coin":"ETH","taker_coin_start_block":8196380,"taker_payment_confirmations":1,"uuid":"3447b727-fe93-4357-8e5a-8cf2699b7e86"},"type":"Started"},"timestamp":1563743939211},{"event":{"data":{"taker_payment_locktime":1563751737,"taker_pubkey":"03101ace6b08605b9424b0582b5cce044b70a3c8d8d10cb2965e039b0967ae92b9"},"type":"Negotiated"},"timestamp":1563743979835},{"event":{"data":{"tx_hash":"a59203eb2328827de00bed699a29389792906e4f39fdea145eb40dc6b3821bd6","tx_hex":"f8690284ee6b280082520894d8997941dd1346e9231118d5685d866294f59e5b865af3107a4000801ca0743d2b7c9fad65805d882179062012261be328d7628ae12ee08eff8d7657d993a07eecbd051f49d35279416778faa4664962726d516ce65e18755c9b9406a9c2fd"},"type":"TakerFeeValidated"},"timestamp":1563744052878},{"event":{"data":{"error":"lp_swap:1888] eth:654] RPC error: Error { code: ServerError(-32010), message: \"Transaction with the same hash was already imported.\", data: None }"},"type":"MakerPaymentTransactionFailed"},"timestamp":1563744118577},{"event":{"type":"Finished"},"timestamp":1563763243350}],"success_events":["Started","Negotiated","TakerFeeValidated","MakerPaymentSent","TakerPaymentReceived","TakerPaymentWaitConfirmStarted","TakerPaymentValidatedAndConfirmed","TakerPaymentSpent","TakerPaymentSpendConfirmStarted","TakerPaymentSpendConfirmed","Finished"],"uuid":"3447b727-fe93-4357-8e5a-8cf2699b7e86"}"#;
        let maker_saved_swap: MakerSavedSwap = json::from_str(maker_saved_json).unwrap();
        let key_pair =
            key_pair_from_seed("spice describe gravity federal blast come thank unfair canal monkey style afraid")
                .unwrap();
        let ctx = MmCtxBuilder::default().with_secp256k1_key_pair(key_pair).into_mm_arc();

        TestCoin::ticker.mock_safe(|_| MockResult::Return("ticker"));
        TestCoin::swap_contract_address.mock_safe(|_| MockResult::Return(None));

        static mut MY_PAYMENT_SENT_CALLED: bool = false;
        TestCoin::check_if_my_payment_sent.mock_safe(|_, _, _, _, _, _| {
            unsafe { MY_PAYMENT_SENT_CALLED = true };
            MockResult::Return(Box::new(futures01::future::ok(None)))
        });
        let maker_coin = MmCoinEnum::Test(TestCoin::default());
        let taker_coin = MmCoinEnum::Test(TestCoin::default());
        let (maker_swap, _) = MakerSwap::load_from_saved(ctx, maker_coin, taker_coin, maker_saved_swap).unwrap();
        assert!(maker_swap.recover_funds().is_err());
        assert!(unsafe { MY_PAYMENT_SENT_CALLED });
    }

    #[test]
    fn test_recover_funds_maker_swap_not_finished() {
        // return error if swap is not finished
        let maker_saved_json = r#"{"error_events":["StartFailed","NegotiateFailed","TakerFeeValidateFailed","MakerPaymentTransactionFailed","MakerPaymentDataSendFailed","TakerPaymentValidateFailed","TakerPaymentSpendFailed","TakerPaymentSpendConfirmFailed","MakerPaymentRefunded","MakerPaymentRefundFailed"],"events":[{"event":{"data":{"lock_duration":7800,"maker_amount":"3.54932734","maker_coin":"KMD","maker_coin_start_block":1452970,"maker_payment_confirmations":1,"maker_payment_lock":1563759539,"my_persistent_pub":"031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8","secret":"0000000000000000000000000000000000000000000000000000000000000000","started_at":1563743939,"taker":"101ace6b08605b9424b0582b5cce044b70a3c8d8d10cb2965e039b0967ae92b9","taker_amount":"0.02004833998671660000000000","taker_coin":"ETH","taker_coin_start_block":8196380,"taker_payment_confirmations":1,"uuid":"3447b727-fe93-4357-8e5a-8cf2699b7e86"},"type":"Started"},"timestamp":1563743939211},{"event":{"data":{"taker_payment_locktime":1563751737,"taker_pubkey":"03101ace6b08605b9424b0582b5cce044b70a3c8d8d10cb2965e039b0967ae92b9"},"type":"Negotiated"},"timestamp":1563743979835},{"event":{"data":{"tx_hash":"a59203eb2328827de00bed699a29389792906e4f39fdea145eb40dc6b3821bd6","tx_hex":"f8690284ee6b280082520894d8997941dd1346e9231118d5685d866294f59e5b865af3107a4000801ca0743d2b7c9fad65805d882179062012261be328d7628ae12ee08eff8d7657d993a07eecbd051f49d35279416778faa4664962726d516ce65e18755c9b9406a9c2fd"},"type":"TakerFeeValidated"},"timestamp":1563744052878}],"success_events":["Started","Negotiated","TakerFeeValidated","MakerPaymentSent","TakerPaymentReceived","TakerPaymentWaitConfirmStarted","TakerPaymentValidatedAndConfirmed","TakerPaymentSpent","TakerPaymentSpendConfirmStarted","TakerPaymentSpendConfirmed","Finished"],"uuid":"3447b727-fe93-4357-8e5a-8cf2699b7e86"}"#;
        let maker_saved_swap: MakerSavedSwap = json::from_str(maker_saved_json).unwrap();
        let key_pair =
            key_pair_from_seed("spice describe gravity federal blast come thank unfair canal monkey style afraid")
                .unwrap();
        let ctx = MmCtxBuilder::default().with_secp256k1_key_pair(key_pair).into_mm_arc();

        TestCoin::ticker.mock_safe(|_| MockResult::Return("ticker"));
        TestCoin::swap_contract_address.mock_safe(|_| MockResult::Return(None));
        let maker_coin = MmCoinEnum::Test(TestCoin::default());
        let taker_coin = MmCoinEnum::Test(TestCoin::default());
        let (maker_swap, _) = MakerSwap::load_from_saved(ctx, maker_coin, taker_coin, maker_saved_swap).unwrap();
        assert!(maker_swap.recover_funds().is_err());
    }

    #[test]
    fn test_recover_funds_maker_swap_taker_payment_spent() {
        // return error if taker payment was spent by us
        let maker_saved_json = r#"{"error_events":["StartFailed","NegotiateFailed","TakerFeeValidateFailed","MakerPaymentTransactionFailed","MakerPaymentDataSendFailed","TakerPaymentValidateFailed","TakerPaymentSpendFailed","TakerPaymentSpendConfirmFailed","MakerPaymentRefunded","MakerPaymentRefundFailed"],"events":[{"event":{"data":{"lock_duration":7800,"maker_amount":"1","maker_coin":"BEER","maker_coin_start_block":154892,"maker_payment_confirmations":1,"maker_payment_lock":1563444026,"my_persistent_pub":"02631dcf1d4b1b693aa8c2751afc68e4794b1e5996566cfc701a663f8b7bbbe640","secret":"e1c9bd12a83f810813dc078ac398069b63d56bf1e94657def995c43cd1975302","started_at":1563428426,"taker":"031d4256c4bc9f99ac88bf3dba21773132281f65f9bf23a59928bce08961e2f3","taker_amount":"1","taker_coin":"ETOMIC","taker_coin_start_block":150282,"taker_payment_confirmations":1,"uuid":"983ce732-62a8-4a44-b4ac-7e4271adc977"},"type":"Started"},"timestamp":1563428426510},{"event":{"data":{"taker_payment_locktime":1563436226,"taker_pubkey":"02031d4256c4bc9f99ac88bf3dba21773132281f65f9bf23a59928bce08961e2f3"},"type":"Negotiated"},"timestamp":1563428466880},{"event":{"data":{"tx_hash":"32f5bec2106dd3778dc32e3d856398ed0fa10b71c688672906a4fa0345cc4135","tx_hex":"0400008085202f89015ba9c8f0aec5b409bc824bcddc1a5a40148d4bd065c10169249e44ec44d62db2010000006a473044022050a213db7486e34871b9e7ef850845d55e0d53431350c16fa14fb60b81b1858302204f1042761f84e5f8d22948358b3c4103861adf5293d1d9e7f58f3b7491470b19012102031d4256c4bc9f99ac88bf3dba21773132281f65f9bf23a59928bce08961e2f3ffffffff02bcf60100000000001976a914ca1e04745e8ca0c60d8c5881531d51bec470743f88ac764d12ac010000001976a91405aab5342166f8594baf17a7d9bef5d56744332788ac8806305d000000000000000000000000000000"},"type":"TakerFeeValidated"},"timestamp":1563428507723},{"event":{"data":{"tx_hash":"1619d10a51925d2f3d0ef92d81cb6449b77d5dbe1f3ef5e7ae6c8bc19080cb5a","tx_hex":"0400008085202f890176ead03820bc0c4e92dba39b5d7e7a1e176b165f6cfc7a5e2c000ed62e8a8134010000006b48304502210086ca9a6ea5e787f4c3001c4ddb7b2f4732d8bb2642e9e43d0f39df4b736a4aa402206dbd17753f728d70c9631b6c2d1bba125745a5bc9be6112febf0e0c8ada786b1012102631dcf1d4b1b693aa8c2751afc68e4794b1e5996566cfc701a663f8b7bbbe640ffffffff0200e1f5050000000017a91410503cfea67f03f025c5e1eeb18524464adf77ee877f360c18c00000001976a91464ae8510aac9546d5e7704e31ce177451386455588ac9b06305d000000000000000000000000000000"},"type":"MakerPaymentSent"},"timestamp":1563428512925},{"event":{"data":{"tx_hash":"ee8b904efdee0d3bf0215d14a236489cde0b0efa92f7fa49faaa5fd97ed38ac0","tx_hex":"0400008085202f89013541cc4503faa406296788c6710ba10fed9863853d2ec38d77d36d10c2bef532010000006b483045022100a32e290d3a047ad75a512f9fd581c561c5153aa1b6be2b36915a9dd452cd0d4102204d1838b3cd15698ab424d15651d50983f0196e59b0b34abaad9cb792c97b527a012102031d4256c4bc9f99ac88bf3dba21773132281f65f9bf23a59928bce08961e2f3ffffffff0200e1f5050000000017a91424fc6f967eaa2751adbeb42a97c3497fbd9ddcce878e681ca6010000001976a91405aab5342166f8594baf17a7d9bef5d56744332788acbf06305d000000000000000000000000000000"},"type":"TakerPaymentReceived"},"timestamp":1563428664418},{"event":{"type":"TakerPaymentWaitConfirmStarted"},"timestamp":1563428664420},{"event":{"type":"TakerPaymentValidatedAndConfirmed"},"timestamp":1563428664824},{"event":{"data":{"tx_hash":"8b48d7452a2a1c6b1128aa83ab946e5a624037c5327b527b18c3dcadb404f139","tx_hex":"0400008085202f8901c08ad37ed95faafa49faf792fa0e0bde9c4836a2145d21f03b0deefd4e908bee00000000d747304402206ac1f2b5b856b86585b4d2147309e3a7ef9dd4c35ffd85a49c409a4acd11602902204be03e2114888fae460eaf99675bae0c834ff80be8531a5bd30ee14baf0a52e30120e1c9bd12a83f810813dc078ac398069b63d56bf1e94657def995c43cd1975302004c6b6304c224305db1752102031d4256c4bc9f99ac88bf3dba21773132281f65f9bf23a59928bce08961e2f3ac6782012088a9143501575fb9a12a689bb94adad33cc78c13b0688c882102631dcf1d4b1b693aa8c2751afc68e4794b1e5996566cfc701a663f8b7bbbe640ac68ffffffff0118ddf505000000001976a91464ae8510aac9546d5e7704e31ce177451386455588ac28f92f5d000000000000000000000000000000"},"type":"TakerPaymentSpent"},"timestamp":1563428666150},{"event":{"type":"Finished"},"timestamp":1563428666152}],"my_info":{"my_amount":"1","my_coin":"BEER","other_amount":"1","other_coin":"ETOMIC","started_at":1563428426},"success_events":["Started","Negotiated","TakerFeeValidated","MakerPaymentSent","TakerPaymentReceived","TakerPaymentWaitConfirmStarted","TakerPaymentValidatedAndConfirmed","TakerPaymentSpent","TakerPaymentSpendConfirmStarted","TakerPaymentSpendConfirmed","Finished"],"type":"Maker","uuid":"983ce732-62a8-4a44-b4ac-7e4271adc977"}"#;
        let maker_saved_swap: MakerSavedSwap = json::from_str(maker_saved_json).unwrap();
        let key_pair =
            key_pair_from_seed("spice describe gravity federal blast come thank unfair canal monkey style afraid")
                .unwrap();
        let ctx = MmCtxBuilder::default().with_secp256k1_key_pair(key_pair).into_mm_arc();

        TestCoin::ticker.mock_safe(|_| MockResult::Return("ticker"));
        TestCoin::swap_contract_address.mock_safe(|_| MockResult::Return(None));

        static mut SEARCH_FOR_SWAP_TX_SPEND_MY_CALLED: bool = true;
        TestCoin::search_for_swap_tx_spend_my.mock_safe(|_, _, _, _, _, _, _| {
            unsafe { SEARCH_FOR_SWAP_TX_SPEND_MY_CALLED = true }
            MockResult::Return(Ok(Some(FoundSwapTxSpend::Spent(eth_tx_for_test().into()))))
        });

        static mut SEARCH_FOR_SWAP_TX_SPEND_OTHER_CALLED: bool = true;
        TestCoin::search_for_swap_tx_spend_other.mock_safe(|_, _, _, _, _, _, _| {
            unsafe { SEARCH_FOR_SWAP_TX_SPEND_OTHER_CALLED = true }
            MockResult::Return(Ok(Some(FoundSwapTxSpend::Spent(eth_tx_for_test().into()))))
        });

        let maker_coin = MmCoinEnum::Test(TestCoin::default());
        let taker_coin = MmCoinEnum::Test(TestCoin::default());
        let (maker_swap, _) = MakerSwap::load_from_saved(ctx, maker_coin, taker_coin, maker_saved_swap).unwrap();
        let err = maker_swap.recover_funds().expect_err("Expected an error");
        log!("Error: "(err));
        assert!(err.contains("Taker payment was already spent"));
        assert!(unsafe { SEARCH_FOR_SWAP_TX_SPEND_MY_CALLED });
        assert!(unsafe { SEARCH_FOR_SWAP_TX_SPEND_OTHER_CALLED });
    }

    #[test]
    fn test_recover_funds_maker_swap_maker_payment_refunded() {
        // return error if maker payment was refunded
        let maker_saved_json = r#"{"error_events":["StartFailed","NegotiateFailed","TakerFeeValidateFailed","MakerPaymentTransactionFailed","MakerPaymentDataSendFailed","TakerPaymentValidateFailed","TakerPaymentSpendFailed","TakerPaymentSpendConfirmFailed","MakerPaymentRefunded","MakerPaymentRefundFailed"],"events":[{"event":{"data":{"lock_duration":7800,"maker_amount":"9.38455187130897","maker_coin":"VRSC","maker_coin_start_block":604407,"maker_payment_confirmations":1,"maker_payment_lock":1564317372,"my_persistent_pub":"03c2e08e48e6541b3265ccd430c5ecec7efc7d0d9fc4e310a9b052f9642673fb0a","secret":"0000000000000000000000000000000000000000000000000000000000000000","started_at":1564301772,"taker":"39c4bcdb1e6bbb29a3b131c2b82eba2552f4f8a804021b2064114ab857f00848","taker_amount":"0.999999999999999880468812552729","taker_coin":"KMD","taker_coin_start_block":1462209,"taker_payment_confirmations":1,"uuid":"8f5b267a-efa8-49d6-a92d-ec0523cca891"},"type":"Started"},"timestamp":1564301773193},{"event":{"data":{"taker_payment_locktime":1564309572,"taker_pubkey":"0339c4bcdb1e6bbb29a3b131c2b82eba2552f4f8a804021b2064114ab857f00848"},"type":"Negotiated"},"timestamp":1564301813664},{"event":{"data":{"tx_hash":"cf54a5f5dfdf2eb404855eaba6a05b41f893a20327d43770c0138bb9ed2cf9eb","tx_hex":"0400008085202f89018f03a4d46831ec541279d01998be6092a98ee0f103b69ab84697cdc3eea7e93c000000006a473044022046eb76ecf610832ef063a6d210b5d07bc90fd0f3b68550fd2945ce86b317252a02202d3438d2e83df49f1c8ab741553af65a0d97e6edccbb6c4d0c769b05426c637001210339c4bcdb1e6bbb29a3b131c2b82eba2552f4f8a804021b2064114ab857f00848ffffffff0276c40100000000001976a914ca1e04745e8ca0c60d8c5881531d51bec470743f88acddf7bd54000000001976a9144df806990ae0197402aeaa6d9b1ec60078d9eadf88ac01573d5d000000000000000000000000000000"},"type":"TakerFeeValidated"},"timestamp":1564301864738},{"event":{"data":{"tx_hash":"2252c9929707995aff6dbb03d23b7e7eb786611d26b6ae748ca13007e71d1de6","tx_hex":"0400008085202f8901f63aed15c53b794df1a9446755f452e9fd9db250e1f608636f6172b7d795358c010000006b483045022100b5adb583fbb4b1a628b9c58ec292bb7b1319bb881c2cf018af6fe33b7a182854022020d89a2d6cbf15a117e2e1122046941f95466af7507883c4fa05955f0dfb81f2012103c2e08e48e6541b3265ccd430c5ecec7efc7d0d9fc4e310a9b052f9642673fb0affffffff0293b0ef370000000017a914ca41def369fc07d8aea10ba26cf3e64a12470d4087163149f61c0000001976a914f4f89313803d610fa472a5849d2389ca6df3b90088ac285a3d5d000000000000000000000000000000"},"type":"MakerPaymentSent"},"timestamp":1564301867675},{"event":{"data":{"error":"timeout (2690.6 > 2690.0)"},"type":"TakerPaymentValidateFailed"},"timestamp":1564304558269},{"event":{"data":{"tx_hash":"96d0b50bc2371ab88052bc4d656f1b91b3e3e64eba650eac28ebce9387d234cb","tx_hex":"0400008085202f8901e61d1de70730a18c74aeb6261d6186b77e7e3bd203bb6dff5a99079792c9522200000000b647304402207d36206295eee6c936d0204552cc5a001d4de4bbc0c5ae1c6218cf8548b4f08b02204c2a6470e06a6caf407ea8f2704fdc1b1dee39f89d145f8c0460130cb1875b2b01514c6b6304bc963d5db1752103c2e08e48e6541b3265ccd430c5ecec7efc7d0d9fc4e310a9b052f9642673fb0aac6782012088a9145f5598259da7c0c0beffcc3e9da35e553bac727388210339c4bcdb1e6bbb29a3b131c2b82eba2552f4f8a804021b2064114ab857f00848ac68feffffff01abacef37000000001976a914f4f89313803d610fa472a5849d2389ca6df3b90088ac26973d5d000000000000000000000000000000"},"type":"MakerPaymentRefunded"},"timestamp":1564321080407},{"event":{"type":"Finished"},"timestamp":1564321080409}],"success_events":["Started","Negotiated","TakerFeeValidated","MakerPaymentSent","TakerPaymentReceived","TakerPaymentWaitConfirmStarted","TakerPaymentValidatedAndConfirmed","TakerPaymentSpent","TakerPaymentSpendConfirmStarted","TakerPaymentSpendConfirmed","Finished"],"uuid":"8f5b267a-efa8-49d6-a92d-ec0523cca891"}"#;
        let maker_saved_swap: MakerSavedSwap = json::from_str(maker_saved_json).unwrap();
        let key_pair =
            key_pair_from_seed("spice describe gravity federal blast come thank unfair canal monkey style afraid")
                .unwrap();
        let ctx = MmCtxBuilder::default().with_secp256k1_key_pair(key_pair).into_mm_arc();

        TestCoin::ticker.mock_safe(|_| MockResult::Return("ticker"));
        TestCoin::swap_contract_address.mock_safe(|_| MockResult::Return(None));
        let maker_coin = MmCoinEnum::Test(TestCoin::default());
        let taker_coin = MmCoinEnum::Test(TestCoin::default());
        let (maker_swap, _) = MakerSwap::load_from_saved(ctx, maker_coin, taker_coin, maker_saved_swap).unwrap();
        assert!(maker_swap.recover_funds().is_err());
    }

    #[test]
    /// https://github.com/KomodoPlatform/atomicDEX-API/issues/774
    fn test_recover_funds_my_payment_spent_other_not() {
        // The swap ends up with TakerPaymentSpendConfirmFailed error because the TakerPaymentSpend transaction was in mempool long time and finally not mined.
        // sent, need to find it and refund, prevent refund if payment is not spendable due to locktime restrictions
        let maker_saved_json = r#"{"uuid":"7f95db1d-2ea5-4cce-b056-400e8b288042","events":[{"timestamp":1607887364672,"event":{"type":"Started","data":{"taker_coin":"KMD","maker_coin":"EMC2","taker":"ae3cc37d2a7cc9077fb5b1baa962d1539e1ffe5fb318c99dcba43059ed97900d","secret":"0000000000000000000000000000000000000000000000000000000000000000","secret_hash":"4a40a42a7d7192e5cbeaa3871f734612acfeaf76","my_persistent_pub":"03005e349c71a17334a3d7b712ebeb593c692e2401e611bbd829b6948c3acc15e5","lock_duration":31200,"maker_amount":"24.69126200952912480678056188200573124484152642245967528226624133800833910867311611640128371388479323","taker_amount":"3.094308955034189920785740015052958239603540091262646506373605364479205057098914911707408875024042288","maker_payment_confirmations":1,"maker_payment_requires_nota":true,"taker_payment_confirmations":2,"taker_payment_requires_nota":true,"maker_payment_lock":1607949764,"uuid":"7f95db1d-2ea5-4cce-b056-400e8b288042","started_at":1607887364,"maker_coin_start_block":3526364,"taker_coin_start_block":2178701}}},{"timestamp":1607887366684,"event":{"type":"Negotiated","data":{"taker_payment_locktime":1607918567,"taker_pubkey":"03ae3cc37d2a7cc9077fb5b1baa962d1539e1ffe5fb318c99dcba43059ed97900d"}}},{"timestamp":1607887367745,"event":{"type":"TakerFeeValidated","data":{"tx_hex":"0400008085202f8902f02f23931783009e01b7f250234eb7b3a96bd7e7e16dd61f21988bbc7600b6f7020000006b483045022100d4610ef1f147417476877aa09b1f110f7e6773355d6dc8cae7af429707f9da4d02203f5d1890da9d6efffee55869761f9353dbd3bcafb2a560f4564eba658ae2807b012103ae3cc37d2a7cc9077fb5b1baa962d1539e1ffe5fb318c99dcba43059ed97900dffffffffb7599816287b72f939b8e6b59fe4706d7b6826e5f6c04db18e3689cb76e846ce000000006a473044022021a486a9920ff8b3d892c00c10abaf58b4509fe9d3a6f8320e198e312e220db402203fab57d7ccfda1eded606ab3de6a32f626b5904ecf9f41e4a7a4800376952d67012103ae3cc37d2a7cc9077fb5b1baa962d1539e1ffe5fb318c99dcba43059ed97900dffffffff020e780500000000001976a914ca1e04745e8ca0c60d8c5881531d51bec470743f88ac5bdb0c00000000001976a914851bf1c11fb48beecc0a0e50982b9d43357743e688ac0c62d65f000000000000000000000000000000","tx_hash":"8a7c0ddbc2a0e94e1f58c920780563eb71266d932fe9435cf66def905db91efb"}}},{"timestamp":1607887367887,"event":{"type":"MakerPaymentSent","data":{"tx_hex":"010000000118c570eab3ec0f07a33640aff42a7b3565f4fa72561473b9f55d23b7a5360351090000006a4730440220047c5a917e7ac72b55357c657581ebf60b7281466be3dd00a92f68b85e03ae2e02204b07643faf3ae88a1775794c5afc406be4aaec87d0a98fb49a2125e02b9ed21d012103005e349c71a17334a3d7b712ebeb593c692e2401e611bbd829b6948c3acc15e5ffffffff0338e02b930000000017a914e0ddc80814f50249d097c3242e355a5d6fae462b870000000000000000166a144a40a42a7d7192e5cbeaa3871f734612acfeaf76284bcb93000000001976a914b86cb58669cc65e2f880e1df5d6e11c3dcb7230988ac076ad65f","tx_hash":"aa06c1647cb0418ed6ca7666dc517bfe8de92bc163b6765f9370bb316af1c1ff"}}},{"timestamp":1607887416097,"event":{"type":"TakerPaymentReceived","data":{"tx_hex":"0400008085202f8903fb1eb95d90ef6df65c43e92f936d2671eb63057820c9581f4ee9a0c2db0d7c8a010000006b483045022100d55c02f8536f0c1e5f10833b901adc3d2a77d7f0701371a29dcc155426b8f280022028f540607c349f9a73489801c45445f594f2552d165b2bd004af00c0297ce494012103ae3cc37d2a7cc9077fb5b1baa962d1539e1ffe5fb318c99dcba43059ed97900dffffffffe83e674cb46d0862cbc3ade7e363c5eb3a73a9ad977fff60544093efc6a2682b000000006a47304402200a324d95e7e7193a479aed48c608068f6d81ee8d7dd9b13735427e18f53bb25b0220606e075b56c675f10f6cb3332e4a28011d88428f54e3f09b016deec16c1ac41b012103ae3cc37d2a7cc9077fb5b1baa962d1539e1ffe5fb318c99dcba43059ed97900dffffffff987b47469a16aac4adaf0a47d8fbf813f8b20cf28eef82e14b74d732a263584e000000006a473044022062badf692bea3f13b5adb5cd66ff87f8f3224624762a75caaffa6fd856a0cfa602204d2477941cb781bba824d20e562f2d52a85c6c1103dccf28ddbfcadead87925b012103ae3cc37d2a7cc9077fb5b1baa962d1539e1ffe5fb318c99dcba43059ed97900dffffffff036f8a71120000000017a91415d9f7c7ad4e88b91d92c1480902fedfea92b981870000000000000000166a144a40a42a7d7192e5cbeaa3871f734612acfeaf76967e1500000000001976a914851bf1c11fb48beecc0a0e50982b9d43357743e688ac3c62d65f000000000000000000000000000000","tx_hash":"3e01651b399901192067e24f60371f640e840d240956676a416d26dec6f051f4"}}},{"timestamp":1607887416115,"event":{"type":"TakerPaymentWaitConfirmStarted"}},{"timestamp":1607901459795,"event":{"type":"TakerPaymentValidatedAndConfirmed"}},{"timestamp":1607901459843,"event":{"type":"TakerPaymentSpent","data":{"tx_hex":"0400008085202f8901f451f0c6de266d416a675609240d840e641f37604fe26720190199391b65013e00000000d8483045022100f8a8dade217e2595d3aaa287adc6cbf895b3d9c13f28aa707873943c1412c36d022053efe8c35fefbab2b298e0e4e8b93ad05b1d98d872b656616520dee15d79ea6801202e3d520b3d396cd2fc4aaac03257d13b2c82772ffe4479b7e0841987f8f673a7004c6b6304e7e3d65fb1752103ae3cc37d2a7cc9077fb5b1baa962d1539e1ffe5fb318c99dcba43059ed97900dac6782012088a9144a40a42a7d7192e5cbeaa3871f734612acfeaf76882103005e349c71a17334a3d7b712ebeb593c692e2401e611bbd829b6948c3acc15e5ac68ffffffff0187867112000000001976a914b86cb58669cc65e2f880e1df5d6e11c3dcb7230988ac1599d65f000000000000000000000000000000","tx_hash":"21cb40785f3e768c38c502be378448f33634430277360dc1c20fcdc238ebf806"}}},{"timestamp":1607901459850,"event":{"type":"TakerPaymentSpendConfirmStarted"}},{"timestamp":1607957899314,"event":{"type":"TakerPaymentSpendConfirmFailed","data":{"error":"maker_swap:714] !wait for taker payment spend confirmations: rpc_clients:123] Waited too long until 1607953464 for transaction Transaction { version: 4, n_time: None, overwintered: true, version_group_id: 2301567109, inputs: [TransactionInput { previous_output: OutPoint { hash: f451f0c6de266d416a675609240d840e641f37604fe26720190199391b65013e, index: 0 }, script_sig: 483045022100f8a8dade217e2595d3aaa287adc6cbf895b3d9c13f28aa707873943c1412c36d022053efe8c35fefbab2b298e0e4e8b93ad05b1d98d872b656616520dee15d79ea6801202e3d520b3d396cd2fc4aaac03257d13b2c82772ffe4479b7e0841987f8f673a7004c6b6304e7e3d65fb1752103ae3cc37d2a7cc9077fb5b1baa962d1539e1ffe5fb318c99dcba43059ed97900dac6782012088a9144a40a42a7d7192e5cbeaa3871f734612acfeaf76882103005e349c71a17334a3d7b712ebeb593c692e2401e611bbd829b6948c3acc15e5ac68, sequence: 4294967295, script_witness: [] }], outputs: [TransactionOutput { value: 309429895, script_pubkey: 76a914b86cb58669cc65e2f880e1df5d6e11c3dcb7230988ac }], lock_time: 1607899413, expiry_height: 0, shielded_spends: [], shielded_outputs: [], join_splits: [], value_balance: 0, join_split_pubkey: 0000000000000000000000000000000000000000000000000000000000000000, join_split_sig: 00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000, binding_sig: 00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000, zcash: true, str_d_zeel: None } to be confirmed 1 times"}}},{"timestamp":1607957899319,"event":{"type":"MakerPaymentWaitRefundStarted","data":{"wait_until":1607953464}}},{"timestamp":1607957899367,"event":{"type":"MakerPaymentRefundFailed","data":{"error":"maker_swap:746] !maker_coin.send_maker_refunds_payment: utxo_common:791] rpc_clients:1440] JsonRpcError { client_info: \"coin: EMC2\", request: JsonRpcRequest { jsonrpc: \"2.0\", id: \"8\", method: \"blockchain.transaction.broadcast\", params: [String(\"0100000001ffc1f16a31bb70935f76b663c12be98dfe7b51dc6676cad68e41b07c64c106aa00000000b6473044022029d1626dde413ecb7af09c1609a0f1f3791539aef1a5f972787db39c6b8178f302202052b6c37f2334cbee21dc8c6ceeb368cc0bb69ab254da3a8b48a27d4542d84b01514c6b6304c45dd75fb1752103005e349c71a17334a3d7b712ebeb593c692e2401e611bbd829b6948c3acc15e5ac6782012088a9144a40a42a7d7192e5cbeaa3871f734612acfeaf76882103ae3cc37d2a7cc9077fb5b1baa962d1539e1ffe5fb318c99dcba43059ed97900dac68feffffff0198592a93000000001976a914b86cb58669cc65e2f880e1df5d6e11c3dcb7230988ac7b6fd75f\")] }, error: Response(electrum2.cipig.net:10062, Object({\"code\": Number(1), \"message\": String(\"the transaction was rejected by network rules.\\n\\n18: bad-txns-inputs-spent\\n[0100000001ffc1f16a31bb70935f76b663c12be98dfe7b51dc6676cad68e41b07c64c106aa00000000b6473044022029d1626dde413ecb7af09c1609a0f1f3791539aef1a5f972787db39c6b8178f302202052b6c37f2334cbee21dc8c6ceeb368cc0bb69ab254da3a8b48a27d4542d84b01514c6b6304c45dd75fb1752103005e349c71a17334a3d7b712ebeb593c692e2401e611bbd829b6948c3acc15e5ac6782012088a9144a40a42a7d7192e5cbeaa3871f734612acfeaf76882103ae3cc37d2a7cc9077fb5b1baa962d1539e1ffe5fb318c99dcba43059ed97900dac68feffffff0198592a93000000001976a914b86cb58669cc65e2f880e1df5d6e11c3dcb7230988ac7b6fd75f]\")})) }"}}},{"timestamp":1607957899372,"event":{"type":"Finished"}}],"maker_amount":"24.69126200952912480678056188200573124484152642245967528226624133800833910867311611640128371388479323","maker_coin":"EMC2","taker_amount":"3.094308955034189920785740015052958239603540091262646506373605364479205057098914911707408875024042288","taker_coin":"KMD","gui":"AtomicDex Desktop 0.3.1-beta","mm_version":"2.1.2793_mm2.1_19701cc87_Windows_NT_Release","success_events":["Started","Negotiated","TakerFeeValidated","MakerPaymentSent","TakerPaymentReceived","TakerPaymentWaitConfirmStarted","TakerPaymentValidatedAndConfirmed","TakerPaymentSpent","TakerPaymentSpendConfirmStarted","TakerPaymentSpendConfirmed","Finished"],"error_events":["StartFailed","NegotiateFailed","TakerFeeValidateFailed","MakerPaymentTransactionFailed","MakerPaymentDataSendFailed","MakerPaymentWaitConfirmFailed","TakerPaymentValidateFailed","TakerPaymentWaitConfirmFailed","TakerPaymentSpendFailed","TakerPaymentSpendConfirmFailed","MakerPaymentWaitRefundStarted","MakerPaymentRefunded","MakerPaymentRefundFailed"]}"#;
        let maker_saved_swap: MakerSavedSwap = json::from_str(maker_saved_json).unwrap();
        let key_pair =
            key_pair_from_seed("spice describe gravity federal blast come thank unfair canal monkey style afraid")
                .unwrap();
        let ctx = MmCtxBuilder::default().with_secp256k1_key_pair(key_pair).into_mm_arc();

        TestCoin::ticker.mock_safe(|_| MockResult::Return("ticker"));
        TestCoin::swap_contract_address.mock_safe(|_| MockResult::Return(None));

        static mut SEARCH_FOR_SWAP_TX_SPEND_MY_CALLED: bool = false;
        TestCoin::search_for_swap_tx_spend_my.mock_safe(|_, _, _, _, _, _, _| {
            unsafe { SEARCH_FOR_SWAP_TX_SPEND_MY_CALLED = true }
            MockResult::Return(Ok(Some(FoundSwapTxSpend::Spent(eth_tx_for_test().into()))))
        });

        static mut SEARCH_FOR_SWAP_TX_SPEND_OTHER_CALLED: bool = false;
        TestCoin::search_for_swap_tx_spend_other.mock_safe(|_, _, _, _, _, _, _| {
            unsafe { SEARCH_FOR_SWAP_TX_SPEND_OTHER_CALLED = true }
            MockResult::Return(Ok(None))
        });

        static mut SEND_MAKER_SPENDS_TAKER_PAYMENT_CALLED: bool = false;
        TestCoin::send_maker_spends_taker_payment.mock_safe(|_, _, _, _, _, _| {
            unsafe { SEND_MAKER_SPENDS_TAKER_PAYMENT_CALLED = true }
            MockResult::Return(Box::new(futures01::future::ok(eth_tx_for_test().into())))
        });

        let maker_coin = MmCoinEnum::Test(TestCoin::default());
        let taker_coin = MmCoinEnum::Test(TestCoin::default());
        let (maker_swap, _) = MakerSwap::load_from_saved(ctx, maker_coin, taker_coin, maker_saved_swap).unwrap();
        let expected = Ok(RecoveredSwap {
            coin: "ticker".into(),
            action: RecoveredSwapAction::SpentOtherPayment,
            transaction: eth_tx_for_test().into(),
        });
        assert_eq!(maker_swap.recover_funds(), expected);
        assert!(unsafe { SEARCH_FOR_SWAP_TX_SPEND_MY_CALLED });
        assert!(unsafe { SEARCH_FOR_SWAP_TX_SPEND_OTHER_CALLED });
        assert!(unsafe { SEND_MAKER_SPENDS_TAKER_PAYMENT_CALLED });
    }

    #[test]
    fn swap_must_not_lock_funds_by_default() {
        use crate::mm2::lp_swap::get_locked_amount;

        let maker_saved_json = r#"{"error_events":["StartFailed","NegotiateFailed","TakerFeeValidateFailed","MakerPaymentTransactionFailed","MakerPaymentDataSendFailed","TakerPaymentValidateFailed","TakerPaymentSpendFailed","TakerPaymentSpendConfirmFailed","MakerPaymentRefunded","MakerPaymentRefundFailed"],"events":[{"event":{"data":{"lock_duration":7800,"maker_amount":"3.54932734","maker_coin":"KMD","maker_coin_start_block":1452970,"maker_payment_confirmations":1,"maker_payment_lock":1563759539,"my_persistent_pub":"031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8","secret":"0000000000000000000000000000000000000000000000000000000000000000","started_at":1563743939,"taker":"101ace6b08605b9424b0582b5cce044b70a3c8d8d10cb2965e039b0967ae92b9","taker_amount":"0.02004833998671660000000000","taker_coin":"ETH","taker_coin_start_block":8196380,"taker_payment_confirmations":1,"uuid":"3447b727-fe93-4357-8e5a-8cf2699b7e86"},"type":"Started"},"timestamp":1563743939211},{"event":{"data":{"taker_payment_locktime":1563751737,"taker_pubkey":"03101ace6b08605b9424b0582b5cce044b70a3c8d8d10cb2965e039b0967ae92b9"},"type":"Negotiated"},"timestamp":1563743979835},{"event":{"data":{"tx_hash":"a59203eb2328827de00bed699a29389792906e4f39fdea145eb40dc6b3821bd6","tx_hex":"f8690284ee6b280082520894d8997941dd1346e9231118d5685d866294f59e5b865af3107a4000801ca0743d2b7c9fad65805d882179062012261be328d7628ae12ee08eff8d7657d993a07eecbd051f49d35279416778faa4664962726d516ce65e18755c9b9406a9c2fd"},"type":"TakerFeeValidated"},"timestamp":1563744052878}],"success_events":["Started","Negotiated","TakerFeeValidated","MakerPaymentSent","TakerPaymentReceived","TakerPaymentWaitConfirmStarted","TakerPaymentValidatedAndConfirmed","TakerPaymentSpent","TakerPaymentSpendConfirmStarted","TakerPaymentSpendConfirmed","Finished"],"uuid":"3447b727-fe93-4357-8e5a-8cf2699b7e86"}"#;
        let maker_saved_swap: MakerSavedSwap = json::from_str(maker_saved_json).unwrap();
        let key_pair =
            key_pair_from_seed("spice describe gravity federal blast come thank unfair canal monkey style afraid")
                .unwrap();
        let ctx = MmCtxBuilder::default().with_secp256k1_key_pair(key_pair).into_mm_arc();

        TestCoin::ticker.mock_safe(|_| MockResult::Return("ticker"));
        TestCoin::swap_contract_address.mock_safe(|_| MockResult::Return(None));
        let maker_coin = MmCoinEnum::Test(TestCoin::default());
        let taker_coin = MmCoinEnum::Test(TestCoin::default());
        let (_maker_swap, _) =
            MakerSwap::load_from_saved(ctx.clone(), maker_coin, taker_coin, maker_saved_swap).unwrap();

        let actual = get_locked_amount(&ctx, "ticker");
        assert_eq!(actual, MmNumber::from(0));
    }

    #[test]
    fn test_recheck_swap_contract_address_if_none() {
        // swap file contains neither maker_coin_swap_contract_address nor taker_coin_swap_contract_address
        let maker_saved_json = r#"{"error_events":["StartFailed","NegotiateFailed","TakerFeeValidateFailed","MakerPaymentTransactionFailed","MakerPaymentDataSendFailed","TakerPaymentValidateFailed","TakerPaymentSpendFailed","TakerPaymentSpendConfirmFailed","MakerPaymentRefunded","MakerPaymentRefundFailed"],"events":[{"event":{"data":{"lock_duration":7800,"maker_amount":"3.54932734","maker_coin":"KMD","maker_coin_start_block":1452970,"maker_payment_confirmations":1,"maker_payment_lock":1563759539,"my_persistent_pub":"031bb83b58ec130e28e0a6d5d2acf2eb01b0d3f1670e021d47d31db8a858219da8","secret":"0000000000000000000000000000000000000000000000000000000000000000","started_at":1563743939,"taker":"101ace6b08605b9424b0582b5cce044b70a3c8d8d10cb2965e039b0967ae92b9","taker_amount":"0.02004833998671660000000000","taker_coin":"ETH","taker_coin_start_block":8196380,"taker_payment_confirmations":1,"uuid":"3447b727-fe93-4357-8e5a-8cf2699b7e86"},"type":"Started"},"timestamp":1563743939211},{"event":{"data":{"taker_payment_locktime":1563751737,"taker_pubkey":"03101ace6b08605b9424b0582b5cce044b70a3c8d8d10cb2965e039b0967ae92b9"},"type":"Negotiated"},"timestamp":1563743979835},{"event":{"data":{"tx_hash":"a59203eb2328827de00bed699a29389792906e4f39fdea145eb40dc6b3821bd6","tx_hex":"f8690284ee6b280082520894d8997941dd1346e9231118d5685d866294f59e5b865af3107a4000801ca0743d2b7c9fad65805d882179062012261be328d7628ae12ee08eff8d7657d993a07eecbd051f49d35279416778faa4664962726d516ce65e18755c9b9406a9c2fd"},"type":"TakerFeeValidated"},"timestamp":1563744052878}],"success_events":["Started","Negotiated","TakerFeeValidated","MakerPaymentSent","TakerPaymentReceived","TakerPaymentWaitConfirmStarted","TakerPaymentValidatedAndConfirmed","TakerPaymentSpent","TakerPaymentSpendConfirmStarted","TakerPaymentSpendConfirmed","Finished"],"uuid":"3447b727-fe93-4357-8e5a-8cf2699b7e86"}"#;
        let maker_saved_swap: MakerSavedSwap = json::from_str(maker_saved_json).unwrap();
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
        let (maker_swap, _) =
            MakerSwap::load_from_saved(ctx.clone(), maker_coin, taker_coin, maker_saved_swap).unwrap();

        assert_eq!(unsafe { SWAP_CONTRACT_ADDRESS_CALLED }, 2);
        assert_eq!(
            maker_swap.r().data.maker_coin_swap_contract_address,
            Some(BytesJson::default())
        );
        assert_eq!(
            maker_swap.r().data.taker_coin_swap_contract_address,
            Some(BytesJson::default())
        );
    }

    #[test]
    fn test_recheck_only_one_swap_contract_address() {
        // swap file contains only maker_coin_swap_contract_address
        let maker_saved_json = r#"{"type":"Maker","uuid":"c52659d7-4e13-41f5-9c1a-30cc2f646033","events":[{"timestamp":1608541830095,"event":{"type":"Started","data":{"taker_coin":"JST","maker_coin":"ETH","taker":"031d4256c4bc9f99ac88bf3dba21773132281f65f9bf23a59928bce08961e2f3","secret":"dc45c1d22028970d8d30d1ddacbfc50eb92403b0d6076c94f2216c4c44512b41","secret_hash":"943e11f7c74e2d6493ef8ad01a06ef2ce9bd1fb3","my_persistent_pub":"03c6a78589e18b482aea046975e6d0acbdea7bf7dbf04d9d5bd67fda917815e3ed","lock_duration":7800,"maker_amount":"0.1","taker_amount":"0.1","maker_payment_confirmations":1,"maker_payment_requires_nota":false,"taker_payment_confirmations":1,"taker_payment_requires_nota":false,"maker_payment_lock":1608557429,"uuid":"c52659d7-4e13-41f5-9c1a-30cc2f646033","started_at":1608541829,"maker_coin_start_block":14353,"taker_coin_start_block":14353,"maker_coin_swap_contract_address":"a09ad3cd7e96586ebd05a2607ee56b56fb2db8fd"}}},{"timestamp":1608541830399,"event":{"type":"Negotiated","data":{"taker_payment_locktime":1608549629,"taker_pubkey":"02031d4256c4bc9f99ac88bf3dba21773132281f65f9bf23a59928bce08961e2f3"}}},{"timestamp":1608541831810,"event":{"type":"TakerFeeValidated","data":{"tx_hex":"f8a7821fb58083033450942b294f029fde858b2c62184e8390591755521d8e80b844a9059cbb000000000000000000000000d8997941dd1346e9231118d5685d866294f59e5b0000000000000000000000000000000000000000000000000000750d557426e01ba06ddad2dfe6933b8d70d5739beb3005c8f367bc72eac4e5609b81c2f8e5843cd9a07fa695cc42f8c6b6a7b10f6ae9e4dca3e750e37f64a85b54dec736236790f05e","tx_hash":"b13c3428f70b46d8c1d7f5863af020a27c380a8ede0927554beabf234998bcc8"}}},{"timestamp":1608541832884,"event":{"type":"MakerPaymentSent","data":{"tx_hex":"f8ef82021980830249f094a09ad3cd7e96586ebd05a2607ee56b56fb2db8fd88016345785d8a0000b884152cf3af7c7ce37fac65bd995eae3d58ccdc367d79f3a10e6ca55f609e6dcefac960982b000000000000000000000000bab36286672fbdc7b250804bf6d14be0df69fa29943e11f7c74e2d6493ef8ad01a06ef2ce9bd1fb3000000000000000000000000000000000000000000000000000000000000000000000000000000005fe0a3751ca03ab6306b8b8875c7d2cbaa71a3991eb8e7ae44e192dc9974cecc1f9dcfe5e4d6a04ec2808db06fe7b246134997fcce81ca201ced1257f1f8e93cacadd6554ca653","tx_hash":"ceba36dff0b2c7aec69cb2d5be7055858e09889959ba63f7957b45a15dceade4"}}},{"timestamp":1608541835207,"event":{"type":"TakerPaymentReceived","data":{"tx_hex":"f90127821fb680830249f094a09ad3cd7e96586ebd05a2607ee56b56fb2db8fd80b8c49b415b2a64bdf61f195a1767f547bb0886ed697f3c1a063ce928ff9a47222c0b5d099200000000000000000000000000000000000000000000000000016345785d8a00000000000000000000000000002b294f029fde858b2c62184e8390591755521d8e0000000000000000000000004b2d0d6c2c785217457b69b922a2a9cea98f71e9943e11f7c74e2d6493ef8ad01a06ef2ce9bd1fb3000000000000000000000000000000000000000000000000000000000000000000000000000000005fe084fd1ba0a5b6ef54217c5a03a588d01410ef1187ce6107bdb075306ced06a06e25a50984a03f541f1f392079ae2590d0f48f2065f8721a8b46c44a060ae53f00bfb5160118","tx_hash":"1247a1be3da89f3612ca33d83d493808388775e2897036f640c0efe69c3b162f"}}},{"timestamp":1608541835208,"event":{"type":"TakerPaymentWaitConfirmStarted"}},{"timestamp":1608541836196,"event":{"type":"TakerPaymentValidatedAndConfirmed"}},{"timestamp":1608541837173,"event":{"type":"TakerPaymentSpent","data":{"tx_hex":"f9010782021a80830249f094a09ad3cd7e96586ebd05a2607ee56b56fb2db8fd80b8a402ed292b64bdf61f195a1767f547bb0886ed697f3c1a063ce928ff9a47222c0b5d099200000000000000000000000000000000000000000000000000016345785d8a0000dc45c1d22028970d8d30d1ddacbfc50eb92403b0d6076c94f2216c4c44512b410000000000000000000000002b294f029fde858b2c62184e8390591755521d8e000000000000000000000000bab36286672fbdc7b250804bf6d14be0df69fa291ba053af89feb4ab066b26e76de9788c85ec1bf14ae6dcbdd7ff53e561e48e1b822ca043796d45bd4233500a120a1571b3fee95a34e8cc6b616c69552da4352c0d8e39","tx_hash":"d9a839c6eead3fbf538eca0a4ec39e28647104920a5c8b9c107524287dd90165"}}},{"timestamp":1608541837175,"event":{"type":"TakerPaymentSpendConfirmStarted"}},{"timestamp":1608541837612,"event":{"type":"TakerPaymentSpendConfirmed"}},{"timestamp":1608541837614,"event":{"type":"Finished"}}],"maker_amount":"0.1","maker_coin":"ETH","taker_amount":"0.1","taker_coin":"JST","gui":"nogui","mm_version":"1a6082121","success_events":["Started","Negotiated","TakerFeeValidated","MakerPaymentSent","TakerPaymentReceived","TakerPaymentWaitConfirmStarted","TakerPaymentValidatedAndConfirmed","TakerPaymentSpent","TakerPaymentSpendConfirmStarted","TakerPaymentSpendConfirmed","Finished"],"error_events":["StartFailed","NegotiateFailed","TakerFeeValidateFailed","MakerPaymentTransactionFailed","MakerPaymentDataSendFailed","MakerPaymentWaitConfirmFailed","TakerPaymentValidateFailed","TakerPaymentWaitConfirmFailed","TakerPaymentSpendFailed","TakerPaymentSpendConfirmFailed","MakerPaymentWaitRefundStarted","MakerPaymentRefunded","MakerPaymentRefundFailed"]}"#;
        let maker_saved_swap: MakerSavedSwap = json::from_str(maker_saved_json).unwrap();
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
        let (maker_swap, _) =
            MakerSwap::load_from_saved(ctx.clone(), maker_coin, taker_coin, maker_saved_swap).unwrap();

        assert_eq!(unsafe { SWAP_CONTRACT_ADDRESS_CALLED }, 1);
        let expected_addr = addr_from_str("0xa09ad3cd7e96586ebd05a2607ee56b56fb2db8fd").unwrap();
        let expected = BytesJson::from(expected_addr.0.as_ref());
        assert_eq!(maker_swap.r().data.maker_coin_swap_contract_address, Some(expected));
        assert_eq!(
            maker_swap.r().data.taker_coin_swap_contract_address,
            Some(BytesJson::default())
        );
    }

    #[test]
    fn test_maker_swap_event_should_ban() {
        let event = MakerSwapEvent::TakerPaymentWaitConfirmFailed("err".into());
        assert!(!event.should_ban_taker());

        let event = MakerSwapEvent::MakerPaymentWaitConfirmFailed("err".into());
        assert!(!event.should_ban_taker());

        let event = MakerSwapEvent::TakerFeeValidateFailed("err".into());
        assert!(event.should_ban_taker());

        let event = MakerSwapEvent::TakerPaymentValidateFailed("err".into());
        assert!(event.should_ban_taker());
    }
}
