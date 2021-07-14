/******************************************************************************
 * Copyright © 2014-2018 The SuperNET Developers.                             *
 *                                                                            *
 * See the AUTHORS, DEVELOPER-AGREEMENT and LICENSE files at                  *
 * the top-level directory of this distribution for the individual copyright  *
 * holder information and the developer policies on copyright and licensing.  *
 *                                                                            *
 * Unless otherwise agreed in a custom licensing agreement, no part of the    *
 * SuperNET software, including this file may be copied, modified, propagated *
 * or distributed except according to the terms contained in the LICENSE file *
 *                                                                            *
 * Removal or modification of this copyright notice is prohibited.            *
 *                                                                            *
 ******************************************************************************/
//
//  coins.rs
//  marketmaker
//

#![allow(uncommon_codepoints)]
#![feature(integer_atomics)]
#![feature(non_ascii_idents)]
#![feature(async_closure)]
#![feature(hash_raw_entry)]

#[macro_use] extern crate common;
#[macro_use] extern crate fomat_macros;
#[macro_use] extern crate gstuff;
#[macro_use] extern crate lazy_static;
#[macro_use] extern crate serde_derive;
#[macro_use] extern crate serde_json;
#[macro_use] extern crate ser_error_derive;

use async_trait::async_trait;
use bigdecimal::{BigDecimal, ParseBigDecimalError};
use common::executor::{spawn, Timer};
use common::mm_ctx::{from_ctx, MmArc};
use common::mm_error::prelude::*;
use common::mm_metrics::MetricsWeak;
use common::mm_number::MmNumber;
use common::{calc_total_pages, now_ms, HttpStatusCode};
use derive_more::Display;
use futures::compat::Future01CompatExt;
use futures::lock::Mutex as AsyncMutex;
use futures::{FutureExt, TryFutureExt};
use futures01::Future;
use http::{Response, StatusCode};
use rpc::v1::types::Bytes as BytesJson;
use serde::{Deserialize, Deserializer};
use serde_json::{self as json, Value as Json};
use std::collections::hash_map::{HashMap, RawEntryMut};
use std::fmt;
use std::num::NonZeroUsize;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;

// using custom copy of try_fus as futures crate was renamed to futures01
macro_rules! try_fus {
    ($e: expr) => {
        match $e {
            Ok(ok) => ok,
            Err(err) => return Box::new(futures01::future::err(ERRL!("{}", err))),
        }
    };
}

macro_rules! try_f {
    ($e: expr) => {
        match $e {
            Ok(ok) => ok,
            Err(e) => return Box::new(futures01::future::err(e)),
        }
    };
}

#[doc(hidden)]
#[cfg(test)]
pub mod coins_tests;

pub mod eth;
use eth::{eth_coin_from_conf_and_request, EthCoin, EthTxFeeDetails, SignedEthTx};

pub mod utxo;
use utxo::qtum::{self, qtum_coin_from_conf_and_request, QtumCoin};
use utxo::utxo_common::big_decimal_from_sat_unsigned;
use utxo::utxo_standard::{utxo_standard_coin_from_conf_and_request, UtxoStandardCoin};
use utxo::{GenerateTxError, UtxoFeeDetails, UtxoTx};

pub mod qrc20;
use qrc20::{qrc20_coin_from_conf_and_request, Qrc20Coin, Qrc20FeeDetails};

#[doc(hidden)]
#[allow(unused_variables)]
pub mod test_coin;
pub use test_coin::TestCoin;

#[cfg(target_arch = "wasm32")] pub mod tx_history_db;

#[cfg(all(not(target_arch = "wasm32"), feature = "zhtlc"))]
pub mod z_coin;
use crate::utxo::UnsupportedAddr;
#[cfg(all(not(target_arch = "wasm32"), feature = "zhtlc"))]
use z_coin::{z_coin_from_conf_and_request, ZCoin};

cfg_native! {
    use async_std::fs;
    use futures::AsyncWriteExt;
    use std::io;
}

cfg_wasm32! {
    use common::indexed_db::{get_or_initialize_db, ConstructibleDb, DbLocked, DbNamespaceId};
    use tx_history_db::TxHistoryDb;

    pub type TxHistoryDbLocked<'a> = DbLocked<'a, TxHistoryDb>;
}

pub type BalanceResult<T> = Result<T, MmError<BalanceError>>;
pub type BalanceFut<T> = Box<dyn Future<Item = T, Error = MmError<BalanceError>> + Send>;
pub type NumConversResult<T> = Result<T, MmError<NumConversError>>;
pub type WithdrawResult = Result<TransactionDetails, MmError<WithdrawError>>;
pub type WithdrawFut = Box<dyn Future<Item = TransactionDetails, Error = MmError<WithdrawError>> + Send>;
pub type TradePreimageResult<T> = Result<T, MmError<TradePreimageError>>;
pub type TradePreimageFut<T> = Box<dyn Future<Item = T, Error = MmError<TradePreimageError>> + Send>;
pub type CoinFindResult<T> = Result<T, MmError<CoinFindError>>;
pub type TxHistoryFut<T> = Box<dyn Future<Item = T, Error = MmError<TxHistoryError>> + Send>;
pub type TxHistoryResult<T> = Result<T, MmError<TxHistoryError>>;

#[derive(Debug, Display)]
pub enum TxHistoryError {
    ErrorSerializing(String),
    ErrorDeserializing(String),
    ErrorSaving(String),
    ErrorLoading(String),
    ErrorClearing(String),
    NotSupported(String),
    InternalError(String),
}

pub trait Transaction: fmt::Debug + 'static {
    /// Raw transaction bytes of the transaction
    fn tx_hex(&self) -> Vec<u8>;
    /// Serializable representation of tx hash for displaying purpose
    fn tx_hash(&self) -> BytesJson;
}

#[derive(Clone, Debug, PartialEq)]
pub enum TransactionEnum {
    UtxoTx(UtxoTx),
    SignedEthTx(SignedEthTx),
}
ifrom!(TransactionEnum, UtxoTx);
ifrom!(TransactionEnum, SignedEthTx);

// NB: When stable and groked by IDEs, `enum_dispatch` can be used instead of `Deref` to speed things up.
impl Deref for TransactionEnum {
    type Target = dyn Transaction;
    fn deref(&self) -> &dyn Transaction {
        match self {
            TransactionEnum::UtxoTx(ref t) => t,
            TransactionEnum::SignedEthTx(ref t) => t,
        }
    }
}

pub type TransactionFut = Box<dyn Future<Item = TransactionEnum, Error = String> + Send>;

#[derive(Debug, PartialEq)]
pub enum FoundSwapTxSpend {
    Spent(TransactionEnum),
    Refunded(TransactionEnum),
}

pub enum CanRefundHtlc {
    CanRefundNow,
    // returns the number of seconds to sleep before HTLC becomes refundable
    HaveToWait(u64),
}

#[derive(Debug, Display, Eq, PartialEq)]
pub enum NegotiateSwapContractAddrErr {
    #[display(fmt = "InvalidOtherAddrLen, addr supplied {:?}", _0)]
    InvalidOtherAddrLen(BytesJson),
    #[display(fmt = "UnexpectedOtherAddr, addr supplied {:?}", _0)]
    UnexpectedOtherAddr(BytesJson),
    NoOtherAddrAndNoFallback,
}

/// Swap operations (mostly based on the Hash/Time locked transactions implemented by coin wallets).
pub trait SwapOps {
    fn send_taker_fee(&self, fee_addr: &[u8], amount: BigDecimal) -> TransactionFut;

    fn send_maker_payment(
        &self,
        time_lock: u32,
        taker_pub: &[u8],
        secret_hash: &[u8],
        amount: BigDecimal,
        swap_contract_address: &Option<BytesJson>,
    ) -> TransactionFut;

    fn send_taker_payment(
        &self,
        time_lock: u32,
        maker_pub: &[u8],
        secret_hash: &[u8],
        amount: BigDecimal,
        swap_contract_address: &Option<BytesJson>,
    ) -> TransactionFut;

    fn send_maker_spends_taker_payment(
        &self,
        taker_payment_tx: &[u8],
        time_lock: u32,
        taker_pub: &[u8],
        secret: &[u8],
        swap_contract_address: &Option<BytesJson>,
    ) -> TransactionFut;

    fn send_taker_spends_maker_payment(
        &self,
        maker_payment_tx: &[u8],
        time_lock: u32,
        maker_pub: &[u8],
        secret: &[u8],
        swap_contract_address: &Option<BytesJson>,
    ) -> TransactionFut;

    fn send_taker_refunds_payment(
        &self,
        taker_payment_tx: &[u8],
        time_lock: u32,
        maker_pub: &[u8],
        secret_hash: &[u8],
        swap_contract_address: &Option<BytesJson>,
    ) -> TransactionFut;

    fn send_maker_refunds_payment(
        &self,
        maker_payment_tx: &[u8],
        time_lock: u32,
        taker_pub: &[u8],
        secret_hash: &[u8],
        swap_contract_address: &Option<BytesJson>,
    ) -> TransactionFut;

    fn validate_fee(
        &self,
        fee_tx: &TransactionEnum,
        expected_sender: &[u8],
        fee_addr: &[u8],
        amount: &BigDecimal,
        min_block_number: u64,
    ) -> Box<dyn Future<Item = (), Error = String> + Send>;

    fn validate_maker_payment(
        &self,
        payment_tx: &[u8],
        time_lock: u32,
        maker_pub: &[u8],
        priv_bn_hash: &[u8],
        amount: BigDecimal,
        swap_contract_address: &Option<BytesJson>,
    ) -> Box<dyn Future<Item = (), Error = String> + Send>;

    fn validate_taker_payment(
        &self,
        payment_tx: &[u8],
        time_lock: u32,
        taker_pub: &[u8],
        priv_bn_hash: &[u8],
        amount: BigDecimal,
        swap_contract_address: &Option<BytesJson>,
    ) -> Box<dyn Future<Item = (), Error = String> + Send>;

    fn check_if_my_payment_sent(
        &self,
        time_lock: u32,
        other_pub: &[u8],
        secret_hash: &[u8],
        search_from_block: u64,
        swap_contract_address: &Option<BytesJson>,
    ) -> Box<dyn Future<Item = Option<TransactionEnum>, Error = String> + Send>;

    fn search_for_swap_tx_spend_my(
        &self,
        time_lock: u32,
        other_pub: &[u8],
        secret_hash: &[u8],
        tx: &[u8],
        search_from_block: u64,
        swap_contract_address: &Option<BytesJson>,
    ) -> Result<Option<FoundSwapTxSpend>, String>;

    fn search_for_swap_tx_spend_other(
        &self,
        time_lock: u32,
        other_pub: &[u8],
        secret_hash: &[u8],
        tx: &[u8],
        search_from_block: u64,
        swap_contract_address: &Option<BytesJson>,
    ) -> Result<Option<FoundSwapTxSpend>, String>;

    fn extract_secret(&self, secret_hash: &[u8], spend_tx: &[u8]) -> Result<Vec<u8>, String>;

    /// Whether the refund transaction can be sent now
    /// For example: there are no additional conditions for ETH, but for some UTXO coins we should wait for
    /// locktime < MTP
    fn can_refund_htlc(&self, locktime: u64) -> Box<dyn Future<Item = CanRefundHtlc, Error = String> + Send + '_> {
        let now = now_ms() / 1000;
        let result = if now > locktime {
            CanRefundHtlc::CanRefundNow
        } else {
            CanRefundHtlc::HaveToWait(locktime - now + 1)
        };
        Box::new(futures01::future::ok(result))
    }

    fn negotiate_swap_contract_addr(
        &self,
        other_side_address: Option<&[u8]>,
    ) -> Result<Option<BytesJson>, MmError<NegotiateSwapContractAddrErr>>;
}

/// Operations that coins have independently from the MarketMaker.
/// That is, things implemented by the coin wallets or public coin services.
pub trait MarketCoinOps {
    fn ticker(&self) -> &str;

    fn my_address(&self) -> Result<String, String>;

    fn my_balance(&self) -> BalanceFut<CoinBalance>;

    fn my_spendable_balance(&self) -> BalanceFut<BigDecimal> {
        Box::new(self.my_balance().map(|CoinBalance { spendable, .. }| spendable))
    }

    /// Base coin balance for tokens, e.g. ETH balance in ERC20 case
    fn base_coin_balance(&self) -> BalanceFut<BigDecimal>;

    /// Receives raw transaction bytes in hexadecimal format as input and returns tx hash in hexadecimal format
    fn send_raw_tx(&self, tx: &str) -> Box<dyn Future<Item = String, Error = String> + Send>;

    fn wait_for_confirmations(
        &self,
        tx: &[u8],
        confirmations: u64,
        requires_nota: bool,
        wait_until: u64,
        check_every: u64,
    ) -> Box<dyn Future<Item = (), Error = String> + Send>;

    fn wait_for_tx_spend(
        &self,
        transaction: &[u8],
        wait_until: u64,
        from_block: u64,
        swap_contract_address: &Option<BytesJson>,
    ) -> TransactionFut;

    fn tx_enum_from_bytes(&self, bytes: &[u8]) -> Result<TransactionEnum, String>;

    fn current_block(&self) -> Box<dyn Future<Item = u64, Error = String> + Send>;

    fn address_from_pubkey_str(&self, pubkey: &str) -> Result<String, String>;

    fn display_priv_key(&self) -> String;

    /// Get the minimum amount to send.
    fn min_tx_amount(&self) -> BigDecimal;

    /// Get the minimum amount to trade.
    fn min_trading_vol(&self) -> MmNumber;
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum WithdrawFee {
    UtxoFixed {
        amount: BigDecimal,
    },
    UtxoPerKbyte {
        amount: BigDecimal,
    },
    EthGas {
        /// in gwei
        gas_price: BigDecimal,
        gas: u64,
    },
    Qrc20Gas {
        /// in satoshi
        gas_limit: u64,
        gas_price: u64,
    },
}

#[allow(dead_code)]
#[derive(Deserialize)]
pub struct WithdrawRequest {
    coin: String,
    to: String,
    #[serde(default)]
    amount: BigDecimal,
    #[serde(default)]
    max: bool,
    fee: Option<WithdrawFee>,
}

impl WithdrawRequest {
    pub fn new_max(coin: String, to: String) -> WithdrawRequest {
        WithdrawRequest {
            coin,
            to,
            amount: 0.into(),
            max: true,
            fee: None,
        }
    }
}

/// Please note that no type should have the same structure as another type,
/// because this enum has the `untagged` deserialization.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "type")]
pub enum TxFeeDetails {
    Utxo(UtxoFeeDetails),
    Eth(EthTxFeeDetails),
    Qrc20(Qrc20FeeDetails),
}

/// Deserialize the TxFeeDetails as an untagged enum.
impl<'de> Deserialize<'de> for TxFeeDetails {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum TxFeeDetailsUnTagged {
            Utxo(UtxoFeeDetails),
            Eth(EthTxFeeDetails),
            Qrc20(Qrc20FeeDetails),
        }

        match Deserialize::deserialize(deserializer)? {
            TxFeeDetailsUnTagged::Utxo(f) => Ok(TxFeeDetails::Utxo(f)),
            TxFeeDetailsUnTagged::Eth(f) => Ok(TxFeeDetails::Eth(f)),
            TxFeeDetailsUnTagged::Qrc20(f) => Ok(TxFeeDetails::Qrc20(f)),
        }
    }
}

impl From<EthTxFeeDetails> for TxFeeDetails {
    fn from(eth_details: EthTxFeeDetails) -> Self { TxFeeDetails::Eth(eth_details) }
}

impl From<UtxoFeeDetails> for TxFeeDetails {
    fn from(utxo_details: UtxoFeeDetails) -> Self { TxFeeDetails::Utxo(utxo_details) }
}

impl From<Qrc20FeeDetails> for TxFeeDetails {
    fn from(qrc20_details: Qrc20FeeDetails) -> Self { TxFeeDetails::Qrc20(qrc20_details) }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct KmdRewardsDetails {
    amount: BigDecimal,
    claimed_by_me: bool,
}

impl KmdRewardsDetails {
    pub fn claimed_by_me(amount: BigDecimal) -> KmdRewardsDetails {
        KmdRewardsDetails {
            amount,
            claimed_by_me: true,
        }
    }
}

/// Transaction details
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct TransactionDetails {
    /// Raw bytes of signed transaction in hexadecimal string, this should be sent as is to send_raw_transaction RPC to broadcast the transaction
    pub tx_hex: BytesJson,
    /// Transaction hash in hexadecimal format
    tx_hash: BytesJson,
    /// Coins are sent from these addresses
    from: Vec<String>,
    /// Coins are sent to these addresses
    to: Vec<String>,
    /// Total tx amount
    total_amount: BigDecimal,
    /// The amount spent from "my" address
    spent_by_me: BigDecimal,
    /// The amount received by "my" address
    received_by_me: BigDecimal,
    /// Resulting "my" balance change
    my_balance_change: BigDecimal,
    /// Block height
    block_height: u64,
    /// Transaction timestamp
    timestamp: u64,
    /// Every coin can has specific fee details:
    /// In UTXO tx fee is paid with the coin itself (e.g. 1 BTC and 0.0001 BTC fee).
    /// But for ERC20 token transfer fee is paid with another coin: ETH, because it's ETH smart contract function call that requires gas to be burnt.
    fee_details: Option<TxFeeDetails>,
    /// The coin transaction belongs to
    coin: String,
    /// Internal MM2 id used for internal transaction identification, for some coins it might be equal to transaction hash
    internal_id: BytesJson,
    /// Amount of accrued rewards.
    #[serde(skip_serializing_if = "Option::is_none")]
    kmd_rewards: Option<KmdRewardsDetails>,
}

impl TransactionDetails {
    /// Whether the transaction details block height should be updated (when tx is confirmed)
    pub fn should_update_block_height(&self) -> bool {
        // checking for std::u64::MAX because there was integer overflow
        // in case of electrum returned -1 so there could be records with MAX confirmations
        self.block_height == 0 || self.block_height == std::u64::MAX
    }

    /// Whether the transaction timestamp should be updated (when tx is confirmed)
    pub fn should_update_timestamp(&self) -> bool {
        // checking for std::u64::MAX because there was integer overflow
        // in case of electrum returned -1 so there could be records with MAX confirmations
        self.timestamp == 0
    }

    pub fn should_update_kmd_rewards(&self) -> bool { self.coin == "KMD" && self.kmd_rewards.is_none() }

    pub fn firo_negative_fee(&self) -> bool {
        match &self.fee_details {
            Some(TxFeeDetails::Utxo(utxo)) => utxo.amount < 0.into() && self.coin == "FIRO",
            _ => false,
        }
    }

    pub fn should_update(&self) -> bool {
        self.should_update_block_height()
            || self.should_update_timestamp()
            || self.should_update_kmd_rewards()
            || self.firo_negative_fee()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct TradeFee {
    pub coin: String,
    pub amount: MmNumber,
    pub paid_from_trading_vol: bool,
}

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub struct CoinBalance {
    pub spendable: BigDecimal,
    pub unspendable: BigDecimal,
}

/// The approximation is needed to cover the dynamic miner fee changing during a swap.
#[derive(Clone, Debug)]
pub enum FeeApproxStage {
    /// Do not increase the trade fee.
    WithoutApprox,
    /// Increase the trade fee slightly.
    StartSwap,
    /// Increase the trade fee significantly.
    OrderIssue,
    /// Increase the trade fee largely.
    TradePreimage,
}

#[derive(Debug)]
pub enum TradePreimageValue {
    Exact(BigDecimal),
    UpperBound(BigDecimal),
}

#[derive(Debug, Display)]
pub enum TradePreimageError {
    #[display(
        fmt = "Not enough {} to preimage the trade: available {}, required at least {}",
        coin,
        available,
        required
    )]
    NotSufficientBalance {
        coin: String,
        available: BigDecimal,
        required: BigDecimal,
    },
    #[display(fmt = "The amount {} less than minimum transaction amount {}", amount, threshold)]
    AmountIsTooSmall { amount: BigDecimal, threshold: BigDecimal },
    #[display(fmt = "Transport error: {}", _0)]
    Transport(String),
    #[display(fmt = "Internal error: {}", _0)]
    InternalError(String),
}

impl From<NumConversError> for TradePreimageError {
    fn from(e: NumConversError) -> Self { TradePreimageError::InternalError(e.to_string()) }
}

impl TradePreimageError {
    /// Construct [`TradePreimageError`] from [`GenerateTxError`] using additional `coin` and `decimals`.
    pub fn from_generate_tx_error(
        gen_tx_err: GenerateTxError,
        coin: String,
        decimals: u8,
        is_upper_bound: bool,
    ) -> TradePreimageError {
        match gen_tx_err {
            GenerateTxError::EmptyUtxoSet { required } => {
                let required = big_decimal_from_sat_unsigned(required, decimals);
                TradePreimageError::NotSufficientBalance {
                    coin,
                    available: BigDecimal::from(0),
                    required,
                }
            },
            GenerateTxError::EmptyOutputs => TradePreimageError::InternalError(gen_tx_err.to_string()),
            GenerateTxError::OutputValueLessThanDust { value, dust } => {
                if is_upper_bound {
                    // If the preimage value is [`TradePreimageValue::UpperBound`], then we had to pass the account balance as the output value.
                    let error = format!(
                        "Output value {} (equal to the account balance) less than dust {}. Probably, dust is not set or outdated",
                        value, dust
                    );
                    TradePreimageError::InternalError(error)
                } else {
                    let amount = big_decimal_from_sat_unsigned(value, decimals);
                    let threshold = big_decimal_from_sat_unsigned(dust, decimals);
                    TradePreimageError::AmountIsTooSmall { amount, threshold }
                }
            },
            GenerateTxError::DeductFeeFromOutputFailed {
                output_value, required, ..
            } => {
                let available = big_decimal_from_sat_unsigned(output_value, decimals);
                let required = big_decimal_from_sat_unsigned(required, decimals);
                TradePreimageError::NotSufficientBalance {
                    coin,
                    available,
                    required,
                }
            },
            GenerateTxError::NotEnoughUtxos { sum_utxos, required } => {
                let available = big_decimal_from_sat_unsigned(sum_utxos, decimals);
                let required = big_decimal_from_sat_unsigned(required, decimals);
                TradePreimageError::NotSufficientBalance {
                    coin,
                    available,
                    required,
                }
            },
            GenerateTxError::Transport(e) => TradePreimageError::Transport(e),
            GenerateTxError::Internal(e) => TradePreimageError::InternalError(e),
        }
    }
}

/// The reason of unsuccessful conversion of two internal numbers, e.g. `u64` from `BigNumber`.
#[derive(Debug, Display)]
pub struct NumConversError(String);

impl From<ParseBigDecimalError> for NumConversError {
    fn from(e: ParseBigDecimalError) -> Self { NumConversError::new(e.to_string()) }
}

impl NumConversError {
    pub fn new(description: String) -> NumConversError { NumConversError(description) }

    pub fn description(&self) -> &str { &self.0 }
}

#[derive(Debug, Display, PartialEq)]
pub enum BalanceError {
    #[display(fmt = "Transport: {}", _0)]
    Transport(String),
    #[display(fmt = "Invalid response: {}", _0)]
    InvalidResponse(String),
    #[display(fmt = "Internal: {}", _0)]
    Internal(String),
}

impl From<NumConversError> for BalanceError {
    fn from(e: NumConversError) -> Self { BalanceError::Internal(e.to_string()) }
}

#[derive(Debug, Deserialize, Display, Serialize, SerializeErrorType)]
#[serde(tag = "error_type", content = "error_data")]
pub enum WithdrawError {
    #[display(
        fmt = "Not enough {} to withdraw: available {}, required at least {}",
        coin,
        available,
        required
    )]
    NotSufficientBalance {
        coin: String,
        available: BigDecimal,
        required: BigDecimal,
    },
    #[display(fmt = "Balance is zero")]
    ZeroBalanceToWithdrawMax,
    #[display(fmt = "The amount {} is too small, required at least {}", amount, threshold)]
    AmountTooLow { amount: BigDecimal, threshold: BigDecimal },
    #[display(fmt = "Invalid address: {}", _0)]
    InvalidAddress(String),
    #[display(fmt = "Invalid fee policy: {}", _0)]
    InvalidFeePolicy(String),
    #[display(fmt = "No such coin {}", coin)]
    NoSuchCoin { coin: String },
    #[display(fmt = "Transport error: {}", _0)]
    Transport(String),
    #[display(fmt = "Internal error: {}", _0)]
    InternalError(String),
}

impl HttpStatusCode for WithdrawError {
    fn status_code(&self) -> StatusCode {
        match self {
            WithdrawError::NotSufficientBalance { .. }
            | WithdrawError::ZeroBalanceToWithdrawMax
            | WithdrawError::AmountTooLow { .. }
            | WithdrawError::InvalidAddress(_)
            | WithdrawError::InvalidFeePolicy(_)
            | WithdrawError::NoSuchCoin { .. } => StatusCode::BAD_REQUEST,
            WithdrawError::Transport(_) | WithdrawError::InternalError(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl From<NumConversError> for WithdrawError {
    fn from(e: NumConversError) -> Self { WithdrawError::InternalError(e.to_string()) }
}

impl From<BalanceError> for WithdrawError {
    fn from(e: BalanceError) -> Self {
        match e {
            BalanceError::Transport(error) | BalanceError::InvalidResponse(error) => WithdrawError::Transport(error),
            BalanceError::Internal(internal) => WithdrawError::InternalError(internal),
        }
    }
}

impl From<CoinFindError> for WithdrawError {
    fn from(e: CoinFindError) -> Self {
        match e {
            CoinFindError::NoSuchCoin { coin } => WithdrawError::NoSuchCoin { coin },
        }
    }
}

impl From<UnsupportedAddr> for WithdrawError {
    fn from(e: UnsupportedAddr) -> Self { WithdrawError::InvalidAddress(e.to_string()) }
}

impl WithdrawError {
    /// Construct [`WithdrawError`] from [`GenerateTxError`] using additional `coin` and `decimals`.
    pub fn from_generate_tx_error(gen_tx_err: GenerateTxError, coin: String, decimals: u8) -> WithdrawError {
        match gen_tx_err {
            GenerateTxError::EmptyUtxoSet { required } => {
                let required = big_decimal_from_sat_unsigned(required, decimals);
                WithdrawError::NotSufficientBalance {
                    coin,
                    available: BigDecimal::from(0),
                    required,
                }
            },
            GenerateTxError::EmptyOutputs => WithdrawError::InternalError(gen_tx_err.to_string()),
            GenerateTxError::OutputValueLessThanDust { value, dust } => {
                let amount = big_decimal_from_sat_unsigned(value, decimals);
                let threshold = big_decimal_from_sat_unsigned(dust, decimals);
                WithdrawError::AmountTooLow { amount, threshold }
            },
            GenerateTxError::DeductFeeFromOutputFailed {
                output_value, required, ..
            } => {
                let available = big_decimal_from_sat_unsigned(output_value, decimals);
                let required = big_decimal_from_sat_unsigned(required, decimals);
                WithdrawError::NotSufficientBalance {
                    coin,
                    available,
                    required,
                }
            },
            GenerateTxError::NotEnoughUtxos { sum_utxos, required } => {
                let available = big_decimal_from_sat_unsigned(sum_utxos, decimals);
                let required = big_decimal_from_sat_unsigned(required, decimals);
                WithdrawError::NotSufficientBalance {
                    coin,
                    available,
                    required,
                }
            },
            GenerateTxError::Transport(e) => WithdrawError::Transport(e),
            GenerateTxError::Internal(e) => WithdrawError::InternalError(e),
        }
    }
}

/// NB: Implementations are expected to follow the pImpl idiom, providing cheap reference-counted cloning and garbage collection.
pub trait MmCoin: SwapOps + MarketCoinOps + fmt::Debug + Send + Sync + 'static {
    // `MmCoin` is an extension fulcrum for something that doesn't fit the `MarketCoinOps`. Practical examples:
    // name (might be required for some APIs, CoinMarketCap for instance);
    // coin statistics that we might want to share with UI;
    // state serialization, to get full rewind and debugging information about the coins participating in a SWAP operation.
    // status/availability check: https://github.com/artemii235/SuperNET/issues/156#issuecomment-446501816

    fn is_asset_chain(&self) -> bool;

    /// The coin can be initialized, but it cannot participate in the swaps.
    fn wallet_only(&self, ctx: &MmArc) -> bool {
        let coin_conf = coin_conf(&ctx, &self.ticker());
        coin_conf["wallet_only"].as_bool().unwrap_or(false)
    }

    fn withdraw(&self, req: WithdrawRequest) -> WithdrawFut;

    /// Maximum number of digits after decimal point used to denominate integer coin units (satoshis, wei, etc.)
    fn decimals(&self) -> u8;

    /// Convert input address to the specified address format.
    fn convert_to_address(&self, from: &str, to_address_format: Json) -> Result<String, String>;

    fn validate_address(&self, address: &str) -> ValidateAddressResult;

    /// Loop collecting coin transaction history and saving it to local DB
    fn process_history_loop(&self, ctx: MmArc) -> Box<dyn Future<Item = (), Error = ()> + Send>;

    /// Path to tx history file
    fn tx_history_path(&self, ctx: &MmArc) -> PathBuf {
        let my_address = self.my_address().unwrap_or_default();
        // BCH cash address format has colon after prefix, e.g. bitcoincash:
        // Colon can't be used in file names on Windows so it should be escaped
        let my_address = my_address.replace(":", "_");
        ctx.dbdir()
            .join("TRANSACTIONS")
            .join(format!("{}_{}.json", self.ticker(), my_address))
    }

    /// Loads existing tx history from file, returns empty vector if file is not found
    /// Cleans the existing file if deserialization fails
    fn load_history_from_file(&self, ctx: &MmArc) -> TxHistoryFut<Vec<TransactionDetails>> {
        load_history_from_file_impl(self, ctx)
    }

    fn save_history_to_file(&self, ctx: &MmArc, history: Vec<TransactionDetails>) -> TxHistoryFut<()> {
        save_history_to_file_impl(self, ctx, history)
    }

    /// Transaction history background sync status
    fn history_sync_status(&self) -> HistorySyncState;

    /// Get fee to be paid per 1 swap transaction
    fn get_trade_fee(&self) -> Box<dyn Future<Item = TradeFee, Error = String> + Send>;

    /// Get fee to be paid by sender per whole swap using the sending value and check if the wallet has sufficient balance to pay the fee.
    fn get_sender_trade_fee(&self, value: TradePreimageValue, stage: FeeApproxStage) -> TradePreimageFut<TradeFee>;

    /// Get fee to be paid by receiver per whole swap and check if the wallet has sufficient balance to pay the fee.
    fn get_receiver_trade_fee(&self, stage: FeeApproxStage) -> TradePreimageFut<TradeFee>;

    /// Get transaction fee the Taker has to pay to send a `TakerFee` transaction and check if the wallet has sufficient balance to pay the fee.
    fn get_fee_to_send_taker_fee(
        &self,
        dex_fee_amount: BigDecimal,
        stage: FeeApproxStage,
    ) -> TradePreimageFut<TradeFee>;

    /// required transaction confirmations number to ensure double-spend safety
    fn required_confirmations(&self) -> u64;

    /// whether coin requires notarization to ensure double-spend safety
    fn requires_notarization(&self) -> bool;

    /// set required transaction confirmations number
    fn set_required_confirmations(&self, confirmations: u64);

    /// set requires notarization
    fn set_requires_notarization(&self, requires_nota: bool);

    /// Get swap contract address if the coin uses it in Atomic Swaps.
    fn swap_contract_address(&self) -> Option<BytesJson>;

    /// The minimum number of confirmations at which a transaction is considered mature.
    fn mature_confirmations(&self) -> Option<u32>;
}

#[derive(Clone, Debug)]
pub enum MmCoinEnum {
    UtxoCoin(UtxoStandardCoin),
    QtumCoin(QtumCoin),
    Qrc20Coin(Qrc20Coin),
    EthCoin(EthCoin),
    #[cfg(all(not(target_arch = "wasm32"), feature = "zhtlc"))]
    ZCoin(ZCoin),
    Test(TestCoin),
}

impl From<UtxoStandardCoin> for MmCoinEnum {
    fn from(c: UtxoStandardCoin) -> MmCoinEnum { MmCoinEnum::UtxoCoin(c) }
}

impl From<EthCoin> for MmCoinEnum {
    fn from(c: EthCoin) -> MmCoinEnum { MmCoinEnum::EthCoin(c) }
}

impl From<TestCoin> for MmCoinEnum {
    fn from(c: TestCoin) -> MmCoinEnum { MmCoinEnum::Test(c) }
}

impl From<QtumCoin> for MmCoinEnum {
    fn from(coin: QtumCoin) -> Self { MmCoinEnum::QtumCoin(coin) }
}

impl From<Qrc20Coin> for MmCoinEnum {
    fn from(c: Qrc20Coin) -> MmCoinEnum { MmCoinEnum::Qrc20Coin(c) }
}

#[cfg(all(not(target_arch = "wasm32"), feature = "zhtlc"))]
impl From<ZCoin> for MmCoinEnum {
    fn from(c: ZCoin) -> MmCoinEnum { MmCoinEnum::ZCoin(c) }
}

// NB: When stable and groked by IDEs, `enum_dispatch` can be used instead of `Deref` to speed things up.
impl Deref for MmCoinEnum {
    type Target = dyn MmCoin;
    fn deref(&self) -> &dyn MmCoin {
        match self {
            MmCoinEnum::UtxoCoin(ref c) => c,
            MmCoinEnum::QtumCoin(ref c) => c,
            MmCoinEnum::Qrc20Coin(ref c) => c,
            MmCoinEnum::EthCoin(ref c) => c,
            #[cfg(all(not(target_arch = "wasm32"), feature = "zhtlc"))]
            MmCoinEnum::ZCoin(ref c) => c,
            MmCoinEnum::Test(ref c) => c,
        }
    }
}

#[async_trait]
pub trait BalanceTradeFeeUpdatedHandler {
    async fn balance_updated(&self, coin: &MmCoinEnum, new_balance: &BigDecimal);
}

struct CoinsContext {
    /// A map from a currency ticker symbol to the corresponding coin.
    /// Similar to `LP_coins`.
    coins: AsyncMutex<HashMap<String, MmCoinEnum>>,
    balance_update_handlers: AsyncMutex<Vec<Box<dyn BalanceTradeFeeUpdatedHandler + Send + Sync>>>,
    #[cfg(target_arch = "wasm32")]
    db_namespace: DbNamespaceId,
    #[cfg(target_arch = "wasm32")]
    /// The database has to be initialized only once!
    tx_history_db: ConstructibleDb<TxHistoryDb>,
}
impl CoinsContext {
    /// Obtains a reference to this crate context, creating it if necessary.
    fn from_ctx(ctx: &MmArc) -> Result<Arc<CoinsContext>, String> {
        Ok(try_s!(from_ctx(&ctx.coins_ctx, move || {
            Ok(CoinsContext {
                coins: AsyncMutex::new(HashMap::new()),
                balance_update_handlers: AsyncMutex::new(vec![]),
                #[cfg(target_arch = "wasm32")]
                tx_history_db: AsyncMutex::new(None),
                #[cfg(target_arch = "wasm32")]
                db_namespace: ctx.db_namespace,
            })
        })))
    }

    #[cfg(target_arch = "wasm32")]
    async fn tx_history_db(&self) -> TxHistoryResult<TxHistoryDbLocked<'_>> {
        Ok(get_or_initialize_db(&self.tx_history_db, self.db_namespace).await?)
    }
}

#[allow(clippy::upper_case_acronyms)]
#[derive(Serialize, Deserialize)]
#[serde(tag = "type", content = "protocol_data")]
pub enum CoinProtocol {
    UTXO,
    QTUM,
    QRC20 {
        platform: String,
        contract_address: String,
    },
    ETH,
    ERC20 {
        platform: String,
        contract_address: String,
    },
    #[cfg(all(not(target_arch = "wasm32"), feature = "zhtlc"))]
    ZHTLC,
}

pub type RpcTransportEventHandlerShared = Arc<dyn RpcTransportEventHandler + Send + Sync + 'static>;

/// Common methods to measure the outgoing requests and incoming responses statistics.
pub trait RpcTransportEventHandler {
    fn debug_info(&self) -> String;

    fn on_outgoing_request(&self, data: &[u8]);

    fn on_incoming_response(&self, data: &[u8]);

    fn on_connected(&self, address: String) -> Result<(), String>;
}

impl fmt::Debug for dyn RpcTransportEventHandler + Send + Sync {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "{}", self.debug_info()) }
}

impl RpcTransportEventHandler for RpcTransportEventHandlerShared {
    fn debug_info(&self) -> String { self.deref().debug_info() }

    fn on_outgoing_request(&self, data: &[u8]) { self.as_ref().on_outgoing_request(data) }

    fn on_incoming_response(&self, data: &[u8]) { self.as_ref().on_incoming_response(data) }

    fn on_connected(&self, address: String) -> Result<(), String> { self.as_ref().on_connected(address) }
}

impl<T: RpcTransportEventHandler> RpcTransportEventHandler for Vec<T> {
    fn debug_info(&self) -> String {
        let selfi: Vec<String> = self.iter().map(|x| x.debug_info()).collect();
        format!("{:?}", selfi)
    }

    fn on_outgoing_request(&self, data: &[u8]) {
        for handler in self {
            handler.on_outgoing_request(data)
        }
    }

    fn on_incoming_response(&self, data: &[u8]) {
        for handler in self {
            handler.on_incoming_response(data)
        }
    }

    fn on_connected(&self, address: String) -> Result<(), String> {
        for handler in self {
            try_s!(handler.on_connected(address.clone()))
        }
        Ok(())
    }
}

pub enum RpcClientType {
    Native,
    Electrum,
    Ethereum,
}

impl ToString for RpcClientType {
    fn to_string(&self) -> String {
        match self {
            RpcClientType::Native => "native".into(),
            RpcClientType::Electrum => "electrum".into(),
            RpcClientType::Ethereum => "ethereum".into(),
        }
    }
}

#[derive(Clone)]
pub struct CoinTransportMetrics {
    /// Using a weak reference by default in order to avoid circular references and leaks.
    metrics: MetricsWeak,
    /// Name of coin the rpc client is intended to work with.
    ticker: String,
    /// RPC client type.
    client: String,
}

impl CoinTransportMetrics {
    fn new(metrics: MetricsWeak, ticker: String, client: RpcClientType) -> CoinTransportMetrics {
        CoinTransportMetrics {
            metrics,
            ticker,
            client: client.to_string(),
        }
    }

    fn into_shared(self) -> RpcTransportEventHandlerShared { Arc::new(self) }
}

impl RpcTransportEventHandler for CoinTransportMetrics {
    fn debug_info(&self) -> String { "CoinTransportMetrics".into() }

    fn on_outgoing_request(&self, data: &[u8]) {
        mm_counter!(self.metrics, "rpc_client.traffic.out", data.len() as u64,
            "coin" => self.ticker.clone(), "client" => self.client.clone());
        mm_counter!(self.metrics, "rpc_client.request.count", 1,
            "coin" => self.ticker.clone(), "client" => self.client.clone());
    }

    fn on_incoming_response(&self, data: &[u8]) {
        mm_counter!(self.metrics, "rpc_client.traffic.in", data.len() as u64,
            "coin" => self.ticker.clone(), "client" => self.client.clone());
        mm_counter!(self.metrics, "rpc_client.response.count", 1,
            "coin" => self.ticker.clone(), "client" => self.client.clone());
    }

    fn on_connected(&self, _address: String) -> Result<(), String> {
        // Handle a new connected endpoint if necessary.
        // Now just return the Ok
        Ok(())
    }
}

#[async_trait]
impl BalanceTradeFeeUpdatedHandler for CoinsContext {
    async fn balance_updated(&self, coin: &MmCoinEnum, new_balance: &BigDecimal) {
        for sub in self.balance_update_handlers.lock().await.iter() {
            sub.balance_updated(coin, new_balance).await
        }
    }
}

pub fn coin_conf(ctx: &MmArc, ticker: &str) -> Json {
    match ctx.conf["coins"].as_array() {
        Some(coins) => coins
            .iter()
            .find(|coin| coin["coin"].as_str() == Some(ticker))
            .cloned()
            .unwrap_or(Json::Null),
        None => Json::Null,
    }
}

pub fn is_wallet_only_conf(conf: &Json) -> bool { conf["wallet_only"].as_bool().unwrap_or(false) }

pub fn is_wallet_only_ticker(ctx: &MmArc, ticker: &str) -> bool {
    let coin_conf = coin_conf(ctx, ticker);
    coin_conf["wallet_only"].as_bool().unwrap_or(false)
}

/// Adds a new currency into the list of currencies configured.
///
/// Returns an error if the currency already exists. Initializing the same currency twice is a bad habit
/// (might lead to misleading and confusing information during debugging and maintenance, see DRY)
/// and should be fixed on the call site.
///
/// * `req` - Payload of the corresponding "enable" or "electrum" RPC request.
pub async fn lp_coininit(ctx: &MmArc, ticker: &str, req: &Json) -> Result<MmCoinEnum, String> {
    let cctx = try_s!(CoinsContext::from_ctx(ctx));
    {
        let coins = cctx.coins.lock().await;
        if coins.get(ticker).is_some() {
            return ERR!("Coin {} already initialized", ticker);
        }
    }

    let coins_en = coin_conf(ctx, ticker);

    if coins_en.is_null() {
        ctx.log.log(
            "😅",
            #[allow(clippy::unnecessary_cast)]
            &[&("coin" as &str), &ticker, &("no-conf" as &str)],
            &fomat! ("Warning, coin " (ticker) " is used without a corresponding configuration."),
        );
    }

    if coins_en["mm2"].is_null() && req["mm2"].is_null() {
        return ERR!(concat!(
            "mm2 param is not set neither in coins config nor enable request, ",
            "assuming that coin is not supported"
        ));
    }
    let secret = &*ctx.secp256k1_key_pair().private().secret;

    if coins_en["protocol"].is_null() {
        return ERR!(
            r#""protocol" field is missing in coins file. The file format is deprecated, please execute ./mm2 update_config command to convert it or download a new one"#
        );
    }
    let protocol: CoinProtocol = try_s!(json::from_value(coins_en["protocol"].clone()));

    let coin: MmCoinEnum = match &protocol {
        CoinProtocol::UTXO => {
            try_s!(utxo_standard_coin_from_conf_and_request(ctx, ticker, &coins_en, req, secret).await).into()
        },
        CoinProtocol::QTUM => try_s!(qtum_coin_from_conf_and_request(ctx, ticker, &coins_en, req, secret).await).into(),
        CoinProtocol::ETH | CoinProtocol::ERC20 { .. } => {
            try_s!(eth_coin_from_conf_and_request(ctx, ticker, &coins_en, req, secret, protocol).await).into()
        },
        CoinProtocol::QRC20 {
            platform,
            contract_address,
        } => {
            let contract_address = try_s!(qtum::contract_addr_from_str(&contract_address));
            try_s!(
                qrc20_coin_from_conf_and_request(ctx, ticker, &platform, &coins_en, req, secret, contract_address)
                    .await
            )
            .into()
        },
        #[cfg(all(not(target_arch = "wasm32"), feature = "zhtlc"))]
        CoinProtocol::ZHTLC => try_s!(z_coin_from_conf_and_request(ctx, ticker, &coins_en, req, secret).await).into(),
    };

    let block_count = try_s!(coin.current_block().compat().await);
    // TODO, #156: Warn the user when we know that the wallet is under-initialized.
    log! ([=ticker] if !coins_en["etomic"].is_null() {", etomic"} ", " [=block_count]);
    // TODO AP: locking the coins list during the entire initialization prevents different coins from being
    // activated concurrently which results in long activation time: https://github.com/KomodoPlatform/atomicDEX/issues/24
    // So I'm leaving the possibility of race condition intentionally in favor of faster concurrent activation.
    // Should consider refactoring: maybe extract the RPC client initialization part from coin init functions.
    let mut coins = cctx.coins.lock().await;
    match coins.raw_entry_mut().from_key(ticker) {
        RawEntryMut::Occupied(_oe) => return ERR!("Coin {} already initialized", ticker),
        RawEntryMut::Vacant(ve) => ve.insert(ticker.to_string(), coin.clone()),
    };
    let history = req["tx_history"].as_bool().unwrap_or(false);
    if history {
        try_s!(lp_spawn_tx_history(ctx.clone(), coin.clone()));
    }
    let ctxʹ = ctx.clone();
    let ticker = ticker.to_owned();
    spawn(async move { check_balance_update_loop(ctxʹ, ticker).await });
    Ok(coin)
}

#[cfg(not(target_arch = "wasm32"))]
fn lp_spawn_tx_history(ctx: MmArc, coin: MmCoinEnum) -> Result<(), String> {
    try_s!(std::thread::Builder::new()
        .name(format!("tx_history_{}", coin.ticker()))
        .spawn(move || coin.process_history_loop(ctx).wait()));
    Ok(())
}

#[cfg(target_arch = "wasm32")]
fn lp_spawn_tx_history(ctx: MmArc, coin: MmCoinEnum) -> Result<(), String> {
    let fut = async move {
        let _res = coin.process_history_loop(ctx).compat().await;
    };
    common::executor::spawn_local(fut);
    Ok(())
}

/// NB: Returns only the enabled (aka active) coins.
pub async fn lp_coinfind(ctx: &MmArc, ticker: &str) -> Result<Option<MmCoinEnum>, String> {
    let cctx = try_s!(CoinsContext::from_ctx(ctx));
    let coins = cctx.coins.lock().await;
    Ok(coins.get(ticker).cloned())
}

/// Attempts to find a pair of active coins returning None if one is not enabled
pub async fn find_pair(ctx: &MmArc, base: &str, rel: &str) -> Result<Option<(MmCoinEnum, MmCoinEnum)>, String> {
    let fut_base = lp_coinfind(ctx, base);
    let fut_rel = lp_coinfind(ctx, rel);

    futures::future::try_join(fut_base, fut_rel)
        .map_ok(|(base, rel)| base.zip(rel))
        .await
}

#[derive(Display)]
pub enum CoinFindError {
    #[display(fmt = "No such coin: {}", coin)]
    NoSuchCoin { coin: String },
}

pub async fn lp_coinfind_or_err(ctx: &MmArc, ticker: &str) -> CoinFindResult<MmCoinEnum> {
    match lp_coinfind(ctx, ticker).await {
        Ok(Some(coin)) => Ok(coin),
        Ok(None) => MmError::err(CoinFindError::NoSuchCoin {
            coin: ticker.to_owned(),
        }),
        Err(e) => panic!("Unexpected error: {}", e),
    }
}

#[derive(Deserialize)]
struct ConvertAddressReq {
    coin: String,
    from: String,
    /// format to that the input address should be converted
    to_address_format: Json,
}

pub async fn convert_address(ctx: MmArc, req: Json) -> Result<Response<Vec<u8>>, String> {
    let req: ConvertAddressReq = try_s!(json::from_value(req));
    let coin = match lp_coinfind(&ctx, &req.coin).await {
        Ok(Some(t)) => t,
        Ok(None) => return ERR!("No such coin: {}", req.coin),
        Err(err) => return ERR!("!lp_coinfind({}): {}", req.coin, err),
    };
    let result = json!({
        "result": {
            "address": try_s!(coin.convert_to_address(&req.from, req.to_address_format)),
        },
    });
    let body = try_s!(json::to_vec(&result));
    Ok(try_s!(Response::builder().body(body)))
}

pub async fn kmd_rewards_info(ctx: MmArc) -> Result<Response<Vec<u8>>, String> {
    let coin = match lp_coinfind(&ctx, "KMD").await {
        Ok(Some(MmCoinEnum::UtxoCoin(t))) => t,
        Ok(Some(_)) => return ERR!("KMD was expected to be UTXO"),
        Ok(None) => return ERR!("KMD is not activated"),
        Err(err) => return ERR!("!lp_coinfind({}): KMD", err),
    };

    let res = json!({
        "result": try_s!(utxo::kmd_rewards_info(&coin).await),
    });
    let res = try_s!(json::to_vec(&res));
    Ok(try_s!(Response::builder().body(res)))
}

#[derive(Deserialize)]
struct ValidateAddressReq {
    coin: String,
    address: String,
}

#[derive(Serialize)]
pub struct ValidateAddressResult {
    is_valid: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
}

pub async fn validate_address(ctx: MmArc, req: Json) -> Result<Response<Vec<u8>>, String> {
    let req: ValidateAddressReq = try_s!(json::from_value(req));
    let coin = match lp_coinfind(&ctx, &req.coin).await {
        Ok(Some(t)) => t,
        Ok(None) => return ERR!("No such coin: {}", req.coin),
        Err(err) => return ERR!("!lp_coinfind({}): {}", req.coin, err),
    };

    let res = json!({ "result": coin.validate_address(&req.address) });
    let body = try_s!(json::to_vec(&res));
    Ok(try_s!(Response::builder().body(body)))
}

pub async fn withdraw(ctx: MmArc, req: WithdrawRequest) -> WithdrawResult {
    let coin = lp_coinfind_or_err(&ctx, &req.coin).await?;
    coin.withdraw(req).compat().await
}

pub async fn send_raw_transaction(ctx: MmArc, req: Json) -> Result<Response<Vec<u8>>, String> {
    let ticker = try_s!(req["coin"].as_str().ok_or("No 'coin' field")).to_owned();
    let coin = match lp_coinfind(&ctx, &ticker).await {
        Ok(Some(t)) => t,
        Ok(None) => return ERR!("No such coin: {}", ticker),
        Err(err) => return ERR!("!lp_coinfind({}): {}", ticker, err),
    };
    let bytes_string = try_s!(req["tx_hex"].as_str().ok_or("No 'tx_hex' field"));
    let res = try_s!(coin.send_raw_tx(&bytes_string).compat().await);
    let body = try_s!(json::to_vec(&json!({ "tx_hash": res })));
    Ok(try_s!(Response::builder().body(body)))
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "state", content = "additional_info")]
pub enum HistorySyncState {
    NotEnabled,
    NotStarted,
    InProgress(Json),
    Error(Json),
    Finished,
}

fn ten() -> usize { 10 }

#[derive(Deserialize)]
struct MyTxHistoryRequest {
    coin: String,
    from_id: Option<BytesJson>,
    #[serde(default)]
    max: bool,
    #[serde(default = "ten")]
    limit: usize,
    page_number: Option<NonZeroUsize>,
}

/// Returns the transaction history of selected coin. Returns no more than `limit` records (default: 10).
/// Skips the first records up to from_id (skipping the from_id too).
/// Transactions are sorted by number of confirmations in ascending order.
pub async fn my_tx_history(ctx: MmArc, req: Json) -> Result<Response<Vec<u8>>, String> {
    let request: MyTxHistoryRequest = try_s!(json::from_value(req));
    let coin = match lp_coinfind(&ctx, &request.coin).await {
        Ok(Some(t)) => t,
        Ok(None) => return ERR!("No such coin: {}", request.coin),
        Err(err) => return ERR!("!lp_coinfind({}): {}", request.coin, err),
    };

    let history = try_s!(coin.load_history_from_file(&ctx).compat().await);
    let total_records = history.len();
    let limit = if request.max { total_records } else { request.limit };

    let block_number = try_s!(coin.current_block().compat().await);
    let skip = match &request.from_id {
        Some(id) => {
            try_s!(history
                .iter()
                .position(|item| item.internal_id == *id)
                .ok_or(format!("from_id {:02x} is not found", id)))
                + 1
        },
        None => match request.page_number {
            Some(page_n) => (page_n.get() - 1) * request.limit,
            None => 0,
        },
    };

    let history = history.into_iter().skip(skip).take(limit);
    let history: Vec<Json> = history
        .map(|item| {
            let tx_block = item.block_height;
            let mut json = json::to_value(item).unwrap();
            json["confirmations"] = if tx_block == 0 {
                Json::from(0)
            } else if block_number >= tx_block {
                Json::from((block_number - tx_block) + 1)
            } else {
                Json::from(0)
            };
            json
        })
        .collect();

    let response = json!({
        "result": {
            "transactions": history,
            "limit": limit,
            "skipped": skip,
            "from_id": request.from_id,
            "total": total_records,
            "current_block": block_number,
            "sync_status": coin.history_sync_status(),
            "page_number": request.page_number,
            "total_pages": calc_total_pages(total_records, request.limit),
        }
    });
    let body = try_s!(json::to_vec(&response));
    Ok(try_s!(Response::builder().body(body)))
}

pub async fn get_trade_fee(ctx: MmArc, req: Json) -> Result<Response<Vec<u8>>, String> {
    let ticker = try_s!(req["coin"].as_str().ok_or("No 'coin' field")).to_owned();
    let coin = match lp_coinfind(&ctx, &ticker).await {
        Ok(Some(t)) => t,
        Ok(None) => return ERR!("No such coin: {}", ticker),
        Err(err) => return ERR!("!lp_coinfind({}): {}", ticker, err),
    };
    let fee_info = try_s!(coin.get_trade_fee().compat().await);
    let res = try_s!(json::to_vec(&json!({
        "result": {
            "coin": fee_info.coin,
            "amount": fee_info.amount.to_decimal(),
            "amount_fraction": fee_info.amount.to_fraction(),
            "amount_rat": fee_info.amount.to_ratio(),
        }
    })));
    Ok(try_s!(Response::builder().body(res)))
}

#[derive(Serialize)]
struct EnabledCoin {
    ticker: String,
    address: String,
}

pub async fn get_enabled_coins(ctx: MmArc) -> Result<Response<Vec<u8>>, String> {
    let coins_ctx: Arc<CoinsContext> = try_s!(CoinsContext::from_ctx(&ctx));
    let coins = coins_ctx.coins.lock().await;
    let enabled_coins: Vec<_> = try_s!(coins
        .iter()
        .map(|(ticker, coin)| {
            let address = try_s!(coin.my_address());
            Ok(EnabledCoin {
                ticker: ticker.clone(),
                address,
            })
        })
        .collect());

    let res = try_s!(json::to_vec(&json!({ "result": enabled_coins })));
    Ok(try_s!(Response::builder().body(res)))
}

pub async fn disable_coin(ctx: &MmArc, ticker: &str) -> Result<(), String> {
    let coins_ctx = try_s!(CoinsContext::from_ctx(&ctx));
    let mut coins = coins_ctx.coins.lock().await;
    match coins.remove(ticker) {
        Some(_) => Ok(()),
        None => ERR!("{} is disabled already", ticker),
    }
}

#[derive(Deserialize)]
pub struct ConfirmationsReq {
    coin: String,
    confirmations: u64,
}

pub async fn set_required_confirmations(ctx: MmArc, req: Json) -> Result<Response<Vec<u8>>, String> {
    let req: ConfirmationsReq = try_s!(json::from_value(req));
    let coin = match lp_coinfind(&ctx, &req.coin).await {
        Ok(Some(t)) => t,
        Ok(None) => return ERR!("No such coin {}", req.coin),
        Err(err) => return ERR!("!lp_coinfind ({}): {}", req.coin, err),
    };
    coin.set_required_confirmations(req.confirmations);
    let res = try_s!(json::to_vec(&json!({
        "result": {
            "coin": req.coin,
            "confirmations": coin.required_confirmations(),
        }
    })));
    Ok(try_s!(Response::builder().body(res)))
}

#[derive(Deserialize)]
pub struct RequiresNotaReq {
    coin: String,
    requires_notarization: bool,
}

pub async fn set_requires_notarization(ctx: MmArc, req: Json) -> Result<Response<Vec<u8>>, String> {
    let req: RequiresNotaReq = try_s!(json::from_value(req));
    let coin = match lp_coinfind(&ctx, &req.coin).await {
        Ok(Some(t)) => t,
        Ok(None) => return ERR!("No such coin {}", req.coin),
        Err(err) => return ERR!("!lp_coinfind ({}): {}", req.coin, err),
    };
    coin.set_requires_notarization(req.requires_notarization);
    let res = try_s!(json::to_vec(&json!({
        "result": {
            "coin": req.coin,
            "requires_notarization": coin.requires_notarization(),
        }
    })));
    Ok(try_s!(Response::builder().body(res)))
}

pub async fn show_priv_key(ctx: MmArc, req: Json) -> Result<Response<Vec<u8>>, String> {
    let ticker = try_s!(req["coin"].as_str().ok_or("No 'coin' field")).to_owned();
    let coin = match lp_coinfind(&ctx, &ticker).await {
        Ok(Some(t)) => t,
        Ok(None) => return ERR!("No such coin: {}", ticker),
        Err(err) => return ERR!("!lp_coinfind({}): {}", ticker, err),
    };
    let res = try_s!(json::to_vec(&json!({
        "result": {
            "coin": ticker,
            "priv_key": coin.display_priv_key(),
        }
    })));
    Ok(try_s!(Response::builder().body(res)))
}

// TODO: Refactor this, it's actually not required to check balance and trade fee when there no orders using the coin
pub async fn check_balance_update_loop(ctx: MmArc, ticker: String) {
    let mut current_balance = None;
    loop {
        Timer::sleep(10.).await;
        match lp_coinfind(&ctx, &ticker).await {
            Ok(Some(coin)) => {
                let balance = match coin.my_spendable_balance().compat().await {
                    Ok(balance) => balance,
                    Err(_) => continue,
                };
                if Some(&balance) != current_balance.as_ref() {
                    let coins_ctx = CoinsContext::from_ctx(&ctx).unwrap();
                    coins_ctx.balance_updated(&coin, &balance).await;
                    current_balance = Some(balance);
                }
            },
            Ok(None) => break,
            Err(_) => continue,
        }
    }
}

pub async fn register_balance_update_handler(
    ctx: MmArc,
    handler: Box<dyn BalanceTradeFeeUpdatedHandler + Send + Sync>,
) {
    let coins_ctx = CoinsContext::from_ctx(&ctx).unwrap();
    coins_ctx.balance_update_handlers.lock().await.push(handler);
}

pub fn update_coins_config(mut config: Json) -> Result<Json, String> {
    let coins = match config.as_array_mut() {
        Some(c) => c,
        _ => return ERR!("Coins config must be an array"),
    };

    for coin in coins {
        // the coin_as_str is used only to be formatted
        let coin_as_str = format!("{}", coin);
        let coin = try_s!(coin
            .as_object_mut()
            .ok_or(ERRL!("Expected object, found {:?}", coin_as_str)));
        if coin.contains_key("protocol") {
            // the coin is up-to-date
            continue;
        }
        let protocol = match coin.remove("etomic") {
            Some(etomic) => {
                let etomic = etomic
                    .as_str()
                    .ok_or(ERRL!("Expected etomic as string, found {:?}", etomic))?;
                if etomic == "0x0000000000000000000000000000000000000000" {
                    CoinProtocol::ETH
                } else {
                    let contract_address = etomic.to_owned();
                    CoinProtocol::ERC20 {
                        platform: "ETH".into(),
                        contract_address,
                    }
                }
            },
            _ => CoinProtocol::UTXO,
        };

        let protocol = json::to_value(protocol).map_err(|e| ERRL!("Error {:?} on process {:?}", e, coin_as_str))?;
        coin.insert("protocol".into(), protocol);
    }

    Ok(config)
}

#[derive(Deserialize)]
struct ConvertUtxoAddressReq {
    address: String,
    to_coin: String,
}

pub async fn convert_utxo_address(ctx: MmArc, req: Json) -> Result<Response<Vec<u8>>, String> {
    let req: ConvertUtxoAddressReq = try_s!(json::from_value(req));
    let mut addr: utxo::Address = try_s!(req.address.parse());
    let coin = match lp_coinfind(&ctx, &req.to_coin).await {
        Ok(Some(c)) => c,
        _ => return ERR!("Coin {} is not activated", req.to_coin),
    };
    let coin = match coin {
        MmCoinEnum::UtxoCoin(utxo) => utxo,
        _ => return ERR!("Coin {} is not utxo", req.to_coin),
    };
    addr.prefix = coin.as_ref().my_address.prefix;
    addr.t_addr_prefix = coin.as_ref().my_address.t_addr_prefix;
    addr.checksum_type = coin.as_ref().my_address.checksum_type;

    let response = try_s!(json::to_vec(&json!({
        "result": addr.to_string(),
    })));
    Ok(try_s!(Response::builder().body(response)))
}

pub fn address_by_coin_conf_and_pubkey_str(coin: &str, conf: &Json, pubkey: &str) -> Result<String, String> {
    let protocol: CoinProtocol = try_s!(json::from_value(conf["protocol"].clone()));
    match protocol {
        CoinProtocol::ERC20 { .. } | CoinProtocol::ETH => eth::addr_from_pubkey_str(pubkey),
        CoinProtocol::UTXO | CoinProtocol::QTUM | CoinProtocol::QRC20 { .. } => {
            utxo::address_by_conf_and_pubkey_str(coin, conf, pubkey)
        },
        #[cfg(all(not(target_arch = "wasm32"), feature = "zhtlc"))]
        CoinProtocol::ZHTLC => utxo::address_by_conf_and_pubkey_str(coin, conf, pubkey),
    }
}

#[cfg(target_arch = "wasm32")]
fn load_history_from_file_impl<T>(coin: &T, ctx: &MmArc) -> TxHistoryFut<Vec<TransactionDetails>>
where
    T: MmCoin + MarketCoinOps + ?Sized,
{
    let ctx = ctx.clone();
    let ticker = coin.ticker().to_owned();
    let my_address = coin.my_address().unwrap_or_default();

    let fut = async move {
        let coins_ctx = CoinsContext::from_ctx(&ctx).unwrap();
        let db = coins_ctx.tx_history_db().await?;
        let err = match db.load_history(&ticker, &my_address).await {
            Ok(history) => return Ok(history),
            Err(e) => e,
        };

        if let TxHistoryError::ErrorDeserializing(e) = err.get_inner() {
            ctx.log.log(
                "🌋",
                &[&"tx_history", &ticker.to_owned()],
                &ERRL!("Error {} on history deserialization, resetting the cache.", e),
            );
            db.clear(&ticker, &my_address).await?;
            return Ok(Vec::new());
        }

        Err(err)
    };
    Box::new(fut.boxed().compat())
}

#[cfg(not(target_arch = "wasm32"))]
fn load_history_from_file_impl<T>(coin: &T, ctx: &MmArc) -> TxHistoryFut<Vec<TransactionDetails>>
where
    T: MmCoin + MarketCoinOps + ?Sized,
{
    let ticker = coin.ticker().to_owned();
    let history_path = coin.tx_history_path(&ctx);
    let ctx = ctx.clone();

    let fut = async move {
        let content = match fs::read(&history_path).await {
            Ok(content) => content,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                return Ok(Vec::new());
            },
            Err(err) => {
                let error = format!(
                    "Error '{}' reading from the history file {}",
                    err,
                    history_path.display()
                );
                return MmError::err(TxHistoryError::ErrorLoading(error));
            },
        };
        let serde_err = match json::from_slice(&content) {
            Ok(txs) => return Ok(txs),
            Err(e) => e,
        };

        ctx.log.log(
            "🌋",
            &[&"tx_history", &ticker],
            &ERRL!("Error {} on history deserialization, resetting the cache.", serde_err),
        );
        fs::remove_file(&history_path)
            .await
            .map_to_mm(|e| TxHistoryError::ErrorClearing(e.to_string()))?;
        Ok(Vec::new())
    };
    Box::new(fut.boxed().compat())
}

#[cfg(target_arch = "wasm32")]
fn save_history_to_file_impl<T>(coin: &T, ctx: &MmArc, history: Vec<TransactionDetails>) -> TxHistoryFut<()>
where
    T: MmCoin + MarketCoinOps + ?Sized,
{
    let ctx = ctx.clone();
    let ticker = coin.ticker().to_owned();
    let my_address = coin.my_address().unwrap_or_default();

    let fut = async move {
        let coins_ctx = CoinsContext::from_ctx(&ctx).unwrap();
        let db = coins_ctx.tx_history_db().await?;
        db.save_history(&ticker, &my_address, history).await?;
        Ok(())
    };
    Box::new(fut.boxed().compat())
}

#[cfg(not(target_arch = "wasm32"))]
fn save_history_to_file_impl<T>(coin: &T, ctx: &MmArc, history: Vec<TransactionDetails>) -> TxHistoryFut<()>
where
    T: MmCoin + MarketCoinOps + ?Sized,
{
    let history_path = coin.tx_history_path(ctx);
    let tmp_file = format!("{}.tmp", history_path.display());

    let fut = async move {
        let content = json::to_vec(&history).map_to_mm(|e| TxHistoryError::ErrorSerializing(e.to_string()))?;

        let fs_fut = async {
            let mut file = fs::File::create(&tmp_file).await?;
            file.write_all(&content).await?;
            file.flush().await?;
            fs::rename(&tmp_file, &history_path).await?;
            Ok(())
        };

        let res: io::Result<_> = fs_fut.await;
        if let Err(e) = res {
            let error = format!("Error '{}' creating/writing/renaming the tmp file {}", e, tmp_file);
            return MmError::err(TxHistoryError::ErrorSaving(error));
        }
        Ok(())
    };
    Box::new(fut.boxed().compat())
}
