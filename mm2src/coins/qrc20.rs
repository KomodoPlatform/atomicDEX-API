use crate::eth::{self, u256_to_big_decimal, wei_from_big_decimal, TryToAddress};
use crate::qrc20::rpc_clients::{LogEntry, Qrc20ElectrumOps, Qrc20NativeOps, Qrc20RpcOps, TopicFilter, TxReceipt,
                                ViewContractCallType};
use crate::utxo::qtum::QtumBasedCoin;
use crate::utxo::rpc_clients::{ElectrumClient, NativeClient, UnspentInfo, UtxoRpcClientEnum, UtxoRpcClientOps,
                               UtxoRpcError, UtxoRpcResult};
use crate::utxo::utxo_common::{self, big_decimal_from_sat, check_all_inputs_signed_by_pub};
use crate::utxo::{qtum, sign_tx, ActualTxFee, AdditionalTxData, FeePolicy, GenerateTxError, GenerateTxResult,
                  HistoryUtxoTx, HistoryUtxoTxMap, RecentlySpentOutPoints, UtxoCoinBuilder, UtxoCoinFields,
                  UtxoCommonOps, UtxoTx, VerboseTransactionFrom, UTXO_LOCK};
use crate::{BalanceError, BalanceFut, CoinBalance, FeeApproxStage, FoundSwapTxSpend, HistorySyncState, MarketCoinOps,
            MmCoin, NegotiateSwapContractAddrErr, RawTransactionFut, RawTransactionRequest, SwapOps, TradeFee,
            TradePreimageError, TradePreimageFut, TradePreimageResult, TradePreimageValue, TransactionDetails,
            TransactionEnum, TransactionFut, ValidateAddressResult, WithdrawError, WithdrawFee, WithdrawFut,
            WithdrawRequest, WithdrawResult};
use async_trait::async_trait;
use bigdecimal::BigDecimal;
use bitcrypto::{dhash160, sha256};
use chain::TransactionOutput;
use common::block_on;
use common::executor::Timer;
use common::jsonrpc_client::{JsonRpcClient, JsonRpcError, JsonRpcRequest, RpcRes};
use common::log::{error, warn};
use common::mm_ctx::MmArc;
use common::mm_error::prelude::*;
use common::mm_number::MmNumber;
use common::now_ms;
use derive_more::Display;
use ethabi::{Function, Token};
use ethereum_types::{H160, U256};
use futures::compat::Future01CompatExt;
use futures::lock::MutexGuard as AsyncMutexGuard;
use futures::{FutureExt, TryFutureExt};
use futures01::Future;
use keys::bytes::Bytes as ScriptBytes;
use keys::{Address as UtxoAddress, Address, Public};
#[cfg(test)] use mocktopus::macros::*;
use rpc::v1::types::{Bytes as BytesJson, Transaction as RpcTransaction, H160 as H160Json, H256 as H256Json};
use script::{Builder as ScriptBuilder, Opcode, Script, TransactionInputSigner};
use script_pubkey::generate_contract_call_script_pubkey;
use serde_json::{self as json, Value as Json};
use serialization::{deserialize, serialize, CoinVariant};
use std::ops::{Deref, Neg};
#[cfg(not(target_arch = "wasm32"))] use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

mod history;
#[cfg(test)] mod qrc20_tests;
pub mod rpc_clients;
mod script_pubkey;
mod swap;

/// Qtum amount is always 0 for the QRC20 UTXO outputs,
/// because we should pay only a fee in Qtum to send the QRC20 transaction.
const OUTPUT_QTUM_AMOUNT: u64 = 0;
const QRC20_GAS_LIMIT_DEFAULT: u64 = 100_000;
const QRC20_PAYMENT_GAS_LIMIT: u64 = 200_000;
const QRC20_GAS_PRICE_DEFAULT: u64 = 40;
const QRC20_DUST: u64 = 0;
// Keccak-256 hash of `Transfer` event
const QRC20_TRANSFER_TOPIC: &str = "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
const QRC20_PAYMENT_SENT_TOPIC: &str = "ccc9c05183599bd3135da606eaaf535daffe256e9de33c048014cffcccd4ad57";
const QRC20_RECEIVER_SPENT_TOPIC: &str = "36c177bcb01c6d568244f05261e2946c8c977fa50822f3fa098c470770ee1f3e";
const QRC20_SENDER_REFUNDED_TOPIC: &str = "1797d500133f8e427eb9da9523aa4a25cb40f50ebc7dbda3c7c81778973f35ba";

pub type Qrc20AbiResult<T> = Result<T, MmError<Qrc20AbiError>>;

struct Qrc20CoinBuilder<'a> {
    ctx: &'a MmArc,
    ticker: &'a str,
    conf: &'a Json,
    req: &'a Json,
    priv_key: &'a [u8],
    platform: String,
    contract_address: H160,
}

impl<'a> Qrc20CoinBuilder<'a> {
    pub fn new(
        ctx: &'a MmArc,
        ticker: &'a str,
        conf: &'a Json,
        req: &'a Json,
        priv_key: &'a [u8],
        platform: String,
        contract_address: H160,
    ) -> Qrc20CoinBuilder<'a> {
        Qrc20CoinBuilder {
            ctx,
            ticker,
            conf,
            req,
            priv_key,
            platform,
            contract_address,
        }
    }
}

impl Qrc20CoinBuilder<'_> {
    fn swap_contract_address(&self) -> Result<H160, String> {
        match self.req()["swap_contract_address"].as_str() {
            Some(address) => qtum::contract_addr_from_str(address).map_err(|e| ERRL!("{}", e)),
            None => return ERR!("\"swap_contract_address\" field is expected"),
        }
    }

    fn fallback_swap_contract(&self) -> Result<Option<H160>, String> {
        match self.req()["fallback_swap_contract"].as_str() {
            Some(address) => qtum::contract_addr_from_str(address)
                .map_err(|e| ERRL!("{}", e))
                .map(Some),
            None => Ok(None),
        }
    }
}

#[async_trait]
impl UtxoCoinBuilder for Qrc20CoinBuilder<'_> {
    type ResultCoin = Qrc20Coin;

    async fn build(self) -> Result<Self::ResultCoin, String> {
        let swap_contract_address = try_s!(self.swap_contract_address());
        let fallback_swap_contract = try_s!(self.fallback_swap_contract());
        let utxo = try_s!(self.build_utxo_fields().await);
        let inner = Qrc20CoinFields {
            utxo,
            platform: self.platform,
            contract_address: self.contract_address,
            swap_contract_address,
            fallback_swap_contract,
        };
        Ok(Qrc20Coin(Arc::new(inner)))
    }

    fn ctx(&self) -> &MmArc { self.ctx }

    fn conf(&self) -> &Json { self.conf }

    fn req(&self) -> &Json { self.req }

    fn ticker(&self) -> &str { self.ticker }

    fn priv_key(&self) -> &[u8] { self.priv_key }

    async fn decimals(&self, rpc_client: &UtxoRpcClientEnum) -> Result<u8, String> {
        if let Some(d) = self.conf()["decimals"].as_u64() {
            return Ok(d as u8);
        }

        rpc_client
            .token_decimals(&self.contract_address)
            .compat()
            .await
            .map_err(|e| ERRL!("{}", e))
    }

    fn dust_amount(&self) -> u64 { QRC20_DUST }

    #[cfg(not(target_arch = "wasm32"))]
    fn confpath(&self) -> Result<PathBuf, String> {
        use crate::utxo::coin_daemon_data_dir;

        // Documented at https://github.com/jl777/coins#bitcoin-protocol-specific-json
        // "USERHOME/" prefix should be replaced with the user's home folder.
        let declared_confpath = match self.conf()["confpath"].as_str() {
            Some(path) if !path.is_empty() => path.trim(),
            _ => {
                let is_asset_chain = false;
                let platform = self.platform.to_lowercase();
                let data_dir = coin_daemon_data_dir(&platform, is_asset_chain);

                let confname = format!("{}.conf", platform);
                return Ok(data_dir.join(&confname[..]));
            },
        };

        let (confpath, rel_to_home) = match declared_confpath.strip_prefix("~/") {
            Some(stripped) => (stripped, true),
            None => match declared_confpath.strip_prefix("USERHOME/") {
                Some(stripped) => (stripped, true),
                None => (declared_confpath, false),
            },
        };

        if rel_to_home {
            let home = try_s!(dirs::home_dir().ok_or("Can not detect the user home directory"));
            Ok(home.join(confpath))
        } else {
            Ok(confpath.into())
        }
    }
}

pub async fn qrc20_coin_from_conf_and_request(
    ctx: &MmArc,
    ticker: &str,
    platform: &str,
    conf: &Json,
    req: &Json,
    priv_key: &[u8],
    contract_address: H160,
) -> Result<Qrc20Coin, String> {
    let builder = Qrc20CoinBuilder::new(ctx, ticker, conf, req, priv_key, platform.to_owned(), contract_address);
    builder.build().await
}

#[derive(Debug)]
pub struct Qrc20CoinFields {
    pub utxo: UtxoCoinFields,
    pub platform: String,
    pub contract_address: H160,
    pub swap_contract_address: H160,
    pub fallback_swap_contract: Option<H160>,
}

#[derive(Clone, Debug)]
pub struct Qrc20Coin(Arc<Qrc20CoinFields>);

impl Deref for Qrc20Coin {
    type Target = Qrc20CoinFields;
    fn deref(&self) -> &Qrc20CoinFields { &*self.0 }
}

impl AsRef<UtxoCoinFields> for Qrc20Coin {
    fn as_ref(&self) -> &UtxoCoinFields { &self.utxo }
}

impl qtum::QtumBasedCoin for Qrc20Coin {}

#[derive(Clone, Debug, PartialEq)]
pub struct ContractCallOutput {
    pub value: u64,
    pub script_pubkey: ScriptBytes,
    pub gas_limit: u64,
    pub gas_price: u64,
}

impl From<ContractCallOutput> for TransactionOutput {
    fn from(out: ContractCallOutput) -> Self {
        TransactionOutput {
            value: out.value,
            script_pubkey: out.script_pubkey,
        }
    }
}

/// Functions of ERC20/EtomicSwap smart contracts that may change the blockchain state.
#[derive(Debug, Eq, PartialEq)]
pub enum MutContractCallType {
    Transfer,
    Erc20Payment,
    ReceiverSpend,
    SenderRefund,
}

impl MutContractCallType {
    fn as_function_name(&self) -> &'static str {
        match self {
            MutContractCallType::Transfer => "transfer",
            MutContractCallType::Erc20Payment => "erc20Payment",
            MutContractCallType::ReceiverSpend => "receiverSpend",
            MutContractCallType::SenderRefund => "senderRefund",
        }
    }

    fn as_function(&self) -> &'static Function {
        match self {
            MutContractCallType::Transfer => eth::ERC20_CONTRACT.function(self.as_function_name()).unwrap(),
            MutContractCallType::Erc20Payment
            | MutContractCallType::ReceiverSpend
            | MutContractCallType::SenderRefund => eth::SWAP_CONTRACT.function(self.as_function_name()).unwrap(),
        }
    }

    pub fn from_script_pubkey(script: &[u8]) -> Result<Option<MutContractCallType>, String> {
        lazy_static! {
            static ref TRANSFER_SHORT_SIGN: [u8; 4] =
                eth::ERC20_CONTRACT.function("transfer").unwrap().short_signature();
            static ref ERC20_PAYMENT_SHORT_SIGN: [u8; 4] =
                eth::SWAP_CONTRACT.function("erc20Payment").unwrap().short_signature();
            static ref RECEIVER_SPEND_SHORT_SIGN: [u8; 4] =
                eth::SWAP_CONTRACT.function("receiverSpend").unwrap().short_signature();
            static ref SENDER_REFUND_SHORT_SIGN: [u8; 4] =
                eth::SWAP_CONTRACT.function("senderRefund").unwrap().short_signature();
        }

        if script.len() < 4 {
            return ERR!("Length of the script pubkey less than 4: {:?}", script);
        }

        if script.starts_with(TRANSFER_SHORT_SIGN.as_ref()) {
            return Ok(Some(MutContractCallType::Transfer));
        }
        if script.starts_with(ERC20_PAYMENT_SHORT_SIGN.as_ref()) {
            return Ok(Some(MutContractCallType::Erc20Payment));
        }
        if script.starts_with(RECEIVER_SPEND_SHORT_SIGN.as_ref()) {
            return Ok(Some(MutContractCallType::ReceiverSpend));
        }
        if script.starts_with(SENDER_REFUND_SHORT_SIGN.as_ref()) {
            return Ok(Some(MutContractCallType::SenderRefund));
        }
        Ok(None)
    }

    #[allow(dead_code)]
    fn short_signature(&self) -> [u8; 4] { self.as_function().short_signature() }
}

struct GenerateQrc20TxResult {
    signed: UtxoTx,
    miner_fee: u64,
    gas_fee: u64,
}

#[derive(Debug, Display)]
pub enum Qrc20AbiError {
    #[display(fmt = "Invalid QRC20 ABI params: {}", _0)]
    InvalidParams(String),
    #[display(fmt = "QRC20 ABI error: {}", _0)]
    AbiError(String),
}

impl From<ethabi::Error> for Qrc20AbiError {
    fn from(e: ethabi::Error) -> Qrc20AbiError { Qrc20AbiError::AbiError(e.to_string()) }
}

impl From<Qrc20AbiError> for TradePreimageError {
    fn from(e: Qrc20AbiError) -> Self {
        // `Qrc20ABIError` is always an internal error
        TradePreimageError::InternalError(e.to_string())
    }
}

impl From<Qrc20AbiError> for WithdrawError {
    fn from(e: Qrc20AbiError) -> Self {
        // `Qrc20ABIError` is always an internal error
        WithdrawError::InternalError(e.to_string())
    }
}

impl From<Qrc20AbiError> for UtxoRpcError {
    fn from(e: Qrc20AbiError) -> Self {
        // `Qrc20ABIError` is always an internal error
        UtxoRpcError::Internal(e.to_string())
    }
}

impl Qrc20Coin {
    /// `gas_fee` should be calculated by: gas_limit * gas_price * (count of contract calls),
    /// or should be sum of gas fee of all contract calls.
    pub async fn get_qrc20_tx_fee(&self, gas_fee: u64) -> Result<u64, String> {
        match try_s!(self.get_tx_fee().await) {
            ActualTxFee::Fixed(amount) | ActualTxFee::Dynamic(amount) | ActualTxFee::FixedPerKb(amount) => {
                Ok(amount + gas_fee)
            },
        }
    }

    /// Generate and send a transaction with the specified UTXO outputs.
    /// Note this function locks the `UTXO_LOCK`.
    pub async fn send_contract_calls(&self, outputs: Vec<ContractCallOutput>) -> Result<TransactionEnum, String> {
        // TODO: we need to somehow refactor it using RecentlySpentOutpoints cache
        // Move over all QRC20 tokens should share the same cache with each other and base QTUM coin
        let _utxo_lock = UTXO_LOCK.lock().await;

        let platform = self.platform.clone();
        let decimals = self.utxo.decimals;
        let GenerateQrc20TxResult { signed, .. } = self
            .generate_qrc20_transaction(outputs)
            .await
            .mm_err(|e| WithdrawError::from_generate_tx_error(e, platform, decimals))
            .map_err(|e| ERRL!("{}", e))?;
        let _tx = try_s!(self.utxo.rpc_client.send_transaction(&signed).compat().await);
        Ok(signed.into())
    }

    /// Generate Qtum UTXO transaction with contract calls.
    /// Note: lock the UTXO_LOCK mutex before this function will be called.
    async fn generate_qrc20_transaction(
        &self,
        contract_outputs: Vec<ContractCallOutput>,
    ) -> Result<GenerateQrc20TxResult, MmError<GenerateTxError>> {
        let (unspents, _) = self.ordered_mature_unspents(&self.utxo.my_address).await?;

        let mut gas_fee = 0;
        let mut outputs = Vec::with_capacity(contract_outputs.len());
        for output in contract_outputs {
            gas_fee += output.gas_limit * output.gas_price;
            outputs.push(TransactionOutput::from(output));
        }
        let fee_policy = FeePolicy::SendExact;
        let tx_fee = None;

        let (unsigned, data) = self
            .generate_transaction(unspents, outputs, fee_policy, tx_fee, Some(gas_fee))
            .await?;
        let prev_script = ScriptBuilder::build_p2pkh(&self.utxo.my_address.hash);
        let signed = sign_tx(
            unsigned,
            &self.utxo.key_pair,
            prev_script,
            self.utxo.conf.signature_version,
            self.utxo.conf.fork_id,
        )
        .map_to_mm(GenerateTxError::Internal)?;

        let miner_fee = data.fee_amount + data.unused_change.unwrap_or_default();
        Ok(GenerateQrc20TxResult {
            signed,
            miner_fee,
            gas_fee,
        })
    }

    fn transfer_output(
        &self,
        to_addr: H160,
        amount: U256,
        gas_limit: u64,
        gas_price: u64,
    ) -> Qrc20AbiResult<ContractCallOutput> {
        let function = eth::ERC20_CONTRACT.function("transfer")?;
        let params = function.encode_input(&[Token::Address(to_addr), Token::Uint(amount)])?;

        let script_pubkey =
            generate_contract_call_script_pubkey(&params, gas_limit, gas_price, &self.contract_address)?.to_bytes();

        Ok(ContractCallOutput {
            value: OUTPUT_QTUM_AMOUNT,
            script_pubkey,
            gas_limit,
            gas_price,
        })
    }

    async fn preimage_trade_fee_required_to_send_outputs(
        &self,
        contract_outputs: Vec<ContractCallOutput>,
        stage: &FeeApproxStage,
    ) -> TradePreimageResult<BigDecimal> {
        let decimals = self.as_ref().decimals;
        let mut gas_fee = 0;
        let mut outputs = Vec::with_capacity(contract_outputs.len());
        for output in contract_outputs {
            gas_fee += output.gas_limit * output.gas_price;
            outputs.push(TransactionOutput::from(output));
        }
        let fee_policy = FeePolicy::SendExact;
        let miner_fee =
            UtxoCommonOps::preimage_trade_fee_required_to_send_outputs(self, outputs, fee_policy, Some(gas_fee), stage)
                .await?;
        let gas_fee = big_decimal_from_sat(gas_fee as i64, decimals);
        Ok(miner_fee + gas_fee)
    }
}

#[async_trait]
#[cfg_attr(test, mockable)]
impl UtxoCommonOps for Qrc20Coin {
    /// Get only QTUM transaction fee.
    async fn get_tx_fee(&self) -> Result<ActualTxFee, JsonRpcError> { utxo_common::get_tx_fee(&self.utxo).await }

    async fn get_htlc_spend_fee(&self) -> UtxoRpcResult<u64> { utxo_common::get_htlc_spend_fee(self).await }

    fn addresses_from_script(&self, script: &Script) -> Result<Vec<UtxoAddress>, String> {
        utxo_common::addresses_from_script(&self.utxo, script)
    }

    fn denominate_satoshis(&self, satoshi: i64) -> f64 { utxo_common::denominate_satoshis(&self.utxo, satoshi) }

    fn my_public_key(&self) -> &Public { self.utxo.key_pair.public() }

    fn address_from_str(&self, address: &str) -> Result<UtxoAddress, String> {
        utxo_common::checked_address_from_str(&self.utxo, address)
    }

    async fn get_current_mtp(&self) -> UtxoRpcResult<u32> {
        utxo_common::get_current_mtp(&self.utxo, CoinVariant::Qtum).await
    }

    fn is_unspent_mature(&self, output: &RpcTransaction) -> bool { self.is_qtum_unspent_mature(output) }

    /// Generate UTXO transaction with specified unspent inputs and specified outputs.
    async fn generate_transaction(
        &self,
        utxos: Vec<UnspentInfo>,
        outputs: Vec<TransactionOutput>,
        fee_policy: FeePolicy,
        fee: Option<ActualTxFee>,
        gas_fee: Option<u64>,
    ) -> GenerateTxResult {
        utxo_common::generate_transaction(self, utxos, outputs, fee_policy, fee, gas_fee).await
    }

    async fn calc_interest_if_required(
        &self,
        unsigned: TransactionInputSigner,
        data: AdditionalTxData,
        my_script_pub: ScriptBytes,
    ) -> UtxoRpcResult<(TransactionInputSigner, AdditionalTxData)> {
        utxo_common::calc_interest_if_required(self, unsigned, data, my_script_pub).await
    }

    async fn calc_interest_of_tx(
        &self,
        _tx: &UtxoTx,
        _input_transactions: &mut HistoryUtxoTxMap,
    ) -> UtxoRpcResult<u64> {
        MmError::err(UtxoRpcError::Internal(
            "QRC20 coin doesn't support transaction rewards".to_owned(),
        ))
    }

    async fn get_mut_verbose_transaction_from_map_or_rpc<'a, 'b>(
        &'a self,
        tx_hash: H256Json,
        utxo_tx_map: &'b mut HistoryUtxoTxMap,
    ) -> UtxoRpcResult<&'b mut HistoryUtxoTx> {
        utxo_common::get_mut_verbose_transaction_from_map_or_rpc(self, tx_hash, utxo_tx_map).await
    }

    async fn p2sh_spending_tx(
        &self,
        prev_transaction: UtxoTx,
        redeem_script: ScriptBytes,
        outputs: Vec<TransactionOutput>,
        script_data: Script,
        sequence: u32,
        lock_time: u32,
    ) -> Result<UtxoTx, String> {
        utxo_common::p2sh_spending_tx(
            self,
            prev_transaction,
            redeem_script,
            outputs,
            script_data,
            sequence,
            lock_time,
        )
        .await
    }

    async fn ordered_mature_unspents<'a>(
        &'a self,
        address: &Address,
    ) -> UtxoRpcResult<(Vec<UnspentInfo>, AsyncMutexGuard<'a, RecentlySpentOutPoints>)> {
        utxo_common::ordered_mature_unspents(self, address).await
    }

    fn get_verbose_transaction_from_cache_or_rpc(
        &self,
        txid: H256Json,
    ) -> Box<dyn Future<Item = VerboseTransactionFrom, Error = String> + Send> {
        let selfi = self.clone();
        let fut = async move { utxo_common::get_verbose_transaction_from_cache_or_rpc(&selfi.utxo, txid).await };
        Box::new(fut.boxed().compat())
    }

    async fn cache_transaction_if_possible(&self, tx: &RpcTransaction) -> Result<(), String> {
        utxo_common::cache_transaction_if_possible(&self.utxo, tx).await
    }

    async fn list_unspent_ordered<'a>(
        &'a self,
        address: &Address,
    ) -> UtxoRpcResult<(Vec<UnspentInfo>, AsyncMutexGuard<'a, RecentlySpentOutPoints>)> {
        utxo_common::ordered_mature_unspents(self, address).await
    }

    async fn preimage_trade_fee_required_to_send_outputs(
        &self,
        outputs: Vec<TransactionOutput>,
        fee_policy: FeePolicy,
        gas_fee: Option<u64>,
        stage: &FeeApproxStage,
    ) -> TradePreimageResult<BigDecimal> {
        utxo_common::preimage_trade_fee_required_to_send_outputs(self, outputs, fee_policy, gas_fee, stage).await
    }

    fn increase_dynamic_fee_by_stage(&self, dynamic_fee: u64, stage: &FeeApproxStage) -> u64 {
        utxo_common::increase_dynamic_fee_by_stage(self, dynamic_fee, stage)
    }

    async fn p2sh_tx_locktime(&self, htlc_locktime: u32) -> Result<u32, MmError<UtxoRpcError>> {
        utxo_common::p2sh_tx_locktime(self, &self.utxo.conf.ticker, htlc_locktime).await
    }
}

impl SwapOps for Qrc20Coin {
    fn send_taker_fee(&self, fee_addr: &[u8], amount: BigDecimal) -> TransactionFut {
        let to_address = try_fus!(self.contract_address_from_raw_pubkey(fee_addr));
        let amount = try_fus!(wei_from_big_decimal(&amount, self.utxo.decimals));
        let transfer_output =
            try_fus!(self.transfer_output(to_address, amount, QRC20_GAS_LIMIT_DEFAULT, QRC20_GAS_PRICE_DEFAULT));
        let outputs = vec![transfer_output];

        let selfi = self.clone();
        let fut = async move { selfi.send_contract_calls(outputs).await };

        Box::new(fut.boxed().compat())
    }

    fn send_maker_payment(
        &self,
        time_lock: u32,
        taker_pub: &[u8],
        secret_hash: &[u8],
        amount: BigDecimal,
        swap_contract_address: &Option<BytesJson>,
    ) -> TransactionFut {
        let taker_addr = try_fus!(self.contract_address_from_raw_pubkey(taker_pub));
        let id = qrc20_swap_id(time_lock, secret_hash);
        let value = try_fus!(wei_from_big_decimal(&amount, self.utxo.decimals));
        let secret_hash = Vec::from(secret_hash);
        let swap_contract_address = try_fus!(swap_contract_address.try_to_address());

        let selfi = self.clone();
        let fut = async move {
            selfi
                .send_hash_time_locked_payment(id, value, time_lock, secret_hash, taker_addr, swap_contract_address)
                .await
        };
        Box::new(fut.boxed().compat())
    }

    fn send_taker_payment(
        &self,
        time_lock: u32,
        maker_pub: &[u8],
        secret_hash: &[u8],
        amount: BigDecimal,
        swap_contract_address: &Option<BytesJson>,
    ) -> TransactionFut {
        let maker_addr = try_fus!(self.contract_address_from_raw_pubkey(maker_pub));
        let id = qrc20_swap_id(time_lock, secret_hash);
        let value = try_fus!(wei_from_big_decimal(&amount, self.utxo.decimals));
        let secret_hash = Vec::from(secret_hash);
        let swap_contract_address = try_fus!(swap_contract_address.try_to_address());

        let selfi = self.clone();
        let fut = async move {
            selfi
                .send_hash_time_locked_payment(id, value, time_lock, secret_hash, maker_addr, swap_contract_address)
                .await
        };
        Box::new(fut.boxed().compat())
    }

    fn send_maker_spends_taker_payment(
        &self,
        taker_payment_tx: &[u8],
        _time_lock: u32,
        _taker_pub: &[u8],
        secret: &[u8],
        swap_contract_address: &Option<BytesJson>,
    ) -> TransactionFut {
        let payment_tx: UtxoTx = try_fus!(deserialize(taker_payment_tx).map_err(|e| ERRL!("{:?}", e)));
        let swap_contract_address = try_fus!(swap_contract_address.try_to_address());
        let secret = secret.to_vec();

        let selfi = self.clone();
        let fut = async move {
            selfi
                .spend_hash_time_locked_payment(payment_tx, swap_contract_address, secret)
                .await
        };
        Box::new(fut.boxed().compat())
    }

    fn send_taker_spends_maker_payment(
        &self,
        maker_payment_tx: &[u8],
        _time_lock: u32,
        _maker_pub: &[u8],
        secret: &[u8],
        swap_contract_address: &Option<BytesJson>,
    ) -> TransactionFut {
        let payment_tx: UtxoTx = try_fus!(deserialize(maker_payment_tx).map_err(|e| ERRL!("{:?}", e)));
        let secret = secret.to_vec();
        let swap_contract_address = try_fus!(swap_contract_address.try_to_address());

        let selfi = self.clone();
        let fut = async move {
            selfi
                .spend_hash_time_locked_payment(payment_tx, swap_contract_address, secret)
                .await
        };
        Box::new(fut.boxed().compat())
    }

    fn send_taker_refunds_payment(
        &self,
        taker_payment_tx: &[u8],
        _time_lock: u32,
        _maker_pub: &[u8],
        _secret_hash: &[u8],
        swap_contract_address: &Option<BytesJson>,
    ) -> TransactionFut {
        let payment_tx: UtxoTx = try_fus!(deserialize(taker_payment_tx).map_err(|e| ERRL!("{:?}", e)));
        let swap_contract_address = try_fus!(swap_contract_address.try_to_address());

        let selfi = self.clone();
        let fut = async move {
            selfi
                .refund_hash_time_locked_payment(swap_contract_address, payment_tx)
                .await
        };
        Box::new(fut.boxed().compat())
    }

    fn send_maker_refunds_payment(
        &self,
        maker_payment_tx: &[u8],
        _time_lock: u32,
        _taker_pub: &[u8],
        _secret_hash: &[u8],
        swap_contract_address: &Option<BytesJson>,
    ) -> TransactionFut {
        let payment_tx: UtxoTx = try_fus!(deserialize(maker_payment_tx).map_err(|e| ERRL!("{:?}", e)));
        let swap_contract_address = try_fus!(swap_contract_address.try_to_address());

        let selfi = self.clone();
        let fut = async move {
            selfi
                .refund_hash_time_locked_payment(swap_contract_address, payment_tx)
                .await
        };
        Box::new(fut.boxed().compat())
    }

    fn validate_fee(
        &self,
        fee_tx: &TransactionEnum,
        expected_sender: &[u8],
        fee_addr: &[u8],
        amount: &BigDecimal,
        min_block_number: u64,
    ) -> Box<dyn Future<Item = (), Error = String> + Send> {
        let fee_tx = match fee_tx {
            TransactionEnum::UtxoTx(tx) => tx,
            _ => panic!("Unexpected TransactionEnum"),
        };
        let fee_tx_hash = fee_tx.hash().reversed().into();
        if !try_fus!(check_all_inputs_signed_by_pub(&fee_tx, expected_sender)) {
            return Box::new(futures01::future::err(ERRL!("The dex fee was sent from wrong address")));
        }
        let fee_addr = try_fus!(self.contract_address_from_raw_pubkey(fee_addr));
        let expected_value = try_fus!(wei_from_big_decimal(amount, self.utxo.decimals));

        let selfi = self.clone();
        let fut = async move {
            selfi
                .validate_fee_impl(fee_tx_hash, fee_addr, expected_value, min_block_number)
                .await
        };
        Box::new(fut.boxed().compat())
    }

    fn validate_maker_payment(
        &self,
        payment_tx: &[u8],
        time_lock: u32,
        maker_pub: &[u8],
        secret_hash: &[u8],
        amount: BigDecimal,
        swap_contract_address: &Option<BytesJson>,
    ) -> Box<dyn Future<Item = (), Error = String> + Send> {
        let payment_tx: UtxoTx = try_fus!(deserialize(payment_tx).map_err(|e| ERRL!("{:?}", e)));
        let sender = try_fus!(self.contract_address_from_raw_pubkey(maker_pub));
        let secret_hash = secret_hash.to_vec();
        let swap_contract_address = try_fus!(swap_contract_address.try_to_address());

        let selfi = self.clone();
        let fut = async move {
            selfi
                .validate_payment(
                    payment_tx,
                    time_lock,
                    sender,
                    secret_hash,
                    amount,
                    swap_contract_address,
                )
                .await
        };
        Box::new(fut.boxed().compat())
    }

    fn validate_taker_payment(
        &self,
        payment_tx: &[u8],
        time_lock: u32,
        taker_pub: &[u8],
        secret_hash: &[u8],
        amount: BigDecimal,
        swap_contract_address: &Option<BytesJson>,
    ) -> Box<dyn Future<Item = (), Error = String> + Send> {
        let swap_contract_address = try_fus!(swap_contract_address.try_to_address());
        let payment_tx: UtxoTx = try_fus!(deserialize(payment_tx).map_err(|e| ERRL!("{:?}", e)));
        let sender = try_fus!(self.contract_address_from_raw_pubkey(taker_pub));
        let secret_hash = secret_hash.to_vec();

        let selfi = self.clone();
        let fut = async move {
            selfi
                .validate_payment(
                    payment_tx,
                    time_lock,
                    sender,
                    secret_hash,
                    amount,
                    swap_contract_address,
                )
                .await
        };
        Box::new(fut.boxed().compat())
    }

    fn check_if_my_payment_sent(
        &self,
        time_lock: u32,
        _other_pub: &[u8],
        secret_hash: &[u8],
        search_from_block: u64,
        swap_contract_address: &Option<BytesJson>,
    ) -> Box<dyn Future<Item = Option<TransactionEnum>, Error = String> + Send> {
        let swap_id = qrc20_swap_id(time_lock, secret_hash);
        let swap_contract_address = try_fus!(swap_contract_address.try_to_address());

        let selfi = self.clone();
        let fut = async move {
            selfi
                .check_if_my_payment_sent_impl(swap_contract_address, swap_id, search_from_block)
                .await
        };
        Box::new(fut.boxed().compat())
    }

    fn search_for_swap_tx_spend_my(
        &self,
        time_lock: u32,
        _other_pub: &[u8],
        secret_hash: &[u8],
        tx: &[u8],
        search_from_block: u64,
        _swap_contract_address: &Option<BytesJson>,
    ) -> Result<Option<FoundSwapTxSpend>, String> {
        let tx: UtxoTx = try_s!(deserialize(tx).map_err(|e| ERRL!("{:?}", e)));

        let selfi = self.clone();
        let fut = selfi.search_for_swap_tx_spend(time_lock, secret_hash.to_vec(), tx, search_from_block);
        block_on(fut)
    }

    fn search_for_swap_tx_spend_other(
        &self,
        time_lock: u32,
        _other_pub: &[u8],
        secret_hash: &[u8],
        tx: &[u8],
        search_from_block: u64,
        _swap_contract_address: &Option<BytesJson>,
    ) -> Result<Option<FoundSwapTxSpend>, String> {
        let tx: UtxoTx = try_s!(deserialize(tx).map_err(|e| ERRL!("{:?}", e)));

        let selfi = self.clone();
        let fut = selfi.search_for_swap_tx_spend(time_lock, secret_hash.to_vec(), tx, search_from_block);
        block_on(fut)
    }

    fn extract_secret(&self, secret_hash: &[u8], spend_tx: &[u8]) -> Result<Vec<u8>, String> {
        self.extract_secret_impl(secret_hash, spend_tx)
    }

    fn negotiate_swap_contract_addr(
        &self,
        other_side_address: Option<&[u8]>,
    ) -> Result<Option<BytesJson>, MmError<NegotiateSwapContractAddrErr>> {
        match other_side_address {
            Some(bytes) => {
                if bytes.len() != 20 {
                    return MmError::err(NegotiateSwapContractAddrErr::InvalidOtherAddrLen(bytes.into()));
                }
                let other_addr = H160::from(bytes);
                if other_addr == self.swap_contract_address {
                    return Ok(Some(self.swap_contract_address.to_vec().into()));
                }

                if Some(other_addr) == self.fallback_swap_contract {
                    return Ok(self.fallback_swap_contract.map(|addr| addr.to_vec().into()));
                }
                MmError::err(NegotiateSwapContractAddrErr::UnexpectedOtherAddr(bytes.into()))
            },
            None => self
                .fallback_swap_contract
                .map(|addr| Some(addr.to_vec().into()))
                .ok_or_else(|| MmError::new(NegotiateSwapContractAddrErr::NoOtherAddrAndNoFallback)),
        }
    }
}

impl MarketCoinOps for Qrc20Coin {
    fn ticker(&self) -> &str { &self.utxo.conf.ticker }

    fn my_address(&self) -> Result<String, String> { utxo_common::my_address(self) }

    fn my_balance(&self) -> BalanceFut<CoinBalance> {
        let my_address = self.my_addr_as_contract_addr();
        let params = [Token::Address(my_address)];
        let contract_address = self.contract_address;
        let decimals = self.utxo.decimals;

        let coin = self.clone();
        let fut = async move {
            let tokens = coin
                .utxo
                .rpc_client
                .rpc_contract_call(ViewContractCallType::BalanceOf, &contract_address, &params)
                .compat()
                .await?;
            let spendable = match tokens.first() {
                Some(Token::Uint(bal)) => u256_to_big_decimal(*bal, decimals)?,
                _ => {
                    let error = format!("Expected U256 as balanceOf result but got {:?}", tokens);
                    return MmError::err(BalanceError::InvalidResponse(error));
                },
            };
            Ok(CoinBalance {
                spendable,
                unspendable: BigDecimal::from(0),
            })
        };
        Box::new(fut.boxed().compat())
    }

    fn base_coin_balance(&self) -> BalanceFut<BigDecimal> {
        let selfi = self.clone();
        let fut = async move {
            let CoinBalance { spendable, .. } = selfi.qtum_balance().await?;
            Ok(spendable)
        };
        Box::new(fut.boxed().compat())
    }

    fn send_raw_tx(&self, tx: &str) -> Box<dyn Future<Item = String, Error = String> + Send> {
        utxo_common::send_raw_tx(&self.utxo, tx)
    }

    fn wait_for_confirmations(
        &self,
        tx: &[u8],
        confirmations: u64,
        requires_nota: bool,
        wait_until: u64,
        check_every: u64,
    ) -> Box<dyn Future<Item = (), Error = String> + Send> {
        let tx: UtxoTx = try_fus!(deserialize(tx).map_err(|e| ERRL!("{:?}", e)));
        let selfi = self.clone();
        let fut = async move {
            selfi
                .wait_for_confirmations_and_check_result(tx, confirmations, requires_nota, wait_until, check_every)
                .await
        };
        Box::new(fut.boxed().compat())
    }

    fn wait_for_tx_spend(
        &self,
        transaction: &[u8],
        wait_until: u64,
        from_block: u64,
        _swap_contract_address: &Option<BytesJson>,
    ) -> TransactionFut {
        let tx: UtxoTx = try_fus!(deserialize(transaction).map_err(|e| ERRL!("{:?}", e)));

        let selfi = self.clone();
        let fut = async move { selfi.wait_for_tx_spend_impl(tx, wait_until, from_block).await };
        Box::new(fut.boxed().compat())
    }

    fn tx_enum_from_bytes(&self, bytes: &[u8]) -> Result<TransactionEnum, String> {
        utxo_common::tx_enum_from_bytes(self.as_ref(), bytes)
    }

    fn current_block(&self) -> Box<dyn Future<Item = u64, Error = String> + Send> {
        utxo_common::current_block(&self.utxo)
    }

    fn address_from_pubkey_str(&self, pubkey: &str) -> Result<String, String> {
        utxo_common::address_from_pubkey_str(self, pubkey)
    }

    fn display_priv_key(&self) -> String { utxo_common::display_priv_key(&self.utxo) }

    fn min_tx_amount(&self) -> BigDecimal { BigDecimal::from(0) }

    fn min_trading_vol(&self) -> MmNumber {
        let pow = self.utxo.decimals / 3;
        MmNumber::from(1) / MmNumber::from(10u64.pow(pow as u32))
    }
}

impl MmCoin for Qrc20Coin {
    fn is_asset_chain(&self) -> bool { utxo_common::is_asset_chain(&self.utxo) }

    fn withdraw(&self, req: WithdrawRequest) -> WithdrawFut {
        Box::new(qrc20_withdraw(self.clone(), req).boxed().compat())
    }

    fn get_raw_transaction(&self, req: RawTransactionRequest) -> RawTransactionFut {
        Box::new(utxo_common::get_raw_transaction(self.clone(), req).boxed().compat())
    }

    fn decimals(&self) -> u8 { utxo_common::decimals(&self.utxo) }

    fn convert_to_address(&self, from: &str, to_address_format: Json) -> Result<String, String> {
        qtum::QtumBasedCoin::convert_to_address(self, from, to_address_format)
    }

    fn validate_address(&self, address: &str) -> ValidateAddressResult { utxo_common::validate_address(self, address) }

    fn process_history_loop(&self, ctx: MmArc) -> Box<dyn Future<Item = (), Error = ()> + Send> {
        Box::new(self.clone().history_loop(ctx).map(|_| Ok(())).boxed().compat())
    }

    fn history_sync_status(&self) -> HistorySyncState { utxo_common::history_sync_status(&self.utxo) }

    /// This method is called to check our QTUM balance.
    fn get_trade_fee(&self) -> Box<dyn Future<Item = TradeFee, Error = String> + Send> {
        // `erc20Payment` may require two `approve` contract calls in worst case,
        let gas_fee = (2 * QRC20_GAS_LIMIT_DEFAULT + QRC20_PAYMENT_GAS_LIMIT) * QRC20_GAS_PRICE_DEFAULT;

        let selfi = self.clone();
        let fut = async move {
            let fee = try_s!(selfi.get_qrc20_tx_fee(gas_fee).await);
            Ok(TradeFee {
                coin: selfi.platform.clone(),
                amount: big_decimal_from_sat(fee as i64, selfi.utxo.decimals).into(),
                paid_from_trading_vol: false,
            })
        };
        Box::new(fut.boxed().compat())
    }

    fn get_sender_trade_fee(&self, value: TradePreimageValue, stage: FeeApproxStage) -> TradePreimageFut<TradeFee> {
        let selfi = self.clone();
        let decimals = self.utxo.decimals;
        let fut = async move {
            // pass the dummy params
            let timelock = (now_ms() / 1000) as u32;
            let secret_hash = vec![0; 20];
            let swap_id = qrc20_swap_id(timelock, &secret_hash);
            let receiver_addr = H160::default();
            // we can avoid the requesting balance, because it doesn't affect the total fee
            let my_balance = U256::max_value();
            let value = match value {
                TradePreimageValue::Exact(value) | TradePreimageValue::UpperBound(value) => {
                    wei_from_big_decimal(&value, decimals)?
                },
            };

            let erc20_payment_fee = {
                let erc20_payment_outputs = selfi
                    .generate_swap_payment_outputs(
                        my_balance,
                        swap_id.clone(),
                        value,
                        timelock,
                        secret_hash.clone(),
                        receiver_addr,
                        selfi.swap_contract_address,
                    )
                    .await?;
                selfi
                    .preimage_trade_fee_required_to_send_outputs(erc20_payment_outputs, &stage)
                    .await?
            };

            let sender_refund_fee = {
                let sender_refund_output = selfi.sender_refund_output(
                    &selfi.swap_contract_address,
                    swap_id,
                    value,
                    secret_hash,
                    receiver_addr,
                )?;
                selfi
                    .preimage_trade_fee_required_to_send_outputs(vec![sender_refund_output], &stage)
                    .await?
            };

            let total_fee = erc20_payment_fee + sender_refund_fee;
            Ok(TradeFee {
                coin: selfi.platform.clone(),
                amount: total_fee.into(),
                paid_from_trading_vol: false,
            })
        };
        Box::new(fut.boxed().compat())
    }

    fn get_receiver_trade_fee(&self, stage: FeeApproxStage) -> TradePreimageFut<TradeFee> {
        let selfi = self.clone();
        let fut = async move {
            // pass the dummy params
            let timelock = (now_ms() / 1000) as u32;
            let secret = vec![0; 32];
            let swap_id = qrc20_swap_id(timelock, &secret[0..20]);
            let sender_addr = H160::default();
            // get the max available value that we can pass into the contract call params
            // see `generate_contract_call_script_pubkey`
            let value = u64::max_value().into();
            let output =
                selfi.receiver_spend_output(&selfi.swap_contract_address, swap_id, value, secret, sender_addr)?;

            let total_fee = selfi
                .preimage_trade_fee_required_to_send_outputs(vec![output], &stage)
                .await?;
            Ok(TradeFee {
                coin: selfi.platform.clone(),
                amount: total_fee.into(),
                paid_from_trading_vol: false,
            })
        };
        Box::new(fut.boxed().compat())
    }

    fn get_fee_to_send_taker_fee(
        &self,
        dex_fee_amount: BigDecimal,
        stage: FeeApproxStage,
    ) -> TradePreimageFut<TradeFee> {
        let selfi = self.clone();
        let fut = async move {
            let amount = wei_from_big_decimal(&dex_fee_amount, selfi.utxo.decimals)?;

            // pass the dummy params
            let to_addr = H160::default();
            let transfer_output =
                selfi.transfer_output(to_addr, amount, QRC20_GAS_LIMIT_DEFAULT, QRC20_GAS_PRICE_DEFAULT)?;

            let total_fee = selfi
                .preimage_trade_fee_required_to_send_outputs(vec![transfer_output], &stage)
                .await?;
            Ok(TradeFee {
                coin: selfi.platform.clone(),
                amount: total_fee.into(),
                paid_from_trading_vol: false,
            })
        };
        Box::new(fut.boxed().compat())
    }

    fn required_confirmations(&self) -> u64 { utxo_common::required_confirmations(&self.utxo) }

    fn requires_notarization(&self) -> bool { utxo_common::requires_notarization(&self.utxo) }

    fn set_required_confirmations(&self, confirmations: u64) {
        utxo_common::set_required_confirmations(&self.utxo, confirmations)
    }

    fn set_requires_notarization(&self, requires_nota: bool) {
        utxo_common::set_requires_notarization(&self.utxo, requires_nota)
    }

    fn swap_contract_address(&self) -> Option<BytesJson> {
        Some(BytesJson::from(self.swap_contract_address.0.as_ref()))
    }

    fn mature_confirmations(&self) -> Option<u32> { Some(self.utxo.conf.mature_confirmations) }
}

pub fn qrc20_swap_id(time_lock: u32, secret_hash: &[u8]) -> Vec<u8> {
    let mut input = vec![];
    input.extend_from_slice(&time_lock.to_le_bytes());
    input.extend_from_slice(secret_hash);
    sha256(&input).to_vec()
}

fn contract_addr_into_rpc_format(address: &H160) -> H160Json { H160Json::from(address.0) }

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Qrc20FeeDetails {
    /// Coin name
    coin: String,
    /// Standard UTXO miner fee based on transaction size
    miner_fee: BigDecimal,
    /// Gas limit in satoshi.
    gas_limit: u64,
    /// Gas price in satoshi.
    gas_price: u64,
    /// Total used gas.
    total_gas_fee: BigDecimal,
}

async fn qrc20_withdraw(coin: Qrc20Coin, req: WithdrawRequest) -> WithdrawResult {
    let to_addr = UtxoAddress::from_str(&req.to)
        .map_err(|e| e.to_string())
        .map_to_mm(WithdrawError::InvalidAddress)?;
    let conf = &coin.utxo.conf;
    let is_p2pkh = to_addr.prefix == conf.pub_addr_prefix && to_addr.t_addr_prefix == conf.pub_t_addr_prefix;
    let is_p2sh =
        to_addr.prefix == conf.p2sh_addr_prefix && to_addr.t_addr_prefix == conf.p2sh_t_addr_prefix && conf.segwit;
    if !is_p2pkh && !is_p2sh {
        let error = "Expected either P2PKH or P2SH".to_owned();
        return MmError::err(WithdrawError::InvalidAddress(error));
    }

    let _utxo_lock = UTXO_LOCK.lock().await;

    let qrc20_balance = coin.my_spendable_balance().compat().await?;

    // the qrc20_amount_sat is used only within smart contract calls
    let (qrc20_amount_sat, qrc20_amount) = if req.max {
        let amount = wei_from_big_decimal(&qrc20_balance, coin.utxo.decimals)?;
        if amount.is_zero() {
            return MmError::err(WithdrawError::ZeroBalanceToWithdrawMax);
        }
        (amount, qrc20_balance.clone())
    } else {
        let amount_sat = wei_from_big_decimal(&req.amount, coin.utxo.decimals)?;
        if req.amount > qrc20_balance {
            return MmError::err(WithdrawError::NotSufficientBalance {
                coin: coin.ticker().to_owned(),
                available: qrc20_balance,
                required: req.amount,
            });
        }
        (amount_sat, req.amount)
    };

    let (gas_limit, gas_price) = match req.fee {
        Some(WithdrawFee::Qrc20Gas { gas_limit, gas_price }) => (gas_limit, gas_price),
        Some(fee_policy) => {
            let error = format!("Expected 'Qrc20Gas' fee type, found {:?}", fee_policy);
            return MmError::err(WithdrawError::InvalidFeePolicy(error));
        },
        None => (QRC20_GAS_LIMIT_DEFAULT, QRC20_GAS_PRICE_DEFAULT),
    };

    // [`Qrc20Coin::transfer_output`] shouldn't fail if the arguments are correct
    let transfer_output = coin.transfer_output(
        qtum::contract_addr_from_utxo_addr(to_addr.clone()),
        qrc20_amount_sat,
        gas_limit,
        gas_price,
    )?;
    let outputs = vec![transfer_output];

    let GenerateQrc20TxResult {
        signed,
        miner_fee,
        gas_fee,
    } = coin.generate_qrc20_transaction(outputs).await.mm_err(|gen_tx_error| {
        WithdrawError::from_generate_tx_error(gen_tx_error, coin.platform.clone(), coin.utxo.decimals)
    })?;

    let received_by_me = if to_addr == coin.utxo.my_address {
        qrc20_amount.clone()
    } else {
        0.into()
    };
    let my_balance_change = &received_by_me - &qrc20_amount;

    // [`MarketCoinOps::my_address`] and [`UtxoCommonOps::display_address`] shouldn't fail
    let my_address = coin.my_address().map_to_mm(WithdrawError::InternalError)?;
    let to_address = to_addr.display_address().map_to_mm(WithdrawError::InternalError)?;

    let fee_details = Qrc20FeeDetails {
        // QRC20 fees are paid in base platform currency (in particular Qtum)
        coin: coin.platform.clone(),
        miner_fee: utxo_common::big_decimal_from_sat(miner_fee as i64, coin.utxo.decimals),
        gas_limit,
        gas_price,
        total_gas_fee: utxo_common::big_decimal_from_sat(gas_fee as i64, coin.utxo.decimals),
    };
    Ok(TransactionDetails {
        from: vec![my_address],
        to: vec![to_address],
        total_amount: qrc20_amount.clone(),
        spent_by_me: qrc20_amount,
        received_by_me,
        my_balance_change,
        tx_hash: signed.hash().reversed().to_vec().into(),
        tx_hex: serialize(&signed).into(),
        fee_details: Some(fee_details.into()),
        block_height: 0,
        coin: conf.ticker.clone(),
        internal_id: vec![].into(),
        timestamp: now_ms() / 1000,
        kmd_rewards: None,
    })
}

/// Parse the given topic to `H160` address.
fn address_from_log_topic(topic: &str) -> Result<H160, String> {
    if topic.len() != 64 {
        return ERR!(
            "Topic {:?} is expected to be H256 encoded topic (with length of 64)",
            topic
        );
    }

    // skip the first 24 characters to parse the last 40 characters to H160.
    // https://github.com/qtumproject/qtum-electrum/blob/v4.0.2/electrum/wallet.py#L2112
    let hash = try_s!(H160Json::from_str(&topic[24..]));
    Ok(hash.0.into())
}

fn address_to_log_topic(address: &H160) -> String {
    let zeros = std::str::from_utf8(&[b'0'; 24]).expect("Expected a valid str from slice of '0' chars");
    let mut topic = format!("{:02x}", address);
    topic.insert_str(0, zeros);
    topic
}

pub struct TransferEventDetails {
    contract_address: H160,
    amount: U256,
    sender: H160,
    receiver: H160,
}

fn transfer_event_from_log(log: &LogEntry) -> Result<TransferEventDetails, String> {
    let contract_address = if log.address.starts_with("0x") {
        try_s!(qtum::contract_addr_from_str(&log.address))
    } else {
        let address = format!("0x{}", log.address);
        try_s!(qtum::contract_addr_from_str(&address))
    };

    if log.topics.len() != 3 {
        return ERR!("'Transfer' event must have 3 topics, found, {}", log.topics.len());
    }

    // https://github.com/qtumproject/qtum-electrum/blob/v4.0.2/electrum/wallet.py#L2111
    let amount = try_s!(U256::from_str(&log.data));

    // https://github.com/qtumproject/qtum-electrum/blob/v4.0.2/electrum/wallet.py#L2112
    let sender = try_s!(address_from_log_topic(&log.topics[1]));
    // https://github.com/qtumproject/qtum-electrum/blob/v4.0.2/electrum/wallet.py#L2113
    let receiver = try_s!(address_from_log_topic(&log.topics[2]));
    Ok(TransferEventDetails {
        contract_address,
        amount,
        sender,
        receiver,
    })
}
