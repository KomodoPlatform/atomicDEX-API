/******************************************************************************
 * Copyright © 2014-2019 The SuperNET Developers.                             *
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
//  utxo.rs
//  marketmaker
//
//  Copyright © 2017-2019 SuperNET. All rights reserved.
//

pub mod qtum;
pub mod rpc_clients;
pub mod slp;
pub mod utxo_common;
pub mod utxo_standard;

#[cfg(not(target_arch = "wasm32"))] pub mod tx_cache;

use async_trait::async_trait;
use bigdecimal::BigDecimal;
pub use bitcrypto::{dhash160, sha256, ChecksumType};
use chain::{OutPoint, TransactionInput, TransactionOutput, TxHashAlgo};
use common::executor::{spawn, Timer};
#[cfg(not(target_arch = "wasm32"))]
use common::first_char_to_upper;
use common::jsonrpc_client::JsonRpcError;
use common::mm_ctx::MmArc;
use common::mm_error::prelude::*;
use common::mm_metrics::MetricsArc;
use common::{now_ms, small_rng};
use derive_more::Display;
#[cfg(not(target_arch = "wasm32"))] use dirs::home_dir;
use futures::channel::mpsc;
use futures::compat::Future01CompatExt;
use futures::lock::{Mutex as AsyncMutex, MutexGuard as AsyncMutexGuard};
use futures::stream::StreamExt;
use futures01::Future;
use keys::bytes::Bytes;
pub use keys::{Address, AddressFormat as UtxoAddressFormat, KeyPair, Private, Public, Secret, Type};
#[cfg(test)] use mocktopus::macros::*;
use num_traits::ToPrimitive;
use primitives::hash::{H256, H264, H512};
use rand::seq::SliceRandom;
use rpc::v1::types::{Bytes as BytesJson, Transaction as RpcTransaction, H256 as H256Json};
use script::{Builder, Script, SignatureVersion, TransactionInputSigner};
use serde_json::{self as json, Value as Json};
use serialization::{serialize, serialize_with_flags, SERIALIZE_TRANSACTION_WITNESS};
use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::num::NonZeroU64;
use std::ops::Deref;
#[cfg(not(target_arch = "wasm32"))] use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{Arc, Mutex, Weak};
use utxo_common::big_decimal_from_sat;

pub use chain::Transaction as UtxoTx;

#[cfg(not(target_arch = "wasm32"))]
use self::rpc_clients::{ConcurrentRequestMap, NativeClient, NativeClientImpl};
use self::rpc_clients::{ElectrumClient, ElectrumClientImpl, ElectrumRpcRequest, EstimateFeeMethod, EstimateFeeMode,
                        UnspentInfo, UtxoRpcClientEnum, UtxoRpcError, UtxoRpcResult};
use super::{BalanceError, BalanceFut, BalanceResult, CoinTransportMetrics, CoinsContext, FeeApproxStage,
            FoundSwapTxSpend, HistorySyncState, KmdRewardsDetails, MarketCoinOps, MmCoin, NumConversError,
            NumConversResult, RawTransactionError, RawTransactionFut, RawTransactionRequest, RawTransactionResult,
            RpcClientType, RpcTransportEventHandler, RpcTransportEventHandlerShared, TradeFee, TradePreimageError,
            TradePreimageFut, TradePreimageResult, Transaction, TransactionDetails, TransactionEnum, TransactionFut,
            WithdrawError, WithdrawFee, WithdrawRequest};
#[cfg(test)] pub mod utxo_tests;
#[cfg(target_arch = "wasm32")] pub mod utxo_wasm_tests;

const SWAP_TX_SPEND_SIZE: u64 = 305;
const KILO_BYTE: u64 = 1000;
/// https://bitcoin.stackexchange.com/a/77192
const MAX_DER_SIGNATURE_LEN: usize = 72;
const COMPRESSED_PUBKEY_LEN: usize = 33;
const P2PKH_OUTPUT_LEN: u64 = 34;
const MATURE_CONFIRMATIONS_DEFAULT: u32 = 100;
const UTXO_DUST_AMOUNT: u64 = 1000;
/// Block count for KMD median time past calculation
///
/// # Safety
/// 11 > 0
const KMD_MTP_BLOCK_COUNT: NonZeroU64 = unsafe { NonZeroU64::new_unchecked(11u64) };
const DEFAULT_DYNAMIC_FEE_VOLATILITY_PERCENT: f64 = 0.5;

pub type GenerateTxResult = Result<(TransactionInputSigner, AdditionalTxData), MmError<GenerateTxError>>;
pub type HistoryUtxoTxMap = HashMap<H256Json, HistoryUtxoTx>;

#[cfg(windows)]
#[cfg(not(target_arch = "wasm32"))]
fn get_special_folder_path() -> PathBuf {
    use libc::c_char;
    use std::ffi::CStr;
    use std::mem::zeroed;
    use std::ptr::null_mut;
    use winapi::shared::minwindef::MAX_PATH;
    use winapi::um::shlobj::SHGetSpecialFolderPathA;
    use winapi::um::shlobj::CSIDL_APPDATA;

    let mut buf: [c_char; MAX_PATH + 1] = unsafe { zeroed() };
    // https://docs.microsoft.com/en-us/windows/desktop/api/shlobj_core/nf-shlobj_core-shgetspecialfolderpatha
    let rc = unsafe { SHGetSpecialFolderPathA(null_mut(), buf.as_mut_ptr(), CSIDL_APPDATA, 1) };
    if rc != 1 {
        panic!("!SHGetSpecialFolderPathA")
    }
    Path::new(unsafe { CStr::from_ptr(buf.as_ptr()) }.to_str().unwrap()).to_path_buf()
}

#[cfg(not(windows))]
#[cfg(not(target_arch = "wasm32"))]
fn get_special_folder_path() -> PathBuf { panic!("!windows") }

impl Transaction for UtxoTx {
    fn tx_hex(&self) -> Vec<u8> {
        if self.has_witness() {
            serialize_with_flags(self, SERIALIZE_TRANSACTION_WITNESS).into()
        } else {
            serialize(self).into()
        }
    }

    fn tx_hash(&self) -> BytesJson { self.hash().reversed().to_vec().into() }
}

impl From<JsonRpcError> for BalanceError {
    fn from(e: JsonRpcError) -> Self { BalanceError::Transport(e.to_string()) }
}

impl From<UtxoRpcError> for BalanceError {
    fn from(e: UtxoRpcError) -> Self {
        match e {
            UtxoRpcError::Internal(desc) => BalanceError::Internal(desc),
            _ => BalanceError::Transport(e.to_string()),
        }
    }
}

impl From<UtxoRpcError> for WithdrawError {
    fn from(e: UtxoRpcError) -> Self {
        match e {
            UtxoRpcError::Transport(transport) | UtxoRpcError::ResponseParseError(transport) => {
                WithdrawError::Transport(transport.to_string())
            },
            UtxoRpcError::InvalidResponse(resp) => WithdrawError::Transport(resp),
            UtxoRpcError::Internal(internal) => WithdrawError::InternalError(internal),
        }
    }
}

impl From<JsonRpcError> for TradePreimageError {
    fn from(e: JsonRpcError) -> Self { TradePreimageError::Transport(e.to_string()) }
}

impl From<UtxoRpcError> for TradePreimageError {
    fn from(e: UtxoRpcError) -> Self {
        match e {
            UtxoRpcError::Transport(transport) | UtxoRpcError::ResponseParseError(transport) => {
                TradePreimageError::Transport(transport.to_string())
            },
            UtxoRpcError::InvalidResponse(resp) => TradePreimageError::Transport(resp),
            UtxoRpcError::Internal(internal) => TradePreimageError::InternalError(internal),
        }
    }
}

/// The `UtxoTx` with the block height transaction mined in.
pub struct HistoryUtxoTx {
    pub height: Option<u64>,
    pub tx: UtxoTx,
}

/// Additional transaction data that can't be easily got from raw transaction without calling
/// additional RPC methods, e.g. to get input amount we need to request all previous transactions
/// and check output values
#[derive(Debug)]
pub struct AdditionalTxData {
    pub received_by_me: u64,
    pub spent_by_me: u64,
    pub fee_amount: u64,
    pub unused_change: Option<u64>,
    pub kmd_rewards: Option<KmdRewardsDetails>,
}

/// The fee set from coins config
#[derive(Debug)]
pub enum TxFee {
    /// Tell the coin that it has fixed tx fee not depending on transaction size
    Fixed(u64),
    /// Tell the coin that it should request the fee from daemon RPC and calculate it relying on tx size
    Dynamic(EstimateFeeMethod),
    FixedPerKb(u64),
}

/// The actual "runtime" fee that is received from RPC in case of dynamic calculation
#[derive(Debug, PartialEq)]
pub enum ActualTxFee {
    /// fixed tx fee not depending on transaction size
    Fixed(u64),
    /// fee amount per Kbyte received from coin RPC
    Dynamic(u64),
    /// Use specified amount per each 1 kb of transaction and also per each output less than amount.
    /// Used by DOGE, but more coins might support it too.
    FixedPerKb(u64),
}

/// Fee policy applied on transaction creation
pub enum FeePolicy {
    /// Send the exact amount specified in output(s), fee is added to spent input amount
    SendExact,
    /// Contains the index of output from which fee should be deducted
    DeductFromOutput(usize),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct CachedUnspentInfo {
    pub outpoint: OutPoint,
    pub value: u64,
}

impl From<UnspentInfo> for CachedUnspentInfo {
    fn from(unspent: UnspentInfo) -> CachedUnspentInfo {
        CachedUnspentInfo {
            outpoint: unspent.outpoint,
            value: unspent.value,
        }
    }
}

impl From<CachedUnspentInfo> for UnspentInfo {
    fn from(cached: CachedUnspentInfo) -> UnspentInfo {
        UnspentInfo {
            outpoint: cached.outpoint,
            value: cached.value,
            height: None,
        }
    }
}

/// The cache of recently send transactions used to track the spent UTXOs and replace them with new outputs
/// The daemon needs some time to update the listunspent list for address which makes it return already spent UTXOs
/// This cache helps to prevent UTXO reuse in such cases
pub struct RecentlySpentOutPoints {
    /// Maps CachedUnspentInfo A to a set of CachedUnspentInfo which `spent` A
    input_to_output_map: HashMap<CachedUnspentInfo, HashSet<CachedUnspentInfo>>,
    /// Maps CachedUnspentInfo A to a set of CachedUnspentInfo that `were spent by` A
    output_to_input_map: HashMap<CachedUnspentInfo, HashSet<CachedUnspentInfo>>,
    /// Cache includes only outputs having script_pubkey == for_script_pubkey
    for_script_pubkey: Bytes,
}

impl RecentlySpentOutPoints {
    fn new(for_script_pubkey: Bytes) -> Self {
        RecentlySpentOutPoints {
            input_to_output_map: HashMap::new(),
            output_to_input_map: HashMap::new(),
            for_script_pubkey,
        }
    }

    pub fn add_spent(&mut self, inputs: Vec<UnspentInfo>, spend_tx_hash: H256, outputs: Vec<TransactionOutput>) {
        let inputs: HashSet<_> = inputs.into_iter().map(From::from).collect();
        let to_replace: HashSet<_> = outputs
            .iter()
            .enumerate()
            .filter_map(|(index, output)| {
                if output.script_pubkey == self.for_script_pubkey {
                    Some(CachedUnspentInfo {
                        outpoint: OutPoint {
                            hash: spend_tx_hash.clone(),
                            index: index as u32,
                        },
                        value: output.value,
                    })
                } else {
                    None
                }
            })
            .collect();

        let mut prev_inputs_spent = HashSet::new();

        // check if inputs are already in spending cached chain
        for input in &inputs {
            if let Some(prev_inputs) = self.output_to_input_map.get(input) {
                for prev_input in prev_inputs {
                    if let Some(outputs) = self.input_to_output_map.get_mut(prev_input) {
                        prev_inputs_spent.insert(prev_input.clone());
                        outputs.remove(input);
                        for replace in &to_replace {
                            outputs.insert(replace.clone());
                        }
                    }
                }
            }
        }

        prev_inputs_spent.extend(inputs.clone());
        for output in &to_replace {
            self.output_to_input_map
                .insert(output.clone(), prev_inputs_spent.clone());
        }

        for input in inputs {
            self.input_to_output_map.insert(input, to_replace.clone());
        }
    }

    pub fn replace_spent_outputs_with_cache(&self, mut outputs: HashSet<UnspentInfo>) -> HashSet<UnspentInfo> {
        let mut replacement_unspents = HashSet::new();
        outputs = outputs
            .into_iter()
            .filter(|unspent| {
                let outs = self.input_to_output_map.get(&unspent.clone().into());
                match outs {
                    Some(outs) => {
                        for out in outs.iter() {
                            if !replacement_unspents.contains(out) {
                                replacement_unspents.insert(out.clone());
                            }
                        }
                        false
                    },
                    None => true,
                }
            })
            .collect();
        if replacement_unspents.is_empty() {
            return outputs;
        }
        outputs.extend(replacement_unspents.into_iter().map(From::from));
        self.replace_spent_outputs_with_cache(outputs)
    }
}

#[derive(Debug, Deserialize)]
pub enum BlockchainNetwork {
    #[serde(rename = "mainnet")]
    Mainnet,
    #[serde(rename = "testnet")]
    Testnet,
    #[serde(rename = "regtest")]
    Regtest,
}

#[derive(Debug)]
pub struct UtxoCoinConf {
    pub ticker: String,
    /// https://en.bitcoin.it/wiki/List_of_address_prefixes
    /// https://github.com/jl777/coins/blob/master/coins
    pub pub_addr_prefix: u8,
    pub p2sh_addr_prefix: u8,
    pub wif_prefix: u8,
    pub pub_t_addr_prefix: u8,
    pub p2sh_t_addr_prefix: u8,
    // https://github.com/bitcoin/bips/blob/master/bip-0173.mediawiki#Segwit_address_format
    pub bech32_hrp: Option<String>,
    /// True if coins uses Proof of Stake consensus algo
    /// Proof of Work is expected by default
    /// https://en.bitcoin.it/wiki/Proof_of_Stake
    /// https://en.bitcoin.it/wiki/Proof_of_work
    /// The actual meaning of this is nTime field is used in transaction
    pub is_pos: bool,
    /// Special field for Zcash and it's forks
    /// Defines if Overwinter network upgrade was activated
    /// https://z.cash/upgrade/overwinter/
    pub overwintered: bool,
    /// The tx version used to detect the transaction ser/de/signing algo
    /// For now it's mostly used for Zcash and forks because they changed the algo in
    /// Overwinter and then Sapling upgrades
    /// https://github.com/zcash/zips/blob/master/zip-0243.rst
    pub tx_version: i32,
    /// If true - allow coins withdraw to P2SH addresses (Segwit).
    /// the flag will also affect the address that MM2 generates by default in the future
    /// will be the Segwit (starting from 3 for BTC case) instead of legacy
    /// https://en.bitcoin.it/wiki/Segregated_Witness
    pub segwit: bool,
    /// Does coin require transactions to be notarized to be considered as confirmed?
    /// https://komodoplatform.com/security-delayed-proof-of-work-dpow/
    pub requires_notarization: AtomicBool,
    /// The address format indicates the default address format from coin config file
    pub default_address_format: UtxoAddressFormat,
    /// Is current coin KMD asset chain?
    /// https://komodoplatform.atlassian.net/wiki/spaces/KPSD/pages/71729160/What+is+a+Parallel+Chain+Asset+Chain
    pub asset_chain: bool,
    /// Dynamic transaction fee volatility in percent. The value is used to predict a possible increase in dynamic fee.
    pub tx_fee_volatility_percent: f64,
    /// Transaction version group id for Zcash transactions since Overwinter: https://github.com/zcash/zips/blob/master/zip-0202.rst
    pub version_group_id: u32,
    /// Consensus branch id for Zcash transactions since Overwinter: https://github.com/zcash/zcash/blob/master/src/consensus/upgrades.cpp#L11
    /// used in transaction sig hash calculation
    pub consensus_branch_id: u32,
    /// Defines if coin uses Zcash transaction format
    pub zcash: bool,
    /// Address and privkey checksum type
    pub checksum_type: ChecksumType,
    /// Fork id used in sighash
    pub fork_id: u32,
    /// Signature version
    pub signature_version: SignatureVersion,
    pub required_confirmations: AtomicU64,
    /// if set to true MM2 will check whether calculated fee is lower than relay fee and use
    /// relay fee amount instead of calculated
    /// https://github.com/KomodoPlatform/atomicDEX-API/issues/617
    pub force_min_relay_fee: bool,
    /// Block count for median time past calculation
    pub mtp_block_count: NonZeroU64,
    pub estimate_fee_mode: Option<EstimateFeeMode>,
    /// The minimum number of confirmations at which a transaction is considered mature
    pub mature_confirmations: u32,
    /// The number of blocks used for estimate_fee/estimate_smart_fee RPC calls
    pub estimate_fee_blocks: u32,
}

#[derive(Debug)]
pub struct UtxoCoinFields {
    /// UTXO coin config
    pub conf: UtxoCoinConf,
    /// Default decimals amount is 8 (BTC and almost all other UTXO coins)
    /// But there are forks which have different decimals:
    /// Peercoin has 6
    /// Emercoin has 6
    /// Bitcoin Diamond has 7
    pub decimals: u8,
    pub tx_fee: TxFee,
    /// Minimum transaction value at which the value is not less than fee
    pub dust_amount: u64,
    /// RPC client
    pub rpc_client: UtxoRpcClientEnum,
    /// ECDSA key pair
    pub key_pair: KeyPair,
    /// Lock the mutex when we deal with address utxos
    pub my_address: Address,
    pub history_sync_state: Mutex<HistorySyncState>,
    /// Path to the TX cache directory
    pub tx_cache_directory: Option<PathBuf>,
    /// The cache of recently send transactions used to track the spent UTXOs and replace them with new outputs
    /// The daemon needs some time to update the listunspent list for address which makes it return already spent UTXOs
    /// This cache helps to prevent UTXO reuse in such cases
    pub recently_spent_outpoints: AsyncMutex<RecentlySpentOutPoints>,
    pub tx_hash_algo: TxHashAlgo,
}

#[derive(Debug, Display)]
pub enum UnsupportedAddr {
    #[display(
        fmt = "{} address format activated for {}, but {} format used instead",
        activated_format,
        ticker,
        used_format
    )]
    FormatMismatch {
        ticker: String,
        activated_format: String,
        used_format: String,
    },
    #[display(fmt = "Expected a valid P2PKH or P2SH prefix for {}", _0)]
    PrefixError(String),
    #[display(fmt = "Address hrp {} is not a valid hrp for {}", hrp, ticker)]
    HrpError { ticker: String, hrp: String },
    #[display(fmt = "Segwit not activated in the config for {}", _0)]
    SegwitNotActivated(String),
}

impl UtxoCoinFields {
    pub fn transaction_preimage(&self) -> TransactionInputSigner {
        let lock_time = if self.conf.ticker == "KMD" {
            (now_ms() / 1000) as u32 - 3600 + 777 * 2
        } else {
            (now_ms() / 1000) as u32
        };

        TransactionInputSigner {
            version: self.conf.tx_version,
            n_time: None,
            overwintered: self.conf.overwintered,
            version_group_id: self.conf.version_group_id,
            consensus_branch_id: self.conf.consensus_branch_id,
            expiry_height: 0,
            value_balance: 0,
            inputs: vec![],
            outputs: vec![],
            lock_time,
            join_splits: vec![],
            shielded_spends: vec![],
            shielded_outputs: vec![],
            zcash: self.conf.zcash,
            str_d_zeel: None,
            hash_algo: self.tx_hash_algo.into(),
        }
    }

    pub fn check_withdraw_address_supported(&self, addr: &Address) -> Result<(), MmError<UnsupportedAddr>> {
        let conf = &self.conf;

        match addr.addr_format {
            // Considering that legacy is supported with any configured formats
            // This can be changed depending on the coins implementation
            UtxoAddressFormat::Standard => {
                let is_p2pkh = addr.prefix == conf.pub_addr_prefix && addr.t_addr_prefix == conf.pub_t_addr_prefix;
                let is_p2sh = addr.prefix == conf.p2sh_addr_prefix
                    && addr.t_addr_prefix == conf.p2sh_t_addr_prefix
                    && conf.segwit;
                if !is_p2pkh && !is_p2sh {
                    MmError::err(UnsupportedAddr::PrefixError(conf.ticker.clone()))
                } else {
                    Ok(())
                }
            },
            UtxoAddressFormat::Segwit => {
                if !conf.segwit {
                    return MmError::err(UnsupportedAddr::SegwitNotActivated(conf.ticker.clone()));
                }

                if addr.hrp != conf.bech32_hrp {
                    MmError::err(UnsupportedAddr::HrpError {
                        ticker: conf.ticker.clone(),
                        hrp: addr.hrp.clone().unwrap_or_default(),
                    })
                } else {
                    Ok(())
                }
            },
            UtxoAddressFormat::CashAddress { .. } => {
                if addr.addr_format == conf.default_address_format || addr.addr_format == self.my_address.addr_format {
                    Ok(())
                } else {
                    MmError::err(UnsupportedAddr::FormatMismatch {
                        ticker: conf.ticker.clone(),
                        activated_format: self.my_address.addr_format.to_string(),
                        used_format: addr.addr_format.to_string(),
                    })
                }
            },
        }
    }
}

#[async_trait]
#[cfg_attr(test, mockable)]
pub trait UtxoCommonOps {
    async fn get_tx_fee(&self) -> Result<ActualTxFee, JsonRpcError>;

    async fn get_htlc_spend_fee(&self) -> UtxoRpcResult<u64>;

    fn addresses_from_script(&self, script: &Script) -> Result<Vec<Address>, String>;

    fn denominate_satoshis(&self, satoshi: i64) -> f64;

    fn my_public_key(&self) -> &Public;

    /// Try to parse address from string using specified on asset enable format,
    /// and if it failed inform user that he used a wrong format.
    fn address_from_str(&self, address: &str) -> Result<Address, String>;

    async fn get_current_mtp(&self) -> UtxoRpcResult<u32>;

    /// Check if the output is spendable (is not coinbase or it has enough confirmations).
    fn is_unspent_mature(&self, output: &RpcTransaction) -> bool;

    /// Generates unsigned transaction (TransactionInputSigner) from specified utxos and outputs.
    /// This function expects that utxos are sorted by amounts in ascending order
    /// Consider sorting before calling this function
    /// Sends the change (inputs amount - outputs amount) to "my_address"
    /// Also returns additional transaction data
    async fn generate_transaction(
        &self,
        utxos: Vec<UnspentInfo>,
        outputs: Vec<TransactionOutput>,
        fee_policy: FeePolicy,
        fee: Option<ActualTxFee>,
        gas_fee: Option<u64>,
    ) -> GenerateTxResult;

    /// Calculates interest if the coin is KMD
    /// Adds the value to existing output to my_script_pub or creates additional interest output
    /// returns transaction and data as is if the coin is not KMD
    async fn calc_interest_if_required(
        &self,
        mut unsigned: TransactionInputSigner,
        mut data: AdditionalTxData,
        my_script_pub: Bytes,
    ) -> UtxoRpcResult<(TransactionInputSigner, AdditionalTxData)>;

    /// Calculates interest of the specified transaction.
    /// Please note, this method has to be used for KMD transactions only.
    async fn calc_interest_of_tx(&self, tx: &UtxoTx, input_transactions: &mut HistoryUtxoTxMap) -> UtxoRpcResult<u64>;

    /// Try to get a `HistoryUtxoTx` transaction from `utxo_tx_map` or try to request it from Rpc client.
    async fn get_mut_verbose_transaction_from_map_or_rpc<'a, 'b>(
        &'a self,
        tx_hash: H256Json,
        utxo_tx_map: &'b mut HistoryUtxoTxMap,
    ) -> UtxoRpcResult<&'b mut HistoryUtxoTx>;

    async fn p2sh_spending_tx(
        &self,
        prev_transaction: UtxoTx,
        redeem_script: Bytes,
        outputs: Vec<TransactionOutput>,
        script_data: Script,
        sequence: u32,
        lock_time: u32,
    ) -> Result<UtxoTx, String>;

    /// Get transaction outputs available to spend.
    async fn ordered_mature_unspents<'a>(
        &'a self,
        address: &Address,
    ) -> UtxoRpcResult<(Vec<UnspentInfo>, AsyncMutexGuard<'a, RecentlySpentOutPoints>)>;

    /// Try to load verbose transaction from cache or try to request it from Rpc client.
    fn get_verbose_transaction_from_cache_or_rpc(
        &self,
        txid: H256Json,
    ) -> Box<dyn Future<Item = VerboseTransactionFrom, Error = String> + Send>;

    /// Cache transaction if the coin supports `TX_CACHE` and tx height is set and not zero.
    async fn cache_transaction_if_possible(&self, tx: &RpcTransaction) -> Result<(), String>;

    /// Returns available unspents in ascending order + RecentlySpentOutPoints MutexGuard for further interaction
    /// (e.g. to add new transaction to it).
    async fn list_unspent_ordered(
        &self,
        address: &Address,
    ) -> UtxoRpcResult<(Vec<UnspentInfo>, AsyncMutexGuard<'_, RecentlySpentOutPoints>)>;

    async fn preimage_trade_fee_required_to_send_outputs(
        &self,
        outputs: Vec<TransactionOutput>,
        fee_policy: FeePolicy,
        gas_fee: Option<u64>,
        stage: &FeeApproxStage,
    ) -> TradePreimageResult<BigDecimal>;

    /// Increase the given `dynamic_fee` according to the fee approximation `stage`.
    /// The method is used to predict a possible increase in dynamic fee.
    fn increase_dynamic_fee_by_stage(&self, dynamic_fee: u64, stage: &FeeApproxStage) -> u64;

    async fn p2sh_tx_locktime(&self, htlc_locktime: u32) -> Result<u32, MmError<UtxoRpcError>>;
}

#[async_trait]
pub trait UtxoStandardOps {
    /// Gets tx details by hash requesting the coin RPC if required.
    /// * `input_transactions` - the cache of the already requested transactions.
    async fn tx_details_by_hash(
        &self,
        hash: &[u8],
        input_transactions: &mut HistoryUtxoTxMap,
    ) -> Result<TransactionDetails, String>;

    async fn request_tx_history(&self, metrics: MetricsArc) -> RequestTxHistoryResult;

    /// Calculate the KMD rewards and re-calculate the transaction fee
    /// if the specified `tx_details` was generated without considering the KMD rewards.
    /// Please note, this method has to be used for KMD transactions only.
    async fn update_kmd_rewards(
        &self,
        tx_details: &mut TransactionDetails,
        input_transactions: &mut HistoryUtxoTxMap,
    ) -> UtxoRpcResult<()>;
}

#[derive(Clone, Debug)]
pub struct UtxoArc(Arc<UtxoCoinFields>);
impl Deref for UtxoArc {
    type Target = UtxoCoinFields;
    fn deref(&self) -> &UtxoCoinFields { &*self.0 }
}

impl From<UtxoCoinFields> for UtxoArc {
    fn from(coin: UtxoCoinFields) -> UtxoArc { UtxoArc(Arc::new(coin)) }
}

impl From<Arc<UtxoCoinFields>> for UtxoArc {
    fn from(arc: Arc<UtxoCoinFields>) -> UtxoArc { UtxoArc(arc) }
}

impl UtxoArc {
    /// Returns weak reference to the inner UtxoCoinFields
    fn downgrade(&self) -> UtxoWeak {
        let weak = Arc::downgrade(&self.0);
        UtxoWeak(weak)
    }
}

#[derive(Clone, Debug)]
pub struct UtxoWeak(Weak<UtxoCoinFields>);

impl From<Weak<UtxoCoinFields>> for UtxoWeak {
    fn from(weak: Weak<UtxoCoinFields>) -> Self { UtxoWeak(weak) }
}

impl UtxoWeak {
    fn upgrade(&self) -> Option<UtxoArc> { self.0.upgrade().map(UtxoArc::from) }
}

// We can use a shared UTXO lock for all UTXO coins at 1 time.
// It's highly likely that we won't experience any issues with it as we won't need to send "a lot" of transactions concurrently.
lazy_static! {
    pub static ref UTXO_LOCK: AsyncMutex<()> = AsyncMutex::new(());
}

#[derive(Debug, Display)]
pub enum GenerateTxError {
    #[display(
        fmt = "Couldn't generate tx from empty UTXOs set, required no less than {} satoshis",
        required
    )]
    EmptyUtxoSet { required: u64 },
    #[display(fmt = "Couldn't generate tx with empty output set")]
    EmptyOutputs,
    #[display(fmt = "Output value {} less than dust {}", value, dust)]
    OutputValueLessThanDust { value: u64, dust: u64 },
    #[display(
        fmt = "Output {} value {} is too small, required no less than {}",
        output_idx,
        output_value,
        required
    )]
    DeductFeeFromOutputFailed {
        output_idx: usize,
        output_value: u64,
        required: u64,
    },
    #[display(
        fmt = "Sum of input values {} is too small, required no less than {}",
        sum_utxos,
        required
    )]
    NotEnoughUtxos { sum_utxos: u64, required: u64 },
    #[display(fmt = "Transport error: {}", _0)]
    Transport(String),
    #[display(fmt = "Internal error: {}", _0)]
    Internal(String),
}

impl From<JsonRpcError> for GenerateTxError {
    fn from(rpc_err: JsonRpcError) -> Self { GenerateTxError::Transport(rpc_err.to_string()) }
}

impl From<UtxoRpcError> for GenerateTxError {
    fn from(e: UtxoRpcError) -> Self {
        match e {
            UtxoRpcError::Transport(rpc) | UtxoRpcError::ResponseParseError(rpc) => {
                GenerateTxError::Transport(rpc.to_string())
            },
            UtxoRpcError::InvalidResponse(error) => GenerateTxError::Transport(error),
            UtxoRpcError::Internal(error) => GenerateTxError::Internal(error),
        }
    }
}

impl From<NumConversError> for GenerateTxError {
    fn from(e: NumConversError) -> Self { GenerateTxError::Internal(e.to_string()) }
}

pub enum RequestTxHistoryResult {
    Ok(Vec<(H256Json, u64)>),
    Retry { error: String },
    HistoryTooLarge,
    UnknownError(String),
}

pub enum VerboseTransactionFrom {
    Cache(RpcTransaction),
    Rpc(RpcTransaction),
}

pub fn compressed_key_pair_from_bytes(raw: &[u8], prefix: u8, checksum_type: ChecksumType) -> Result<KeyPair, String> {
    if raw.len() != 32 {
        return ERR!("Invalid raw priv key len {}", raw.len());
    }

    let private = Private {
        prefix,
        compressed: true,
        secret: Secret::from(raw),
        checksum_type,
    };
    Ok(try_s!(KeyPair::from_private(private)))
}

pub fn compressed_pub_key_from_priv_raw(raw_priv: &[u8], sum_type: ChecksumType) -> Result<H264, String> {
    let key_pair: KeyPair = try_s!(compressed_key_pair_from_bytes(raw_priv, 0, sum_type));
    Ok(H264::from(&**key_pair.public()))
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct UtxoFeeDetails {
    pub amount: BigDecimal,
}

#[cfg(not(target_arch = "wasm32"))]
// https://github.com/KomodoPlatform/komodo/blob/master/zcutil/fetch-params.sh#L5
// https://github.com/KomodoPlatform/komodo/blob/master/zcutil/fetch-params.bat#L4
pub fn zcash_params_path() -> PathBuf {
    if cfg!(windows) {
        // >= Vista: c:\Users\$username\AppData\Roaming
        get_special_folder_path().join("ZcashParams")
    } else if cfg!(target_os = "macos") {
        home_dir()
            .unwrap()
            .join("Library")
            .join("Application Support")
            .join("ZcashParams")
    } else {
        home_dir().unwrap().join(".zcash-params")
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub fn coin_daemon_data_dir(name: &str, is_asset_chain: bool) -> PathBuf {
    // komodo/util.cpp/GetDefaultDataDir
    let mut data_dir = match dirs::home_dir() {
        Some(hd) => hd,
        None => Path::new("/").to_path_buf(),
    };

    if cfg!(windows) {
        // >= Vista: c:\Users\$username\AppData\Roaming
        data_dir = get_special_folder_path();
        if is_asset_chain {
            data_dir.push("Komodo");
        } else {
            data_dir.push(first_char_to_upper(name));
        }
    } else if cfg!(target_os = "macos") {
        data_dir.push("Library");
        data_dir.push("Application Support");
        if is_asset_chain {
            data_dir.push("Komodo");
        } else {
            data_dir.push(first_char_to_upper(name));
        }
    } else if is_asset_chain {
        data_dir.push(".komodo");
    } else {
        data_dir.push(format!(".{}", name));
    }

    if is_asset_chain {
        data_dir.push(name)
    };
    data_dir
}

/// Attempts to parse native daemon conf file and return rpcport, rpcuser and rpcpassword
#[cfg(not(target_arch = "wasm32"))]
fn read_native_mode_conf(
    filename: &dyn AsRef<Path>,
    network: &BlockchainNetwork,
) -> Result<(Option<u16>, String, String), String> {
    use ini::Ini;

    fn read_property<'a>(conf: &'a ini::Ini, network: &BlockchainNetwork, property: &str) -> Option<&'a String> {
        let subsection = match network {
            BlockchainNetwork::Mainnet => None,
            BlockchainNetwork::Testnet => conf.section(Some("test")),
            BlockchainNetwork::Regtest => conf.section(Some("regtest")),
        };
        subsection
            .and_then(|props| props.get(property))
            .or_else(|| conf.general_section().get(property))
    }

    let conf: Ini = match Ini::load_from_file(&filename) {
        Ok(ini) => ini,
        Err(err) => {
            return ERR!(
                "Error parsing the native wallet configuration '{}': {}",
                filename.as_ref().display(),
                err
            )
        },
    };
    let rpc_port = match read_property(&conf, network, "rpcport") {
        Some(port) => port.parse::<u16>().ok(),
        None => None,
    };
    let rpc_user = try_s!(read_property(&conf, network, "rpcuser").ok_or(ERRL!(
        "Conf file {} doesn't have the rpcuser key",
        filename.as_ref().display()
    )));
    let rpc_password = try_s!(read_property(&conf, network, "rpcpassword").ok_or(ERRL!(
        "Conf file {} doesn't have the rpcpassword key",
        filename.as_ref().display()
    )));
    Ok((rpc_port, rpc_user.clone(), rpc_password.clone()))
}

/// Electrum protocol version verifier.
/// The structure is used to handle the `on_connected` event and notify `electrum_version_loop`.
struct ElectrumProtoVerifier {
    on_connect_tx: mpsc::UnboundedSender<String>,
}

impl ElectrumProtoVerifier {
    fn into_shared(self) -> RpcTransportEventHandlerShared { Arc::new(self) }
}

impl RpcTransportEventHandler for ElectrumProtoVerifier {
    fn debug_info(&self) -> String { "ElectrumProtoVerifier".into() }

    fn on_outgoing_request(&self, _data: &[u8]) {}

    fn on_incoming_response(&self, _data: &[u8]) {}

    fn on_connected(&self, address: String) -> Result<(), String> {
        try_s!(self.on_connect_tx.unbounded_send(address));
        Ok(())
    }
}

pub struct UtxoConfBuilder<'a> {
    conf: &'a Json,
    req: &'a Json,
    ticker: &'a str,
}

impl<'a> UtxoConfBuilder<'a> {
    pub fn new(conf: &'a Json, req: &'a Json, ticker: &'a str) -> Self { UtxoConfBuilder { conf, req, ticker } }

    pub fn build(&self) -> Result<UtxoCoinConf, String> {
        let checksum_type = self.checksum_type();
        let pub_addr_prefix = self.pub_addr_prefix();
        let p2sh_addr_prefix = self.p2sh_address_prefix();
        let pub_t_addr_prefix = self.pub_t_address_prefix();
        let p2sh_t_addr_prefix = self.p2sh_t_address_prefix();

        let wif_prefix = self.wif_prefix();

        let bech32_hrp = self.bech32_hrp();

        let default_address_format = self.default_address_format();

        let asset_chain = self.asset_chain();
        let tx_version = self.tx_version();
        let overwintered = self.overwintered();

        let tx_fee_volatility_percent = self.tx_fee_volatility_percent();
        let version_group_id = try_s!(self.version_group_id(tx_version, overwintered));
        let consensus_branch_id = try_s!(self.consensus_branch_id(tx_version));
        let signature_version = self.signature_version();
        let fork_id = self.fork_id();

        // should be sufficient to detect zcash by overwintered flag
        let zcash = overwintered;

        let required_confirmations = self.required_confirmations();
        let requires_notarization = self.requires_notarization();

        let mature_confirmations = self.mature_confirmations();

        let is_pos = self.is_pos();
        let segwit = self.segwit();
        let force_min_relay_fee = self.conf["force_min_relay_fee"].as_bool().unwrap_or(false);
        let mtp_block_count = self.mtp_block_count();
        let estimate_fee_mode = self.estimate_fee_mode();
        let estimate_fee_blocks = self.estimate_fee_blocks();

        Ok(UtxoCoinConf {
            ticker: self.ticker.to_owned(),
            is_pos,
            requires_notarization,
            overwintered,
            pub_addr_prefix,
            p2sh_addr_prefix,
            pub_t_addr_prefix,
            p2sh_t_addr_prefix,
            bech32_hrp,
            segwit,
            wif_prefix,
            tx_version,
            default_address_format,
            asset_chain,
            tx_fee_volatility_percent,
            version_group_id,
            consensus_branch_id,
            zcash,
            checksum_type,
            signature_version,
            fork_id,
            required_confirmations: required_confirmations.into(),
            force_min_relay_fee,
            mtp_block_count,
            estimate_fee_mode,
            mature_confirmations,
            estimate_fee_blocks,
        })
    }

    fn checksum_type(&self) -> ChecksumType {
        match self.ticker {
            "GRS" => ChecksumType::DGROESTL512,
            "SMART" => ChecksumType::KECCAK256,
            _ => ChecksumType::DSHA256,
        }
    }

    fn pub_addr_prefix(&self) -> u8 {
        let pubtype = self.conf["pubtype"]
            .as_u64()
            .unwrap_or(if self.ticker == "BTC" { 0 } else { 60 });
        pubtype as u8
    }

    fn p2sh_address_prefix(&self) -> u8 {
        self.conf["p2shtype"]
            .as_u64()
            .unwrap_or(if self.ticker == "BTC" { 5 } else { 85 }) as u8
    }

    fn pub_t_address_prefix(&self) -> u8 { self.conf["taddr"].as_u64().unwrap_or(0) as u8 }

    fn p2sh_t_address_prefix(&self) -> u8 { self.conf["taddr"].as_u64().unwrap_or(0) as u8 }

    fn wif_prefix(&self) -> u8 {
        let wiftype = self.conf["wiftype"]
            .as_u64()
            .unwrap_or(if self.ticker == "BTC" { 128 } else { 188 });
        wiftype as u8
    }

    fn bech32_hrp(&self) -> Option<String> { json::from_value(self.conf["bech32_hrp"].clone()).unwrap_or(None) }

    fn default_address_format(&self) -> UtxoAddressFormat {
        let mut address_format: UtxoAddressFormat =
            json::from_value(self.conf["address_format"].clone()).unwrap_or(UtxoAddressFormat::Standard);

        if let UtxoAddressFormat::CashAddress {
            network: _,
            ref mut pub_addr_prefix,
            ref mut p2sh_addr_prefix,
        } = address_format
        {
            *pub_addr_prefix = self.pub_addr_prefix();
            *p2sh_addr_prefix = self.p2sh_address_prefix();
        }

        address_format
    }

    fn asset_chain(&self) -> bool { self.conf["asset"].as_str().is_some() }

    fn tx_version(&self) -> i32 { self.conf["txversion"].as_i64().unwrap_or(1) as i32 }

    fn overwintered(&self) -> bool { self.conf["overwintered"].as_u64().unwrap_or(0) == 1 }

    fn tx_fee_volatility_percent(&self) -> f64 {
        match self.conf["txfee_volatility_percent"].as_f64() {
            Some(volatility) => volatility,
            None => DEFAULT_DYNAMIC_FEE_VOLATILITY_PERCENT,
        }
    }

    fn version_group_id(&self, tx_version: i32, overwintered: bool) -> Result<u32, String> {
        let version_group_id = match self.conf["version_group_id"].as_str() {
            Some(mut s) => {
                if s.starts_with("0x") {
                    s = &s[2..];
                }
                let bytes = try_s!(hex::decode(s));
                u32::from_be_bytes(try_s!(bytes.as_slice().try_into()))
            },
            None => {
                if tx_version == 3 && overwintered {
                    0x03c4_8270
                } else if tx_version == 4 && overwintered {
                    0x892f_2085
                } else {
                    0
                }
            },
        };
        Ok(version_group_id)
    }

    fn consensus_branch_id(&self, tx_version: i32) -> Result<u32, String> {
        let consensus_branch_id = match self.conf["consensus_branch_id"].as_str() {
            Some(mut s) => {
                if s.starts_with("0x") {
                    s = &s[2..];
                }
                let bytes = try_s!(hex::decode(s));
                u32::from_be_bytes(try_s!(bytes.as_slice().try_into()))
            },
            None => match tx_version {
                3 => 0x5ba8_1b19,
                4 => 0x76b8_09bb,
                _ => 0,
            },
        };
        Ok(consensus_branch_id)
    }

    fn signature_version(&self) -> SignatureVersion {
        if self.ticker == "BCH" {
            SignatureVersion::ForkId
        } else {
            SignatureVersion::Base
        }
    }

    fn fork_id(&self) -> u32 {
        let default_fork_id = match self.ticker {
            "BCH" => "0x40",
            _ => "0x0",
        };
        let hex_string = self.conf["fork_id"].as_str().unwrap_or(default_fork_id);
        let fork_id = u32::from_str_radix(hex_string.trim_start_matches("0x"), 16).unwrap();
        fork_id
    }

    fn required_confirmations(&self) -> u64 {
        // param from request should override the config
        self.req["required_confirmations"]
            .as_u64()
            .unwrap_or_else(|| self.conf["required_confirmations"].as_u64().unwrap_or(1))
    }

    fn requires_notarization(&self) -> AtomicBool {
        self.req["requires_notarization"]
            .as_bool()
            .unwrap_or_else(|| self.conf["requires_notarization"].as_bool().unwrap_or(false))
            .into()
    }

    fn mature_confirmations(&self) -> u32 {
        self.conf["mature_confirmations"]
            .as_u64()
            .map(|x| x as u32)
            .unwrap_or(MATURE_CONFIRMATIONS_DEFAULT)
    }

    fn is_pos(&self) -> bool { self.conf["isPoS"].as_u64() == Some(1) }

    fn segwit(&self) -> bool { self.conf["segwit"].as_bool().unwrap_or(false) }

    fn mtp_block_count(&self) -> NonZeroU64 {
        json::from_value(self.conf["mtp_block_count"].clone()).unwrap_or(KMD_MTP_BLOCK_COUNT)
    }

    fn estimate_fee_mode(&self) -> Option<EstimateFeeMode> {
        json::from_value(self.conf["estimate_fee_mode"].clone()).unwrap_or(None)
    }

    fn estimate_fee_blocks(&self) -> u32 { json::from_value(self.conf["estimate_fee_blocks"].clone()).unwrap_or(1) }
}

#[async_trait]
pub trait UtxoCoinBuilder {
    type ResultCoin;

    async fn build(self) -> Result<Self::ResultCoin, String>;

    fn ctx(&self) -> &MmArc;

    fn conf(&self) -> &Json;

    fn req(&self) -> &Json;

    fn ticker(&self) -> &str;

    fn priv_key(&self) -> &[u8];

    async fn build_utxo_fields(&self) -> Result<UtxoCoinFields, String> {
        let conf = try_s!(UtxoConfBuilder::new(self.conf(), self.req(), self.ticker()).build());

        let private = Private {
            prefix: conf.wif_prefix,
            secret: H256::from(self.priv_key()),
            compressed: true,
            checksum_type: conf.checksum_type,
        };
        let key_pair = try_s!(KeyPair::from_private(private));
        let addr_format = try_s!(self.address_format());
        let my_address = Address {
            prefix: conf.pub_addr_prefix,
            t_addr_prefix: conf.pub_t_addr_prefix,
            hash: key_pair.public().address_hash(),
            checksum_type: conf.checksum_type,
            hrp: conf.bech32_hrp.clone(),
            addr_format,
        };
        let my_script_pubkey = output_script(&my_address).to_bytes();
        let rpc_client = try_s!(self.rpc_client().await);
        let tx_fee = try_s!(self.tx_fee(&rpc_client).await);
        let decimals = try_s!(self.decimals(&rpc_client).await);
        let dust_amount = self.dust_amount();

        let initial_history_state = self.initial_history_state();
        let tx_cache_directory = Some(self.ctx().dbdir().join("TX_CACHE"));
        let tx_hash_algo = self.tx_hash_algo();

        let coin = UtxoCoinFields {
            conf,
            decimals,
            dust_amount,
            rpc_client,
            key_pair,
            my_address,
            history_sync_state: Mutex::new(initial_history_state),
            tx_cache_directory,
            recently_spent_outpoints: AsyncMutex::new(RecentlySpentOutPoints::new(my_script_pubkey)),
            tx_fee,
            tx_hash_algo,
        };
        Ok(coin)
    }

    fn address_format(&self) -> Result<UtxoAddressFormat, String> {
        let mut format: Option<UtxoAddressFormat> = try_s!(json::from_value(self.req()["address_format"].clone()));
        if format.is_none() {
            format = try_s!(json::from_value(self.conf()["address_format"].clone()))
        }

        let mut address_format = format.unwrap_or(UtxoAddressFormat::Standard);

        if let UtxoAddressFormat::CashAddress {
            network: _,
            ref mut pub_addr_prefix,
            ref mut p2sh_addr_prefix,
        } = address_format
        {
            *pub_addr_prefix = self.pub_addr_prefix();
            *p2sh_addr_prefix = self.p2sh_address_prefix();
        }

        if address_format.is_segwit()
            && (!self.conf()["segwit"].as_bool().unwrap_or(false) || self.conf()["bech32_hrp"].is_null())
        {
            ERR!("Cannot use Segwit address format for coin without segwit support or bech32_hrp in config")
        } else {
            Ok(address_format)
        }
    }

    fn pub_addr_prefix(&self) -> u8 {
        let pubtype = self.conf()["pubtype"]
            .as_u64()
            .unwrap_or(if self.ticker() == "BTC" { 0 } else { 60 });
        pubtype as u8
    }

    fn p2sh_address_prefix(&self) -> u8 {
        self.conf()["p2shtype"]
            .as_u64()
            .unwrap_or(if self.ticker() == "BTC" { 5 } else { 85 }) as u8
    }

    fn dust_amount(&self) -> u64 { json::from_value(self.conf()["dust"].clone()).unwrap_or(UTXO_DUST_AMOUNT) }

    fn network(&self) -> Result<BlockchainNetwork, String> {
        let conf = self.conf();
        if !conf["network"].is_null() {
            return json::from_value(conf["network"].clone()).map_err(|e| ERRL!("{}", e));
        }
        Ok(BlockchainNetwork::Mainnet)
    }

    async fn decimals(&self, _rpc_client: &UtxoRpcClientEnum) -> Result<u8, String> {
        Ok(self.conf()["decimals"].as_u64().unwrap_or(8) as u8)
    }

    async fn tx_fee(&self, rpc_client: &UtxoRpcClientEnum) -> Result<TxFee, String> {
        const ONE_DOGE: u64 = 100000000;

        if self.ticker() == "DOGE" {
            return Ok(TxFee::FixedPerKb(ONE_DOGE));
        }

        let tx_fee = match self.conf()["txfee"].as_u64() {
            None => TxFee::Fixed(1000),
            Some(0) => {
                let fee_method = match &rpc_client {
                    UtxoRpcClientEnum::Electrum(_) => EstimateFeeMethod::Standard,
                    UtxoRpcClientEnum::Native(client) => try_s!(client.detect_fee_method().compat().await),
                };
                TxFee::Dynamic(fee_method)
            },
            Some(fee) => TxFee::Fixed(fee),
        };
        Ok(tx_fee)
    }

    fn initial_history_state(&self) -> HistorySyncState {
        if self.req()["tx_history"].as_bool().unwrap_or(false) {
            HistorySyncState::NotStarted
        } else {
            HistorySyncState::NotEnabled
        }
    }

    async fn rpc_client(&self) -> Result<UtxoRpcClientEnum, String> {
        match self.req()["method"].as_str() {
            Some("enable") => {
                #[cfg(target_arch = "wasm32")]
                {
                    ERR!("Native UTXO mode is only supported in native mode")
                }
                #[cfg(not(target_arch = "wasm32"))]
                {
                    let native = try_s!(self.native_client());
                    Ok(UtxoRpcClientEnum::Native(native))
                }
            },
            Some("electrum") => {
                let electrum = try_s!(self.electrum_client().await);
                Ok(UtxoRpcClientEnum::Electrum(electrum))
            },
            _ => ERR!("Expected enable or electrum request"),
        }
    }

    async fn electrum_client(&self) -> Result<ElectrumClient, String> {
        let (on_connect_tx, on_connect_rx) = mpsc::unbounded();
        let ticker = self.ticker().to_owned();
        let ctx = self.ctx();
        let event_handlers = vec![
            CoinTransportMetrics::new(ctx.metrics.weak(), ticker.clone(), RpcClientType::Electrum).into_shared(),
            ElectrumProtoVerifier { on_connect_tx }.into_shared(),
        ];

        let mut servers: Vec<ElectrumRpcRequest> = try_s!(json::from_value(self.req()["servers"].clone()));
        let mut rng = small_rng();
        servers.as_mut_slice().shuffle(&mut rng);
        let client = ElectrumClientImpl::new(ticker, event_handlers);
        for server in servers.iter() {
            match client.add_server(server).await {
                Ok(_) => (),
                Err(e) => log!("Error " (e) " connecting to " [server] ". Address won't be used"),
            };
        }

        let mut attempts = 0i32;
        while !client.is_connected().await {
            if attempts >= 10 {
                return ERR!("Failed to connect to at least 1 of {:?} in 5 seconds.", servers);
            }

            Timer::sleep(0.5).await;
            attempts += 1;
        }

        let client = Arc::new(client);

        let weak_client = Arc::downgrade(&client);
        let client_name = format!("{} GUI/MM2 {}", ctx.gui().unwrap_or("UNKNOWN"), ctx.mm_version());
        spawn_electrum_version_loop(weak_client, on_connect_rx, client_name);

        try_s!(wait_for_protocol_version_checked(&client).await);

        let weak_client = Arc::downgrade(&client);
        spawn_electrum_ping_loop(weak_client, servers);

        Ok(ElectrumClient(client))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn native_client(&self) -> Result<NativeClient, String> {
        use base64::{encode_config as base64_encode, URL_SAFE};

        let native_conf_path = try_s!(self.confpath());
        let network = try_s!(self.network());
        let (rpc_port, rpc_user, rpc_password) = try_s!(read_native_mode_conf(&native_conf_path, &network));
        let auth_str = fomat!((rpc_user)":"(rpc_password));
        let rpc_port = match rpc_port {
            Some(p) => p,
            None => try_s!(self.conf()["rpcport"].as_u64().ok_or(ERRL!(
                "Rpc port is not set neither in `coins` file nor in native daemon config"
            ))) as u16,
        };

        let ctx = self.ctx();
        let coin_ticker = self.ticker().to_owned();
        let event_handlers =
            vec![
                CoinTransportMetrics::new(ctx.metrics.weak(), coin_ticker.clone(), RpcClientType::Native).into_shared(),
            ];
        let client = Arc::new(NativeClientImpl {
            coin_ticker,
            uri: fomat!("http://127.0.0.1:"(rpc_port)),
            auth: format!("Basic {}", base64_encode(&auth_str, URL_SAFE)),
            event_handlers,
            request_id: 0u64.into(),
            list_unspent_concurrent_map: ConcurrentRequestMap::new(),
        });

        Ok(NativeClient(client))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn confpath(&self) -> Result<PathBuf, String> {
        let conf = self.conf();
        // Documented at https://github.com/jl777/coins#bitcoin-protocol-specific-json
        // "USERHOME/" prefix should be replaced with the user's home folder.
        let declared_confpath = match self.conf()["confpath"].as_str() {
            Some(path) if !path.is_empty() => path.trim(),
            _ => {
                let (name, is_asset_chain) = {
                    match conf["asset"].as_str() {
                        Some(a) => (a, true),
                        None => (
                            try_s!(conf["name"].as_str().ok_or("'name' field is not found in config")),
                            false,
                        ),
                    }
                };
                let data_dir = coin_daemon_data_dir(name, is_asset_chain);
                let confname = format!("{}.conf", name);

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
            let home = try_s!(home_dir().ok_or("Can not detect the user home directory"));
            Ok(home.join(confpath))
        } else {
            Ok(confpath.into())
        }
    }

    fn tx_hash_algo(&self) -> TxHashAlgo {
        if self.ticker() == "GRS" {
            TxHashAlgo::SHA256
        } else {
            TxHashAlgo::DSHA256
        }
    }
}

/// Ping the electrum servers every 30 seconds to prevent them from disconnecting us.
/// According to docs server can do it if there are no messages in ~10 minutes.
/// https://electrumx.readthedocs.io/en/latest/protocol-methods.html?highlight=keep#server-ping
/// Weak reference will allow to stop the thread if client is dropped.
fn spawn_electrum_ping_loop(weak_client: Weak<ElectrumClientImpl>, servers: Vec<ElectrumRpcRequest>) {
    spawn(async move {
        loop {
            if let Some(client) = weak_client.upgrade() {
                if let Err(e) = ElectrumClient(client).server_ping().compat().await {
                    log!("Electrum servers " [servers] " ping error " [e]);
                }
            } else {
                log!("Electrum servers " [servers] " ping loop stopped");
                break;
            }
            Timer::sleep(30.).await
        }
    });
}

/// Follow the `on_connect_rx` stream and verify the protocol version of each connected electrum server.
/// https://electrumx.readthedocs.io/en/latest/protocol-methods.html?highlight=keep#server-version
/// Weak reference will allow to stop the thread if client is dropped.
fn spawn_electrum_version_loop(
    weak_client: Weak<ElectrumClientImpl>,
    mut on_connect_rx: mpsc::UnboundedReceiver<String>,
    client_name: String,
) {
    // client.remove_server() is called too often
    async fn remove_server(client: ElectrumClient, electrum_addr: &str) {
        if let Err(e) = client.remove_server(electrum_addr).await {
            log!("Error on remove server "[e]);
        }
    }

    spawn(async move {
        while let Some(electrum_addr) = on_connect_rx.next().await {
            let client = match weak_client.upgrade() {
                Some(c) => ElectrumClient(c),
                _ => break,
            };

            let available_protocols = client.protocol_version();
            let version = match client
                .server_version(&electrum_addr, &client_name, available_protocols)
                .compat()
                .await
            {
                Ok(version) => version,
                Err(e) => {
                    log!("Electrum " (electrum_addr) " server.version error \"" [e] "\". Remove the connection");
                    remove_server(client, &electrum_addr).await;
                    continue;
                },
            };

            // check if the version is allowed
            let actual_version = match version.protocol_version.parse::<f32>() {
                Ok(v) => v,
                Err(e) => {
                    log!("Error on parse protocol_version "[e]);
                    remove_server(client, &electrum_addr).await;
                    continue;
                },
            };

            if !available_protocols.contains(&actual_version) {
                log!("Received unsupported protocol version " [actual_version] " from " [electrum_addr] ". Remove the connection");
                remove_server(client, &electrum_addr).await;
                continue;
            }

            if let Err(e) = client.set_protocol_version(&electrum_addr, actual_version).await {
                log!("Error on set protocol_version "[e]);
            };

            log!("Use protocol version " [actual_version] " for Electrum " [electrum_addr]);
        }

        log!("Electrum server.version loop stopped");
    });
}

/// Wait until the protocol version of at least one client's Electrum is checked.
async fn wait_for_protocol_version_checked(client: &ElectrumClientImpl) -> Result<(), String> {
    let mut attempts = 0;
    loop {
        if attempts >= 10 {
            return ERR!("Failed protocol version verifying of at least 1 of Electrums in 5 seconds.");
        }

        if client.count_connections().await == 0 {
            // All of the connections were removed because of server.version checking
            return ERR!(
                "There are no Electrums with the required protocol version {:?}",
                client.protocol_version()
            );
        }

        if client.is_protocol_version_checked().await {
            break;
        }

        Timer::sleep(0.5).await;
        attempts += 1;
    }

    Ok(())
}

/// Function calculating KMD interest
/// https://komodoplatform.atlassian.net/wiki/spaces/KPSD/pages/71729215/What+is+the+5+Komodo+Stake+Reward
/// https://github.com/KomodoPlatform/komodo/blob/master/src/komodo_interest.h
fn kmd_interest(
    height: Option<u64>,
    value: u64,
    lock_time: u64,
    current_time: u64,
) -> Result<u64, KmdRewardsNotAccruedReason> {
    const KOMODO_ENDOFERA: u64 = 7_777_777;
    const LOCKTIME_THRESHOLD: u64 = 500_000_000;

    // value must be at least 10 KMD
    if value < 1_000_000_000 {
        return Err(KmdRewardsNotAccruedReason::UtxoAmountLessThanTen);
    }
    // locktime must be set
    if lock_time == 0 {
        return Err(KmdRewardsNotAccruedReason::LocktimeNotSet);
    }
    // interest doesn't accrue for lock_time < 500_000_000
    if lock_time < LOCKTIME_THRESHOLD {
        return Err(KmdRewardsNotAccruedReason::LocktimeLessThanThreshold);
    }
    let height = match height {
        Some(h) => h,
        None => return Err(KmdRewardsNotAccruedReason::TransactionInMempool), // consider that the transaction is not mined yet
    };
    // interest will stop accrue after block 7_777_777
    if height >= KOMODO_ENDOFERA {
        return Err(KmdRewardsNotAccruedReason::UtxoHeightGreaterThanEndOfEra);
    };
    // current time must be greater than tx lock_time
    if current_time < lock_time {
        return Err(KmdRewardsNotAccruedReason::OneHourNotPassedYet);
    }

    let mut minutes = (current_time - lock_time) / 60;

    // at least 1 hour should pass
    if minutes < 60 {
        return Err(KmdRewardsNotAccruedReason::OneHourNotPassedYet);
    }

    // interest stop accruing after 1 year before block 1000000
    if minutes > 365 * 24 * 60 {
        minutes = 365 * 24 * 60
    };
    // interest stop accruing after 1 month past 1000000 block
    if height >= 1_000_000 && minutes > 31 * 24 * 60 {
        minutes = 31 * 24 * 60;
    }
    // next 2 lines ported as is from Komodo codebase
    minutes -= 59;
    let accrued = (value / 10_512_000) * minutes;

    Ok(accrued)
}

fn kmd_interest_accrue_stop_at(height: u64, lock_time: u64) -> u64 {
    let seconds = if height < 1_000_000 {
        // interest stop accruing after 1 year before block 1000000
        365 * 24 * 60 * 60
    } else {
        // interest stop accruing after 1 month past 1000000 block
        31 * 24 * 60 * 60
    };

    lock_time + seconds
}

fn kmd_interest_accrue_start_at(lock_time: u64) -> u64 {
    let one_hour = 60 * 60;
    lock_time + one_hour
}

#[derive(Debug, Serialize, Eq, PartialEq)]
enum KmdRewardsNotAccruedReason {
    LocktimeNotSet,
    LocktimeLessThanThreshold,
    UtxoHeightGreaterThanEndOfEra,
    UtxoAmountLessThanTen,
    OneHourNotPassedYet,
    TransactionInMempool,
}

#[derive(Serialize)]
enum KmdRewardsAccrueInfo {
    Accrued(BigDecimal),
    NotAccruedReason(KmdRewardsNotAccruedReason),
}

#[derive(Serialize)]
pub struct KmdRewardsInfoElement {
    tx_hash: H256Json,
    #[serde(skip_serializing_if = "Option::is_none")]
    height: Option<u64>,
    /// The zero-based index of the output in the transaction’s list of outputs.
    output_index: u32,
    amount: BigDecimal,
    locktime: u64,
    /// Amount of accrued rewards.
    accrued_rewards: KmdRewardsAccrueInfo,
    /// Rewards start to accrue at this time for the given transaction.
    /// None if the rewards will not be accrued.
    #[serde(skip_serializing_if = "Option::is_none")]
    accrue_start_at: Option<u64>,
    /// Rewards stop to accrue at this time for the given transaction.
    /// None if the rewards will not be accrued.
    #[serde(skip_serializing_if = "Option::is_none")]
    accrue_stop_at: Option<u64>,
}

/// Get rewards info of unspent outputs.
/// The list is ordered by the output value.
pub async fn kmd_rewards_info<T>(coin: &T) -> Result<Vec<KmdRewardsInfoElement>, String>
where
    T: AsRef<UtxoCoinFields> + UtxoCommonOps,
{
    if coin.as_ref().conf.ticker != "KMD" {
        return ERR!("rewards info can be obtained for KMD only");
    }

    let utxo = coin.as_ref();
    let rpc_client = &utxo.rpc_client;
    let mut unspents = try_s!(rpc_client.list_unspent(&utxo.my_address, utxo.decimals).compat().await);
    // list_unspent_ordered() returns ordered from lowest to highest by value unspent outputs.
    // reverse it to reorder from highest to lowest outputs.
    unspents.reverse();

    let mut result = Vec::with_capacity(unspents.len());
    for unspent in unspents {
        let tx_hash: H256Json = unspent.outpoint.hash.reversed().into();
        let tx_info = try_s!(rpc_client.get_verbose_transaction(tx_hash.clone()).compat().await);

        let value = unspent.value;
        let locktime = tx_info.locktime as u64;
        let current_time = try_s!(coin.get_current_mtp().await) as u64;
        let accrued_rewards = match kmd_interest(tx_info.height, value, locktime, current_time) {
            Ok(interest) => {
                KmdRewardsAccrueInfo::Accrued(big_decimal_from_sat(interest as i64, coin.as_ref().decimals))
            },
            Err(reason) => KmdRewardsAccrueInfo::NotAccruedReason(reason),
        };

        // `accrue_start_at` and `accrue_stop_at` should be None if the rewards will never be obtained for the given transaction
        let (accrue_start_at, accrue_stop_at) = match &accrued_rewards {
            KmdRewardsAccrueInfo::Accrued(_)
            | KmdRewardsAccrueInfo::NotAccruedReason(KmdRewardsNotAccruedReason::TransactionInMempool)
            | KmdRewardsAccrueInfo::NotAccruedReason(KmdRewardsNotAccruedReason::OneHourNotPassedYet) => {
                let start_at = Some(kmd_interest_accrue_start_at(locktime));
                let stop_at = match tx_info.height {
                    Some(height) => Some(kmd_interest_accrue_stop_at(height, locktime)),
                    _ => None,
                };
                (start_at, stop_at)
            },
            _ => (None, None),
        };

        result.push(KmdRewardsInfoElement {
            tx_hash,
            height: tx_info.height,
            output_index: unspent.outpoint.index,
            amount: big_decimal_from_sat(value as i64, coin.as_ref().decimals),
            locktime,
            accrued_rewards,
            accrue_start_at,
            accrue_stop_at,
        });
    }

    Ok(result)
}

/// Denominate BigDecimal amount of coin units to satoshis
pub fn sat_from_big_decimal(amount: &BigDecimal, decimals: u8) -> NumConversResult<u64> {
    (amount * BigDecimal::from(10u64.pow(decimals as u32)))
        .to_u64()
        .or_mm_err(|| {
            let err = format!("Could not get sat from amount {} with decimals {}", amount, decimals);
            NumConversError::new(err)
        })
}

pub(crate) fn sign_tx(
    unsigned: TransactionInputSigner,
    key_pair: &KeyPair,
    prev_script: Script,
    signature_version: SignatureVersion,
    fork_id: u32,
) -> Result<UtxoTx, String> {
    let mut signed_inputs = vec![];
    match signature_version {
        SignatureVersion::WitnessV0 => {
            for (i, _) in unsigned.inputs.iter().enumerate() {
                signed_inputs.push(try_s!(p2wpkh_spend(
                    &unsigned,
                    i,
                    key_pair,
                    &prev_script,
                    signature_version,
                    fork_id
                )));
            }
        },
        _ => {
            for (i, _) in unsigned.inputs.iter().enumerate() {
                signed_inputs.push(try_s!(p2pkh_spend(
                    &unsigned,
                    i,
                    key_pair,
                    &prev_script,
                    signature_version,
                    fork_id
                )));
            }
        },
    }

    Ok(UtxoTx {
        inputs: signed_inputs,
        n_time: unsigned.n_time,
        outputs: unsigned.outputs.clone(),
        version: unsigned.version,
        overwintered: unsigned.overwintered,
        lock_time: unsigned.lock_time,
        expiry_height: unsigned.expiry_height,
        join_splits: vec![],
        shielded_spends: vec![],
        shielded_outputs: vec![],
        value_balance: 0,
        version_group_id: unsigned.version_group_id,
        binding_sig: H512::default(),
        join_split_sig: H512::default(),
        join_split_pubkey: H256::default(),
        zcash: unsigned.zcash,
        str_d_zeel: unsigned.str_d_zeel,
        tx_hash_algo: unsigned.hash_algo.into(),
    })
}

async fn send_outputs_from_my_address_impl<T>(coin: T, outputs: Vec<TransactionOutput>) -> Result<UtxoTx, String>
where
    T: AsRef<UtxoCoinFields> + UtxoCommonOps,
{
    let (unspents, recently_sent_txs) = try_s!(coin.list_unspent_ordered(&coin.as_ref().my_address).await);
    generate_and_send_tx(&coin, unspents, outputs, FeePolicy::SendExact, recently_sent_txs).await
}

/// Generates and sends tx using unspents and outputs adding new record to the recently_spent in case of success
async fn generate_and_send_tx<T>(
    coin: &T,
    unspents: Vec<UnspentInfo>,
    outputs: Vec<TransactionOutput>,
    fee_policy: FeePolicy,
    mut recently_spent: AsyncMutexGuard<'_, RecentlySpentOutPoints>,
) -> Result<UtxoTx, String>
where
    T: AsRef<UtxoCoinFields> + UtxoCommonOps,
{
    let (unsigned, _) = try_s!(
        coin.generate_transaction(unspents, outputs, fee_policy, None, None)
            .await
    );

    let spent_unspents = unsigned
        .inputs
        .iter()
        .map(|input| UnspentInfo {
            outpoint: input.previous_output.clone(),
            value: input.amount,
            height: None,
        })
        .collect();

    let signature_version = match &coin.as_ref().my_address.addr_format {
        UtxoAddressFormat::Segwit => SignatureVersion::WitnessV0,
        _ => coin.as_ref().conf.signature_version,
    };

    let prev_script = Builder::build_p2pkh(&coin.as_ref().my_address.hash);
    let signed = try_s!(sign_tx(
        unsigned,
        &coin.as_ref().key_pair,
        prev_script,
        signature_version,
        coin.as_ref().conf.fork_id
    ));

    try_s!(
        coin.as_ref()
            .rpc_client
            .send_transaction(&signed)
            .map_err(|e| ERRL!("{}", e))
            .compat()
            .await
    );

    recently_spent.add_spent(spent_unspents, signed.hash(), signed.outputs.clone());

    Ok(signed)
}

/// Creates signed input spending p2pkh output
pub fn p2pkh_spend(
    signer: &TransactionInputSigner,
    input_index: usize,
    key_pair: &KeyPair,
    prev_script: &Script,
    signature_version: SignatureVersion,
    fork_id: u32,
) -> Result<TransactionInput, String> {
    let script = Builder::build_p2pkh(&key_pair.public().address_hash());
    if script != *prev_script {
        return ERR!(
            "p2pkh script {} built from input key pair doesn't match expected prev script {}",
            script,
            prev_script
        );
    }
    let sighash_type = 1 | fork_id;
    let sighash = signer.signature_hash(
        input_index,
        signer.inputs[input_index].amount,
        &script,
        signature_version,
        sighash_type,
    );

    let script_sig = try_s!(script_sig_with_pub(&sighash, key_pair, fork_id));

    Ok(TransactionInput {
        script_sig,
        sequence: signer.inputs[input_index].sequence,
        script_witness: vec![],
        previous_output: signer.inputs[input_index].previous_output.clone(),
    })
}

/// Creates signed input spending p2pkh output
pub fn p2pk_spend(
    signer: &TransactionInputSigner,
    input_index: usize,
    key_pair: &KeyPair,
    signature_version: SignatureVersion,
    fork_id: u32,
) -> Result<TransactionInput, String> {
    let script = Builder::build_p2pk(key_pair.public());
    let sighash_type = 1 | fork_id;
    let sighash = signer.signature_hash(
        input_index,
        signer.inputs[input_index].amount,
        &script,
        signature_version,
        sighash_type,
    );

    let script_sig = try_s!(script_sig(&sighash, key_pair, fork_id));

    Ok(TransactionInput {
        script_sig: Builder::default().push_bytes(&script_sig).into_bytes(),
        sequence: signer.inputs[input_index].sequence,
        script_witness: vec![],
        previous_output: signer.inputs[input_index].previous_output.clone(),
    })
}

/// Creates signed input spending p2wpkh output
fn p2wpkh_spend(
    signer: &TransactionInputSigner,
    input_index: usize,
    key_pair: &KeyPair,
    prev_script: &Script,
    signature_version: SignatureVersion,
    fork_id: u32,
) -> Result<TransactionInput, String> {
    let script = Builder::build_p2pkh(&key_pair.public().address_hash());

    if script != *prev_script {
        return ERR!(
            "p2pkh script {} built from input key pair doesn't match expected prev script {}",
            script,
            prev_script
        );
    }
    let sighash_type = 1 | fork_id;
    let sighash = signer.signature_hash(
        input_index,
        signer.inputs[input_index].amount,
        &script,
        signature_version,
        sighash_type,
    );

    let sig_script = try_s!(script_sig(&sighash, key_pair, fork_id));

    Ok(TransactionInput {
        previous_output: signer.inputs[input_index].previous_output.clone(),
        script_sig: Bytes::from(Vec::new()),
        sequence: signer.inputs[input_index].sequence,
        script_witness: vec![sig_script, Bytes::from(key_pair.public().deref())],
    })
}

fn script_sig_with_pub(message: &H256, key_pair: &KeyPair, fork_id: u32) -> Result<Bytes, String> {
    let sig_script = try_s!(script_sig(message, key_pair, fork_id));

    let builder = Builder::default();

    Ok(builder
        .push_data(&sig_script)
        .push_data(&key_pair.public().to_vec())
        .into_bytes())
}

fn script_sig(message: &H256, key_pair: &KeyPair, fork_id: u32) -> Result<Bytes, String> {
    let signature = try_s!(key_pair.private().sign(message));

    let mut sig_script = Bytes::default();
    sig_script.append(&mut Bytes::from((*signature).to_vec()));
    // Using SIGHASH_ALL only for now
    sig_script.append(&mut Bytes::from(vec![1 | fork_id as u8]));

    Ok(sig_script)
}

pub fn output_script(address: &Address) -> Script {
    match address.addr_format {
        UtxoAddressFormat::Segwit => Builder::build_p2wpkh(&address.hash),
        _ => Builder::build_p2pkh(&address.hash),
    }
}

pub fn address_by_conf_and_pubkey_str(coin: &str, conf: &Json, pubkey: &str) -> Result<String, String> {
    let null = Json::Null;
    let conf_builder = UtxoConfBuilder::new(&conf, &null, coin);
    let utxo_conf = try_s!(conf_builder.build());
    let pubkey_bytes = try_s!(hex::decode(pubkey));
    let hash = dhash160(&pubkey_bytes);

    let address = Address {
        prefix: utxo_conf.pub_addr_prefix,
        t_addr_prefix: utxo_conf.pub_t_addr_prefix,
        hash,
        checksum_type: utxo_conf.checksum_type,
        hrp: utxo_conf.bech32_hrp.clone(),
        addr_format: utxo_conf.default_address_format,
    };
    address.display_address()
}
