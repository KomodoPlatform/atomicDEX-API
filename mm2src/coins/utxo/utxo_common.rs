use super::*;
use bigdecimal::{BigDecimal, Zero};
pub use bitcrypto::{dhash160, sha256, ChecksumType};
use chain::constants::SEQUENCE_FINAL;
use chain::{OutPoint, TransactionInput, TransactionOutput};
use common::executor::Timer;
use common::jsonrpc_client::{JsonRpcError, JsonRpcErrorType};
use common::log::{error, info, warn};
use common::mm_ctx::MmArc;
use common::mm_error::prelude::*;
use common::mm_metrics::MetricsArc;
use common::mm_number::MmNumber;
use common::{block_on, now_ms};
use futures::compat::Future01CompatExt;
use futures::future::{FutureExt, TryFutureExt};
use futures01::future::Either;
use keys::bytes::Bytes;
use keys::{Address, AddressFormat as UtxoAddressFormat, AddressHash, KeyPair, Public, SegwitAddress, Type};
use primitives::hash::H512;
use rpc::v1::types::{Bytes as BytesJson, TransactionInputEnum, H256 as H256Json};
use script::{Builder, Opcode, Script, ScriptAddress, SignatureVersion, TransactionInputSigner,
             UnsignedTransactionInput};
use secp256k1::{PublicKey, Signature};
use serde_json::{self as json};
use serialization::{deserialize, serialize, serialize_with_flags, CoinVariant, SERIALIZE_TRANSACTION_WITNESS};
use std::cmp::Ordering;
use std::collections::hash_map::{Entry, HashMap};
use std::str::FromStr;
use std::sync::atomic::Ordering as AtomicOrderding;

pub use chain::Transaction as UtxoTx;

use self::rpc_clients::{electrum_script_hash, UnspentInfo, UtxoRpcClientEnum, UtxoRpcClientOps, UtxoRpcResult};
use crate::{CanRefundHtlc, CoinBalance, RawTransactionRequest, RawTransactionRes, TradePreimageValue, TxFeeDetails,
            ValidateAddressResult, WithdrawResult};

const MIN_BTC_TRADING_VOL: &str = "0.00777";
pub const DEFAULT_SWAP_VOUT: usize = 0;
pub const DEFAULT_FEE_VOUT: usize = 0;

macro_rules! true_or {
    ($cond: expr, $etype: expr) => {
        if !$cond {
            return Err(MmError::new($etype));
        }
    };
}

lazy_static! {
    pub static ref HISTORY_TOO_LARGE_ERROR: Json = json!({
        "code": 1,
        "message": "history too large"
    });
}

pub const HISTORY_TOO_LARGE_ERR_CODE: i64 = -1;

pub struct UtxoArcBuilder<'a> {
    ctx: &'a MmArc,
    ticker: &'a str,
    conf: &'a Json,
    req: &'a Json,
    priv_key: &'a [u8],
}

impl<'a> UtxoArcBuilder<'a> {
    pub fn new(
        ctx: &'a MmArc,
        ticker: &'a str,
        conf: &'a Json,
        req: &'a Json,
        priv_key: &'a [u8],
    ) -> UtxoArcBuilder<'a> {
        UtxoArcBuilder {
            ctx,
            ticker,
            conf,
            req,
            priv_key,
        }
    }
}

#[async_trait]
impl UtxoCoinBuilder for UtxoArcBuilder<'_> {
    type ResultCoin = UtxoArc;

    async fn build(self) -> Result<Self::ResultCoin, String> {
        let utxo = try_s!(self.build_utxo_fields().await);
        Ok(UtxoArc(Arc::new(utxo)))
    }

    fn ctx(&self) -> &MmArc { self.ctx }

    fn conf(&self) -> &Json { self.conf }

    fn req(&self) -> &Json { self.req }

    fn ticker(&self) -> &str { self.ticker }

    fn priv_key(&self) -> &[u8] { self.priv_key }
}

pub async fn utxo_arc_from_conf_and_request<T>(
    ctx: &MmArc,
    ticker: &str,
    conf: &Json,
    req: &Json,
    priv_key: &[u8],
) -> Result<T, String>
where
    T: From<UtxoArc> + AsRef<UtxoCoinFields> + UtxoCommonOps + Send + Sync + 'static,
{
    let builder = UtxoArcBuilder::new(ctx, ticker, conf, req, priv_key);
    let utxo_arc = try_s!(builder.build().await);

    let merge_params: Option<UtxoMergeParams> = try_s!(json::from_value(req["utxo_merge_params"].clone()));
    if let Some(merge_params) = merge_params {
        let weak = utxo_arc.downgrade();
        let merge_loop = merge_utxo_loop::<T>(
            weak,
            merge_params.merge_at,
            merge_params.check_every,
            merge_params.max_merge_at_once,
        );
        info!("Starting UTXO merge loop for coin {}", ticker);
        spawn(merge_loop);
    }
    Ok(T::from(utxo_arc))
}

fn ten_f64() -> f64 { 10. }

fn one_hundred() -> usize { 100 }

#[derive(Debug, Deserialize)]
struct UtxoMergeParams {
    merge_at: usize,
    #[serde(default = "ten_f64")]
    check_every: f64,
    #[serde(default = "one_hundred")]
    max_merge_at_once: usize,
}

pub async fn get_tx_fee(coin: &UtxoCoinFields) -> Result<ActualTxFee, JsonRpcError> {
    let conf = &coin.conf;
    match &coin.tx_fee {
        TxFee::Fixed(fee) => Ok(ActualTxFee::Fixed(*fee)),
        TxFee::Dynamic(method) => {
            let fee = coin
                .rpc_client
                .estimate_fee_sat(coin.decimals, method, &conf.estimate_fee_mode, conf.estimate_fee_blocks)
                .compat()
                .await?;
            Ok(ActualTxFee::Dynamic(fee))
        },
        TxFee::FixedPerKb(satoshis) => Ok(ActualTxFee::FixedPerKb(*satoshis)),
    }
}

/// returns the fee required to be paid for HTLC spend transaction
pub async fn get_htlc_spend_fee<T>(coin: &T) -> UtxoRpcResult<u64>
where
    T: AsRef<UtxoCoinFields> + UtxoCommonOps,
{
    let coin_fee = coin.get_tx_fee().await?;
    let mut fee = match coin_fee {
        ActualTxFee::Fixed(fee) => fee,
        // atomic swap payment spend transaction is slightly more than 300 bytes in average as of now
        ActualTxFee::Dynamic(fee_per_kb) => (fee_per_kb * SWAP_TX_SPEND_SIZE) / KILO_BYTE,
        // return satoshis here as swap spend transaction size is always less than 1 kb
        ActualTxFee::FixedPerKb(satoshis) => satoshis,
    };
    if coin.as_ref().conf.force_min_relay_fee {
        let relay_fee = coin.as_ref().rpc_client.get_relay_fee().compat().await?;
        let relay_fee_sat = sat_from_big_decimal(&relay_fee, coin.as_ref().decimals)?;
        if fee < relay_fee_sat {
            fee = relay_fee_sat;
        }
    }
    Ok(fee)
}

pub fn addresses_from_script(coin: &UtxoCoinFields, script: &Script) -> Result<Vec<Address>, String> {
    let destinations: Vec<ScriptAddress> = try_s!(script.extract_destinations());

    let conf = &coin.conf;

    let addresses = destinations
        .into_iter()
        .map(|dst| {
            let (prefix, t_addr_prefix) = match dst.kind {
                Type::P2PKH => (conf.pub_addr_prefix, conf.pub_t_addr_prefix),
                Type::P2SH => (conf.p2sh_addr_prefix, conf.p2sh_t_addr_prefix),
            };

            Address {
                hash: dst.hash,
                checksum_type: conf.checksum_type,
                prefix,
                t_addr_prefix,
                hrp: conf.bech32_hrp.clone(),
                addr_format: coin.my_address.addr_format.clone(),
            }
        })
        .collect();

    Ok(addresses)
}

pub fn denominate_satoshis(coin: &UtxoCoinFields, satoshi: i64) -> f64 {
    satoshi as f64 / 10f64.powf(coin.decimals as f64)
}

pub fn base_coin_balance<T>(coin: &T) -> BalanceFut<BigDecimal>
where
    T: MarketCoinOps,
{
    coin.my_spendable_balance()
}

pub fn address_from_str_unchecked(coin: &UtxoCoinFields, address: &str) -> Result<Address, String> {
    if let Ok(legacy) = Address::from_str(address) {
        return Ok(legacy);
    }

    if let Ok(segwit) = Address::from_segwitaddress(
        address,
        coin.conf.checksum_type,
        coin.my_address.prefix,
        coin.my_address.t_addr_prefix,
    ) {
        return Ok(segwit);
    }

    if let Ok(cashaddress) = Address::from_cashaddress(
        address,
        coin.conf.checksum_type,
        coin.conf.pub_addr_prefix,
        coin.conf.p2sh_addr_prefix,
        coin.my_address.t_addr_prefix,
    ) {
        return Ok(cashaddress);
    }

    return ERR!("Invalid address: {}", address);
}

pub fn checked_address_from_str(coin: &UtxoCoinFields, address: &str) -> Result<Address, String> {
    let addr = try_s!(address_from_str_unchecked(coin, address));
    try_s!(coin.check_withdraw_address_supported(&addr));
    Ok(addr)
}

pub async fn get_current_mtp(coin: &UtxoCoinFields, coin_variant: CoinVariant) -> UtxoRpcResult<u32> {
    let current_block = coin.rpc_client.get_block_count().compat().await?;
    coin.rpc_client
        .get_median_time_past(current_block, coin.conf.mtp_block_count, coin_variant)
        .compat()
        .await
}

pub fn send_outputs_from_my_address<T>(coin: T, outputs: Vec<TransactionOutput>) -> TransactionFut
where
    T: AsRef<UtxoCoinFields> + UtxoCommonOps + Send + Sync + 'static,
{
    let fut = send_outputs_from_my_address_impl(coin, outputs);
    Box::new(fut.boxed().compat().map(|tx| tx.into()))
}

/// Generates unsigned transaction (TransactionInputSigner) from specified utxos and outputs.
/// This function expects that utxos are sorted by amounts in ascending order
/// Consider sorting before calling this function
/// Sends the change (inputs amount - outputs amount) to "my_address"
/// Also returns additional transaction data
///
/// Note `gas_fee` should be enough to execute all of the contract calls within UTXO outputs.
/// QRC20 specific: `gas_fee` should be calculated by: gas_limit * gas_price * (count of contract calls),
/// or should be sum of gas fee of all contract calls.
pub async fn generate_transaction<T>(
    coin: &T,
    utxos: Vec<UnspentInfo>,
    outputs: Vec<TransactionOutput>,
    fee_policy: FeePolicy,
    fee: Option<ActualTxFee>,
    gas_fee: Option<u64>,
) -> GenerateTxResult
where
    T: AsRef<UtxoCoinFields> + UtxoCommonOps,
{
    let dust: u64 = coin.as_ref().dust_amount;
    let lock_time = (now_ms() / 1000) as u32;

    let change_script_pubkey = output_script(&coin.as_ref().my_address).to_bytes();
    let coin_tx_fee = match fee {
        Some(f) => f,
        None => coin.get_tx_fee().await?,
    };

    true_or!(!outputs.is_empty(), GenerateTxError::EmptyOutputs);

    let mut sum_outputs_value = 0;
    let mut received_by_me = 0;
    for output in outputs.iter() {
        let script: Script = output.script_pubkey.clone().into();
        if script.opcodes().next() != Some(Ok(Opcode::OP_RETURN)) {
            true_or!(output.value >= dust, GenerateTxError::OutputValueLessThanDust {
                value: output.value,
                dust
            });
        }
        sum_outputs_value += output.value;
        if output.script_pubkey == change_script_pubkey {
            received_by_me += output.value;
        }
    }

    if let Some(gas_fee) = gas_fee {
        sum_outputs_value += gas_fee;
    }

    true_or!(!utxos.is_empty(), GenerateTxError::EmptyUtxoSet {
        required: sum_outputs_value
    });

    let str_d_zeel = if coin.as_ref().conf.ticker == "NAV" {
        Some("".into())
    } else {
        None
    };
    let hash_algo = coin.as_ref().tx_hash_algo.into();
    let mut tx = TransactionInputSigner {
        inputs: vec![],
        outputs,
        lock_time,
        version: coin.as_ref().conf.tx_version,
        n_time: if coin.as_ref().conf.is_pos {
            Some((now_ms() / 1000) as u32)
        } else {
            None
        },
        overwintered: coin.as_ref().conf.overwintered,
        expiry_height: 0,
        join_splits: vec![],
        shielded_spends: vec![],
        shielded_outputs: vec![],
        value_balance: 0,
        version_group_id: coin.as_ref().conf.version_group_id,
        consensus_branch_id: coin.as_ref().conf.consensus_branch_id,
        zcash: coin.as_ref().conf.zcash,
        str_d_zeel,
        hash_algo,
    };
    let mut sum_inputs = 0;
    let mut tx_fee = 0;
    let min_relay_fee = if coin.as_ref().conf.force_min_relay_fee {
        let fee_dec = coin.as_ref().rpc_client.get_relay_fee().compat().await?;
        let min_relay_fee = sat_from_big_decimal(&fee_dec, coin.as_ref().decimals)?;
        Some(min_relay_fee)
    } else {
        None
    };
    for utxo in utxos.iter() {
        sum_inputs += utxo.value;
        tx.inputs.push(UnsignedTransactionInput {
            previous_output: utxo.outpoint.clone(),
            sequence: SEQUENCE_FINAL,
            amount: utxo.value,
            witness: Vec::new(),
        });
        tx_fee = match &coin_tx_fee {
            ActualTxFee::Fixed(f) => *f,
            ActualTxFee::Dynamic(f) => {
                let transaction = UtxoTx::from(tx.clone());
                let transaction_bytes = serialize(&transaction);
                // 2 bytes are used to indicate the length of signature and pubkey
                // total is 107
                let additional_len = 2 + MAX_DER_SIGNATURE_LEN + COMPRESSED_PUBKEY_LEN;
                let tx_size = transaction_bytes.len() + transaction.inputs().len() * additional_len;
                (f * tx_size as u64) / KILO_BYTE
            },
            ActualTxFee::FixedPerKb(f) => {
                let transaction = UtxoTx::from(tx.clone());
                let transaction_bytes = serialize(&transaction);
                // 2 bytes are used to indicate the length of signature and pubkey
                // total is 107
                let additional_len = 2 + MAX_DER_SIGNATURE_LEN + COMPRESSED_PUBKEY_LEN;
                let tx_size_bytes = (transaction_bytes.len() + transaction.inputs().len() * additional_len) as u64;
                let tx_size_kb = if tx_size_bytes % KILO_BYTE == 0 {
                    tx_size_bytes / KILO_BYTE
                } else {
                    tx_size_bytes / KILO_BYTE + 1
                };
                f * tx_size_kb
            },
        };

        match fee_policy {
            FeePolicy::SendExact => {
                let mut outputs_plus_fee = sum_outputs_value + tx_fee;
                if sum_inputs >= outputs_plus_fee {
                    let change = sum_inputs - outputs_plus_fee;
                    if change > dust {
                        // there will be change output
                        if let ActualTxFee::Dynamic(ref f) = coin_tx_fee {
                            tx_fee += (f * P2PKH_OUTPUT_LEN) / KILO_BYTE;
                            outputs_plus_fee += (f * P2PKH_OUTPUT_LEN) / KILO_BYTE;
                        }
                    }
                    if let Some(min_relay) = min_relay_fee {
                        if tx_fee < min_relay {
                            outputs_plus_fee -= tx_fee;
                            outputs_plus_fee += min_relay;
                            tx_fee = min_relay;
                        }
                    }
                    if sum_inputs >= outputs_plus_fee {
                        break;
                    }
                }
            },
            FeePolicy::DeductFromOutput(_) => {
                if sum_inputs >= sum_outputs_value {
                    let change = sum_inputs - sum_outputs_value;
                    if change > dust {
                        if let ActualTxFee::Dynamic(ref f) = coin_tx_fee {
                            tx_fee += (f * P2PKH_OUTPUT_LEN) / KILO_BYTE;
                        }
                        if let Some(min_relay) = min_relay_fee {
                            if tx_fee < min_relay {
                                tx_fee = min_relay;
                            }
                        }
                    }
                    break;
                }
            },
        };
    }
    match fee_policy {
        FeePolicy::SendExact => sum_outputs_value += tx_fee,
        FeePolicy::DeductFromOutput(i) => {
            let min_output = tx_fee + dust;
            let val = tx.outputs[i].value;
            true_or!(val >= min_output, GenerateTxError::DeductFeeFromOutputFailed {
                output_idx: i,
                output_value: val,
                required: min_output,
            });
            tx.outputs[i].value -= tx_fee;
            if tx.outputs[i].script_pubkey == change_script_pubkey {
                received_by_me -= tx_fee;
            }
        },
    };
    true_or!(sum_inputs >= sum_outputs_value, GenerateTxError::NotEnoughUtxos {
        sum_utxos: sum_inputs,
        required: sum_outputs_value
    });

    let change = sum_inputs - sum_outputs_value;
    let unused_change = if change > dust {
        tx.outputs.push({
            TransactionOutput {
                value: change,
                script_pubkey: change_script_pubkey.clone(),
            }
        });
        received_by_me += change;
        None
    } else if change > 0 {
        Some(change)
    } else {
        None
    };

    let data = AdditionalTxData {
        fee_amount: tx_fee,
        received_by_me,
        spent_by_me: sum_inputs,
        unused_change,
        // will be changed if the ticker is KMD
        kmd_rewards: None,
    };

    Ok(coin.calc_interest_if_required(tx, data, change_script_pubkey).await?)
}

/// Calculates interest if the coin is KMD
/// Adds the value to existing output to my_script_pub or creates additional interest output
/// returns transaction and data as is if the coin is not KMD
pub async fn calc_interest_if_required<T>(
    coin: &T,
    mut unsigned: TransactionInputSigner,
    mut data: AdditionalTxData,
    my_script_pub: Bytes,
) -> UtxoRpcResult<(TransactionInputSigner, AdditionalTxData)>
where
    T: AsRef<UtxoCoinFields> + UtxoCommonOps,
{
    if coin.as_ref().conf.ticker != "KMD" {
        return Ok((unsigned, data));
    }
    unsigned.lock_time = coin.get_current_mtp().await?;
    let mut interest = 0;
    for input in unsigned.inputs.iter() {
        let prev_hash = input.previous_output.hash.reversed().into();
        let tx = coin
            .as_ref()
            .rpc_client
            .get_verbose_transaction(prev_hash)
            .compat()
            .await?;
        if let Ok(output_interest) =
            kmd_interest(tx.height, input.amount, tx.locktime as u64, unsigned.lock_time as u64)
        {
            interest += output_interest;
        };
    }
    if interest > 0 {
        data.received_by_me += interest;
        let mut output_to_me = unsigned
            .outputs
            .iter_mut()
            .find(|out| out.script_pubkey == my_script_pub);
        // add calculated interest to existing output to my address
        // or create the new one if it's not found
        match output_to_me {
            Some(ref mut output) => output.value += interest,
            None => {
                let interest_output = TransactionOutput {
                    script_pubkey: my_script_pub,
                    value: interest,
                };
                unsigned.outputs.push(interest_output);
            },
        };
    } else {
        // if interest is zero attempt to set the lowest possible lock_time to claim it later
        unsigned.lock_time = (now_ms() / 1000) as u32 - 3600 + 777 * 2;
    }
    let rewards_amount = big_decimal_from_sat_unsigned(interest, coin.as_ref().decimals);
    data.kmd_rewards = Some(KmdRewardsDetails::claimed_by_me(rewards_amount));
    Ok((unsigned, data))
}

pub async fn p2sh_spending_tx<T>(
    coin: &T,
    prev_transaction: UtxoTx,
    redeem_script: Bytes,
    outputs: Vec<TransactionOutput>,
    script_data: Script,
    sequence: u32,
    lock_time: u32,
) -> Result<UtxoTx, String>
where
    T: AsRef<UtxoCoinFields> + UtxoCommonOps,
{
    let lock_time = try_s!(coin.p2sh_tx_locktime(lock_time).await);
    let n_time = if coin.as_ref().conf.is_pos {
        Some((now_ms() / 1000) as u32)
    } else {
        None
    };
    let str_d_zeel = if coin.as_ref().conf.ticker == "NAV" {
        Some("".into())
    } else {
        None
    };
    let hash_algo = coin.as_ref().tx_hash_algo.into();
    let unsigned = TransactionInputSigner {
        lock_time,
        version: coin.as_ref().conf.tx_version,
        n_time,
        overwintered: coin.as_ref().conf.overwintered,
        inputs: vec![UnsignedTransactionInput {
            sequence,
            previous_output: OutPoint {
                hash: prev_transaction.hash(),
                index: DEFAULT_SWAP_VOUT as u32,
            },
            amount: prev_transaction.outputs[0].value,
            witness: Vec::new(),
        }],
        outputs: outputs.clone(),
        expiry_height: 0,
        join_splits: vec![],
        shielded_spends: vec![],
        shielded_outputs: vec![],
        value_balance: 0,
        version_group_id: coin.as_ref().conf.version_group_id,
        consensus_branch_id: coin.as_ref().conf.consensus_branch_id,
        zcash: coin.as_ref().conf.zcash,
        str_d_zeel,
        hash_algo,
    };
    let signed_input = try_s!(p2sh_spend(
        &unsigned,
        DEFAULT_SWAP_VOUT,
        &coin.as_ref().key_pair,
        script_data,
        redeem_script.into(),
        coin.as_ref().conf.signature_version,
        coin.as_ref().conf.fork_id
    ));
    Ok(UtxoTx {
        version: unsigned.version,
        n_time: unsigned.n_time,
        overwintered: unsigned.overwintered,
        lock_time: unsigned.lock_time,
        inputs: vec![signed_input],
        outputs,
        expiry_height: unsigned.expiry_height,
        join_splits: vec![],
        shielded_spends: vec![],
        shielded_outputs: vec![],
        value_balance: 0,
        version_group_id: coin.as_ref().conf.version_group_id,
        binding_sig: H512::default(),
        join_split_sig: H512::default(),
        join_split_pubkey: H256::default(),
        zcash: coin.as_ref().conf.zcash,
        str_d_zeel: unsigned.str_d_zeel,
        tx_hash_algo: unsigned.hash_algo.into(),
    })
}

pub fn send_taker_fee<T>(coin: T, fee_pub_key: &[u8], amount: BigDecimal) -> TransactionFut
where
    T: AsRef<UtxoCoinFields> + UtxoCommonOps + Send + Sync + 'static,
{
    let address = try_fus!(address_from_raw_pubkey(
        fee_pub_key,
        coin.as_ref().conf.pub_addr_prefix,
        coin.as_ref().conf.pub_t_addr_prefix,
        coin.as_ref().conf.checksum_type,
        coin.as_ref().conf.bech32_hrp.clone(),
        coin.as_ref().my_address.addr_format.clone()
    ));
    let amount = try_fus!(sat_from_big_decimal(&amount, coin.as_ref().decimals));
    let output = TransactionOutput {
        value: amount,
        script_pubkey: Builder::build_p2pkh(&address.hash).to_bytes(),
    };
    send_outputs_from_my_address(coin, vec![output])
}

pub fn send_maker_payment<T>(
    coin: T,
    time_lock: u32,
    taker_pub: &[u8],
    secret_hash: &[u8],
    amount: BigDecimal,
) -> TransactionFut
where
    T: AsRef<UtxoCoinFields> + UtxoCommonOps + Clone + Send + Sync + 'static,
{
    let SwapPaymentOutputsResult {
        payment_address,
        outputs,
    } = try_fus!(generate_swap_payment_outputs(
        &coin,
        time_lock,
        taker_pub,
        secret_hash,
        amount
    ));
    let send_fut = match &coin.as_ref().rpc_client {
        UtxoRpcClientEnum::Electrum(_) => Either::A(send_outputs_from_my_address(coin, outputs)),
        UtxoRpcClientEnum::Native(client) => {
            let addr_string = try_fus!(payment_address.display_address());
            Either::B(
                client
                    .import_address(&addr_string, &addr_string, false)
                    .map_err(|e| ERRL!("{}", e))
                    .and_then(move |_| send_outputs_from_my_address(coin, outputs)),
            )
        },
    };
    Box::new(send_fut)
}

pub fn send_taker_payment<T>(
    coin: T,
    time_lock: u32,
    maker_pub: &[u8],
    secret_hash: &[u8],
    amount: BigDecimal,
) -> TransactionFut
where
    T: AsRef<UtxoCoinFields> + UtxoCommonOps + Clone + Send + Sync + 'static,
{
    let SwapPaymentOutputsResult {
        payment_address,
        outputs,
    } = try_fus!(generate_swap_payment_outputs(
        &coin,
        time_lock,
        maker_pub,
        secret_hash,
        amount
    ));
    let send_fut = match &coin.as_ref().rpc_client {
        UtxoRpcClientEnum::Electrum(_) => Either::A(send_outputs_from_my_address(coin, outputs)),
        UtxoRpcClientEnum::Native(client) => {
            let addr_string = try_fus!(payment_address.display_address());
            Either::B(
                client
                    .import_address(&addr_string, &addr_string, false)
                    .map_err(|e| ERRL!("{}", e))
                    .and_then(move |_| send_outputs_from_my_address(coin, outputs)),
            )
        },
    };
    Box::new(send_fut)
}

pub fn send_maker_spends_taker_payment<T>(
    coin: T,
    taker_payment_tx: &[u8],
    time_lock: u32,
    taker_pub: &[u8],
    secret: &[u8],
) -> TransactionFut
where
    T: AsRef<UtxoCoinFields> + UtxoCommonOps + Send + Sync + 'static,
{
    let mut prev_tx: UtxoTx = try_fus!(deserialize(taker_payment_tx).map_err(|e| ERRL!("{:?}", e)));
    prev_tx.tx_hash_algo = coin.as_ref().tx_hash_algo;
    let script_data = Builder::default()
        .push_data(secret)
        .push_opcode(Opcode::OP_0)
        .into_script();
    let redeem_script = payment_script(
        time_lock,
        &*dhash160(secret),
        &try_fus!(Public::from_slice(taker_pub)),
        coin.as_ref().key_pair.public(),
    );
    let fut = async move {
        let fee = try_s!(coin.get_htlc_spend_fee().await);
        let script_pubkey = output_script(&coin.as_ref().my_address).to_bytes();
        let output = TransactionOutput {
            value: prev_tx.outputs[0].value - fee,
            script_pubkey,
        };
        let transaction = try_s!(
            coin.p2sh_spending_tx(
                prev_tx,
                redeem_script.into(),
                vec![output],
                script_data,
                SEQUENCE_FINAL,
                time_lock
            )
            .await
        );
        let tx_fut = coin.as_ref().rpc_client.send_transaction(&transaction).compat();
        try_s!(tx_fut.await);
        Ok(transaction.into())
    };
    Box::new(fut.boxed().compat())
}

pub fn send_taker_spends_maker_payment<T>(
    coin: T,
    maker_payment_tx: &[u8],
    time_lock: u32,
    maker_pub: &[u8],
    secret: &[u8],
) -> TransactionFut
where
    T: AsRef<UtxoCoinFields> + UtxoCommonOps + Send + Sync + 'static,
{
    let mut prev_tx: UtxoTx = try_fus!(deserialize(maker_payment_tx).map_err(|e| ERRL!("{:?}", e)));
    prev_tx.tx_hash_algo = coin.as_ref().tx_hash_algo;
    let script_data = Builder::default()
        .push_data(secret)
        .push_opcode(Opcode::OP_0)
        .into_script();
    let redeem_script = payment_script(
        time_lock,
        &*dhash160(secret),
        &try_fus!(Public::from_slice(maker_pub)),
        coin.as_ref().key_pair.public(),
    );
    let fut = async move {
        let fee = try_s!(coin.get_htlc_spend_fee().await);
        let script_pubkey = output_script(&coin.as_ref().my_address).to_bytes();
        let output = TransactionOutput {
            value: prev_tx.outputs[0].value - fee,
            script_pubkey,
        };
        let transaction = try_s!(
            coin.p2sh_spending_tx(
                prev_tx,
                redeem_script.into(),
                vec![output],
                script_data,
                SEQUENCE_FINAL,
                time_lock
            )
            .await
        );
        let tx_fut = coin.as_ref().rpc_client.send_transaction(&transaction).compat();
        try_s!(tx_fut.await);
        Ok(transaction.into())
    };
    Box::new(fut.boxed().compat())
}

pub fn send_taker_refunds_payment<T>(
    coin: T,
    taker_payment_tx: &[u8],
    time_lock: u32,
    maker_pub: &[u8],
    secret_hash: &[u8],
) -> TransactionFut
where
    T: AsRef<UtxoCoinFields> + UtxoCommonOps + Send + Sync + 'static,
{
    let mut prev_tx: UtxoTx = try_fus!(deserialize(taker_payment_tx).map_err(|e| ERRL!("{:?}", e)));
    prev_tx.tx_hash_algo = coin.as_ref().tx_hash_algo;
    let script_data = Builder::default().push_opcode(Opcode::OP_1).into_script();
    let redeem_script = payment_script(
        time_lock,
        secret_hash,
        coin.as_ref().key_pair.public(),
        &try_fus!(Public::from_slice(maker_pub)),
    );
    let fut = async move {
        let fee = try_s!(coin.get_htlc_spend_fee().await);
        let script_pubkey = output_script(&coin.as_ref().my_address).to_bytes();
        let output = TransactionOutput {
            value: prev_tx.outputs[0].value - fee,
            script_pubkey,
        };
        let transaction = try_s!(
            coin.p2sh_spending_tx(
                prev_tx,
                redeem_script.into(),
                vec![output],
                script_data,
                SEQUENCE_FINAL - 1,
                time_lock,
            )
            .await
        );
        let tx_fut = coin.as_ref().rpc_client.send_transaction(&transaction).compat();
        try_s!(tx_fut.await);
        Ok(transaction.into())
    };
    Box::new(fut.boxed().compat())
}

pub fn send_maker_refunds_payment<T>(
    coin: T,
    maker_payment_tx: &[u8],
    time_lock: u32,
    taker_pub: &[u8],
    secret_hash: &[u8],
) -> TransactionFut
where
    T: AsRef<UtxoCoinFields> + UtxoCommonOps + Send + Sync + 'static,
{
    let mut prev_tx: UtxoTx = try_fus!(deserialize(maker_payment_tx).map_err(|e| ERRL!("{:?}", e)));
    prev_tx.tx_hash_algo = coin.as_ref().tx_hash_algo;
    let script_data = Builder::default().push_opcode(Opcode::OP_1).into_script();
    let redeem_script = payment_script(
        time_lock,
        secret_hash,
        coin.as_ref().key_pair.public(),
        &try_fus!(Public::from_slice(taker_pub)),
    );
    let fut = async move {
        let fee = try_s!(coin.get_htlc_spend_fee().await);
        let script_pubkey = output_script(&coin.as_ref().my_address).to_bytes();
        let output = TransactionOutput {
            value: prev_tx.outputs[0].value - fee,
            script_pubkey,
        };
        let transaction = try_s!(
            coin.p2sh_spending_tx(
                prev_tx,
                redeem_script.into(),
                vec![output],
                script_data,
                SEQUENCE_FINAL - 1,
                time_lock,
            )
            .await
        );
        let tx_fut = coin.as_ref().rpc_client.send_transaction(&transaction).compat();
        try_s!(tx_fut.await);
        Ok(transaction.into())
    };
    Box::new(fut.boxed().compat())
}

/// Extracts pubkey from script sig
fn pubkey_from_script_sig(script: &Script) -> Result<H264, String> {
    match script.get_instruction(0) {
        Some(Ok(instruction)) => match instruction.opcode {
            Opcode::OP_PUSHBYTES_70 | Opcode::OP_PUSHBYTES_71 | Opcode::OP_PUSHBYTES_72 => match instruction.data {
                Some(bytes) => try_s!(Signature::parse_der(&bytes[..bytes.len() - 1])),
                None => return ERR!("No data at instruction 0 of script {:?}", script),
            },
            _ => return ERR!("Unexpected opcode {:?}", instruction.opcode),
        },
        Some(Err(e)) => return ERR!("Error {} on getting instruction 0 of script {:?}", e, script),
        None => return ERR!("None instruction 0 of script {:?}", script),
    };

    let pubkey = match script.get_instruction(1) {
        Some(Ok(instruction)) => match instruction.opcode {
            Opcode::OP_PUSHBYTES_33 => match instruction.data {
                Some(bytes) => try_s!(PublicKey::parse_slice(bytes, None)),
                None => return ERR!("No data at instruction 1 of script {:?}", script),
            },
            _ => return ERR!("Unexpected opcode {:?}", instruction.opcode),
        },
        Some(Err(e)) => return ERR!("Error {} on getting instruction 1 of script {:?}", e, script),
        None => return ERR!("None instruction 1 of script {:?}", script),
    };

    if script.get_instruction(2).is_some() {
        return ERR!("Unexpected instruction at position 2 of script {:?}", script);
    }
    Ok(pubkey.serialize_compressed().into())
}

/// Extracts pubkey from witness script
fn pubkey_from_witness_script(witness_script: &[Bytes]) -> Result<H264, String> {
    if witness_script.len() != 2 {
        return ERR!("Invalid witness length {}", witness_script.len());
    }

    let signature = witness_script[0].clone().take();
    if signature.is_empty() {
        return ERR!("Empty signature data in witness script");
    }
    try_s!(Signature::parse_der(&signature[..signature.len() - 1]));

    let pubkey = try_s!(PublicKey::parse_slice(&witness_script[1], None));

    Ok(pubkey.serialize_compressed().into())
}

pub async fn is_tx_confirmed_before_block<T>(coin: &T, tx: &RpcTransaction, block_number: u64) -> Result<bool, String>
where
    T: AsRef<UtxoCoinFields> + Send + Sync + 'static,
{
    match tx.height {
        Some(confirmed_at) => Ok(confirmed_at <= block_number),
        // fallback to a number of confirmations
        None => {
            if tx.confirmations > 0 {
                let current_block = try_s!(coin.as_ref().rpc_client.get_block_count().compat().await);
                let confirmed_at = current_block + 1 - tx.confirmations as u64;
                Ok(confirmed_at <= block_number)
            } else {
                Ok(false)
            }
        },
    }
}

pub fn check_all_inputs_signed_by_pub(tx: &UtxoTx, expected_pub: &[u8]) -> Result<bool, String> {
    for input in &tx.inputs {
        let pubkey = if input.has_witness() {
            try_s!(pubkey_from_witness_script(&input.script_witness))
        } else {
            let script: Script = input.script_sig.clone().into();
            try_s!(pubkey_from_script_sig(&script))
        };
        if *pubkey != expected_pub {
            return Ok(false);
        }
    }

    Ok(true)
}

pub fn validate_fee<T>(
    coin: T,
    tx: UtxoTx,
    output_index: usize,
    sender_pubkey: &[u8],
    amount: &BigDecimal,
    min_block_number: u64,
    fee_addr: &[u8],
) -> Box<dyn Future<Item = (), Error = String> + Send>
where
    T: AsRef<UtxoCoinFields> + Send + Sync + 'static,
{
    let amount = amount.clone();
    let address = try_fus!(address_from_raw_pubkey(
        fee_addr,
        coin.as_ref().conf.pub_addr_prefix,
        coin.as_ref().conf.pub_t_addr_prefix,
        coin.as_ref().conf.checksum_type,
        coin.as_ref().conf.bech32_hrp.clone(),
        coin.as_ref().my_address.addr_format.clone()
    ));

    if !try_fus!(check_all_inputs_signed_by_pub(&tx, sender_pubkey)) {
        return Box::new(futures01::future::err(ERRL!("The dex fee was sent from wrong address")));
    }
    let fut = async move {
        let amount = try_s!(sat_from_big_decimal(&amount, coin.as_ref().decimals));
        let tx_from_rpc = try_s!(
            coin.as_ref()
                .rpc_client
                .get_verbose_transaction(tx.hash().reversed().into())
                .compat()
                .await
        );

        if try_s!(is_tx_confirmed_before_block(&coin, &tx_from_rpc, min_block_number).await) {
            return ERR!(
                "Fee tx {:?} confirmed before min_block {}",
                tx_from_rpc,
                min_block_number,
            );
        }
        if tx_from_rpc.hex.0 != serialize(&tx).take()
            && tx_from_rpc.hex.0 != serialize_with_flags(&tx, SERIALIZE_TRANSACTION_WITNESS).take()
        {
            return ERR!(
                "Provided dex fee tx {:?} doesn't match tx data from rpc {:?}",
                tx,
                tx_from_rpc
            );
        }

        match tx.outputs.get(output_index) {
            Some(out) => {
                let expected_script_pubkey = Builder::build_p2pkh(&address.hash).to_bytes();
                if out.script_pubkey != expected_script_pubkey {
                    return ERR!(
                        "Provided dex fee tx output script_pubkey doesn't match expected {:?} {:?}",
                        out.script_pubkey,
                        expected_script_pubkey
                    );
                }
                if out.value < amount {
                    return ERR!(
                        "Provided dex fee tx output value is less than expected {:?} {:?}",
                        out.value,
                        amount
                    );
                }
            },
            None => {
                return ERR!("Provided dex fee tx {:?} does not have output {}", tx, output_index);
            },
        }
        Ok(())
    };
    Box::new(fut.boxed().compat())
}

pub fn validate_maker_payment<T>(
    coin: &T,
    payment_tx: &[u8],
    time_lock: u32,
    maker_pub: &[u8],
    priv_bn_hash: &[u8],
    amount: BigDecimal,
) -> Box<dyn Future<Item = (), Error = String> + Send>
where
    T: AsRef<UtxoCoinFields> + Clone + Send + Sync + 'static,
{
    let my_public = coin.as_ref().key_pair.public();
    let mut tx: UtxoTx = try_fus!(deserialize(payment_tx).map_err(|e| ERRL!("{:?}", e)));
    tx.tx_hash_algo = coin.as_ref().tx_hash_algo;

    validate_payment(
        coin.clone(),
        tx,
        DEFAULT_SWAP_VOUT,
        &try_fus!(Public::from_slice(maker_pub)),
        my_public,
        priv_bn_hash,
        amount,
        time_lock,
    )
}

pub fn validate_taker_payment<T>(
    coin: &T,
    payment_tx: &[u8],
    time_lock: u32,
    taker_pub: &[u8],
    priv_bn_hash: &[u8],
    amount: BigDecimal,
) -> Box<dyn Future<Item = (), Error = String> + Send>
where
    T: AsRef<UtxoCoinFields> + Clone + Send + Sync + 'static,
{
    let my_public = coin.as_ref().key_pair.public();
    let mut tx: UtxoTx = try_fus!(deserialize(payment_tx).map_err(|e| ERRL!("{:?}", e)));
    tx.tx_hash_algo = coin.as_ref().tx_hash_algo;

    validate_payment(
        coin.clone(),
        tx,
        DEFAULT_SWAP_VOUT,
        &try_fus!(Public::from_slice(taker_pub)),
        my_public,
        priv_bn_hash,
        amount,
        time_lock,
    )
}

pub fn check_if_my_payment_sent<T>(
    coin: T,
    time_lock: u32,
    other_pub: &[u8],
    secret_hash: &[u8],
) -> Box<dyn Future<Item = Option<TransactionEnum>, Error = String> + Send>
where
    T: AsRef<UtxoCoinFields> + UtxoCommonOps + Send + Sync + 'static,
{
    let script = payment_script(
        time_lock,
        secret_hash,
        coin.as_ref().key_pair.public(),
        &try_fus!(Public::from_slice(other_pub)),
    );
    let hash = dhash160(&script);
    let p2sh = Builder::build_p2sh(&hash);
    let script_hash = electrum_script_hash(&p2sh);
    let fut = async move {
        match &coin.as_ref().rpc_client {
            UtxoRpcClientEnum::Electrum(client) => {
                let history = try_s!(client.scripthash_get_history(&hex::encode(script_hash)).compat().await);
                match history.first() {
                    Some(item) => {
                        let tx_bytes = try_s!(client.get_transaction_bytes(item.tx_hash.clone()).compat().await);
                        let mut tx: UtxoTx = try_s!(deserialize(tx_bytes.0.as_slice()).map_err(|e| ERRL!("{:?}", e)));
                        tx.tx_hash_algo = coin.as_ref().tx_hash_algo;
                        Ok(Some(tx.into()))
                    },
                    None => Ok(None),
                }
            },
            UtxoRpcClientEnum::Native(client) => {
                let target_addr = Address {
                    t_addr_prefix: coin.as_ref().conf.p2sh_t_addr_prefix,
                    prefix: coin.as_ref().conf.p2sh_addr_prefix,
                    hash,
                    checksum_type: coin.as_ref().conf.checksum_type,
                    hrp: coin.as_ref().conf.bech32_hrp.clone(),
                    addr_format: coin.as_ref().my_address.addr_format.clone(),
                };
                let target_addr = target_addr.to_string();
                let is_imported = try_s!(client.is_address_imported(&target_addr).await);
                if !is_imported {
                    return Ok(None);
                }
                let received_by_addr = try_s!(client.list_received_by_address(0, true, true).compat().await);
                for item in received_by_addr {
                    if item.address == target_addr && !item.txids.is_empty() {
                        let tx_bytes = try_s!(client.get_transaction_bytes(item.txids[0].clone()).compat().await);
                        let mut tx: UtxoTx = try_s!(deserialize(tx_bytes.0.as_slice()).map_err(|e| ERRL!("{:?}", e)));
                        tx.tx_hash_algo = coin.as_ref().tx_hash_algo;
                        return Ok(Some(tx.into()));
                    }
                }
                Ok(None)
            },
        }
    };
    Box::new(fut.boxed().compat())
}

pub fn search_for_swap_tx_spend_my(
    coin: &UtxoCoinFields,
    time_lock: u32,
    other_pub: &[u8],
    secret_hash: &[u8],
    tx: &[u8],
    output_index: usize,
    search_from_block: u64,
) -> Result<Option<FoundSwapTxSpend>, String> {
    block_on(search_for_swap_output_spend(
        coin,
        time_lock,
        coin.key_pair.public(),
        &try_s!(Public::from_slice(other_pub)),
        secret_hash,
        tx,
        output_index,
        search_from_block,
    ))
}

pub fn search_for_swap_tx_spend_other(
    coin: &UtxoCoinFields,
    time_lock: u32,
    other_pub: &[u8],
    secret_hash: &[u8],
    tx: &[u8],
    output_index: usize,
    search_from_block: u64,
) -> Result<Option<FoundSwapTxSpend>, String> {
    block_on(search_for_swap_output_spend(
        coin,
        time_lock,
        &try_s!(Public::from_slice(other_pub)),
        coin.key_pair.public(),
        secret_hash,
        tx,
        output_index,
        search_from_block,
    ))
}

/// Extract a secret from the `spend_tx`.
/// Note spender could generate the spend with several inputs where the only one input is the p2sh script.
pub fn extract_secret(secret_hash: &[u8], spend_tx: &[u8]) -> Result<Vec<u8>, String> {
    let spend_tx: UtxoTx = try_s!(deserialize(spend_tx).map_err(|e| ERRL!("{:?}", e)));
    for (input_idx, input) in spend_tx.inputs.into_iter().enumerate() {
        let script: Script = input.script_sig.clone().into();
        let instruction = match script.get_instruction(1) {
            Some(Ok(instr)) => instr,
            Some(Err(e)) => {
                log!("Warning: "[e]);
                continue;
            },
            None => {
                log!("Warning: couldn't find secret in "[input_idx]" input");
                continue;
            },
        };

        if instruction.opcode != Opcode::OP_PUSHBYTES_32 {
            log!("Warning: expected "[Opcode::OP_PUSHBYTES_32]" opcode, found "[instruction.opcode] " in "[input_idx]" input");
            continue;
        }

        let secret = match instruction.data {
            Some(data) => data.to_vec(),
            None => {
                log!("Warning: secret is empty in "[input_idx] " input");
                continue;
            },
        };

        let actual_secret_hash = &*dhash160(&secret);
        if actual_secret_hash != secret_hash {
            log!("Warning: invalid 'dhash160(secret)' "[actual_secret_hash]", expected "[secret_hash]);
            continue;
        }
        return Ok(secret);
    }
    ERR!("Couldn't extract secret")
}

pub fn my_address<T>(coin: &T) -> Result<String, String>
where
    T: AsRef<UtxoCoinFields> + UtxoCommonOps,
{
    coin.as_ref().my_address.display_address()
}

pub fn my_balance(coin: &UtxoCoinFields) -> BalanceFut<CoinBalance> {
    Box::new(
        coin.rpc_client
            .display_balance(coin.my_address.clone(), coin.decimals)
            .map_to_mm_fut(BalanceError::from)
            // at the moment standard UTXO coins do not have an unspendable balance
            .map(|spendable| CoinBalance {
                spendable,
                unspendable: BigDecimal::from(0),
            }),
    )
}

pub fn send_raw_tx(coin: &UtxoCoinFields, tx: &str) -> Box<dyn Future<Item = String, Error = String> + Send> {
    let bytes = try_fus!(hex::decode(tx));
    Box::new(
        coin.rpc_client
            .send_raw_transaction(bytes.into())
            .map_err(|e| ERRL!("{}", e))
            .map(|hash| format!("{:?}", hash)),
    )
}

pub fn wait_for_confirmations(
    coin: &UtxoCoinFields,
    tx: &[u8],
    confirmations: u64,
    requires_nota: bool,
    wait_until: u64,
    check_every: u64,
) -> Box<dyn Future<Item = (), Error = String> + Send> {
    let mut tx: UtxoTx = try_fus!(deserialize(tx).map_err(|e| ERRL!("{:?}", e)));
    tx.tx_hash_algo = coin.tx_hash_algo;
    coin.rpc_client
        .wait_for_confirmations(&tx, confirmations as u32, requires_nota, wait_until, check_every)
}

pub fn wait_for_output_spend(
    coin: &UtxoCoinFields,
    tx_bytes: &[u8],
    output_index: usize,
    from_block: u64,
    wait_until: u64,
) -> TransactionFut {
    let mut tx: UtxoTx = try_fus!(deserialize(tx_bytes).map_err(|e| ERRL!("{:?}", e)));
    tx.tx_hash_algo = coin.tx_hash_algo;
    let client = coin.rpc_client.clone();
    let tx_hash_algo = coin.tx_hash_algo;
    let fut = async move {
        loop {
            match client.find_output_spend(&tx, output_index, from_block).compat().await {
                Ok(Some(mut tx)) => {
                    tx.tx_hash_algo = tx_hash_algo;
                    return Ok(tx.into());
                },
                Ok(None) => (),
                Err(e) => {
                    log!("Error " (e) " on find_output_spend of tx " [e]);
                },
            };

            if now_ms() / 1000 > wait_until {
                return ERR!(
                    "Waited too long until {} for transaction {:?} {} to be spent ",
                    wait_until,
                    tx,
                    output_index,
                );
            }
            Timer::sleep(10.).await;
        }
    };
    Box::new(fut.boxed().compat())
}

pub fn tx_enum_from_bytes(coin: &UtxoCoinFields, bytes: &[u8]) -> Result<TransactionEnum, String> {
    let mut transaction: UtxoTx = try_s!(deserialize(bytes).map_err(|err| format!("{:?}", err)));
    transaction.tx_hash_algo = coin.tx_hash_algo;
    Ok(transaction.into())
}

pub fn current_block(coin: &UtxoCoinFields) -> Box<dyn Future<Item = u64, Error = String> + Send> {
    Box::new(coin.rpc_client.get_block_count().map_err(|e| ERRL!("{}", e)))
}

pub fn address_from_pubkey_str<T>(coin: &T, pubkey: &str) -> Result<String, String>
where
    T: AsRef<UtxoCoinFields> + UtxoCommonOps,
{
    let pubkey_bytes = try_s!(hex::decode(pubkey));
    let addr = try_s!(address_from_raw_pubkey(
        &pubkey_bytes,
        coin.as_ref().conf.pub_addr_prefix,
        coin.as_ref().conf.pub_t_addr_prefix,
        coin.as_ref().conf.checksum_type,
        coin.as_ref().conf.bech32_hrp.clone(),
        coin.as_ref().my_address.addr_format.clone()
    ));
    addr.display_address()
}

pub fn display_priv_key(coin: &UtxoCoinFields) -> String { format!("{}", coin.key_pair.private()) }

pub fn min_tx_amount(coin: &UtxoCoinFields) -> BigDecimal {
    big_decimal_from_sat(coin.dust_amount as i64, coin.decimals)
}

pub fn min_trading_vol(coin: &UtxoCoinFields) -> MmNumber {
    if coin.conf.ticker == "BTC" {
        return MmNumber::from(MIN_BTC_TRADING_VOL);
    }
    let dust_multiplier = MmNumber::from(10);
    dust_multiplier * min_tx_amount(coin).into()
}

pub fn is_asset_chain(coin: &UtxoCoinFields) -> bool { coin.conf.asset_chain }

pub async fn get_raw_transaction<T>(coin: T, req: RawTransactionRequest) -> RawTransactionResult
where
    T: AsRef<UtxoCoinFields> + UtxoCommonOps + MarketCoinOps,
{
    let hash = H256Json::from_str(&req.tx_hash).map_to_mm(|e| RawTransactionError::InvalidHashError(e.to_string()))?;
    let hex = coin
        .as_ref()
        .rpc_client
        .get_transaction_bytes(hash)
        .compat()
        .await
        .map_err(|e| RawTransactionError::Transport(e.to_string()))?;
    Ok(RawTransactionRes { tx_hex: hex })
}

pub async fn withdraw<T>(coin: T, req: WithdrawRequest) -> WithdrawResult
where
    T: AsRef<UtxoCoinFields> + UtxoCommonOps + MarketCoinOps,
{
    let decimals = coin.as_ref().decimals;

    let conf = &coin.as_ref().conf;

    let to = coin
        .address_from_str(&req.to)
        .map_to_mm(WithdrawError::InvalidAddress)?;

    let script_pubkey = output_script(&to).to_bytes();

    let signature_version = match coin.as_ref().my_address.addr_format {
        UtxoAddressFormat::Segwit => SignatureVersion::WitnessV0,
        _ => conf.signature_version,
    };

    let _utxo_lock = UTXO_LOCK.lock().await;
    let (unspents, _) = coin.ordered_mature_unspents(&coin.as_ref().my_address).await?;
    let (value, fee_policy) = if req.max {
        (
            unspents.iter().fold(0, |sum, unspent| sum + unspent.value),
            FeePolicy::DeductFromOutput(0),
        )
    } else {
        let value = sat_from_big_decimal(&req.amount, decimals)?;
        (value, FeePolicy::SendExact)
    };
    let outputs = vec![TransactionOutput { value, script_pubkey }];
    let fee = match req.fee {
        Some(WithdrawFee::UtxoFixed { amount }) => {
            let fixed = sat_from_big_decimal(&amount, decimals)?;
            Some(ActualTxFee::Fixed(fixed))
        },
        Some(WithdrawFee::UtxoPerKbyte { amount }) => {
            let dynamic = sat_from_big_decimal(&amount, decimals)?;
            Some(ActualTxFee::Dynamic(dynamic))
        },
        Some(fee_policy) => {
            let error = format!(
                "Expected 'UtxoFixed' or 'UtxoPerKbyte' fee types, found {:?}",
                fee_policy
            );
            return MmError::err(WithdrawError::InvalidFeePolicy(error));
        },
        None => None,
    };
    let gas_fee = None;
    let (unsigned, data) = coin
        .generate_transaction(unspents, outputs, fee_policy, fee, gas_fee)
        .await
        .mm_err(|gen_tx_error| {
            WithdrawError::from_generate_tx_error(gen_tx_error, coin.ticker().to_owned(), decimals)
        })?;
    let prev_script = Builder::build_p2pkh(&coin.as_ref().my_address.hash);
    let signed = sign_tx(
        unsigned,
        &coin.as_ref().key_pair,
        prev_script,
        signature_version,
        coin.as_ref().conf.fork_id,
    )
    .map_to_mm(WithdrawError::InternalError)?;

    let fee_amount = data.fee_amount + data.unused_change.unwrap_or_default();
    let fee_details = UtxoFeeDetails {
        amount: big_decimal_from_sat(fee_amount as i64, decimals),
    };
    let my_address = coin.my_address().map_to_mm(WithdrawError::InternalError)?;
    let tx_hex = match coin.as_ref().my_address.addr_format {
        UtxoAddressFormat::Segwit => serialize_with_flags(&signed, SERIALIZE_TRANSACTION_WITNESS).into(),
        _ => serialize(&signed).into(),
    };
    Ok(TransactionDetails {
        from: vec![my_address],
        to: vec![req.to],
        total_amount: big_decimal_from_sat(data.spent_by_me as i64, decimals),
        spent_by_me: big_decimal_from_sat(data.spent_by_me as i64, decimals),
        received_by_me: big_decimal_from_sat(data.received_by_me as i64, decimals),
        my_balance_change: big_decimal_from_sat(data.received_by_me as i64 - data.spent_by_me as i64, decimals),
        tx_hash: signed.hash().reversed().to_vec().into(),
        tx_hex,
        fee_details: Some(fee_details.into()),
        block_height: 0,
        coin: coin.as_ref().conf.ticker.clone(),
        internal_id: vec![].into(),
        timestamp: now_ms() / 1000,
        kmd_rewards: data.kmd_rewards,
    })
}

pub fn decimals(coin: &UtxoCoinFields) -> u8 { coin.decimals }

pub fn convert_to_address<T>(coin: &T, from: &str, to_address_format: Json) -> Result<String, String>
where
    T: AsRef<UtxoCoinFields> + UtxoCommonOps,
{
    let to_address_format: UtxoAddressFormat =
        json::from_value(to_address_format).map_err(|e| ERRL!("Error on parse UTXO address format {:?}", e))?;
    let mut from_address = try_s!(coin.address_from_str(from));
    match to_address_format {
        UtxoAddressFormat::Standard => {
            from_address.addr_format = UtxoAddressFormat::Standard;
            Ok(from_address.to_string())
        },
        UtxoAddressFormat::Segwit => {
            let bech32_hrp = &coin.as_ref().conf.bech32_hrp;
            match bech32_hrp {
                Some(hrp) => Ok(SegwitAddress::new(&from_address.hash, hrp.clone()).to_string()),
                None => ERR!("Cannot convert to a segwit address for a coin with no bech32_hrp in config"),
            }
        },
        UtxoAddressFormat::CashAddress { network, .. } => Ok(try_s!(from_address
            .to_cashaddress(
                &network,
                coin.as_ref().conf.pub_addr_prefix,
                coin.as_ref().conf.p2sh_addr_prefix
            )
            .and_then(|cashaddress| cashaddress.encode()))),
    }
}

pub fn validate_address<T>(coin: &T, address: &str) -> ValidateAddressResult
where
    T: AsRef<UtxoCoinFields> + UtxoCommonOps,
{
    let result = coin.address_from_str(address);
    let address = match result {
        Ok(addr) => addr,
        Err(e) => {
            return ValidateAddressResult {
                is_valid: false,
                reason: Some(e),
            }
        },
    };

    let is_p2pkh = address.prefix == coin.as_ref().conf.pub_addr_prefix
        && address.t_addr_prefix == coin.as_ref().conf.pub_t_addr_prefix;
    let is_p2sh = address.prefix == coin.as_ref().conf.p2sh_addr_prefix
        && address.t_addr_prefix == coin.as_ref().conf.p2sh_t_addr_prefix
        && coin.as_ref().conf.segwit;
    let is_segwit = address.hrp.is_some() && address.hrp == coin.as_ref().conf.bech32_hrp && coin.as_ref().conf.segwit;

    if is_p2pkh || is_p2sh || is_segwit {
        ValidateAddressResult {
            is_valid: true,
            reason: None,
        }
    } else {
        ValidateAddressResult {
            is_valid: false,
            reason: Some(ERRL!("Address {} has invalid prefixes", address)),
        }
    }
}

#[allow(clippy::cognitive_complexity)]
pub async fn process_history_loop<T>(coin: T, ctx: MmArc)
where
    T: AsRef<UtxoCoinFields> + UtxoStandardOps + UtxoCommonOps + MmCoin + MarketCoinOps,
{
    let mut my_balance: Option<CoinBalance> = None;
    let history = match coin.load_history_from_file(&ctx).compat().await {
        Ok(history) => history,
        Err(e) => {
            ctx.log.log(
                "",
                &[&"tx_history", &coin.as_ref().conf.ticker],
                &ERRL!("Error {} on 'load_history_from_file', stop the history loop", e),
            );
            return;
        },
    };
    let mut history_map: HashMap<H256Json, TransactionDetails> = history
        .into_iter()
        .map(|tx| (H256Json::from(tx.tx_hash.as_slice()), tx))
        .collect();

    let mut success_iteration = 0i32;
    loop {
        if ctx.is_stopping() {
            break;
        };
        {
            let coins_ctx = CoinsContext::from_ctx(&ctx).unwrap();
            let coins = coins_ctx.coins.lock().await;
            if !coins.contains_key(&coin.as_ref().conf.ticker) {
                ctx.log
                    .log("", &[&"tx_history", &coin.as_ref().conf.ticker], "Loop stopped");
                break;
            };
        }

        let actual_balance = match coin.my_balance().compat().await {
            Ok(actual_balance) => Some(actual_balance),
            Err(err) => {
                ctx.log.log(
                    "",
                    &[&"tx_history", &coin.as_ref().conf.ticker],
                    &ERRL!("Error {:?} on getting balance", err),
                );
                None
            },
        };

        let need_update = history_map.iter().any(|(_, tx)| tx.should_update());
        match (&my_balance, &actual_balance) {
            (Some(prev_balance), Some(actual_balance)) if prev_balance == actual_balance && !need_update => {
                // my balance hasn't been changed, there is no need to reload tx_history
                Timer::sleep(30.).await;
                continue;
            },
            _ => (),
        }

        let metrics = ctx.metrics.clone();
        let tx_ids = match coin.request_tx_history(metrics).await {
            RequestTxHistoryResult::Ok(tx_ids) => tx_ids,
            RequestTxHistoryResult::Retry { error } => {
                ctx.log.log(
                    "",
                    &[&"tx_history", &coin.as_ref().conf.ticker],
                    &ERRL!("{}, retrying", error),
                );
                Timer::sleep(10.).await;
                continue;
            },
            RequestTxHistoryResult::HistoryTooLarge => {
                ctx.log.log(
                    "",
                    &[&"tx_history", &coin.as_ref().conf.ticker],
                    &ERRL!("Got `history too large`, stopping further attempts to retrieve it"),
                );
                *coin.as_ref().history_sync_state.lock().unwrap() = HistorySyncState::Error(json!({
                    "code": HISTORY_TOO_LARGE_ERR_CODE,
                    "message": "Got `history too large` error from Electrum server. History is not available",
                }));
                break;
            },
            RequestTxHistoryResult::UnknownError(e) => {
                ctx.log.log(
                    "",
                    &[&"tx_history", &coin.as_ref().conf.ticker],
                    &ERRL!("{}, stopping futher attempts to retreive it", e),
                );
                break;
            },
        };
        let mut transactions_left = if tx_ids.len() > history_map.len() {
            *coin.as_ref().history_sync_state.lock().unwrap() = HistorySyncState::InProgress(json!({
                "transactions_left": tx_ids.len() - history_map.len()
            }));
            tx_ids.len() - history_map.len()
        } else {
            *coin.as_ref().history_sync_state.lock().unwrap() = HistorySyncState::InProgress(json!({
                "transactions_left": 0
            }));
            0
        };

        // This is the cache of the already requested transactions.
        let mut input_transactions = HistoryUtxoTxMap::default();
        for (txid, height) in tx_ids {
            let mut updated = false;
            match history_map.entry(txid.clone()) {
                Entry::Vacant(e) => {
                    mm_counter!(ctx.metrics, "tx.history.request.count", 1, "coin" => coin.as_ref().conf.ticker.clone(), "method" => "tx_detail_by_hash");

                    match coin.tx_details_by_hash(&txid.0, &mut input_transactions).await {
                        Ok(mut tx_details) => {
                            mm_counter!(ctx.metrics, "tx.history.response.count", 1, "coin" => coin.as_ref().conf.ticker.clone(), "method" => "tx_detail_by_hash");

                            if tx_details.block_height == 0 && height > 0 {
                                tx_details.block_height = height;
                            }

                            e.insert(tx_details);
                            if transactions_left > 0 {
                                transactions_left -= 1;
                                *coin.as_ref().history_sync_state.lock().unwrap() =
                                    HistorySyncState::InProgress(json!({ "transactions_left": transactions_left }));
                            }
                            updated = true;
                        },
                        Err(e) => ctx.log.log(
                            "",
                            &[&"tx_history", &coin.as_ref().conf.ticker],
                            &ERRL!("Error {:?} on getting the details of {:?}, skipping the tx", e, txid),
                        ),
                    }
                },
                Entry::Occupied(mut e) => {
                    // update block height for previously unconfirmed transaction
                    if e.get().should_update_block_height() && height > 0 {
                        e.get_mut().block_height = height;
                        updated = true;
                    }
                    if e.get().should_update_timestamp() || e.get().firo_negative_fee() {
                        mm_counter!(ctx.metrics, "tx.history.request.count", 1, "coin" => coin.as_ref().conf.ticker.clone(), "method" => "tx_detail_by_hash");

                        if let Ok(tx_details) = coin.tx_details_by_hash(&txid.0, &mut input_transactions).await {
                            mm_counter!(ctx.metrics, "tx.history.response.count", 1, "coin" => coin.as_ref().conf.ticker.clone(), "method" => "tx_detail_by_hash");
                            // replace with new tx details in case we need to update any data
                            e.insert(tx_details);
                            updated = true;
                        }
                    }
                    if e.get().should_update_kmd_rewards() && e.get().block_height > 0 {
                        mm_counter!(ctx.metrics, "tx.history.update.kmd_rewards", 1);
                        match coin.update_kmd_rewards(e.get_mut(), &mut input_transactions).await {
                            Ok(()) => updated = true,
                            Err(e) => ctx.log.log(
                                "😟",
                                &[&"tx_history", &coin.as_ref().conf.ticker],
                                &ERRL!(
                                    "Error {:?} on updating the KMD rewards of {:?}, skipping the tx",
                                    e,
                                    txid
                                ),
                            ),
                        }
                    }
                },
            }
            if updated {
                let mut to_write: Vec<TransactionDetails> =
                    history_map.iter().map(|(_, value)| value.clone()).collect();
                // the transactions with block_height == 0 are the most recent so we need to separately handle them while sorting
                to_write.sort_unstable_by(|a, b| {
                    if a.block_height == 0 {
                        Ordering::Less
                    } else if b.block_height == 0 {
                        Ordering::Greater
                    } else {
                        b.block_height.cmp(&a.block_height)
                    }
                });
                if let Err(e) = coin.save_history_to_file(&ctx, to_write).compat().await {
                    ctx.log.log(
                        "",
                        &[&"tx_history", &coin.as_ref().conf.ticker],
                        &ERRL!("Error {} on 'save_history_to_file', stop the history loop", e),
                    );
                    return;
                };
            }
        }
        *coin.as_ref().history_sync_state.lock().unwrap() = HistorySyncState::Finished;

        if success_iteration == 0 {
            ctx.log.log(
                "😅",
                &[&"tx_history", &("coin", coin.as_ref().conf.ticker.clone().as_str())],
                "history has been loaded successfully",
            );
        }

        my_balance = actual_balance;
        success_iteration += 1;
        Timer::sleep(30.).await;
    }
}

pub async fn request_tx_history<T>(coin: &T, metrics: MetricsArc) -> RequestTxHistoryResult
where
    T: AsRef<UtxoCoinFields> + MmCoin + MarketCoinOps,
{
    let my_address = match coin.my_address() {
        Ok(addr) => addr,
        Err(e) => {
            return RequestTxHistoryResult::UnknownError(ERRL!("Error on getting self address: {}. Stop tx history", e))
        },
    };

    let tx_ids = match &coin.as_ref().rpc_client {
        UtxoRpcClientEnum::Native(client) => {
            let mut from = 0;
            let mut all_transactions = vec![];
            loop {
                mm_counter!(metrics, "tx.history.request.count", 1,
                    "coin" => coin.as_ref().conf.ticker.clone(), "client" => "native", "method" => "listtransactions");

                let transactions = match client.list_transactions(100, from).compat().await {
                    Ok(value) => value,
                    Err(e) => {
                        return RequestTxHistoryResult::Retry {
                            error: ERRL!("Error {} on list transactions", e),
                        };
                    },
                };

                mm_counter!(metrics, "tx.history.response.count", 1,
                    "coin" => coin.as_ref().conf.ticker.clone(), "client" => "native", "method" => "listtransactions");

                if transactions.is_empty() {
                    break;
                }
                from += 100;
                all_transactions.extend(transactions);
            }

            mm_counter!(metrics, "tx.history.response.total_length", all_transactions.len() as u64,
                "coin" => coin.as_ref().conf.ticker.clone(), "client" => "native", "method" => "listtransactions");

            all_transactions
                .into_iter()
                .filter_map(|item| {
                    if item.address == my_address {
                        Some((item.txid, item.blockindex))
                    } else {
                        None
                    }
                })
                .collect()
        },
        UtxoRpcClientEnum::Electrum(client) => {
            let script = Builder::build_p2pkh(&coin.as_ref().my_address.hash);
            let script_hash = electrum_script_hash(&script);

            mm_counter!(metrics, "tx.history.request.count", 1,
                "coin" => coin.as_ref().conf.ticker.clone(), "client" => "electrum", "method" => "blockchain.scripthash.get_history");

            let electrum_history = match client.scripthash_get_history(&hex::encode(script_hash)).compat().await {
                Ok(value) => value,
                Err(e) => match &e.error {
                    JsonRpcErrorType::Transport(e) | JsonRpcErrorType::Parse(_, e) => {
                        return RequestTxHistoryResult::Retry {
                            error: ERRL!("Error {} on scripthash_get_history", e),
                        };
                    },
                    JsonRpcErrorType::Response(_addr, err) => {
                        if HISTORY_TOO_LARGE_ERROR.eq(err) {
                            return RequestTxHistoryResult::HistoryTooLarge;
                        } else {
                            return RequestTxHistoryResult::Retry {
                                error: ERRL!("Error {:?} on scripthash_get_history", e),
                            };
                        }
                    },
                },
            };
            mm_counter!(metrics, "tx.history.response.count", 1,
                "coin" => coin.as_ref().conf.ticker.clone(), "client" => "electrum", "method" => "blockchain.scripthash.get_history");

            mm_counter!(metrics, "tx.history.response.total_length", electrum_history.len() as u64,
                "coin" => coin.as_ref().conf.ticker.clone(), "client" => "electrum", "method" => "blockchain.scripthash.get_history");

            // electrum returns the most recent transactions in the end but we need to
            // process them first so rev is required
            electrum_history
                .into_iter()
                .rev()
                .map(|item| {
                    let height = if item.height < 0 { 0 } else { item.height as u64 };
                    (item.tx_hash, height)
                })
                .collect()
        },
    };
    RequestTxHistoryResult::Ok(tx_ids)
}

pub async fn tx_details_by_hash<T>(
    coin: &T,
    hash: &[u8],
    input_transactions: &mut HistoryUtxoTxMap,
) -> Result<TransactionDetails, String>
where
    T: AsRef<UtxoCoinFields> + UtxoCommonOps + Send + Sync + 'static,
{
    let ticker = &coin.as_ref().conf.ticker;
    let hash = H256Json::from(hash);
    let verbose_tx = try_s!(
        coin.as_ref()
            .rpc_client
            .get_verbose_transaction(hash.clone())
            .compat()
            .await
    );
    let mut tx: UtxoTx = try_s!(deserialize(verbose_tx.hex.as_slice()).map_err(|e| ERRL!("{:?}", e)));
    tx.tx_hash_algo = coin.as_ref().tx_hash_algo;

    input_transactions.insert(hash, HistoryUtxoTx {
        tx: tx.clone(),
        height: verbose_tx.height,
    });

    let mut input_amount = 0;
    let mut output_amount = 0;
    let mut from_addresses = Vec::new();
    let mut to_addresses = Vec::new();
    let mut spent_by_me = 0;
    let mut received_by_me = 0;

    for input in tx.inputs.iter() {
        // input transaction is zero if the tx is the coinbase transaction
        if input.previous_output.hash.is_zero() {
            continue;
        }

        let prev_tx_hash: H256Json = input.previous_output.hash.reversed().into();
        let prev_tx = try_s!(
            coin.get_mut_verbose_transaction_from_map_or_rpc(prev_tx_hash.clone(), input_transactions)
                .await
        );
        let prev_tx = &mut prev_tx.tx;
        prev_tx.tx_hash_algo = coin.as_ref().tx_hash_algo;

        let prev_tx_value = prev_tx.outputs[input.previous_output.index as usize].value;
        input_amount += prev_tx_value;
        let from: Vec<Address> = try_s!(coin.addresses_from_script(
            &prev_tx.outputs[input.previous_output.index as usize]
                .script_pubkey
                .clone()
                .into()
        ));
        if from.contains(&coin.as_ref().my_address) {
            spent_by_me += prev_tx_value;
        }
        from_addresses.extend(from.into_iter());
    }

    for output in tx.outputs.iter() {
        output_amount += output.value;
        let to = try_s!(coin.addresses_from_script(&output.script_pubkey.clone().into()));
        if to.contains(&coin.as_ref().my_address) {
            received_by_me += output.value;
        }
        to_addresses.extend(to.into_iter());
    }

    let (fee, kmd_rewards) = if ticker == "KMD" {
        let kmd_rewards = try_s!(coin.calc_interest_of_tx(&tx, input_transactions).await);
        // `input_amount = output_amount + fee`, where `output_amount = actual_output_amount + kmd_rewards`,
        // so to calculate an actual transaction fee, we have to subtract the `kmd_rewards` from the total `output_amount`:
        // `fee = input_amount - actual_output_amount` or simplified `fee = input_amount - output_amount + kmd_rewards`
        let fee = input_amount as i64 - output_amount as i64 + kmd_rewards as i64;

        let my_address = &coin.as_ref().my_address;
        let claimed_by_me = from_addresses.iter().all(|from| from == my_address) && to_addresses.contains(my_address);
        let kmd_rewards_details = KmdRewardsDetails {
            amount: big_decimal_from_sat_unsigned(kmd_rewards, coin.as_ref().decimals),
            claimed_by_me,
        };
        (
            big_decimal_from_sat(fee, coin.as_ref().decimals),
            Some(kmd_rewards_details),
        )
    } else if input_amount == 0 {
        let fee = verbose_tx.vin.iter().fold(0., |cur, input| {
            let fee = match input {
                TransactionInputEnum::Lelantus(lelantus) => lelantus.n_fees,
                _ => 0.,
            };
            cur + fee
        });
        (fee.into(), None)
    } else {
        let fee = input_amount as i64 - output_amount as i64;
        (big_decimal_from_sat(fee, coin.as_ref().decimals), None)
    };

    // remove address duplicates in case several inputs were spent from same address
    // or several outputs are sent to same address
    let mut from_addresses: Vec<String> =
        try_s!(from_addresses.into_iter().map(|addr| addr.display_address()).collect());
    from_addresses.sort();
    from_addresses.dedup();
    let mut to_addresses: Vec<String> = try_s!(to_addresses.into_iter().map(|addr| addr.display_address()).collect());
    to_addresses.sort();
    to_addresses.dedup();

    Ok(TransactionDetails {
        from: from_addresses,
        to: to_addresses,
        received_by_me: big_decimal_from_sat_unsigned(received_by_me, coin.as_ref().decimals),
        spent_by_me: big_decimal_from_sat_unsigned(spent_by_me, coin.as_ref().decimals),
        my_balance_change: big_decimal_from_sat(received_by_me as i64 - spent_by_me as i64, coin.as_ref().decimals),
        total_amount: big_decimal_from_sat_unsigned(input_amount, coin.as_ref().decimals),
        tx_hash: tx.hash().reversed().to_vec().into(),
        tx_hex: verbose_tx.hex,
        fee_details: Some(UtxoFeeDetails { amount: fee }.into()),
        block_height: verbose_tx.height.unwrap_or(0),
        coin: ticker.clone(),
        internal_id: tx.hash().reversed().to_vec().into(),
        timestamp: verbose_tx.time.into(),
        kmd_rewards,
    })
}

pub async fn get_mut_verbose_transaction_from_map_or_rpc<'a, 'b, T>(
    coin: &'a T,
    tx_hash: H256Json,
    utxo_tx_map: &'b mut HistoryUtxoTxMap,
) -> UtxoRpcResult<&'b mut HistoryUtxoTx>
where
    T: AsRef<UtxoCoinFields>,
{
    let tx = match utxo_tx_map.entry(tx_hash.clone()) {
        Entry::Vacant(e) => {
            let verbose = coin
                .as_ref()
                .rpc_client
                .get_verbose_transaction(tx_hash.clone())
                .compat()
                .await?;
            let tx = HistoryUtxoTx {
                tx: deserialize(verbose.hex.as_slice())
                    .map_to_mm(|e| UtxoRpcError::InvalidResponse(format!("{:?}, tx: {:?}", e, tx_hash)))?,
                height: verbose.height,
            };
            e.insert(tx)
        },
        Entry::Occupied(e) => e.into_mut(),
    };
    Ok(tx)
}

/// This function is used when the transaction details were calculated without considering the KMD rewards.
/// We know that [`TransactionDetails::fee`] was calculated by `fee = input_amount - output_amount`,
/// where `output_amount = actual_output_amount + kmd_rewards` or `actual_output_amount = output_amount - kmd_rewards`.
/// To calculate an actual fee amount, we have to replace `output_amount` with `actual_output_amount`:
/// `actual_fee = input_amount - actual_output_amount` or `actual_fee = input_amount - output_amount + kmd_rewards`.
/// Substitute [`TransactionDetails::fee`] to the last equation:
/// `actual_fee = TransactionDetails::fee + kmd_rewards`
pub async fn update_kmd_rewards<T>(
    coin: &T,
    tx_details: &mut TransactionDetails,
    input_transactions: &mut HistoryUtxoTxMap,
) -> UtxoRpcResult<()>
where
    T: AsRef<UtxoCoinFields> + UtxoCommonOps + UtxoStandardOps + MarketCoinOps + Send + Sync + 'static,
{
    if !tx_details.should_update_kmd_rewards() {
        let error = "There is no need to update KMD rewards".to_owned();
        return MmError::err(UtxoRpcError::Internal(error));
    }
    let tx: UtxoTx = deserialize(tx_details.tx_hex.as_slice()).map_to_mm(|e| {
        UtxoRpcError::Internal(format!(
            "Error deserializing the {:?} transaction hex: {:?}",
            tx_details.tx_hash, e
        ))
    })?;
    let kmd_rewards = coin.calc_interest_of_tx(&tx, input_transactions).await?;
    let kmd_rewards = big_decimal_from_sat_unsigned(kmd_rewards, coin.as_ref().decimals);

    if let Some(TxFeeDetails::Utxo(UtxoFeeDetails { ref amount })) = tx_details.fee_details {
        let actual_fee_amount = amount + &kmd_rewards;
        tx_details.fee_details = Some(TxFeeDetails::Utxo(UtxoFeeDetails {
            amount: actual_fee_amount,
        }));
    }

    let my_address = &coin.my_address().map_to_mm(UtxoRpcError::Internal)?;
    let claimed_by_me = tx_details.from.iter().all(|from| from == my_address) && tx_details.to.contains(my_address);

    tx_details.kmd_rewards = Some(KmdRewardsDetails {
        amount: kmd_rewards,
        claimed_by_me,
    });
    Ok(())
}

pub async fn calc_interest_of_tx<T>(
    coin: &T,
    tx: &UtxoTx,
    input_transactions: &mut HistoryUtxoTxMap,
) -> UtxoRpcResult<u64>
where
    T: AsRef<UtxoCoinFields> + UtxoCommonOps + Send + Sync + 'static,
{
    if coin.as_ref().conf.ticker != "KMD" {
        let error = format!("Expected KMD ticker, found {}", coin.as_ref().conf.ticker);
        return MmError::err(UtxoRpcError::Internal(error));
    }

    let mut kmd_rewards = 0;
    for input in tx.inputs.iter() {
        // input transaction is zero if the tx is the coinbase transaction
        if input.previous_output.hash.is_zero() {
            continue;
        }

        let prev_tx_hash: H256Json = input.previous_output.hash.reversed().into();
        let prev_tx = coin
            .get_mut_verbose_transaction_from_map_or_rpc(prev_tx_hash.clone(), input_transactions)
            .await?;

        let prev_tx_value = prev_tx.tx.outputs[input.previous_output.index as usize].value;
        let prev_tx_locktime = prev_tx.tx.lock_time as u64;
        let this_tx_locktime = tx.lock_time as u64;
        if let Ok(interest) = kmd_interest(prev_tx.height, prev_tx_value, prev_tx_locktime, this_tx_locktime) {
            kmd_rewards += interest;
        }
    }
    Ok(kmd_rewards)
}

pub fn history_sync_status(coin: &UtxoCoinFields) -> HistorySyncState {
    coin.history_sync_state.lock().unwrap().clone()
}

pub fn get_trade_fee<T>(coin: T) -> Box<dyn Future<Item = TradeFee, Error = String> + Send>
where
    T: AsRef<UtxoCoinFields> + UtxoCommonOps + Send + Sync + 'static,
{
    let ticker = coin.as_ref().conf.ticker.clone();
    let decimals = coin.as_ref().decimals;
    let fut = async move {
        let fee = try_s!(coin.get_tx_fee().await);
        let amount = match fee {
            ActualTxFee::Fixed(f) => f,
            ActualTxFee::Dynamic(f) => f,
            ActualTxFee::FixedPerKb(f) => f,
        };
        Ok(TradeFee {
            coin: ticker,
            amount: big_decimal_from_sat(amount as i64, decimals).into(),
            paid_from_trading_vol: false,
        })
    };
    Box::new(fut.boxed().compat())
}

/// To ensure the `get_sender_trade_fee(x) <= get_sender_trade_fee(y)` condition is satisfied for any `x < y`,
/// we should include a `change` output into the result fee. Imagine this case:
/// Let `sum_inputs = 11000` and `total_tx_fee: { 200, if there is no the change output; 230, if there is the change output }`.
///
/// If `value = TradePreimageValue::Exact(10000)`, therefore `sum_outputs = 10000`.
/// then `change = sum_inputs - sum_outputs - total_tx_fee = 800`, so `change < dust` and `total_tx_fee = 200` (including the change output).
///
/// But if `value = TradePreimageValue::Exact(9000)`, therefore `sum_outputs = 9000`. Let `sum_inputs = 11000`, `total_tx_fee = 230`
/// where `change = sum_inputs - sum_outputs - total_tx_fee = 1770`, so `change > dust` and `total_tx_fee = 230` (including the change output).
///
/// To sum up, `get_sender_trade_fee(TradePreimageValue::Exact(9000)) > get_sender_trade_fee(TradePreimageValue::Exact(10000))`.
/// So we should always return a fee as if a transaction includes the change output.
pub async fn preimage_trade_fee_required_to_send_outputs<T>(
    coin: &T,
    outputs: Vec<TransactionOutput>,
    fee_policy: FeePolicy,
    gas_fee: Option<u64>,
    stage: &FeeApproxStage,
) -> TradePreimageResult<BigDecimal>
where
    T: AsRef<UtxoCoinFields> + UtxoCommonOps,
{
    let ticker = coin.as_ref().conf.ticker.clone();
    let decimals = coin.as_ref().decimals;
    let tx_fee = coin.get_tx_fee().await?;
    // [`FeePolicy::DeductFromOutput`] is used if the value is [`TradePreimageValue::UpperBound`] only
    let is_amount_upper_bound = matches!(fee_policy, FeePolicy::DeductFromOutput(_));

    match tx_fee {
        ActualTxFee::Fixed(fee_amount) => {
            let amount = big_decimal_from_sat(fee_amount as i64, decimals);
            return Ok(amount);
        },
        // if it's a dynamic fee, we should generate a swap transaction to get an actual trade fee
        ActualTxFee::Dynamic(fee) => {
            // take into account that the dynamic tx fee may increase during the swap
            let dynamic_fee = coin.increase_dynamic_fee_by_stage(fee, stage);

            let outputs_count = outputs.len();
            let (unspents, _recently_sent_txs) = coin.list_unspent_ordered(&coin.as_ref().my_address).await?;

            let actual_tx_fee = Some(ActualTxFee::Dynamic(dynamic_fee));
            let (tx, data) = generate_transaction(coin, unspents, outputs, fee_policy, actual_tx_fee, gas_fee)
                .await
                .mm_err(|e| TradePreimageError::from_generate_tx_error(e, ticker, decimals, is_amount_upper_bound))?;

            let total_fee = if tx.outputs.len() == outputs_count {
                // take into account the change output
                data.fee_amount + (dynamic_fee * P2PKH_OUTPUT_LEN) / KILO_BYTE
            } else {
                // the change output is included already
                data.fee_amount
            };

            Ok(big_decimal_from_sat(total_fee as i64, decimals))
        },
        ActualTxFee::FixedPerKb(fee) => {
            let outputs_count = outputs.len();
            let (unspents, _recently_sent_txs) = coin.list_unspent_ordered(&coin.as_ref().my_address).await?;

            let (tx, data) = generate_transaction(coin, unspents, outputs, fee_policy, Some(tx_fee), gas_fee)
                .await
                .mm_err(|e| TradePreimageError::from_generate_tx_error(e, ticker, decimals, is_amount_upper_bound))?;

            let total_fee = if tx.outputs.len() == outputs_count {
                // take into account the change output if tx_size_kb(tx with change) > tx_size_kb(tx without change)
                let tx = UtxoTx::from(tx);
                let tx_bytes = serialize(&tx);
                if tx_bytes.len() as u64 % KILO_BYTE + P2PKH_OUTPUT_LEN > KILO_BYTE {
                    data.fee_amount + fee
                } else {
                    data.fee_amount
                }
            } else {
                // the change output is included already
                data.fee_amount
            };

            Ok(big_decimal_from_sat(total_fee as i64, decimals))
        },
    }
}

/// Maker or Taker should pay fee only for sending his payment.
/// Even if refund will be required the fee will be deducted from P2SH input.
/// Please note the `get_sender_trade_fee` satisfies the following condition:
/// `get_sender_trade_fee(x) <= get_sender_trade_fee(y)` for any `x < y`.
pub fn get_sender_trade_fee<T>(coin: T, value: TradePreimageValue, stage: FeeApproxStage) -> TradePreimageFut<TradeFee>
where
    T: AsRef<UtxoCoinFields> + MarketCoinOps + UtxoCommonOps + Send + Sync + 'static,
{
    let fut = async move {
        let (amount, fee_policy) = match value {
            TradePreimageValue::UpperBound(upper_bound) => (upper_bound, FeePolicy::DeductFromOutput(0)),
            TradePreimageValue::Exact(amount) => (amount, FeePolicy::SendExact),
        };

        // pass the dummy params
        let time_lock = (now_ms() / 1000) as u32;
        let other_pub = &[0; 33]; // H264 is 33 bytes
        let secret_hash = &[0; 20]; // H160 is 20 bytes

        // `generate_swap_payment_outputs` may fail due to either invalid `other_pub` or a number conversation error
        let SwapPaymentOutputsResult { outputs, .. } =
            generate_swap_payment_outputs(&coin, time_lock, other_pub, secret_hash, amount)
                .map_to_mm(TradePreimageError::InternalError)?;
        let gas_fee = None;
        let fee_amount = coin
            .preimage_trade_fee_required_to_send_outputs(outputs, fee_policy, gas_fee, &stage)
            .await?;
        Ok(TradeFee {
            coin: coin.as_ref().conf.ticker.clone(),
            amount: fee_amount.into(),
            paid_from_trading_vol: false,
        })
    };
    Box::new(fut.boxed().compat())
}

/// The fee to spend (receive) other payment is deducted from the trading amount so we should display it
pub fn get_receiver_trade_fee<T>(coin: T) -> TradePreimageFut<TradeFee>
where
    T: AsRef<UtxoCoinFields> + UtxoCommonOps + Send + Sync + 'static,
{
    let fut = async move {
        let amount_sat = get_htlc_spend_fee(&coin).await?;
        let amount = big_decimal_from_sat_unsigned(amount_sat, coin.as_ref().decimals).into();
        Ok(TradeFee {
            coin: coin.as_ref().conf.ticker.clone(),
            amount,
            paid_from_trading_vol: true,
        })
    };
    Box::new(fut.boxed().compat())
}

pub fn get_fee_to_send_taker_fee<T>(
    coin: T,
    dex_fee_amount: BigDecimal,
    stage: FeeApproxStage,
) -> TradePreimageFut<TradeFee>
where
    T: AsRef<UtxoCoinFields> + MarketCoinOps + UtxoCommonOps + Send + Sync + 'static,
{
    let decimals = coin.as_ref().decimals;
    let fut = async move {
        let value = sat_from_big_decimal(&dex_fee_amount, decimals)?;
        let output = TransactionOutput {
            value,
            script_pubkey: Builder::build_p2pkh(&AddressHash::default()).to_bytes(),
        };
        let gas_fee = None;
        let fee_amount = coin
            .preimage_trade_fee_required_to_send_outputs(vec![output], FeePolicy::SendExact, gas_fee, &stage)
            .await?;
        Ok(TradeFee {
            coin: coin.ticker().to_owned(),
            amount: fee_amount.into(),
            paid_from_trading_vol: false,
        })
    };
    Box::new(fut.boxed().compat())
}

pub fn required_confirmations(coin: &UtxoCoinFields) -> u64 {
    coin.conf.required_confirmations.load(AtomicOrderding::Relaxed)
}

pub fn requires_notarization(coin: &UtxoCoinFields) -> bool {
    coin.conf.requires_notarization.load(AtomicOrderding::Relaxed)
}

pub fn set_required_confirmations(coin: &UtxoCoinFields, confirmations: u64) {
    coin.conf
        .required_confirmations
        .store(confirmations, AtomicOrderding::Relaxed);
}

pub fn set_requires_notarization(coin: &UtxoCoinFields, requires_nota: bool) {
    coin.conf
        .requires_notarization
        .store(requires_nota, AtomicOrderding::Relaxed);
}

#[allow(clippy::needless_lifetimes)]
pub async fn ordered_mature_unspents<'a, T>(
    coin: &'a T,
    address: &Address,
) -> UtxoRpcResult<(Vec<UnspentInfo>, AsyncMutexGuard<'a, RecentlySpentOutPoints>)>
where
    T: AsRef<UtxoCoinFields> + UtxoCommonOps,
{
    fn calc_actual_cached_tx_confirmations(tx: &RpcTransaction, block_count: u64) -> UtxoRpcResult<u32> {
        let tx_height = tx.height.or_mm_err(|| {
            UtxoRpcError::Internal(format!(r#"Warning, height of cached "{:?}" tx is unknown"#, tx.txid))
        })?;
        // utxo_common::cache_transaction_if_possible() shouldn't cache transaction with height == 0
        if tx_height == 0 {
            let error = format!(
                r#"Warning, height of cached "{:?}" tx is expected to be non-zero"#,
                tx.txid
            );
            return MmError::err(UtxoRpcError::Internal(error));
        }
        if block_count < tx_height {
            let error = format!(
                r#"Warning, actual block_count {} less than cached tx_height {} of {:?}"#,
                block_count, tx_height, tx.txid
            );
            return MmError::err(UtxoRpcError::Internal(error));
        }

        let confirmations = block_count - tx_height + 1;
        Ok(confirmations as u32)
    }

    let (unspents, recently_spent) = list_unspent_ordered(coin, address).await?;
    let block_count = coin.as_ref().rpc_client.get_block_count().compat().await?;

    let mut result = Vec::with_capacity(unspents.len());
    for unspent in unspents {
        let tx_hash: H256Json = unspent.outpoint.hash.reversed().into();
        let tx_info = match coin
            .get_verbose_transaction_from_cache_or_rpc(tx_hash.clone())
            .compat()
            .await
        {
            Ok(x) => x,
            Err(err) => {
                log!("Error " [err] " getting the transaction " [tx_hash] ", skip the unspent output");
                continue;
            },
        };

        let tx_info = match tx_info {
            VerboseTransactionFrom::Cache(mut tx) => {
                if unspent.height.is_some() {
                    tx.height = unspent.height;
                }
                match calc_actual_cached_tx_confirmations(&tx, block_count) {
                    Ok(conf) => tx.confirmations = conf,
                    // do not skip the transaction with unknown confirmations,
                    // because the transaction can be matured
                    Err(e) => log!((e)),
                }
                tx
            },
            VerboseTransactionFrom::Rpc(mut tx) => {
                if tx.height.is_none() {
                    tx.height = unspent.height;
                }
                if let Err(e) = coin.cache_transaction_if_possible(&tx).await {
                    log!((e));
                }
                tx
            },
        };

        if coin.is_unspent_mature(&tx_info) {
            result.push(unspent);
        }
    }

    Ok((result, recently_spent))
}

pub fn is_unspent_mature(mature_confirmations: u32, output: &RpcTransaction) -> bool {
    // don't skip outputs with confirmations == 0, because we can spend them
    !output.is_coinbase() || output.confirmations >= mature_confirmations
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn get_verbose_transaction_from_cache_or_rpc(
    coin: &UtxoCoinFields,
    txid: H256Json,
) -> Result<VerboseTransactionFrom, String> {
    let tx_cache_path = match &coin.tx_cache_directory {
        Some(p) => p.clone(),
        _ => {
            // the coin doesn't support TX local cache, don't try to load from cache and don't cache it
            let tx = try_s!(coin.rpc_client.get_verbose_transaction(txid.clone()).compat().await);
            return Ok(VerboseTransactionFrom::Rpc(tx));
        },
    };

    match tx_cache::load_transaction_from_cache(&tx_cache_path, &txid).await {
        Ok(Some(tx)) => return Ok(VerboseTransactionFrom::Cache(tx)),
        Err(err) => log!("Error " [err] " loading the " [txid] " transaction. Try request tx using Rpc client"),
        // txid just not found
        _ => (),
    }

    let tx = try_s!(coin.rpc_client.get_verbose_transaction(txid).compat().await);
    Ok(VerboseTransactionFrom::Rpc(tx))
}

#[cfg(target_arch = "wasm32")]
pub async fn get_verbose_transaction_from_cache_or_rpc(
    coin: &UtxoCoinFields,
    txid: H256Json,
) -> Result<VerboseTransactionFrom, String> {
    let tx = try_s!(coin.rpc_client.get_verbose_transaction(txid.clone()).compat().await);
    Ok(VerboseTransactionFrom::Rpc(tx))
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn cache_transaction_if_possible(coin: &UtxoCoinFields, tx: &RpcTransaction) -> Result<(), String> {
    let tx_cache_path = match &coin.tx_cache_directory {
        Some(p) => p.clone(),
        _ => {
            return Ok(());
        },
    };
    // check if the transaction height is set and not zero
    match tx.height {
        Some(0) => return Ok(()),
        Some(_) => (),
        None => return Ok(()),
    }

    tx_cache::cache_transaction(&tx_cache_path, &tx)
        .await
        .map_err(|e| ERRL!("Error {:?} on caching transaction {:?}", e, tx.txid))
}

#[cfg(target_arch = "wasm32")]
pub async fn cache_transaction_if_possible(_coin: &UtxoCoinFields, _tx: &RpcTransaction) -> Result<(), String> {
    Ok(())
}

pub async fn my_unspendable_balance<T>(coin: &T, total_balance: &BigDecimal) -> BalanceResult<BigDecimal>
where
    T: AsRef<UtxoCoinFields> + UtxoCommonOps + MarketCoinOps + ?Sized,
{
    let mut attempts = 0i32;
    loop {
        let (mature_unspents, _) = coin.ordered_mature_unspents(&coin.as_ref().my_address).await?;
        let spendable_balance = mature_unspents.iter().fold(BigDecimal::zero(), |acc, x| {
            acc + big_decimal_from_sat(x.value as i64, coin.as_ref().decimals)
        });
        if total_balance >= &spendable_balance {
            return Ok(total_balance - spendable_balance);
        }

        if attempts == 2 {
            let error = format!(
                "Spendable balance {} greater than total balance {}",
                spendable_balance, total_balance
            );
            return MmError::err(BalanceError::Internal(error));
        }

        warn!(
            "Attempt N{}: spendable balance {} greater than total balance {}",
            attempts, spendable_balance, total_balance
        );

        // the balance could be changed by other instance between my_balance() and ordered_mature_unspents() calls
        // try again
        attempts += 1;
        Timer::sleep(0.3).await;
    }
}

/// Swap contract address is not used by standard UTXO coins.
pub fn swap_contract_address() -> Option<BytesJson> { None }

/// Convert satoshis to BigDecimal amount of coin units
pub fn big_decimal_from_sat(satoshis: i64, decimals: u8) -> BigDecimal {
    BigDecimal::from(satoshis) / BigDecimal::from(10u64.pow(decimals as u32))
}

pub fn big_decimal_from_sat_unsigned(satoshis: u64, decimals: u8) -> BigDecimal {
    BigDecimal::from(satoshis) / BigDecimal::from(10u64.pow(decimals as u32))
}

pub fn address_from_raw_pubkey(
    pub_key: &[u8],
    prefix: u8,
    t_addr_prefix: u8,
    checksum_type: ChecksumType,
    hrp: Option<String>,
    addr_format: UtxoAddressFormat,
) -> Result<Address, String> {
    Ok(Address {
        t_addr_prefix,
        prefix,
        hash: try_s!(Public::from_slice(pub_key)).address_hash(),
        checksum_type,
        hrp,
        addr_format,
    })
}

#[allow(clippy::too_many_arguments)]
pub fn validate_payment<T>(
    coin: T,
    tx: UtxoTx,
    output_index: usize,
    first_pub0: &Public,
    second_pub0: &Public,
    priv_bn_hash: &[u8],
    amount: BigDecimal,
    time_lock: u32,
) -> Box<dyn Future<Item = (), Error = String> + Send>
where
    T: AsRef<UtxoCoinFields> + Send + Sync + 'static,
{
    let amount = try_fus!(sat_from_big_decimal(&amount, coin.as_ref().decimals));

    let expected_redeem = payment_script(time_lock, priv_bn_hash, first_pub0, second_pub0);
    let fut = async move {
        let mut attempts = 0;
        loop {
            let tx_from_rpc = match coin
                .as_ref()
                .rpc_client
                .get_transaction_bytes(tx.hash().reversed().into())
                .compat()
                .await
            {
                Ok(t) => t,
                Err(e) => {
                    if attempts > 2 {
                        return ERR!(
                            "Got error {:?} after 3 attempts of getting tx {:?} from RPC",
                            e,
                            tx.tx_hash()
                        );
                    };
                    attempts += 1;
                    log!("Error " [e] " getting the tx " [tx.tx_hash()] " from rpc");
                    Timer::sleep(10.).await;
                    continue;
                },
            };
            if serialize(&tx).take() != tx_from_rpc.0
                && serialize_with_flags(&tx, SERIALIZE_TRANSACTION_WITNESS).take() != tx_from_rpc.0
            {
                return ERR!(
                    "Provided payment tx {:?} doesn't match tx data from rpc {:?}",
                    tx,
                    tx_from_rpc
                );
            }

            let expected_output = TransactionOutput {
                value: amount,
                script_pubkey: Builder::build_p2sh(&dhash160(&expected_redeem)).into(),
            };

            let actual_output = tx.outputs.get(output_index);
            if actual_output != Some(&expected_output) {
                return ERR!(
                    "Provided payment tx output doesn't match expected {:?} {:?}",
                    actual_output,
                    expected_output
                );
            }
            return Ok(());
        }
    };
    Box::new(fut.boxed().compat())
}

#[allow(clippy::too_many_arguments)]
async fn search_for_swap_output_spend(
    coin: &UtxoCoinFields,
    time_lock: u32,
    first_pub: &Public,
    second_pub: &Public,
    secret_hash: &[u8],
    tx: &[u8],
    output_index: usize,
    search_from_block: u64,
) -> Result<Option<FoundSwapTxSpend>, String> {
    let mut tx: UtxoTx = try_s!(deserialize(tx).map_err(|e| ERRL!("{:?}", e)));
    tx.tx_hash_algo = coin.tx_hash_algo;
    let script = payment_script(time_lock, secret_hash, first_pub, second_pub);
    let expected_script_pubkey = Builder::build_p2sh(&dhash160(&script)).to_bytes();
    if tx.outputs[0].script_pubkey != expected_script_pubkey {
        return ERR!(
            "Transaction {:?} output 0 script_pubkey doesn't match expected {:?}",
            tx,
            expected_script_pubkey
        );
    }

    let spend = try_s!(
        coin.rpc_client
            .find_output_spend(&tx, output_index, search_from_block)
            .compat()
            .await
    );
    match spend {
        Some(mut tx) => {
            tx.tx_hash_algo = coin.tx_hash_algo;
            let script: Script = tx.inputs[0].script_sig.clone().into();
            if let Some(Ok(ref i)) = script.iter().nth(2) {
                if i.opcode == Opcode::OP_0 {
                    return Ok(Some(FoundSwapTxSpend::Spent(tx.into())));
                }
            }

            if let Some(Ok(ref i)) = script.iter().nth(1) {
                if i.opcode == Opcode::OP_1 {
                    return Ok(Some(FoundSwapTxSpend::Refunded(tx.into())));
                }
            }

            ERR!(
                "Couldn't find required instruction in script_sig of input 0 of tx {:?}",
                tx
            )
        },
        None => Ok(None),
    }
}

struct SwapPaymentOutputsResult {
    payment_address: Address,
    outputs: Vec<TransactionOutput>,
}

fn generate_swap_payment_outputs<T>(
    coin: T,
    time_lock: u32,
    other_pub: &[u8],
    secret_hash: &[u8],
    amount: BigDecimal,
) -> Result<SwapPaymentOutputsResult, String>
where
    T: AsRef<UtxoCoinFields>,
{
    let redeem_script = payment_script(
        time_lock,
        secret_hash,
        coin.as_ref().key_pair.public(),
        &try_s!(Public::from_slice(other_pub)),
    );
    let redeem_script_hash = dhash160(&redeem_script);
    let amount = try_s!(sat_from_big_decimal(&amount, coin.as_ref().decimals));
    let htlc_out = TransactionOutput {
        value: amount,
        script_pubkey: Builder::build_p2sh(&redeem_script_hash).into(),
    };
    // record secret hash to blockchain too making it impossible to lose
    // lock time may be easily brute forced so it is not mandatory to record it
    let mut op_return_builder = Builder::default().push_opcode(Opcode::OP_RETURN);

    // add the full redeem script to the OP_RETURN for ARRR to simplify the validation for the daemon
    op_return_builder = if coin.as_ref().conf.ticker == "ARRR" {
        op_return_builder.push_data(&redeem_script)
    } else {
        op_return_builder.push_bytes(secret_hash)
    };

    let op_return_script = op_return_builder.into_bytes();

    let op_return_out = TransactionOutput {
        value: 0,
        script_pubkey: op_return_script,
    };

    let payment_address = Address {
        checksum_type: coin.as_ref().conf.checksum_type,
        hash: redeem_script_hash,
        prefix: coin.as_ref().conf.p2sh_addr_prefix,
        t_addr_prefix: coin.as_ref().conf.p2sh_t_addr_prefix,
        hrp: coin.as_ref().conf.bech32_hrp.clone(),
        addr_format: coin.as_ref().my_address.addr_format.clone(),
    };
    let result = SwapPaymentOutputsResult {
        payment_address,
        outputs: vec![htlc_out, op_return_out],
    };
    Ok(result)
}

pub fn payment_script(time_lock: u32, secret_hash: &[u8], pub_0: &Public, pub_1: &Public) -> Script {
    let builder = Builder::default();
    builder
        .push_opcode(Opcode::OP_IF)
        .push_bytes(&time_lock.to_le_bytes())
        .push_opcode(Opcode::OP_CHECKLOCKTIMEVERIFY)
        .push_opcode(Opcode::OP_DROP)
        .push_bytes(pub_0)
        .push_opcode(Opcode::OP_CHECKSIG)
        .push_opcode(Opcode::OP_ELSE)
        .push_opcode(Opcode::OP_SIZE)
        .push_bytes(&[32])
        .push_opcode(Opcode::OP_EQUALVERIFY)
        .push_opcode(Opcode::OP_HASH160)
        .push_bytes(secret_hash)
        .push_opcode(Opcode::OP_EQUALVERIFY)
        .push_bytes(pub_1)
        .push_opcode(Opcode::OP_CHECKSIG)
        .push_opcode(Opcode::OP_ENDIF)
        .into_script()
}

pub fn dex_fee_script(uuid: [u8; 16], time_lock: u32, watcher_pub: &Public, sender_pub: &Public) -> Script {
    let builder = Builder::default();
    builder
        .push_bytes(&uuid)
        .push_opcode(Opcode::OP_DROP)
        .push_opcode(Opcode::OP_IF)
        .push_bytes(&time_lock.to_le_bytes())
        .push_opcode(Opcode::OP_CHECKLOCKTIMEVERIFY)
        .push_opcode(Opcode::OP_DROP)
        .push_bytes(sender_pub)
        .push_opcode(Opcode::OP_CHECKSIG)
        .push_opcode(Opcode::OP_ELSE)
        .push_bytes(watcher_pub)
        .push_opcode(Opcode::OP_CHECKSIG)
        .push_opcode(Opcode::OP_ENDIF)
        .into_script()
}

/// Creates signed input spending hash time locked p2sh output
pub fn p2sh_spend(
    signer: &TransactionInputSigner,
    input_index: usize,
    key_pair: &KeyPair,
    script_data: Script,
    redeem_script: Script,
    signature_version: SignatureVersion,
    fork_id: u32,
) -> Result<TransactionInput, String> {
    let sighash = signer.signature_hash(
        input_index,
        signer.inputs[input_index].amount,
        &redeem_script,
        signature_version,
        1 | fork_id,
    );

    let sig = try_s!(script_sig(&sighash, &key_pair, fork_id));

    let mut resulting_script = Builder::default().push_data(&sig).into_bytes();
    if !script_data.is_empty() {
        resulting_script.extend_from_slice(&script_data);
    }

    let redeem_part = Builder::default().push_data(&redeem_script).into_bytes();
    resulting_script.extend_from_slice(&redeem_part);

    Ok(TransactionInput {
        script_sig: resulting_script,
        sequence: signer.inputs[input_index].sequence,
        script_witness: vec![],
        previous_output: signer.inputs[input_index].previous_output.clone(),
    })
}

#[allow(clippy::needless_lifetimes)]
pub async fn list_unspent_ordered<'a, T>(
    coin: &'a T,
    address: &Address,
) -> UtxoRpcResult<(Vec<UnspentInfo>, AsyncMutexGuard<'a, RecentlySpentOutPoints>)>
where
    T: AsRef<UtxoCoinFields>,
{
    let decimals = coin.as_ref().decimals;
    let mut unspents = coin
        .as_ref()
        .rpc_client
        .list_unspent(address, decimals)
        .compat()
        .await?;
    let recently_spent = coin.as_ref().recently_spent_outpoints.lock().await;
    unspents = recently_spent
        .replace_spent_outputs_with_cache(unspents.into_iter().collect())
        .into_iter()
        .collect();
    unspents.sort_unstable_by(|a, b| {
        if a.value < b.value {
            Ordering::Less
        } else {
            Ordering::Greater
        }
    });
    // dedup just in case we add duplicates of same unspent out
    // all duplicates will be removed because vector in sorted before dedup
    unspents.dedup_by(|one, another| one.outpoint == another.outpoint);
    Ok((unspents, recently_spent))
}

/// Increase the given `dynamic_fee` according to the fee approximation `stage` using the [`UtxoCoinFields::tx_fee_volatility_percent`].
pub fn increase_dynamic_fee_by_stage<T>(coin: &T, dynamic_fee: u64, stage: &FeeApproxStage) -> u64
where
    T: AsRef<UtxoCoinFields>,
{
    let base_percent = coin.as_ref().conf.tx_fee_volatility_percent;
    let percent = match stage {
        FeeApproxStage::WithoutApprox => return dynamic_fee,
        // Take into account that the dynamic fee may increase during the swap by [`UtxoCoinFields::tx_fee_volatility_percent`].
        FeeApproxStage::StartSwap => base_percent,
        // Take into account that the dynamic fee may increase at each of the following stages up to [`UtxoCoinFields::tx_fee_volatility_percent`]:
        // - until a swap is started;
        // - during the swap.
        FeeApproxStage::OrderIssue => base_percent * 2.,
        // Take into account that the dynamic fee may increase at each of the following stages up to [`UtxoCoinFields::tx_fee_volatility_percent`]:
        // - until an order is issued;
        // - until a swap is started;
        // - during the swap.
        FeeApproxStage::TradePreimage => base_percent * 2.5,
    };
    increase_by_percent(dynamic_fee, percent)
}

fn increase_by_percent(num: u64, percent: f64) -> u64 {
    let percent = num as f64 / 100. * percent;
    num + (percent.round() as u64)
}

async fn merge_utxo_loop<T>(weak: UtxoWeak, merge_at: usize, check_every: f64, max_merge_at_once: usize)
where
    T: From<UtxoArc> + AsRef<UtxoCoinFields> + UtxoCommonOps,
{
    loop {
        Timer::sleep(check_every).await;

        let coin = match weak.upgrade() {
            Some(arc) => T::from(arc),
            None => break,
        };

        let ticker = &coin.as_ref().conf.ticker;
        let (unspents, recently_spent) = match coin.list_unspent_ordered(&coin.as_ref().my_address).await {
            Ok((unspents, recently_spent)) => (unspents, recently_spent),
            Err(e) => {
                error!("Error {} on list_unspent_ordered of coin {}", e, ticker);
                continue;
            },
        };
        if unspents.len() >= merge_at {
            let unspents: Vec<_> = unspents.into_iter().take(max_merge_at_once).collect();
            info!("Trying to merge {} UTXOs of coin {}", unspents.len(), ticker);
            let value = unspents.iter().fold(0, |sum, unspent| sum + unspent.value);
            let script_pubkey = Builder::build_p2pkh(&coin.as_ref().my_address.hash).to_bytes();
            let output = TransactionOutput { value, script_pubkey };
            let merge_tx_fut = generate_and_send_tx(
                &coin,
                unspents,
                vec![output],
                FeePolicy::DeductFromOutput(0),
                recently_spent,
            );
            match merge_tx_fut.await {
                Ok(tx) => info!(
                    "UTXO merge successful for coin {}, tx_hash {:?}",
                    ticker,
                    tx.hash().reversed()
                ),
                Err(e) => error!("Error {} on UTXO merge attempt for coin {}", e, ticker),
            }
        }
    }
}

pub async fn can_refund_htlc<T>(coin: &T, locktime: u64) -> Result<CanRefundHtlc, MmError<UtxoRpcError>>
where
    T: UtxoCommonOps,
{
    let now = now_ms() / 1000;
    if now < locktime {
        let to_wait = locktime - now + 1;
        return Ok(CanRefundHtlc::HaveToWait(to_wait.max(3600)));
    }

    let mtp = coin.get_current_mtp().await?;
    let locktime = coin.p2sh_tx_locktime(locktime as u32).await?;

    if locktime < mtp {
        Ok(CanRefundHtlc::CanRefundNow)
    } else {
        let to_wait = (locktime - mtp + 1) as u64;
        Ok(CanRefundHtlc::HaveToWait(to_wait.max(3600)))
    }
}

pub async fn p2sh_tx_locktime<T>(coin: &T, ticker: &str, htlc_locktime: u32) -> Result<u32, MmError<UtxoRpcError>>
where
    T: UtxoCommonOps,
{
    let lock_time = if ticker == "KMD" {
        (now_ms() / 1000) as u32 - 3600 + 2 * 777
    } else {
        coin.get_current_mtp().await? - 1
    };
    Ok(lock_time.max(htlc_locktime))
}

#[test]
fn test_increase_by_percent() {
    assert_eq!(increase_by_percent(4300, 1.), 4343);
    assert_eq!(increase_by_percent(30, 6.9), 32);
    assert_eq!(increase_by_percent(30, 6.), 32);
    assert_eq!(increase_by_percent(10, 6.), 11);
    assert_eq!(increase_by_percent(1000, 0.1), 1001);
    assert_eq!(increase_by_percent(0, 20.), 0);
    assert_eq!(increase_by_percent(20, 0.), 20);
    assert_eq!(increase_by_percent(23, 100.), 46);
    assert_eq!(increase_by_percent(100, 2.4), 102);
    assert_eq!(increase_by_percent(100, 2.5), 103);
}

#[test]
fn test_pubkey_from_script_sig() {
    let script_sig = Script::from("473044022071edae37cf518e98db3f7637b9073a7a980b957b0c7b871415dbb4898ec3ebdc022031b402a6b98e64ffdf752266449ca979a9f70144dba77ed7a6a25bfab11648f6012103ad6f89abc2e5beaa8a3ac28e22170659b3209fe2ddf439681b4b8f31508c36fa");
    let expected_pub = H264::from("03ad6f89abc2e5beaa8a3ac28e22170659b3209fe2ddf439681b4b8f31508c36fa");
    let actual_pub = pubkey_from_script_sig(&script_sig).unwrap();
    assert_eq!(expected_pub, actual_pub);

    let script_sig_err = Script::from("473044022071edae37cf518e98db3f7637b9073a7a980b957b0c7b871415dbb4898ec3ebdc022031b402a6b98e64ffdf752266449ca979a9f70144dba77ed7a6a25bfab11648f6012103ad6f89abc2e5beaa8a3ac28e22170659b3209fe2ddf439681b4b8f31508c36fa21");
    pubkey_from_script_sig(&script_sig_err).unwrap_err();

    let script_sig_err = Script::from("493044022071edae37cf518e98db3f7637b9073a7a980b957b0c7b871415dbb4898ec3ebdc022031b402a6b98e64ffdf752266449ca979a9f70144dba77ed7a6a25bfab11648f6012103ad6f89abc2e5beaa8a3ac28e22170659b3209fe2ddf439681b4b8f31508c36fa");
    pubkey_from_script_sig(&script_sig_err).unwrap_err();
}
