use super::p2pkh_spend;
use super::utxo_standard::UtxoStandardCoin;

use crate::utxo::rpc_clients::{UnspentInfo, UtxoRpcClientEnum, UtxoRpcError};
use crate::utxo::utxo_common::{self, big_decimal_from_sat_unsigned, generate_transaction, p2sh_spend, payment_script};
use crate::utxo::{generate_and_send_tx, sat_from_big_decimal, FeePolicy, GenerateTxError, RecentlySpentOutPoints,
                  UtxoCommonOps, UtxoTx};
use crate::{BalanceError, BalanceFut, CoinBalance, FeeApproxStage, FoundSwapTxSpend, HistorySyncState, MarketCoinOps,
            MmCoin, NegotiateSwapContractAddrErr, NumConversError, RawTransactionFut, RawTransactionRequest, SwapOps,
            TradeFee, TradePreimageFut, TradePreimageValue, TransactionEnum, TransactionFut, ValidateAddressResult,
            WithdrawFut, WithdrawRequest};

use bitcoin_cash_slp::{slp_send_output, SlpTokenType, TokenId};
use bitcrypto::dhash160;
use chain::constants::SEQUENCE_FINAL;
use chain::{OutPoint, TransactionOutput};
use common::mm_ctx::MmArc;
use common::mm_error::prelude::*;
use common::mm_number::{BigDecimal, MmNumber};
use derive_more::Display;
use futures::compat::Future01CompatExt;
use futures::lock::MutexGuard as AsyncMutexGuard;
use futures::{FutureExt, TryFutureExt};
use futures01::Future;
use keys::Public;
use primitives::hash::H256;
use rpc::v1::types::Bytes as BytesJson;
use script::bytes::Bytes;
use script::{Builder as ScriptBuilder, Opcode, Script};
use serde_json::Value as Json;
use serialization::{deserialize, serialize, Deserializable, Error, Reader};
use serialization_derive::Deserializable;
use std::convert::TryInto;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

const SLP_SWAP_VOUT: usize = 1;
const SLP_FEE_VOUT: usize = 1;

#[derive(Debug)]
pub struct SlpTokenConf {
    decimals: u8,
    ticker: String,
    token_id: H256,
    required_confirmations: AtomicU64,
}

#[derive(Clone, Debug)]
pub struct SlpToken {
    conf: Arc<SlpTokenConf>,
    platform_utxo: UtxoStandardCoin,
}

#[derive(Clone, Debug)]
pub struct SlpUnspent {
    pub bch_unspent: UnspentInfo,
    pub slp_amount: u64,
}

#[derive(Clone, Debug)]
struct SlpOutput {
    amount: u64,
    script_pubkey: Bytes,
}

/// The SLP transaction preimage
struct SlpTxPreimage<'a> {
    inputs: Vec<UnspentInfo>,
    outputs: Vec<TransactionOutput>,
    recently_spent: AsyncMutexGuard<'a, RecentlySpentOutPoints>,
}

#[derive(Debug, Display)]
enum ValidateHtlcError {
    TxLackOfOutputs,
    #[display(fmt = "TxParseError: {:?}", _0)]
    TxParseError(Error),
    #[display(fmt = "OpReturnParseError: {:?}", _0)]
    OpReturnParseError(Error),
    InvalidSlpDetails,
    NumConversionErr(NumConversError),
    ValidatePaymentError(String),
}

impl From<NumConversError> for ValidateHtlcError {
    fn from(err: NumConversError) -> ValidateHtlcError { ValidateHtlcError::NumConversionErr(err) }
}

#[derive(Debug, Display)]
enum ValidateDexFeeError {
    TxLackOfOutputs,
    #[display(fmt = "OpReturnParseError: {:?}", _0)]
    OpReturnParseError(Error),
    InvalidSlpDetails,
    NumConversionErr(NumConversError),
    ValidatePaymentError(String),
}

impl From<NumConversError> for ValidateDexFeeError {
    fn from(err: NumConversError) -> ValidateDexFeeError { ValidateDexFeeError::NumConversionErr(err) }
}

#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Display)]
pub enum SpendP2SHError {
    GenerateTxErr(GenerateTxError),
    Rpc(UtxoRpcError),
    GetUnspentsErr(SlpUnspentsErr),
    String(String),
}

impl From<GenerateTxError> for SpendP2SHError {
    fn from(err: GenerateTxError) -> SpendP2SHError { SpendP2SHError::GenerateTxErr(err) }
}

impl From<UtxoRpcError> for SpendP2SHError {
    fn from(err: UtxoRpcError) -> SpendP2SHError { SpendP2SHError::Rpc(err) }
}

impl From<SlpUnspentsErr> for SpendP2SHError {
    fn from(err: SlpUnspentsErr) -> SpendP2SHError { SpendP2SHError::GetUnspentsErr(err) }
}

impl From<String> for SpendP2SHError {
    fn from(err: String) -> SpendP2SHError { SpendP2SHError::String(err) }
}

#[derive(Debug, Display)]
pub enum SpendHtlcError {
    TxLackOfOutputs,
    #[display(fmt = "DeserializationErr: {:?}", _0)]
    DeserializationErr(Error),
    #[display(fmt = "PubkeyParseError: {:?}", _0)]
    PubkeyParseErr(keys::Error),
    InvalidSlpDetails,
    NumConversionErr(NumConversError),
    RpcErr(UtxoRpcError),
    #[allow(clippy::upper_case_acronyms)]
    SpendP2SHErr(SpendP2SHError),
}

impl From<NumConversError> for SpendHtlcError {
    fn from(err: NumConversError) -> SpendHtlcError { SpendHtlcError::NumConversionErr(err) }
}

impl From<Error> for SpendHtlcError {
    fn from(err: Error) -> SpendHtlcError { SpendHtlcError::DeserializationErr(err) }
}

impl From<keys::Error> for SpendHtlcError {
    fn from(err: keys::Error) -> SpendHtlcError { SpendHtlcError::PubkeyParseErr(err) }
}

impl From<SpendP2SHError> for SpendHtlcError {
    fn from(err: SpendP2SHError) -> SpendHtlcError { SpendHtlcError::SpendP2SHErr(err) }
}

impl From<UtxoRpcError> for SpendHtlcError {
    fn from(err: UtxoRpcError) -> SpendHtlcError { SpendHtlcError::RpcErr(err) }
}

impl SlpToken {
    pub fn new(
        decimals: u8,
        ticker: String,
        token_id: H256,
        platform_utxo: UtxoStandardCoin,
        required_confirmations: u64,
    ) -> SlpToken {
        let conf = Arc::new(SlpTokenConf {
            decimals,
            ticker,
            token_id,
            required_confirmations: AtomicU64::new(required_confirmations),
        });
        SlpToken { conf, platform_utxo }
    }

    fn rpc(&self) -> &UtxoRpcClientEnum { &self.platform_utxo.as_ref().rpc_client }

    /// Returns unspents of the SLP token plus plain BCH UTXOs plus RecentlySpentOutPoints mutex guard
    async fn slp_unspents(
        &self,
    ) -> Result<
        (
            Vec<SlpUnspent>,
            Vec<UnspentInfo>,
            AsyncMutexGuard<'_, RecentlySpentOutPoints>,
        ),
        MmError<SlpUnspentsErr>,
    > {
        let (unspents, recently_spent) = self
            .platform_utxo
            .list_unspent_ordered(&self.platform_utxo.as_ref().my_address)
            .await?;

        let mut slp_unspents = vec![];
        let mut bch_unspents = vec![];

        for unspent in unspents {
            let prev_tx_bytes = self
                .rpc()
                .get_transaction_bytes(unspent.outpoint.hash.reversed().into())
                .compat()
                .await?;
            let prev_tx: UtxoTx = deserialize(prev_tx_bytes.0.as_slice())?;
            match parse_slp_script(&prev_tx.outputs[0].script_pubkey) {
                Ok(slp_data) => match slp_data.transaction {
                    SlpTransaction::Send { token_id, amounts } => {
                        if token_id == self.token_id() && unspent.outpoint.index > 0 {
                            match amounts.get(unspent.outpoint.index as usize - 1) {
                                Some(slp_amount) => slp_unspents.push(SlpUnspent {
                                    bch_unspent: unspent,
                                    slp_amount: *slp_amount,
                                }),
                                None => bch_unspents.push(unspent),
                            }
                        }
                    },
                    SlpTransaction::Genesis {
                        initial_token_mint_quantity,
                        ..
                    } => {
                        if prev_tx.hash().reversed() == self.token_id()
                            && initial_token_mint_quantity.len() == 8
                            && unspent.outpoint.index == 1
                        {
                            let slp_amount = u64::from_be_bytes(initial_token_mint_quantity.try_into().unwrap());
                            slp_unspents.push(SlpUnspent {
                                bch_unspent: unspent,
                                slp_amount,
                            });
                        } else {
                            bch_unspents.push(unspent)
                        }
                    },
                    SlpTransaction::Mint {
                        token_id,
                        additional_token_quantity,
                        ..
                    } => {
                        if token_id == self.token_id() && additional_token_quantity.len() == 8 {
                            let slp_amount = u64::from_be_bytes(additional_token_quantity.try_into().unwrap());
                            slp_unspents.push(SlpUnspent {
                                bch_unspent: unspent,
                                slp_amount,
                            });
                        }
                    },
                },
                Err(_) => bch_unspents.push(unspent),
            }
        }

        slp_unspents.sort_by(|a, b| a.slp_amount.cmp(&b.slp_amount));
        Ok((slp_unspents, bch_unspents, recently_spent))
    }

    /// Generates the tx preimage that spends the SLP from my address to the desired destinations (script pubkeys)
    async fn generate_slp_tx_preimage(
        &self,
        slp_outputs: Vec<SlpOutput>,
    ) -> Result<SlpTxPreimage<'_>, MmError<GenSlpSpendErr>> {
        let (slp_unspents, bch_unspents, recently_spent) = self.slp_unspents().await?;
        let total_slp_output = slp_outputs.iter().fold(0, |cur, slp_out| cur + slp_out.amount);
        let mut total_slp_input = 0;

        let mut inputs = vec![];
        for slp_utxo in slp_unspents {
            if total_slp_input >= total_slp_output {
                break;
            }

            total_slp_input += slp_utxo.slp_amount;
            inputs.push(slp_utxo.bch_unspent);
        }

        if total_slp_input < total_slp_output {
            return MmError::err(GenSlpSpendErr::InsufficientSlpBalance);
        }
        let change = total_slp_input - total_slp_output;

        inputs.extend(bch_unspents);

        let mut amounts_for_op_return: Vec<_> = slp_outputs.iter().map(|spend_to| spend_to.amount).collect();
        if change > 0 {
            amounts_for_op_return.push(change);
        }

        // TODO generate the script in MM2 instead of using the external library
        let op_return_out = slp_send_output(
            SlpTokenType::Fungible,
            &TokenId::from_slice(self.token_id().as_slice()).unwrap(),
            &amounts_for_op_return,
        );
        let op_return_out_mm = TransactionOutput {
            value: 0,
            script_pubkey: op_return_out.script.serialize().unwrap().to_vec().into(),
        };
        let mut outputs = vec![op_return_out_mm];

        outputs.extend(slp_outputs.into_iter().map(|spend_to| TransactionOutput {
            value: self.dust(),
            script_pubkey: spend_to.script_pubkey,
        }));

        if change > 0 {
            let slp_change_out = TransactionOutput {
                value: self.dust(),
                script_pubkey: ScriptBuilder::build_p2pkh(&self.platform_utxo.my_public_key().address_hash())
                    .to_bytes(),
            };
            outputs.push(slp_change_out);
        }

        Ok(SlpTxPreimage {
            inputs,
            outputs,
            recently_spent,
        })
    }

    async fn send_htlc(
        &self,
        other_pub: &Public,
        time_lock: u32,
        secret_hash: &[u8],
        amount: u64,
    ) -> Result<UtxoTx, String> {
        let payment_script = payment_script(time_lock, secret_hash, self.platform_utxo.my_public_key(), other_pub);
        let script_pubkey = ScriptBuilder::build_p2sh(&dhash160(&payment_script)).to_bytes();
        let slp_out = SlpOutput { amount, script_pubkey };
        let preimage = try_s!(self.generate_slp_tx_preimage(vec![slp_out]).await);
        generate_and_send_tx(
            &self.platform_utxo,
            preimage.inputs,
            preimage.outputs,
            FeePolicy::SendExact,
            preimage.recently_spent,
        )
        .await
    }

    async fn validate_htlc(
        &self,
        tx: &[u8],
        other_pub: &Public,
        time_lock: u32,
        secret_hash: &[u8],
        amount: BigDecimal,
    ) -> Result<(), MmError<ValidateHtlcError>> {
        let mut tx: UtxoTx = deserialize(tx).map_to_mm(ValidateHtlcError::TxParseError)?;
        tx.tx_hash_algo = self.platform_utxo.as_ref().tx_hash_algo;
        if tx.outputs.len() < 2 {
            return MmError::err(ValidateHtlcError::TxLackOfOutputs);
        }

        let slp_tx: SlpTxDetails =
            deserialize(tx.outputs[0].script_pubkey.as_slice()).map_to_mm(ValidateHtlcError::OpReturnParseError)?;

        match slp_tx.transaction {
            SlpTransaction::Send { token_id, amounts } => {
                if token_id != self.token_id() {
                    return MmError::err(ValidateHtlcError::InvalidSlpDetails);
                }

                if amounts.is_empty() {
                    return MmError::err(ValidateHtlcError::InvalidSlpDetails);
                }

                let expected = sat_from_big_decimal(&amount, self.decimals())?;

                if amounts[0] != expected {
                    return MmError::err(ValidateHtlcError::InvalidSlpDetails);
                }
            },
            _ => return MmError::err(ValidateHtlcError::InvalidSlpDetails),
        }

        let dust_decimal = big_decimal_from_sat_unsigned(self.dust(), self.platform_utxo.decimals());
        let validate_fut = utxo_common::validate_payment(
            self.platform_utxo.clone(),
            tx,
            SLP_SWAP_VOUT,
            other_pub,
            self.platform_utxo.my_public_key(),
            secret_hash,
            dust_decimal,
            time_lock,
        );

        validate_fut
            .compat()
            .await
            .map_to_mm(ValidateHtlcError::ValidatePaymentError)?;

        Ok(())
    }

    pub async fn refund_htlc(
        &self,
        htlc_tx: &[u8],
        other_pub: &Public,
        time_lock: u32,
        secret_hash: &[u8],
    ) -> Result<UtxoTx, MmError<SpendHtlcError>> {
        let tx: UtxoTx = deserialize(htlc_tx)?;
        if tx.outputs.is_empty() {
            return MmError::err(SpendHtlcError::TxLackOfOutputs);
        }

        let slp_tx: SlpTxDetails = deserialize(tx.outputs[0].script_pubkey.as_slice())?;

        let other_pub = Public::from_slice(other_pub)?;
        let redeem_script = payment_script(time_lock, secret_hash, self.platform_utxo.my_public_key(), &other_pub);

        let slp_amount = match slp_tx.transaction {
            SlpTransaction::Send { token_id, amounts } => {
                if token_id != self.token_id() {
                    return MmError::err(SpendHtlcError::InvalidSlpDetails);
                }
                *amounts.get(0).ok_or(SpendHtlcError::InvalidSlpDetails)?
            },
            _ => return MmError::err(SpendHtlcError::InvalidSlpDetails),
        };
        let slp_utxo = SlpUnspent {
            bch_unspent: UnspentInfo {
                outpoint: OutPoint {
                    hash: tx.hash(),
                    index: SLP_SWAP_VOUT as u32,
                },
                value: tx.outputs[1].value,
                height: None,
            },
            slp_amount,
        };

        let tx_locktime = self.platform_utxo.p2sh_tx_locktime(time_lock).await?;
        let script_data = ScriptBuilder::default().push_opcode(Opcode::OP_1).into_script();
        let tx = self
            .spend_p2sh(slp_utxo, tx_locktime, SEQUENCE_FINAL - 1, script_data, redeem_script)
            .await?;
        Ok(tx)
    }

    pub async fn spend_htlc(
        &self,
        htlc_tx: &[u8],
        other_pub: &Public,
        time_lock: u32,
        secret: &[u8],
    ) -> Result<UtxoTx, MmError<SpendHtlcError>> {
        let tx: UtxoTx = deserialize(htlc_tx)?;
        let slp_tx: SlpTxDetails = deserialize(tx.outputs[0].script_pubkey.as_slice())?;

        let other_pub = Public::from_slice(other_pub)?;
        let redeem_script = payment_script(
            time_lock,
            &*dhash160(&secret),
            &other_pub,
            self.platform_utxo.my_public_key(),
        );

        let slp_amount = match slp_tx.transaction {
            SlpTransaction::Send { token_id, amounts } => {
                if token_id != self.token_id() {
                    return MmError::err(SpendHtlcError::InvalidSlpDetails);
                }
                *amounts.get(0).ok_or(SpendHtlcError::InvalidSlpDetails)?
            },
            _ => return MmError::err(SpendHtlcError::InvalidSlpDetails),
        };
        let slp_utxo = SlpUnspent {
            bch_unspent: UnspentInfo {
                outpoint: OutPoint {
                    hash: tx.hash(),
                    index: SLP_SWAP_VOUT as u32,
                },
                value: tx.outputs[1].value,
                height: None,
            },
            slp_amount,
        };

        let tx_locktime = self.platform_utxo.p2sh_tx_locktime(time_lock).await?;
        let script_data = ScriptBuilder::default()
            .push_data(secret)
            .push_opcode(Opcode::OP_0)
            .into_script();
        let tx = self
            .spend_p2sh(slp_utxo, tx_locktime, SEQUENCE_FINAL, script_data, redeem_script)
            .await?;
        Ok(tx)
    }

    pub async fn spend_p2sh(
        &self,
        p2sh_utxo: SlpUnspent,
        tx_locktime: u32,
        input_sequence: u32,
        script_data: Script,
        redeem_script: Script,
    ) -> Result<UtxoTx, MmError<SpendP2SHError>> {
        let op_return = slp_send_output(
            SlpTokenType::Fungible,
            &TokenId::from_slice(self.token_id().as_slice()).unwrap(),
            &[p2sh_utxo.slp_amount],
        );
        let op_return_out_mm = TransactionOutput {
            value: 0,
            script_pubkey: op_return.script.serialize().unwrap().to_vec().into(),
        };
        let mut outputs = Vec::with_capacity(3);
        outputs.push(op_return_out_mm);

        let my_script_pubkey = ScriptBuilder::build_p2pkh(&self.platform_utxo.my_public_key().address_hash());
        let slp_output = TransactionOutput {
            value: self.dust(),
            script_pubkey: my_script_pubkey.to_bytes(),
        };
        outputs.push(slp_output);

        let (_, mut bch_inputs, _recently_spent) = self.slp_unspents().await?;
        bch_inputs.insert(0, p2sh_utxo.bch_unspent);
        let (mut unsigned, _) = generate_transaction(
            &self.platform_utxo,
            bch_inputs,
            outputs,
            FeePolicy::SendExact,
            None,
            None,
        )
        .await?;

        unsigned.lock_time = tx_locktime;
        unsigned.inputs[0].sequence = input_sequence;

        let signed_p2sh_input = p2sh_spend(
            &unsigned,
            0,
            &self.platform_utxo.as_ref().key_pair,
            script_data,
            redeem_script,
            self.platform_utxo.as_ref().conf.signature_version,
            self.platform_utxo.as_ref().conf.fork_id,
        )?;

        let signed_inputs: Result<Vec<_>, _> = unsigned
            .inputs
            .iter()
            .enumerate()
            .skip(1)
            .map(|(i, _)| {
                p2pkh_spend(
                    &unsigned,
                    i,
                    &self.platform_utxo.as_ref().key_pair,
                    &my_script_pubkey,
                    self.platform_utxo.as_ref().conf.signature_version,
                    self.platform_utxo.as_ref().conf.fork_id,
                )
            })
            .collect();

        let mut signed_inputs = signed_inputs?;

        signed_inputs.insert(0, signed_p2sh_input);

        let signed = UtxoTx {
            version: unsigned.version,
            n_time: unsigned.n_time,
            overwintered: unsigned.overwintered,
            version_group_id: unsigned.version_group_id,
            inputs: signed_inputs,
            outputs: unsigned.outputs,
            lock_time: unsigned.lock_time,
            expiry_height: unsigned.expiry_height,
            shielded_spends: unsigned.shielded_spends,
            shielded_outputs: unsigned.shielded_outputs,
            join_splits: unsigned.join_splits,
            value_balance: unsigned.value_balance,
            join_split_pubkey: Default::default(),
            join_split_sig: Default::default(),
            binding_sig: Default::default(),
            zcash: unsigned.zcash,
            str_d_zeel: unsigned.str_d_zeel,
            tx_hash_algo: self.platform_utxo.as_ref().tx_hash_algo,
        };

        let _broadcast = self
            .rpc()
            .send_raw_transaction(serialize(&signed).into())
            .compat()
            .await?;
        Ok(signed)
    }

    async fn validate_dex_fee(
        &self,
        tx: UtxoTx,
        expected_sender: &[u8],
        fee_addr: &[u8],
        amount: BigDecimal,
        min_block_number: u64,
    ) -> Result<(), MmError<ValidateDexFeeError>> {
        if tx.outputs.len() < 2 {
            return MmError::err(ValidateDexFeeError::TxLackOfOutputs);
        }

        let slp_tx: SlpTxDetails =
            deserialize(tx.outputs[0].script_pubkey.as_slice()).map_to_mm(ValidateDexFeeError::OpReturnParseError)?;

        match slp_tx.transaction {
            SlpTransaction::Send { token_id, amounts } => {
                if token_id != self.token_id() {
                    return MmError::err(ValidateDexFeeError::InvalidSlpDetails);
                }

                if amounts.is_empty() {
                    return MmError::err(ValidateDexFeeError::InvalidSlpDetails);
                }

                let expected = sat_from_big_decimal(&amount, self.decimals())?;

                if amounts[0] != expected {
                    return MmError::err(ValidateDexFeeError::InvalidSlpDetails);
                }
            },
            _ => return MmError::err(ValidateDexFeeError::InvalidSlpDetails),
        }

        let dust_decimal = big_decimal_from_sat_unsigned(self.dust(), self.platform_utxo.decimals());
        let validate_fut = utxo_common::validate_fee(
            self.platform_utxo.clone(),
            tx,
            SLP_FEE_VOUT,
            expected_sender,
            &dust_decimal,
            min_block_number,
            fee_addr,
        );

        validate_fut
            .compat()
            .await
            .map_to_mm(ValidateDexFeeError::ValidatePaymentError)?;

        Ok(())
    }

    pub fn dust(&self) -> u64 { self.platform_utxo.as_ref().dust_amount }

    pub fn decimals(&self) -> u8 { self.conf.decimals }

    pub fn token_id(&self) -> &H256 { &self.conf.token_id }
}

/// https://slp.dev/specs/slp-token-type-1/#transaction-detail
#[derive(Debug, Eq, PartialEq)]
enum SlpTransaction {
    /// https://slp.dev/specs/slp-token-type-1/#genesis-token-genesis-transaction
    Genesis {
        token_ticker: String,
        token_name: String,
        token_document_url: String,
        token_document_hash: Vec<u8>,
        decimals: Vec<u8>,
        mint_baton_vout: Vec<u8>,
        initial_token_mint_quantity: Vec<u8>,
    },
    /// https://slp.dev/specs/slp-token-type-1/#mint-extended-minting-transaction
    Mint {
        token_id: H256,
        mint_baton_vout: Vec<u8>,
        additional_token_quantity: Vec<u8>,
    },
    /// https://slp.dev/specs/slp-token-type-1/#send-spend-transaction
    Send { token_id: H256, amounts: Vec<u64> },
}

impl Deserializable for SlpTransaction {
    fn deserialize<T>(reader: &mut Reader<T>) -> Result<Self, Error>
    where
        Self: Sized,
        T: std::io::Read,
    {
        let transaction_type: String = reader.read()?;
        match transaction_type.as_str() {
            "GENESIS" => {
                let token_ticker = reader.read()?;
                let token_name = reader.read()?;
                let maybe_push_op_code: u8 = reader.read()?;
                let token_document_url = if maybe_push_op_code == Opcode::OP_PUSHDATA1 as u8 {
                    reader.read()?
                } else {
                    let mut url = vec![0; maybe_push_op_code as usize];
                    reader.read_slice(&mut url)?;
                    String::from_utf8(url).map_err(|e| Error::Custom(e.to_string()))?
                };

                let maybe_push_op_code: u8 = reader.read()?;
                let token_document_hash = if maybe_push_op_code == Opcode::OP_PUSHDATA1 as u8 {
                    reader.read_list()?
                } else {
                    let mut hash = vec![0; maybe_push_op_code as usize];
                    reader.read_slice(&mut hash)?;
                    hash
                };
                let decimals = reader.read_list()?;
                let maybe_push_op_code: u8 = reader.read()?;
                let mint_baton_vout = if maybe_push_op_code == Opcode::OP_PUSHDATA1 as u8 {
                    reader.read_list()?
                } else {
                    let mut baton = vec![0; maybe_push_op_code as usize];
                    reader.read_slice(&mut baton)?;
                    baton
                };
                let initial_token_mint_quantity = reader.read_list()?;

                Ok(SlpTransaction::Genesis {
                    token_ticker,
                    token_name,
                    token_document_url,
                    token_document_hash,
                    decimals,
                    mint_baton_vout,
                    initial_token_mint_quantity,
                })
            },
            "MINT" => {
                let maybe_id: Vec<u8> = reader.read_list()?;
                if maybe_id.len() != 32 {
                    return Err(Error::Custom(format!("Unexpected token id length {}", maybe_id.len())));
                }

                Ok(SlpTransaction::Mint {
                    token_id: H256::from(maybe_id.as_slice()),
                    mint_baton_vout: reader.read_list()?,
                    additional_token_quantity: reader.read_list()?,
                })
            },
            "SEND" => {
                let maybe_id: Vec<u8> = reader.read_list()?;
                if maybe_id.len() != 32 {
                    return Err(Error::Custom(format!("Unexpected token id length {}", maybe_id.len())));
                }

                let token_id = H256::from(maybe_id.as_slice());
                let mut amounts = Vec::with_capacity(1);
                while !reader.is_finished() {
                    let bytes: Vec<u8> = reader.read_list()?;
                    if bytes.len() != 8 {
                        return Err(Error::Custom(format!("Expected 8 bytes, got {}", bytes.len())));
                    }
                    let amount = u64::from_be_bytes(bytes.try_into().expect("length is 8 bytes"));
                    amounts.push(amount)
                }

                Ok(SlpTransaction::Send { token_id, amounts })
            },
            _ => Err(Error::Custom(format!(
                "Unsupported transaction type {}",
                transaction_type
            ))),
        }
    }
}

#[derive(Debug, Deserializable)]
struct SlpTxDetails {
    op_code: u8,
    lokad_id: String,
    token_type: String,
    transaction: SlpTransaction,
}

#[derive(Debug)]
enum ParseSlpScriptError {
    NotOpReturn,
    DeserializeFailed(Error),
}

impl From<Error> for ParseSlpScriptError {
    fn from(err: Error) -> ParseSlpScriptError { ParseSlpScriptError::DeserializeFailed(err) }
}

fn parse_slp_script(script: &[u8]) -> Result<SlpTxDetails, MmError<ParseSlpScriptError>> {
    let details: SlpTxDetails = deserialize(script).map_to_mm(ParseSlpScriptError::from)?;
    if Opcode::from_u8(details.op_code) != Some(Opcode::OP_RETURN) {
        return MmError::err(ParseSlpScriptError::NotOpReturn);
    }
    Ok(details)
}

#[derive(Debug, Display)]
pub enum SlpUnspentsErr {
    RpcError(UtxoRpcError),
    #[display(fmt = "TxDeserializeError: {:?}", _0)]
    TxDeserializeError(Error),
}

impl From<UtxoRpcError> for SlpUnspentsErr {
    fn from(err: UtxoRpcError) -> SlpUnspentsErr { SlpUnspentsErr::RpcError(err) }
}

impl From<Error> for SlpUnspentsErr {
    fn from(err: Error) -> SlpUnspentsErr { SlpUnspentsErr::TxDeserializeError(err) }
}

impl From<SlpUnspentsErr> for BalanceError {
    fn from(err: SlpUnspentsErr) -> BalanceError {
        match err {
            SlpUnspentsErr::RpcError(e) => BalanceError::Transport(e.to_string()),
            SlpUnspentsErr::TxDeserializeError(e) => BalanceError::Internal(format!("{:?}", e)),
        }
    }
}

#[derive(Debug, Display)]
enum GenSlpSpendErr {
    GetUnspentsErr(SlpUnspentsErr),
    InsufficientSlpBalance,
}

impl From<SlpUnspentsErr> for GenSlpSpendErr {
    fn from(err: SlpUnspentsErr) -> GenSlpSpendErr { GenSlpSpendErr::GetUnspentsErr(err) }
}

impl MarketCoinOps for SlpToken {
    fn ticker(&self) -> &str { &self.conf.ticker }

    fn my_address(&self) -> Result<String, String> { unimplemented!() }

    fn my_balance(&self) -> BalanceFut<CoinBalance> {
        let coin = self.clone();
        let fut = async move {
            let (slp_unspents, _, _) = coin.slp_unspents().await?;
            let spendable_sat = slp_unspents.iter().fold(0, |cur, unspent| cur + unspent.slp_amount);
            let spendable = big_decimal_from_sat_unsigned(spendable_sat, coin.decimals());
            Ok(CoinBalance {
                spendable,
                unspendable: 0.into(),
            })
        };
        Box::new(fut.boxed().compat())
    }

    fn base_coin_balance(&self) -> BalanceFut<BigDecimal> {
        Box::new(self.platform_utxo.my_balance().map(|res| res.spendable))
    }

    /// Receives raw transaction bytes in hexadecimal format as input and returns tx hash in hexadecimal format
    fn send_raw_tx(&self, tx: &str) -> Box<dyn Future<Item = String, Error = String> + Send> {
        self.platform_utxo.send_raw_tx(tx)
    }

    fn wait_for_confirmations(
        &self,
        tx: &[u8],
        confirmations: u64,
        requires_nota: bool,
        wait_until: u64,
        check_every: u64,
    ) -> Box<dyn Future<Item = (), Error = String> + Send> {
        self.platform_utxo
            .wait_for_confirmations(tx, confirmations, requires_nota, wait_until, check_every)
    }

    fn wait_for_tx_spend(
        &self,
        transaction: &[u8],
        wait_until: u64,
        from_block: u64,
        _swap_contract_address: &Option<BytesJson>,
    ) -> TransactionFut {
        utxo_common::wait_for_output_spend(
            self.platform_utxo.as_ref(),
            transaction,
            SLP_SWAP_VOUT,
            from_block,
            wait_until,
        )
    }

    fn tx_enum_from_bytes(&self, bytes: &[u8]) -> Result<TransactionEnum, String> {
        self.platform_utxo.tx_enum_from_bytes(bytes)
    }

    fn current_block(&self) -> Box<dyn Future<Item = u64, Error = String> + Send> { self.platform_utxo.current_block() }

    fn address_from_pubkey_str(&self, _pubkey: &str) -> Result<String, String> { unimplemented!() }

    fn display_priv_key(&self) -> String { self.platform_utxo.display_priv_key() }

    fn min_tx_amount(&self) -> BigDecimal { big_decimal_from_sat_unsigned(1, self.decimals()) }

    fn min_trading_vol(&self) -> MmNumber { big_decimal_from_sat_unsigned(1, self.decimals()).into() }
}

impl SwapOps for SlpToken {
    fn send_taker_fee(&self, fee_addr: &[u8], amount: BigDecimal) -> TransactionFut {
        let coin = self.clone();
        let fee_pubkey = try_fus!(Public::from_slice(fee_addr));
        let script_pubkey = ScriptBuilder::build_p2pkh(&fee_pubkey.address_hash()).into();
        let amount = try_fus!(sat_from_big_decimal(&amount, self.decimals()));

        let fut = async move {
            let slp_out = SlpOutput { amount, script_pubkey };
            let preimage = try_s!(coin.generate_slp_tx_preimage(vec![slp_out]).await);
            generate_and_send_tx(
                &coin.platform_utxo,
                preimage.inputs,
                preimage.outputs,
                FeePolicy::SendExact,
                preimage.recently_spent,
            )
            .await
        };
        Box::new(fut.boxed().compat().map(|tx| tx.into()))
    }

    fn send_maker_payment(
        &self,
        time_lock: u32,
        taker_pub: &[u8],
        secret_hash: &[u8],
        amount: BigDecimal,
        _swap_contract_address: &Option<BytesJson>,
    ) -> TransactionFut {
        let taker_pub = try_fus!(Public::from_slice(taker_pub));
        let amount = try_fus!(sat_from_big_decimal(&amount, self.decimals()));
        let secret_hash = secret_hash.to_owned();

        let coin = self.clone();
        let fut = async move {
            let tx = try_s!(coin.send_htlc(&taker_pub, time_lock, &secret_hash, amount).await);
            Ok(tx.into())
        };
        Box::new(fut.boxed().compat())
    }

    fn send_taker_payment(
        &self,
        time_lock: u32,
        maker_pub: &[u8],
        secret_hash: &[u8],
        amount: BigDecimal,
        _swap_contract_address: &Option<BytesJson>,
    ) -> TransactionFut {
        let maker_pub = try_fus!(Public::from_slice(maker_pub));
        let amount = try_fus!(sat_from_big_decimal(&amount, self.decimals()));
        let secret_hash = secret_hash.to_owned();

        let coin = self.clone();
        let fut = async move {
            let tx = try_s!(coin.send_htlc(&maker_pub, time_lock, &secret_hash, amount).await);
            Ok(tx.into())
        };
        Box::new(fut.boxed().compat())
    }

    fn send_maker_spends_taker_payment(
        &self,
        taker_payment_tx: &[u8],
        time_lock: u32,
        taker_pub: &[u8],
        secret: &[u8],
        _swap_contract_address: &Option<BytesJson>,
    ) -> TransactionFut {
        let tx = taker_payment_tx.to_owned();
        let taker_pub = try_fus!(Public::from_slice(taker_pub));
        let secret = secret.to_owned();
        let coin = self.clone();

        let fut = async move {
            let tx = try_s!(coin.spend_htlc(&tx, &taker_pub, time_lock, &secret).await);
            Ok(tx.into())
        };
        Box::new(fut.boxed().compat())
    }

    fn send_taker_spends_maker_payment(
        &self,
        maker_payment_tx: &[u8],
        time_lock: u32,
        maker_pub: &[u8],
        secret: &[u8],
        _swap_contract_address: &Option<BytesJson>,
    ) -> TransactionFut {
        let tx = maker_payment_tx.to_owned();
        let maker_pub = try_fus!(Public::from_slice(maker_pub));
        let secret = secret.to_owned();
        let coin = self.clone();

        let fut = async move {
            let tx = try_s!(coin.spend_htlc(&tx, &maker_pub, time_lock, &secret).await);
            Ok(tx.into())
        };
        Box::new(fut.boxed().compat())
    }

    fn send_taker_refunds_payment(
        &self,
        taker_payment_tx: &[u8],
        time_lock: u32,
        maker_pub: &[u8],
        secret_hash: &[u8],
        _swap_contract_address: &Option<BytesJson>,
    ) -> TransactionFut {
        let tx = taker_payment_tx.to_owned();
        let maker_pub = try_fus!(Public::from_slice(maker_pub));
        let secret_hash = secret_hash.to_owned();
        let coin = self.clone();

        let fut = async move {
            let tx = try_s!(coin.refund_htlc(&tx, &maker_pub, time_lock, &secret_hash).await);
            Ok(tx.into())
        };
        Box::new(fut.boxed().compat())
    }

    fn send_maker_refunds_payment(
        &self,
        maker_payment_tx: &[u8],
        time_lock: u32,
        taker_pub: &[u8],
        secret_hash: &[u8],
        _swap_contract_address: &Option<BytesJson>,
    ) -> TransactionFut {
        let tx = maker_payment_tx.to_owned();
        let taker_pub = try_fus!(Public::from_slice(taker_pub));
        let secret_hash = secret_hash.to_owned();
        let coin = self.clone();

        let fut = async move {
            let tx = try_s!(coin.refund_htlc(&tx, &taker_pub, time_lock, &secret_hash).await);
            Ok(tx.into())
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
        let tx = match fee_tx {
            TransactionEnum::UtxoTx(tx) => tx.clone(),
            _ => panic!(),
        };
        let coin = self.clone();
        let expected_sender = expected_sender.to_owned();
        let fee_addr = fee_addr.to_owned();
        let amount = amount.to_owned();

        let fut = async move {
            try_s!(
                coin.validate_dex_fee(tx, &expected_sender, &fee_addr, amount, min_block_number)
                    .await
            );
            Ok(())
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
        _swap_contract_address: &Option<BytesJson>,
    ) -> Box<dyn Future<Item = (), Error = String> + Send> {
        let maker_pub = try_fus!(Public::from_slice(maker_pub));
        let tx = payment_tx.to_owned();
        let secret_hash = secret_hash.to_owned();
        let coin = self.clone();
        let fut = async move {
            try_s!(
                coin.validate_htlc(&tx, &maker_pub, time_lock, &secret_hash, amount)
                    .await
            );
            Ok(())
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
        _swap_contract_address: &Option<BytesJson>,
    ) -> Box<dyn Future<Item = (), Error = String> + Send> {
        let taker_pub = try_fus!(Public::from_slice(taker_pub));
        let tx = payment_tx.to_owned();
        let secret_hash = secret_hash.to_owned();
        let coin = self.clone();
        let fut = async move {
            try_s!(
                coin.validate_htlc(&tx, &taker_pub, time_lock, &secret_hash, amount)
                    .await
            );
            Ok(())
        };
        Box::new(fut.boxed().compat())
    }

    fn check_if_my_payment_sent(
        &self,
        time_lock: u32,
        other_pub: &[u8],
        secret_hash: &[u8],
        _search_from_block: u64,
        _swap_contract_address: &Option<BytesJson>,
    ) -> Box<dyn Future<Item = Option<TransactionEnum>, Error = String> + Send> {
        utxo_common::check_if_my_payment_sent(self.platform_utxo.clone(), time_lock, other_pub, secret_hash)
    }

    fn search_for_swap_tx_spend_my(
        &self,
        time_lock: u32,
        other_pub: &[u8],
        secret_hash: &[u8],
        tx: &[u8],
        search_from_block: u64,
        _swap_contract_address: &Option<BytesJson>,
    ) -> Result<Option<FoundSwapTxSpend>, String> {
        utxo_common::search_for_swap_tx_spend_my(
            self.platform_utxo.as_ref(),
            time_lock,
            other_pub,
            secret_hash,
            tx,
            SLP_SWAP_VOUT,
            search_from_block,
        )
    }

    fn search_for_swap_tx_spend_other(
        &self,
        time_lock: u32,
        other_pub: &[u8],
        secret_hash: &[u8],
        tx: &[u8],
        search_from_block: u64,
        _swap_contract_address: &Option<BytesJson>,
    ) -> Result<Option<FoundSwapTxSpend>, String> {
        utxo_common::search_for_swap_tx_spend_other(
            self.platform_utxo.as_ref(),
            time_lock,
            other_pub,
            secret_hash,
            tx,
            SLP_SWAP_VOUT,
            search_from_block,
        )
    }

    fn extract_secret(&self, secret_hash: &[u8], spend_tx: &[u8]) -> Result<Vec<u8>, String> {
        utxo_common::extract_secret(secret_hash, spend_tx)
    }

    fn negotiate_swap_contract_addr(
        &self,
        _other_side_address: Option<&[u8]>,
    ) -> Result<Option<BytesJson>, MmError<NegotiateSwapContractAddrErr>> {
        Ok(None)
    }
}

impl MmCoin for SlpToken {
    fn is_asset_chain(&self) -> bool { false }

    fn withdraw(&self, _req: WithdrawRequest) -> WithdrawFut { unimplemented!() }

    fn get_raw_transaction(&self, _req: RawTransactionRequest) -> RawTransactionFut { unimplemented!() }

    fn decimals(&self) -> u8 { unimplemented!() }

    fn convert_to_address(&self, _from: &str, _to_address_format: Json) -> Result<String, String> { unimplemented!() }

    fn validate_address(&self, _address: &str) -> ValidateAddressResult { unimplemented!() }

    fn process_history_loop(&self, _ctx: MmArc) -> Box<dyn Future<Item = (), Error = ()> + Send> { unimplemented!() }

    fn history_sync_status(&self) -> HistorySyncState { unimplemented!() }

    /// Get fee to be paid per 1 swap transaction
    fn get_trade_fee(&self) -> Box<dyn Future<Item = TradeFee, Error = String> + Send> { unimplemented!() }

    fn get_sender_trade_fee(&self, _value: TradePreimageValue, _stage: FeeApproxStage) -> TradePreimageFut<TradeFee> {
        unimplemented!()
    }

    fn get_receiver_trade_fee(&self, _stage: FeeApproxStage) -> TradePreimageFut<TradeFee> { unimplemented!() }

    fn get_fee_to_send_taker_fee(
        &self,
        _dex_fee_amount: BigDecimal,
        _stage: FeeApproxStage,
    ) -> TradePreimageFut<TradeFee> {
        unimplemented!()
    }

    fn required_confirmations(&self) -> u64 { 1 }

    fn requires_notarization(&self) -> bool { false }

    fn set_required_confirmations(&self, _confirmations: u64) { unimplemented!() }

    fn set_requires_notarization(&self, _requires_nota: bool) { unimplemented!() }

    fn swap_contract_address(&self) -> Option<BytesJson> { None }

    fn mature_confirmations(&self) -> Option<u32> { self.platform_utxo.mature_confirmations() }
}

#[cfg(test)]
mod slp_tests {
    use super::*;
    use crate::utxo::utxo_standard::utxo_standard_coin_from_conf_and_request;
    use common::mm_ctx::MmCtxBuilder;
    use common::privkey::key_pair_from_seed;
    use common::{block_on, now_ms};

    // https://slp.dev/specs/slp-token-type-1/#examples
    #[test]
    fn test_parse_slp_script() {
        // Send single output
        let script = hex::decode("6a04534c500001010453454e4420e73b2b28c14db8ebbf97749988b539508990e1708021067f206f49d55807dbf4080000000005f5e100").unwrap();
        let slp_data = parse_slp_script(&script).unwrap();
        assert_eq!(slp_data.lokad_id, "SLP\0");
        let expected_amount = 100000000u64;
        let expected_transaction = SlpTransaction::Send {
            token_id: "e73b2b28c14db8ebbf97749988b539508990e1708021067f206f49d55807dbf4".into(),
            amounts: vec![expected_amount],
        };

        assert_eq!(expected_transaction, slp_data.transaction);

        // Genesis
        let script =
            hex::decode("6a04534c500001010747454e45534953044144455804414445584c004c0001084c0008000000174876e800")
                .unwrap();
        let slp_data = parse_slp_script(&script).unwrap();
        assert_eq!(slp_data.lokad_id, "SLP\0");
        let initial_token_mint_quantity = 1000_0000_0000u64.to_be_bytes().to_vec();
        let expected_transaction = SlpTransaction::Genesis {
            token_ticker: "ADEX".to_string(),
            token_name: "ADEX".to_string(),
            token_document_url: "".to_string(),
            token_document_hash: vec![],
            decimals: vec![8],
            mint_baton_vout: vec![],
            initial_token_mint_quantity,
        };

        assert_eq!(expected_transaction, slp_data.transaction);

        // Genesis from docs example
        let script =
            hex::decode("6a04534c500001010747454e45534953045553445423546574686572204c74642e20555320646f6c6c6172206261636b656420746f6b656e734168747470733a2f2f7465746865722e746f2f77702d636f6e74656e742f75706c6f6164732f323031362f30362f546574686572576869746550617065722e70646620db4451f11eda33950670aaf59e704da90117ff7057283b032cfaec77793139160108010208002386f26fc10000").unwrap();
        let slp_data = parse_slp_script(&script).unwrap();
        assert_eq!(slp_data.lokad_id, "SLP\0");
        let initial_token_mint_quantity = 10000000000000000u64.to_be_bytes().to_vec();
        let expected_transaction = SlpTransaction::Genesis {
            token_ticker: "USDT".to_string(),
            token_name: "Tether Ltd. US dollar backed tokens".to_string(),
            token_document_url: "https://tether.to/wp-content/uploads/2016/06/TetherWhitePaper.pdf".to_string(),
            token_document_hash: hex::decode("db4451f11eda33950670aaf59e704da90117ff7057283b032cfaec7779313916")
                .unwrap(),
            decimals: vec![8],
            mint_baton_vout: vec![2],
            initial_token_mint_quantity,
        };

        assert_eq!(expected_transaction, slp_data.transaction);

        // Mint
        let script =
            hex::decode("6a04534c50000101044d494e5420550d19eb820e616a54b8a73372c4420b5a0567d8dc00f613b71c5234dc884b35010208002386f26fc10000").unwrap();
        let slp_data = parse_slp_script(&script).unwrap();
        assert_eq!(slp_data.lokad_id, "SLP\0");
        let expected_transaction = SlpTransaction::Mint {
            token_id: "550d19eb820e616a54b8a73372c4420b5a0567d8dc00f613b71c5234dc884b35".into(),
            mint_baton_vout: vec![2],
            additional_token_quantity: hex::decode("002386f26fc10000").unwrap(),
        };

        assert_eq!(expected_transaction, slp_data.transaction);

        let script = hex::decode("6a04534c500001010453454e4420550d19eb820e616a54b8a73372c4420b5a0567d8dc00f613b71c5234dc884b350800000000000003e80800000000000003e90800000000000003ea").unwrap();
        let token_id = "550d19eb820e616a54b8a73372c4420b5a0567d8dc00f613b71c5234dc884b35".into();

        let slp_data = parse_slp_script(&script).unwrap();
        assert_eq!(slp_data.lokad_id, "SLP\0");
        let expected_transaction = SlpTransaction::Send {
            token_id,
            amounts: vec![1000, 1001, 1002],
        };
        assert_eq!(expected_transaction, slp_data.transaction);
    }

    #[test]
    #[ignore]
    fn send_and_spend_htlc_on_testnet() {
        let ctx = MmCtxBuilder::default().into_mm_arc();
        let keypair = key_pair_from_seed("BCH SLP test").unwrap();

        let conf = json!({"coin":"BCH","pubtype":0,"p2shtype":5,"mm2":1,"fork_id":"0x40","protocol":{"type":"UTXO"},
         "address_format":{"format":"cashaddress","network":"bchtest"}});
        let req = json!({
            "method": "electrum",
            "coin": "BCH",
            "servers": [{"url":"blackie.c3-soft.com:60001"},{"url":"testnet.imaginary.cash:50001"}],
        });
        let bch = block_on(utxo_standard_coin_from_conf_and_request(
            &ctx,
            "BCH",
            &conf,
            &req,
            &*keypair.private().secret,
        ))
        .unwrap();

        let balance = bch.my_balance().wait().unwrap();
        println!("{}", balance.spendable);

        let address = bch.my_address().unwrap();
        println!("{}", address);

        let token_id = H256::from("bb309e48930671582bea508f9a1d9b491e49b69be3d6f372dc08da2ac6e90eb7");
        let fusd = SlpToken::new(4, "FUSD".into(), token_id, bch, 0);

        let fusd_balance = fusd.my_balance().wait().unwrap();
        println!("FUSD {}", fusd_balance.spendable);

        let secret = [0; 32];
        let secret_hash = dhash160(&secret);
        let time_lock = (now_ms() / 1000) as u32;
        let amount: BigDecimal = "0.1".parse().unwrap();

        let tx = fusd
            .send_taker_payment(time_lock, &*keypair.public(), &*secret_hash, amount.clone(), &None)
            .wait()
            .unwrap();
        println!("{}", hex::encode(tx.tx_hex()));

        fusd.validate_taker_payment(
            &tx.tx_hex(),
            time_lock,
            &*keypair.public(),
            &*secret_hash,
            amount,
            &None,
        )
        .wait()
        .unwrap();

        let spending_tx = fusd
            .send_maker_spends_taker_payment(&tx.tx_hex(), time_lock, &*keypair.public(), &secret, &None)
            .wait()
            .unwrap();
        println!("spend hex {}", hex::encode(spending_tx.tx_hex()));
        println!("spend hash {}", hex::encode(spending_tx.tx_hash().0));

        let wait_for_spend = fusd
            .wait_for_tx_spend(&tx.tx_hex(), (now_ms() / 1000) + 60, 0, &None)
            .wait()
            .unwrap();
        println!("spend hex {}", hex::encode(wait_for_spend.tx_hex()));
        println!("spend hash {}", hex::encode(wait_for_spend.tx_hash().0));

        let secret = fusd.extract_secret(&*secret_hash, &wait_for_spend.tx_hex()).unwrap();
        println!("{:?}", secret);
    }

    #[test]
    #[ignore]
    fn send_and_refund_htlc_on_testnet() {
        let ctx = MmCtxBuilder::default().into_mm_arc();
        let keypair = key_pair_from_seed("BCH SLP test").unwrap();

        let conf = json!({"coin":"BCH","pubtype":0,"p2shtype":5,"mm2":1,"fork_id":"0x40","protocol":{"type":"UTXO"},
         "address_format":{"format":"cashaddress","network":"bchtest"}});
        let req = json!({
            "method": "electrum",
            "coin": "BCH",
            "servers": [{"url":"blackie.c3-soft.com:60001"},{"url":"testnet.imaginary.cash:50001"}],
        });
        let bch = block_on(utxo_standard_coin_from_conf_and_request(
            &ctx,
            "BCH",
            &conf,
            &req,
            &*keypair.private().secret,
        ))
        .unwrap();

        let balance = bch.my_balance().wait().unwrap();
        println!("{}", balance.spendable);

        let address = bch.my_address().unwrap();
        println!("{}", address);

        let token_id = H256::from("bb309e48930671582bea508f9a1d9b491e49b69be3d6f372dc08da2ac6e90eb7");
        let fusd = SlpToken::new(4, "FUSD".into(), token_id, bch, 0);

        let fusd_balance = fusd.my_balance().wait().unwrap();
        println!("FUSD {}", fusd_balance.spendable);

        let secret = [0; 32];
        let secret_hash = dhash160(&secret);
        let time_lock = (now_ms() / 1000) as u32 - 7200;

        let tx = fusd
            .send_taker_payment(time_lock, &[1; 33], &*secret_hash, 1.into(), &None)
            .wait()
            .unwrap();
        println!("{}", hex::encode(tx.tx_hex()));

        let refund_tx = fusd
            .send_taker_refunds_payment(&tx.tx_hex(), time_lock, &[1; 33], &*secret_hash, &None)
            .wait()
            .unwrap();
        println!("refund hex {}", hex::encode(refund_tx.tx_hex()));
        println!("refund hash {}", hex::encode(refund_tx.tx_hash().0));
    }
}
