use base58::{FromBase58, ToBase58};
use bigdecimal::BigDecimal;
use bitcrypto::{dhash256};
use chrono::prelude::*;
use common::crypto::{blake2b_160, blake2b_256, CryptoOps, CurveType, EcPrivkey, EcPubkey, SecretHash,
                     SecretHashAlgo};
use common::executor::Timer;
use common::impl_base58_checksum_encoding;
use common::mm_ctx::MmArc;
use common::mm_number::MmNumber;
use crate::{TradeInfo, FoundSwapTxSpend, WithdrawRequest};
use derive_more::{Add, Deref, Display};
use futures::{FutureExt, TryFutureExt};
use futures::lock::{Mutex as AsyncMutex};
use futures01::Future;
use num_bigint::{BigInt, BigUint, ToBigInt};
use num_traits::Num;
use num_traits::pow::Pow;
use num_traits::cast::ToPrimitive;
use primitives::hash::{H32, H160, H256, H512};
use rpc::v1::types::{Bytes as BytesJson};
use serde::{Serialize, Serializer, Deserialize};
use serde::de::{Deserializer, Visitor};
use serde_json::{self as json, Value as Json};
use serialization::{Deserializable, deserialize, Reader, Serializable, serialize, Stream};
use std::borrow::Cow;
use std::cmp::PartialEq;
use std::collections::HashMap;
use std::convert::{TryInto, TryFrom};
use std::fmt;
use std::io::Read;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use super::{HistorySyncState, MarketCoinOps, MmCoin, SwapOps, TradeActor, TradeFee,
            Transaction, TransactionDetails, TransactionDetailsFut, TransactionEnum, TransactionFut};
use common::{block_on, now_ms};

pub mod tezos_constants;
use tezos_constants::*;
mod tezos_rpc;

lazy_static! {static ref COUNTER_LOCK: AsyncMutex<()> = AsyncMutex::new(());}

macro_rules! tezos_func {
    ($func:expr $(, $arg_name:ident)*) => {{
        let mut params: Vec<TezosValue> = vec![];
        $(
            params.push($arg_name.into());
        )*
        let args = match params.pop() {
            Some(a) => a,
            None => TezosValue::TezosPrim(TezosPrim::Unit),
        };
        let args = params.into_iter().rev().fold(args, |arg, cur| TezosValue::TezosPrim(TezosPrim::Pair((
            Box::new(cur),
            Box::new(arg)
        ))));
        construct_function_call($func, args)
    }}
}

#[cfg(test)]
mod tezos_tests;

use self::tezos_rpc::{BigMapReq, ForgeOperationsRequest, Operation, PreapplyOperation,
                      PreapplyOperationsRequest, TezosInputType, TezosRpcClient, Transaction as Tx};
use crate::tezos::tezos_rpc::{Reveal, Origination, Status, TransactionParameters, OperationResult};
use crate::tezos::tezos_constants::SECP_PK_PREFIX;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TezosSignature {
    prefix: Vec<u8>,
    data: Vec<u8>,
}

impl_base58_checksum_encoding!(TezosSignature, TezosSignatureVisitor, (5, 73), (3, 71));

pub type TezosAddrPrefix = [u8; 3];
pub type OpHashPrefix = [u8; 2];
pub type BlockHashPrefix = [u8; 2];

const OP_HASH_PREFIX: OpHashPrefix = [5, 116];

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct TezosAddress {
    prefix: TezosAddrPrefix,
    data: H160,
}

impl_base58_checksum_encoding!(TezosAddress, TezosAddressVisitor, (3, 27));

#[derive(Debug, PartialEq)]
pub struct OpHash {
    prefix: OpHashPrefix,
    data: H256,
}

impl_base58_checksum_encoding!(OpHash, OpHashVisitor, (2, 38));

impl OpHash {
    fn from_op_bytes(bytes: &[u8]) -> Self {
        OpHash {
            prefix: OP_HASH_PREFIX,
            data: blake2b_256(bytes),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct TezosBlockHash {
    prefix: BlockHashPrefix,
    data: H256,
}

impl_base58_checksum_encoding!(TezosBlockHash, TezosBlockHashVisitor, (2, 38));

#[derive(Debug, PartialEq)]
struct TezosSecret {
    prefix: [u8; 4],
    data: Vec<u8>,
}

impl_base58_checksum_encoding!(TezosSecret, TezosSecretVisitor, (4, 40), (4, 72));

fn tagged_swap_uuid(uuid: &[u8], i_am: TradeActor) -> Vec<u8> {
    let mut vec = uuid.to_vec();
    match i_am {
        TradeActor::Maker => vec.push(0),
        TradeActor::Taker => vec.push(1),
    };
    vec
}

#[derive(Clone, Debug, PartialEq)]
pub struct TezosPubkey {
    prefix: [u8; 4],
    data: Vec<u8>,
}

impl_base58_checksum_encoding!(TezosPubkey, TezosPubkeyVisitor, (4, 40), (4, 41));

impl From<EcPubkey> for TezosPubkey {
    fn from(pubkey: EcPubkey) -> TezosPubkey {
        let prefix = match pubkey.curve_type {
            CurveType::ED25519 => ED_PK_PREFIX,
            CurveType::SECP256K1 => SECP_PK_PREFIX,
            CurveType::P256 => P256_PK_PREFIX,
        };
        TezosPubkey {
            prefix,
            data: pubkey.bytes
        }
    }
}

impl Serializable for TezosPubkey {
    fn serialize(&self, s: &mut Stream) {
        match self.prefix {
            ED_PK_PREFIX => s.append(&0u8),
            SECP_PK_PREFIX => s.append(&1u8),
            P256_PK_PREFIX => s.append(&2u8),
            _ => unimplemented!(),
        };
        s.append_slice(&self.data);
    }
}

impl Deserializable for TezosPubkey {
    fn deserialize<T>(reader: &mut Reader<T>) -> Result<Self, serialization::Error>
        where Self: Sized, T: std::io::Read
    {
        let tag: u8 = reader.read()?;
        let (prefix, len) = match tag {
            0 => (ED_PK_PREFIX, 32),
            1 => (SECP_PK_PREFIX, 33),
            2 => (P256_PK_PREFIX, 33),
            _ => return Err(serialization::Error::Custom(ERRL!("Unsupported tag {}", tag))),
        };
        let mut data = vec![0; len];
        reader.read_slice(&mut data)?;
        Ok(TezosPubkey {
            prefix,
            data,
        })
    }
}

#[derive(Debug)]
pub enum TezosCoinType {
    /// Tezos or it's forks (Dune, etc.)
    Tezos,
    /// ERC like token with smart contract address
    ERC(TezosAddress),
}

#[derive(Debug)]
struct AddressPrefixes {
    ed25519: TezosAddrPrefix,
    secp256k1: TezosAddrPrefix,
    p256: TezosAddrPrefix,
    originated: TezosAddrPrefix,
}

#[derive(Debug)]
pub struct TezosCoinImpl {
    addr_prefixes: AddressPrefixes,
    coin_type: TezosCoinType,
    decimals: u8,
    priv_key: EcPrivkey,
    pub my_address: TezosAddress,
    required_confirmations: AtomicU64,
    rpc_client: TezosRpcClient,
    pub swap_contract_address: TezosAddress,
    ticker: String,
}

fn address_from_ec_pubkey(prefix: TezosAddrPrefix, pubkey: &EcPubkey) -> TezosAddress {
    TezosAddress {
        prefix,
        data: blake2b_160(&pubkey.bytes),
    }
}

impl TezosCoinImpl {
    fn address_to_contract_id(&self, addr: &TezosAddress) -> Result<ContractId, String> {
        if addr.prefix == self.addr_prefixes.ed25519 {
            Ok(ContractId::PubkeyHash(PubkeyHash {
                curve_type: CurveType::ED25519,
                hash: addr.data.clone(),
            }))
        } else if addr.prefix == self.addr_prefixes.secp256k1 {
            Ok(ContractId::PubkeyHash(PubkeyHash {
                curve_type: CurveType::SECP256K1,
                hash: addr.data.clone(),
            }))
        } else if addr.prefix == self.addr_prefixes.p256 {
            Ok(ContractId::PubkeyHash(PubkeyHash {
                curve_type: CurveType::P256,
                hash: addr.data.clone(),
            }))
        } else if addr.prefix == self.addr_prefixes.originated {
            Ok(ContractId::Originated(addr.data.clone()))
        } else {
            ERR!("Address prefix {:?} doesn't match coin prefixes", addr.prefix)
        }
    }

    fn address_to_pubkey_hash(&self, addr: &TezosAddress) -> Result<PubkeyHash, String> {
        if addr.prefix == self.addr_prefixes.ed25519 {
            Ok(PubkeyHash {
                curve_type: CurveType::ED25519,
                hash: addr.data.clone(),
            })
        } else if addr.prefix == self.addr_prefixes.secp256k1 {
            Ok(PubkeyHash {
                curve_type: CurveType::SECP256K1,
                hash: addr.data.clone(),
            })
        } else if addr.prefix == self.addr_prefixes.p256 {
            Ok(PubkeyHash {
                curve_type: CurveType::P256,
                hash: addr.data.clone(),
            })
        } else {
            ERR!("Address prefix {:?} doesn't match coin prefixes", addr.prefix)
        }
    }

    fn contract_id_to_addr(&self, contract_id: &ContractId) -> TezosAddress {
        match contract_id {
            ContractId::PubkeyHash(key_hash) => match key_hash.curve_type {
                CurveType::ED25519 => TezosAddress {
                    prefix: self.addr_prefixes.ed25519,
                    data: key_hash.hash.clone(),
                },
                CurveType::SECP256K1 => TezosAddress {
                    prefix: self.addr_prefixes.secp256k1,
                    data: key_hash.hash.clone(),
                },
                CurveType::P256 => TezosAddress {
                    prefix: self.addr_prefixes.p256,
                    data: key_hash.hash.clone(),
                },
            },
            ContractId::Originated(hash) => TezosAddress {
                prefix: self.addr_prefixes.originated,
                data: hash.clone(),
            }
        }
    }

    async fn my_erc_account(&self, token_addr: &TezosAddress) -> Result<TezosErcAccount, String> {
        let req = BigMapReq {
            r#type: TezosInputType {
                prim: "address".into(),
            },
            key: TezosValue::String {
                string: self.my_address.to_string(),
            }
        };

        let account = try_s!(self.rpc_client.get_big_map(
            &token_addr.to_string(),
            req,
        ).await).unwrap_or(TezosErcAccount::default());
        Ok(account)
    }

    pub async fn sign_and_send_operation(
        &self,
        amount: BigUint,
        destination: &TezosAddress,
        parameters: Option<TransactionParameters>
    ) -> Result<TezosOperation, String> {
        let mut operations = vec![];
        let _counter_lock = COUNTER_LOCK.lock().await;
        let mut counter = TezosUint(try_s!(self.rpc_client.counter(&self.my_address.to_string()).await) + BigUint::from(1u8));
        let head = try_s!(self.rpc_client.block_header("head").await);
        let manager_key = try_s!(self.rpc_client.manager_key(&self.my_address.to_string()).await);
        if manager_key.is_none() {
            let my_pub: TezosPubkey = self.get_pubkey().into();
            let reveal = Operation::reveal(Reveal {
                counter: counter.clone(),
                fee: BigUint::from(1269u32).into(),
                gas_limit: BigUint::from(10000u32).into(),
                public_key: my_pub,
                source: self.my_address.clone(),
                storage_limit: BigUint::from(0u8).into(),
            });
            operations.push(reveal);
            counter = counter + TezosUint(BigUint::from(1u8));
        };
        let op = Operation::transaction(Tx {
            amount: amount.into(),
            counter: counter.clone(),
            destination: destination.clone(),
            fee: BigUint::from(0100000u32).into(),
            gas_limit: BigUint::from(800000u32).into(),
            parameters,
            source: self.my_address.clone(),
            storage_limit: BigUint::from(60000u32).into(),
        });
        operations.push(op);
        let forge_req = ForgeOperationsRequest {
            branch: head.hash.clone(),
            contents: operations.clone()
        };
        let mut tx_bytes = try_s!(self.rpc_client.forge_operations(&head.chain_id, &head.hash, forge_req).await);
        let mut prefixed = vec![3u8];
        prefixed.append(&mut tx_bytes.0);
        let sig_hash = blake2b_256(&prefixed);
        let sig = try_s!(self.sign_message(&*sig_hash));
        let signature = TezosSignature {
            prefix: ED_SIG_PREFIX.to_vec(),
            data: sig,
        };
        let preapply_req = PreapplyOperationsRequest(vec![PreapplyOperation {
            branch: head.hash,
            contents: operations,
            protocol: head.protocol,
            signature: format!("{}", signature),
        }]);
        try_s!(self.rpc_client.preapply_operations(preapply_req).await);
        prefixed.extend_from_slice(&signature.data);
        prefixed.remove(0);
        let hex_encoded = hex::encode(&prefixed);
        try_s!(self.rpc_client.inject_operation(&hex_encoded).await);
        loop {
            let new_counter = TezosUint(try_s!(self.rpc_client.counter(&self.my_address.to_string()).await));
            if new_counter == counter { break; };
            Timer::sleep(1.).await;
        }
        deserialize(prefixed.as_slice())
            .map_err(|e| ERRL!("Error {:?} on tx {} binary deserialization", e, hex_encoded))
    }

    fn address_from_ec_pubkey(&self, pubkey: &EcPubkey) -> Result<TezosAddress, String> {
        let prefix = match pubkey.curve_type {
            CurveType::SECP256K1 => self.addr_prefixes.secp256k1,
            CurveType::ED25519 => self.addr_prefixes.ed25519,
            CurveType::P256 => self.addr_prefixes.p256,
        };
        Ok(address_from_ec_pubkey(prefix, pubkey))
    }

    async fn validate_htlc_payment(
        &self,
        operation: TezosOperation,
        uuid: Vec<u8>,
        time_lock: u32,
        other_addr: TezosAddress,
        secret_hash: SecretHash,
        amount: BigDecimal,
    ) -> Result<(), String> {
        for op in operation.contents {
            match op {
                TezosOperationEnum::Transaction(tx) => {
                    if tx.source != try_s!(self.address_to_contract_id(&other_addr)) {
                        return ERR!("Invalid transaction source");
                    };

                    if tx.destination != try_s!(self.address_to_contract_id(&self.swap_contract_address)) {
                        return ERR!("Invalid transaction destination");
                    }
                    let amount = try_s!(self.big_decimal_to_big_uint(&amount));
                    let expected_params = match self.coin_type {
                        TezosCoinType::Tezos => {
                            if tx.amount.0 != amount {
                                return ERR!("Invalid transaction amount");
                            }
                            init_tezos_swap_call(uuid.into(), time_lock, secret_hash.to_vec().into(),
                                                 secret_hash.get_algo(), self.my_address.clone())
                        },
                        TezosCoinType::ERC(ref token_addr) =>
                            init_tezos_erc_swap_call(uuid.into(), time_lock, secret_hash.to_vec().into(),
                                                     secret_hash.get_algo(), self.my_address.clone(), amount, token_addr),
                    };
                    if tx.parameters != Some(expected_params.value) {
                        return ERR!("Invalid transaction parameters");
                    };
                    return Ok(())
                },
                TezosOperationEnum::BabylonTransaction(tx) => {
                    if tx.source != try_s!(self.address_to_pubkey_hash(&other_addr)) {
                        return ERR!("Invalid transaction source");
                    };

                    if tx.destination != try_s!(self.address_to_contract_id(&self.swap_contract_address)) {
                        return ERR!("Invalid transaction destination");
                    }
                    let amount = try_s!(self.big_decimal_to_big_uint(&amount));
                    let expected_params = match self.coin_type {
                        TezosCoinType::Tezos => {
                            if tx.amount.0 != amount {
                                return ERR!("Invalid transaction amount");
                            }
                            init_tezos_swap_call(uuid.into(), time_lock, secret_hash.to_vec().into(),
                                                 secret_hash.get_algo(), self.my_address.clone())
                        },
                        TezosCoinType::ERC(ref token_addr) =>
                            init_tezos_erc_swap_call(uuid.into(), time_lock, secret_hash.to_vec().into(),
                                                     secret_hash.get_algo(), self.my_address.clone(), amount, token_addr),
                    };
                    if tx.parameters != Some(expected_params.into()) {
                        return ERR!("Invalid transaction parameters");
                    };
                    return Ok(())
                },
                _ => (),
            }
        }
        ERR!("Operation contents doesn't contain Transaction or BabylonTransaction")
    }

    async fn check_if_payment_sent(&self, uuid: BytesJson, search_from_block: u64) -> Result<Option<TezosOperation>, String> {
        let req = BigMapReq {
            r#type: TezosInputType {
                prim: "bytes".into(),
            },
            key: TezosValue::Bytes {
                bytes: uuid.clone(),
            }
        };
        let swap: Option<TezosAtomicSwap> = try_s!(self.rpc_client.get_big_map(&self.swap_contract_address.to_string(), req).await);
        match swap {
            Some(_swap) => {
                let mut current_block = search_from_block;
                let uuid: TezosValue = uuid.into();

                loop {
                    let operations = try_s!(self.rpc_client.operations(&current_block.to_string()).await);
                    for operation in operations {
                        for transaction in operation.contents {
                            match transaction.op {
                                Operation::transaction(tx) => {
                                    if tx.destination == self.swap_contract_address {
                                        match tx.parameters {
                                            Some(ref params) => {
                                                let (path, args) = read_function_call(vec![], params.value.clone());
                                                let (tx_uuid, _) = args.split_and_read_value();
                                                if (path == [Or::L] || path == [Or::R, Or::L]) && tx_uuid == uuid {
                                                    let operation = TezosOperation {
                                                        branch: operation.branch.data,
                                                        signature: operation.signature.map(|sig| H512::from(sig.data.as_slice())),
                                                        contents: vec![TezosOperationEnum::Transaction(TezosTransaction {
                                                            amount: tx.amount.into(),
                                                            counter: tx.counter.into(),
                                                            destination: try_s!(self.address_to_contract_id(&tx.destination)),
                                                            source: try_s!(self.address_to_contract_id(&tx.source)),
                                                            fee: tx.fee.into(),
                                                            gas_limit: tx.gas_limit.into(),
                                                            storage_limit: tx.storage_limit.into(),
                                                            parameters: tx.parameters.map(|p| p.value),
                                                        })]
                                                    };
                                                    return Ok(Some(operation.into()))
                                                }
                                            },
                                            None => continue,
                                        }
                                    }
                                },
                                _ => continue,
                            }
                        }
                    }
                    current_block += 1;
                }
            },
            None => Ok(None),
        }
    }

    async fn find_op_by_uuid_and_entry_path(
        &self,
        dest: &TezosAddress,
        uuid: &TezosValue,
        entrypoint: &str,
        from_block: u64,
    ) -> Result<Option<TezosOperation>, String> {
        let latest_block_header = try_s!(self.rpc_client.block_header("head").await);
        let latest_block_num = latest_block_header.level;
        let mut current_block = from_block;
        while current_block <= latest_block_num {
            let operations = try_s!(self.rpc_client.operations(&current_block.to_string()).await);
            for operation in operations {
                for transaction in operation.contents {
                    match transaction.op {
                        Operation::transaction(tx) => {
                            if tx.destination == *dest {
                                match tx.parameters {
                                    Some(ref params) => {
                                        let (tx_uuid, _) = params.value.clone().split_and_read_value();
                                        if params.entrypoint == entrypoint && tx_uuid == *uuid {
                                            let operation = TezosOperation {
                                                branch: operation.branch.data,
                                                signature: operation.signature.map(|sig| H512::from(sig.data.as_slice())),
                                                contents: vec![TezosOperationEnum::BabylonTransaction(BabylonTransaction {
                                                    amount: tx.amount.into(),
                                                    counter: tx.counter.into(),
                                                    destination: try_s!(self.address_to_contract_id(&tx.destination)),
                                                    source: try_s!(self.address_to_pubkey_hash(&tx.source)),
                                                    fee: tx.fee.into(),
                                                    gas_limit: tx.gas_limit.into(),
                                                    storage_limit: tx.storage_limit.into(),
                                                    parameters: tx.parameters.map(|p| p.into()),
                                                })]
                                            };
                                            return Ok(Some(operation))
                                        }
                                    },
                                    None => continue,
                                }
                            }
                        },
                        _ => continue,
                    }
                }
            }
            current_block += 1;
        }
        Ok(None)
    }

    async fn search_for_htlc_spend(
        &self,
        tx: &TezosOperation,
        search_from_block: u64,
    ) -> Result<Option<FoundSwapTxSpend>, String> {
        for content in tx.contents.iter() {
            match content {
                TezosOperationEnum::Transaction(op) => {
                    match &op.parameters {
                        Some(params) => {
                            let (_, args) = read_function_call(vec![], params.clone());
                            let (uuid, _) = args.split_and_read_value();
                            let req = BigMapReq {
                                r#type: TezosInputType {
                                    prim: "bytes".into(),
                                },
                                key: uuid.clone(),
                            };
                            let destination = self.contract_id_to_addr(&op.destination);
                            let swap: TezosAtomicSwap = match try_s!(self.rpc_client.get_big_map(&destination.to_string(), req).await) {
                                Some(s) => s,
                                None => return ERR!("Swap with uuid {:?} is not found", uuid),
                            };

                            match swap.state {
                                TezosAtomicSwapState::ReceiverSpent => {
                                    let found = try_s!(self.find_op_by_uuid_and_entry_path(
                                        &destination,
                                        &uuid,
                                        "receiver_spends",
                                        search_from_block,
                                    ).await);
                                    let found = try_s!(found.ok_or(ERRL!("Atomic swap state is ReceiverSpent, but corresponding transaction wasn't found")));
                                    return Ok(Some(FoundSwapTxSpend::Spent(found.into())))
                                },
                                TezosAtomicSwapState::SenderRefunded => {
                                    let found = try_s!(self.find_op_by_uuid_and_entry_path(
                                        &destination,
                                        &uuid,
                                        "sender_refunds",
                                        search_from_block,
                                    ).await);
                                    let found = try_s!(found.ok_or(ERRL!("Atomic swap state is SenderRefunded, but corresponding transaction wasn't found")));
                                    return Ok(Some(FoundSwapTxSpend::Refunded(found.into())))
                                },
                                TezosAtomicSwapState::Initialized => return Ok(None),
                            }
                        },
                        None => return ERR!("Operation params can't be None"),
                    }
                },
                TezosOperationEnum::BabylonTransaction(op) => {
                    match &op.parameters {
                        Some(params) => {
                            let (_, args) = read_function_call(vec![], params.params.clone());
                            let (uuid, _) = args.split_and_read_value();
                            let req = BigMapReq {
                                r#type: TezosInputType {
                                    prim: "bytes".into(),
                                },
                                key: uuid.clone(),
                            };
                            let destination = self.contract_id_to_addr(&op.destination);
                            let swap: TezosAtomicSwap = match try_s!(self.rpc_client.get_big_map(&destination.to_string(), req).await) {
                                Some(s) => s,
                                None => return ERR!("Swap with uuid {:?} is not found", uuid),
                            };

                            match swap.state {
                                TezosAtomicSwapState::ReceiverSpent => {
                                    let found = try_s!(self.find_op_by_uuid_and_entry_path(
                                        &destination,
                                        &uuid,
                                        "receiver_spends",
                                        search_from_block,
                                    ).await);
                                    let found = try_s!(found.ok_or(ERRL!("Atomic swap state is ReceiverSpent, but corresponding transaction wasn't found")));
                                    return Ok(Some(FoundSwapTxSpend::Spent(found.into())))
                                },
                                TezosAtomicSwapState::SenderRefunded => {
                                    let found = try_s!(self.find_op_by_uuid_and_entry_path(
                                        &destination,
                                        &uuid,
                                        "sender_refunds",
                                        search_from_block,
                                    ).await);
                                    let found = try_s!(found.ok_or(ERRL!("Atomic swap state is SenderRefunded, but corresponding transaction wasn't found")));
                                    return Ok(Some(FoundSwapTxSpend::Refunded(found.into())))
                                },
                                TezosAtomicSwapState::Initialized => return Ok(None),
                            }
                        },
                        None => return ERR!("Operation params can't be None"),
                    }
                },
                _ => (),
            }
        };
        ERR!("Operation contents do not contain Transaction or BabylonTransaction")
    }

    pub fn big_decimal_to_big_int(&self, decimal: &BigDecimal) -> Result<BigInt, String> {
        let denominated = decimal * BigDecimal::from(10u64.pow(self.decimals as u32));
        denominated.to_bigint().ok_or(ERRL!("Couldn't create BigInt from {}", denominated))
    }

    pub fn big_decimal_to_big_uint(&self, decimal: &BigDecimal) -> Result<BigUint, String> {
        let big_int = try_s!(self.big_decimal_to_big_int(decimal));
        big_int.to_biguint().ok_or(ERRL!("Couldn't create BigUint from {}", big_int))
    }
}

#[derive(Clone, Debug)]
pub struct TezosCoin(Arc<TezosCoinImpl>);

impl Deref for TezosCoin {type Target = TezosCoinImpl; fn deref (&self) -> &TezosCoinImpl {&*self.0}}

impl TezosCoin {
    async fn check_and_update_allowance(&self, token_addr: &TezosAddress, spender: &TezosAddress, amount: &BigUint) -> Result<(), String> {
        let my_account = try_s!(self.my_erc_account(token_addr).await);
        let zero = BigUint::from(0u8);
        let current_allowance = my_account.allowances.get(&spender).unwrap_or(&zero);
        if current_allowance < amount {
            let args = erc_approve_call(spender, &my_account.balance);
            let op = try_s!(self.sign_and_send_operation(zero, token_addr, Some(args)).await);
            try_s!(self.wait_for_operation_confirmation(
                op.op_hash(),
                1,
                now_ms() / 1000 + 120,
                10,
                op.branch.clone(),
            ).await);
            Ok(())
        } else {
            Ok(())
        }
    }

    pub async fn wait_for_operation_confirmation(
        &self,
        op_hash: OpHash,
        confirmations: u64,
        wait_until: u64,
        check_every: u64,
        since_branch: H256,
    ) -> Result<Vec<OperationResult>, String> {
        let block_hash = TezosBlockHash {
            prefix: BLOCK_HASH_PREFIX,
            data: since_branch,
        };
        let since_block_header = try_s!(self.rpc_client.block_header(&block_hash.to_string()).await);
        let mut found_tx_in_block = None;
        loop {
            if now_ms() / 1000 > wait_until {
                return ERR!("Waited too long until {} for transaction {} to be confirmed {} times", wait_until, op_hash, confirmations);
            }

            let current_head = try_s!(self.rpc_client.block_header("head").await);
            if current_head.level > since_block_header.level {
                let mut cur_block = since_block_header.level;
                if found_tx_in_block.is_none() {
                    while cur_block <= current_head.level {
                        let operations = try_s!(self.rpc_client.operation_hashes(&cur_block.to_string()).await);
                        for (validation, operation) in operations.into_iter().enumerate() {
                            for (offset, op) in operation.into_iter().enumerate() {
                                if op == op_hash.to_string() {
                                    found_tx_in_block = Some((cur_block, validation, offset));
                                }
                            }
                        }
                        cur_block += 1;
                        Timer::sleep(1.).await
                    }
                }
            }
            if let Some((block_number, validation, offset)) = found_tx_in_block {
                let op_from_rpc = try_s!(self.rpc_client.single_operation(&
                    block_number.to_string(),
                    validation,
                    offset,
                ).await);
                for content in op_from_rpc.contents.iter() {
                    let operation_result = try_s!(content.metadata.operation_result.as_ref().ok_or(ERRL!("One of {:?} operation_result is None", op_from_rpc)));
                    if operation_result.status != Status::applied {
                        return ERR!("All {:?} statuses must be `applied`", op_from_rpc);
                    }
                }
                let current_confirmations = current_head.level - block_number + 1;
                if current_confirmations >= confirmations {
                    return Ok(op_from_rpc.contents);
                }
            }
            Timer::sleep(check_every as f64).await
        }
    }

    fn spend_htlc_payment(&self, uuid: &[u8], tx: &[u8], secret: &[u8]) -> TransactionFut {
        let operation: TezosOperation = try_fus!(deserialize(tx).map_err(|e| fomat!([e])));
        let args = receiver_spends_call(
            uuid.into(),
            secret.to_vec().into(),
            &self.my_address,
        );

        let dest = try_fus!(operation.first_tx_destination().ok_or(format!("Failed to get destination from operation {:?}", operation)));
        let dest = self.contract_id_to_addr(&dest);
        let coin = self.clone();
        let fut = async move {
            coin.sign_and_send_operation(BigUint::from(0u8), &dest, Some(args)).await
        };
        Box::new(fut.boxed().compat().map(|tx| tx.into()))
    }

    fn refund_htlc_payment(&self, uuid: &[u8], tx: &[u8]) -> TransactionFut {
        let operation: TezosOperation = try_fus!(deserialize(tx).map_err(|e| fomat!([e])));
        let args = sender_refunds_call(
            uuid.into(),
            &self.my_address,
        );

        let dest = try_fus!(operation.first_tx_destination().ok_or(format!("Failed to get destination from operation {:?}", operation)));
        let dest = self.contract_id_to_addr(&dest);
        let coin = self.clone();
        let fut = async move {
            coin.sign_and_send_operation(BigUint::from(0u8), &dest, Some(args)).await
        };
        Box::new(fut.boxed().compat().map(|tx| tx.into()))
    }
}

impl MarketCoinOps for TezosCoin {
    fn ticker (&self) -> &str {
        &self.ticker
    }

    fn my_address(&self) -> Cow<str> {
        format!("{}", self.my_address).into()
    }

    fn my_balance(&self) -> Box<dyn Future<Item=BigDecimal, Error=String> + Send> {
        let selfi = self.clone();
        let addr = format!("{}", self.my_address);
        let fut = Box::pin(async move {
            match &selfi.coin_type {
                TezosCoinType::Tezos => selfi.rpc_client.get_balance(&addr).await,
                TezosCoinType::ERC(token_addr) => {
                    let my_account = try_s!(selfi.my_erc_account(token_addr).await);
                    Ok(BigDecimal::from(BigInt::from(my_account.balance)))
                }
            }
        });
        let divisor = BigDecimal::from(10u64.pow(self.decimals as u32));
        Box::new(fut.compat().map(move |balance| balance / divisor))
    }

    /// Receives raw transaction bytes in hexadecimal format as input and returns tx hash in hexadecimal format
    fn send_raw_tx(&self, tx: &str) -> Box<dyn Future<Item=String, Error=String> + Send> {
        let client = self.rpc_client.clone();
        let tx = tx.to_owned();
        let fut = Box::pin(async move {
            client.inject_operation(&tx).await
        });
        Box::new(fut.compat())
    }

    fn wait_for_confirmations(
        &self,
        tx: &[u8],
        confirmations: u64,
        wait_until: u64,
        check_every: u64,
        _since_block: u64
    ) -> Box<dyn Future<Item=(), Error=String> + Send> {
        let coin = self.clone();
        let op: TezosOperation = try_fus!(deserialize(tx).map_err(|e| fomat!([e])));
        let fut = async move {
            try_s!(coin.wait_for_operation_confirmation(op.op_hash(), confirmations, wait_until, check_every, op.branch.clone()).await);
            Ok(())
        };
        Box::new(Box::pin(fut).compat())
    }

    fn wait_for_tx_spend(&self, transaction: &[u8], wait_until: u64, from_block: u64) -> TransactionFut {
        let coin = self.clone();
        let tx: TezosOperation = try_fus!(deserialize(transaction).map_err(|e| fomat!([e])));
        let fut = async move {
            loop {
                let search_fut = coin.search_for_htlc_spend(&tx, from_block);
                let found_something = match search_fut.await {
                    Ok(f) => f,
                    Err(e) => {
                        log!("Error " (e) " on searching for tx spend");
                        if now_ms() / 1000 > wait_until {
                            return ERR!("Have been waiting too long until {} for transaction {:?} to be spent", wait_until, tx);
                        }

                        Timer::sleep(10.).await;
                        continue;
                    }
                };
                match found_something {
                    Some(FoundSwapTxSpend::Spent(tx)) => break Ok(tx.into()),
                    Some(FoundSwapTxSpend::Refunded(tx)) => break ERR!("Transaction {:?} was refunded", tx),
                    None => {
                        if now_ms() / 1000 > wait_until {
                            return ERR!("Have been waiting too long until {} for transaction {:?} to be spent", wait_until, tx);
                        }

                        Timer::sleep(10.).await;
                    },
                }
            }
        };
        let fut = Box::pin(fut);
        Box::new(fut.compat())
    }

    fn tx_enum_from_bytes(&self, bytes: &[u8]) -> Result<TransactionEnum, String> {
        let tx: TezosOperation = try_s!(deserialize(bytes).map_err(|e| fomat!([e])));
        Ok(tx.into())
    }

    fn current_block(&self) -> Box<dyn Future<Item=u64, Error=String> + Send> {
        let client = self.rpc_client.clone();
        let fut = Box::pin(async move {
            client.block_header("head").await
        });
        Box::new(fut.compat().map(|header| header.level))
    }

    fn address_from_pubkey_str(&self, pubkey: &str) -> Result<String, String> {
        let pubkey_bytes = try_s!(hex::decode(pubkey));
        let pubkey: EcPubkey = try_s!(deserialize(pubkey_bytes.as_slice()).map_err(|e| ERRL!("{:?}", e)));
        let address = try_s!(self.address_from_ec_pubkey(&pubkey));
        Ok(address.to_string())
    }

    fn derive_address_from_ec_pubkey(&self, pubkey: &EcPubkey) -> Result<String, String> {
        self.address_from_ec_pubkey(pubkey).map(|addr| addr.to_string())
    }

    fn tx_hash_to_string(&self, hash: &[u8]) -> String {
        let data = H256::from(hash);
        OpHash {
            prefix: OP_HASH_PREFIX,
            data,
        }.to_string()
    }
}

async fn send_htlc_payment(
    coin: TezosCoin,
    uuid: Vec<u8>,
    time_lock: u32,
    other_pub: EcPubkey,
    secret_hash: SecretHash,
    amount: BigDecimal,
) -> Result<TransactionDetails, String> {
    let other_addr = try_s!(coin.address_from_ec_pubkey(&other_pub));

    let (amount, args) = match &coin.coin_type {
        TezosCoinType::Tezos => {
            let args = init_tezos_swap_call(
                uuid.into(),
                time_lock,
                secret_hash.to_vec().into(),
                secret_hash.get_algo(),
                other_addr,
            );
            let amount = try_s!(coin.big_decimal_to_big_uint(&amount));
            (amount, args)
        },
        TezosCoinType::ERC(token_addr) => {
            let amount = try_s!(coin.big_decimal_to_big_uint(&amount));
            try_s!(coin.check_and_update_allowance(token_addr, &coin.swap_contract_address, &amount).await);
            let args = init_tezos_erc_swap_call(
                uuid.into(),
                time_lock,
                secret_hash.to_vec().into(),
                secret_hash.get_algo(),
                other_addr,
                amount,
                token_addr,
            );
            (BigUint::from(0u8), args)
        }
    };
    let tx = try_s!(coin.sign_and_send_operation(amount, &coin.swap_contract_address, Some(args)).await);
    Ok(TransactionDetails {
        block_height: 0,
        coin: coin.ticker.clone(),
        fee_details: None,
        from: vec![],
        internal_id: vec![].into(),
        my_balance_change: 0.into(),
        received_by_me: 0.into(),
        spent_by_me: 0.into(),
        timestamp: now_ms() / 1000,
        to: vec![],
        tx_hash: coin.tx_hash_to_string(&tx.tx_hash()),
        total_amount: 0.into(),
        tx_hex: tx.tx_hex().into(),
    })
}

impl SwapOps for TezosCoin {
    fn send_taker_fee(&self, fee_pubkey: &EcPubkey, amount: BigDecimal) -> TransactionDetailsFut {
        let prefix = match fee_pubkey.curve_type {
            CurveType::SECP256K1 => self.addr_prefixes.secp256k1,
            CurveType::ED25519 => self.addr_prefixes.ed25519,
            CurveType::P256 => self.addr_prefixes.p256,
        };
        let fee_addr = TezosAddress {
            prefix,
            data: blake2b_160(&fee_pubkey.bytes),
        };
        let ticker = self.ticker.clone();
        let coin = self.clone();
        let fut = Box::pin(async move {
            let (amount, dest, args) = match coin.coin_type {
                TezosCoinType::Tezos => {
                    let amount = try_s!(coin.big_decimal_to_big_uint(&amount));
                    (amount, fee_addr, None)
                },
                TezosCoinType::ERC(ref token_addr) => {
                    let amount = try_s!(coin.big_decimal_to_big_uint(&amount));
                    let args = mla_transfer_call(
                        &coin.my_address,
                        &fee_addr,
                        &amount,
                    );
                    (BigUint::from(0u8), token_addr.clone(), Some(args))
                }
            };
            let tx = try_s!(coin.sign_and_send_operation(amount, &dest, args).await);
            Ok(TransactionDetails {
                block_height: 0,
                coin: ticker,
                fee_details: None,
                from: vec![],
                internal_id: vec![].into(),
                my_balance_change: 0.into(),
                received_by_me: 0.into(),
                spent_by_me: 0.into(),
                timestamp: now_ms() / 1000,
                to: vec![],
                tx_hash: coin.tx_hash_to_string(&tx.tx_hash()),
                total_amount: 0.into(),
                tx_hex: tx.tx_hex().into(),
            })
        }).compat();
        Box::new(fut)
    }

    fn send_maker_payment(
        &self,
        uuid: &[u8],
        time_lock: u32,
        taker_pub: &EcPubkey,
        secret_hash: &SecretHash,
        amount: BigDecimal,
    ) -> TransactionDetailsFut {
        let uuid = tagged_swap_uuid(uuid, TradeActor::Maker);
        let fut = Box::pin(send_htlc_payment(self.clone(), uuid, time_lock, taker_pub.clone(), secret_hash.clone(), amount)).compat();
        Box::new(fut)
    }

    fn send_taker_payment(
        &self,
        uuid: &[u8],
        time_lock: u32,
        maker_pub: &EcPubkey,
        secret_hash: &SecretHash,
        amount: BigDecimal,
    ) -> TransactionDetailsFut {
        let uuid = tagged_swap_uuid(uuid, TradeActor::Taker);
        let fut = Box::pin(send_htlc_payment(self.clone(), uuid, time_lock, maker_pub.clone(), secret_hash.clone(), amount)).compat();
        Box::new(fut)
    }

    fn send_maker_spends_taker_payment(
        &self,
        uuid: &[u8],
        taker_payment_tx: &[u8],
        _time_lock: u32,
        _taker_pub: &EcPubkey,
        secret: &[u8],
        _secret_hash: &SecretHash,
    ) -> TransactionFut {
        let uuid = tagged_swap_uuid(uuid, TradeActor::Taker);
        self.spend_htlc_payment(&uuid, taker_payment_tx, secret)
    }

    fn send_taker_spends_maker_payment(
        &self,
        uuid: &[u8],
        maker_payment_tx: &[u8],
        _time_lock: u32,
        _maker_pub: &EcPubkey,
        secret: &[u8],
        _secret_hash: &SecretHash,
    ) -> TransactionFut {
        let uuid = tagged_swap_uuid(uuid, TradeActor::Maker);
        self.spend_htlc_payment(&uuid, maker_payment_tx, secret)
    }

    fn send_taker_refunds_payment(
        &self,
        uuid: &[u8],
        taker_payment_tx: &[u8],
        _time_lock: u32,
        _maker_pub: &EcPubkey,
        _secret_hash: &SecretHash,
    ) -> TransactionFut {
        let uuid = tagged_swap_uuid(uuid, TradeActor::Taker);
        self.refund_htlc_payment(&uuid, taker_payment_tx)
    }

    fn send_maker_refunds_payment(
        &self,
        uuid: &[u8],
        maker_payment_tx: &[u8],
        _time_lock: u32,
        _taker_pub: &EcPubkey,
        _secret_hash: &SecretHash,
    ) -> TransactionFut {
        let uuid = tagged_swap_uuid(uuid, TradeActor::Maker);
        self.refund_htlc_payment(&uuid, maker_payment_tx)
    }

    fn validate_fee(
        &self,
        fee_tx: &TransactionEnum,
        fee_pubkey: &EcPubkey,
        taker_pubkey: &EcPubkey,
        amount: &BigDecimal,
    ) -> Box<dyn Future<Item=(), Error=String> + Send> {
        let op = match fee_tx {
            TransactionEnum::TezosOperation(op) => op.clone(),
            _ => unimplemented!(),
        };
        let fee_addr = try_fus!(self.address_from_ec_pubkey(fee_pubkey));
        let taker_addr = try_fus!(self.address_from_ec_pubkey(taker_pubkey));
        let amount = try_fus!(self.big_decimal_to_big_uint(&amount));
        let coin = self.clone();
        let fut = async move {
            for content in op.contents.iter() {
                match content {
                    TezosOperationEnum::Transaction(ref tx) => {
                        match coin.coin_type {
                            TezosCoinType::Tezos => {
                                if tx.amount.0 != amount {
                                    return ERR!("Invalid dex fee tx amount");
                                }
                                let fee_contract_id = try_s!(coin.address_to_contract_id(&fee_addr));
                                if tx.destination != fee_contract_id {
                                    return ERR!("Invalid dex fee tx destination");
                                }
                            },
                            TezosCoinType::ERC(ref token_addr) => {
                                let token_contract_id = try_s!(coin.address_to_contract_id(token_addr));
                                if tx.destination != token_contract_id {
                                    return ERR!("Invalid dex fee tx destination");
                                }
                                let expected_params = mla_transfer_call(&taker_addr, &fee_addr, &amount);
                                if tx.parameters != Some(expected_params.value) {
                                    return ERR!("Invalid dex fee tx parameters");
                                }
                            },
                        };
                        try_s!(coin.wait_for_operation_confirmation(
                            op.op_hash(),
                            coin.required_confirmations(),
                            now_ms() / 1000 + 120,
                            10,
                            op.branch.clone(),
                        ).await);
                        return Ok(());
                    },
                    TezosOperationEnum::BabylonTransaction(ref tx) => {
                        match coin.coin_type {
                            TezosCoinType::Tezos => {
                                if tx.amount.0 != amount {
                                    return ERR!("Invalid dex fee tx amount");
                                }
                                let fee_contract_id = try_s!(coin.address_to_contract_id(&fee_addr));
                                if tx.destination != fee_contract_id {
                                    return ERR!("Invalid dex fee tx destination");
                                }
                            },
                            TezosCoinType::ERC(ref token_addr) => {
                                let token_contract_id = try_s!(coin.address_to_contract_id(token_addr));
                                if tx.destination != token_contract_id {
                                    return ERR!("Invalid dex fee tx destination, expected {:?}, actual {:?}", token_contract_id, tx.destination);
                                }
                                let expected_params = Some(mla_transfer_call(&taker_addr, &fee_addr, &amount).into());
                                if tx.parameters != expected_params {
                                    return ERR!("Invalid dex fee tx parameters, expected {:?}, actual {:?}", expected_params, tx.parameters);
                                }
                            },
                        };
                        try_s!(coin.wait_for_operation_confirmation(
                            op.op_hash(),
                            coin.required_confirmations(),
                            now_ms() / 1000 + 120,
                            10,
                            op.branch.clone(),
                        ).await);
                        return Ok(());
                    },
                    _ => (),
                }
            }
            ERR!("Didn't find TezosOperationEnum::Transaction or TezosOperationEnum::BabylonTransaction in operation contents")
        };
        Box::new(Box::pin(fut).compat())
    }

    fn validate_maker_payment(
        &self,
        uuid: &[u8],
        payment_tx: &[u8],
        time_lock: u32,
        maker_pub: &EcPubkey,
        secret_hash: &SecretHash,
        amount: BigDecimal,
    ) -> Box<dyn Future<Item=(), Error=String> + Send> {
        let operation: TezosOperation = try_fus!(deserialize(payment_tx).map_err(|e|  fomat!([e])));
        let maker_addr = try_fus!(self.address_from_ec_pubkey(maker_pub));
        let uuid = tagged_swap_uuid(uuid, TradeActor::Maker);
        let secret_hash = secret_hash.clone();
        let coin = self.clone();
        let fut = async move {
            coin.validate_htlc_payment(operation, uuid, time_lock, maker_addr, secret_hash, amount).await
        };
        Box::new(Box::pin(fut).compat())
    }

    fn validate_taker_payment(
        &self,
        uuid: &[u8],
        payment_tx: &[u8],
        time_lock: u32,
        taker_pub: &EcPubkey,
        secret_hash: &SecretHash,
        amount: BigDecimal,
    ) -> Box<dyn Future<Item=(), Error=String> + Send> {
        let operation: TezosOperation = try_fus!(deserialize(payment_tx).map_err(|e| fomat!([e])));
        let taker_addr = try_fus!(self.address_from_ec_pubkey(taker_pub));
        let uuid = tagged_swap_uuid(uuid, TradeActor::Taker);
        let secret_hash = secret_hash.clone();
        let coin = self.clone();
        let fut = async move {
            coin.validate_htlc_payment(operation, uuid, time_lock, taker_addr, secret_hash, amount).await
        };
        Box::new(Box::pin(fut).compat())
    }

    fn check_if_my_maker_payment_sent(
        &self,
        uuid: &[u8],
        _time_lock: u32,
        _other_pub: &EcPubkey,
        _secret_hash: &SecretHash,
        search_from_block: u64,
    ) -> Box<dyn Future<Item=Option<TransactionEnum>, Error=String> + Send> {
        let uuid = BytesJson(tagged_swap_uuid(uuid, TradeActor::Maker));
        let coin = self.clone();
        let fut = async move {
            let tx = try_s!(coin.check_if_payment_sent(uuid, search_from_block).await);
            Ok(tx.map(|tx| tx.into()))
        };
        Box::new(Box::pin(fut).compat())
    }

    fn check_if_my_taker_payment_sent(
        &self,
        uuid: &[u8],
        _time_lock: u32,
        _other_pub: &EcPubkey,
        _secret_hash: &SecretHash,
        search_from_block: u64,
    ) -> Box<dyn Future<Item=Option<TransactionEnum>, Error=String> + Send> {
        let uuid = BytesJson(tagged_swap_uuid(uuid, TradeActor::Taker));
        let coin = self.clone();
        let fut = async move {
            let tx = try_s!(coin.check_if_payment_sent(uuid, search_from_block).await);
            Ok(tx.map(|tx| tx.into()))
        };
        Box::new(Box::pin(fut).compat())
    }

    fn search_for_swap_tx_spend_my(
        &self,
        _time_lock: u32,
        _other_pub: &EcPubkey,
        _secret_hash: &SecretHash,
        tx: &[u8],
        search_from_block: u64,
    ) -> Box<dyn Future<Item=Option<FoundSwapTxSpend>, Error=String> + Send> {
        let coin = self.clone();
        let tx: TezosOperation = try_fus!(deserialize(tx).map_err(|e| fomat!([e])));
        let fut = async move {
            coin.search_for_htlc_spend(&tx, search_from_block).await
        };
        let fut = Box::pin(fut);
        Box::new(fut.compat())
    }

    fn search_for_swap_tx_spend_other(
        &self,
        _time_lock: u32,
        _other_pub: &EcPubkey,
        _secret_hash: &SecretHash,
        tx: &[u8],
        search_from_block: u64,
    ) -> Box<dyn Future<Item=Option<FoundSwapTxSpend>, Error=String> + Send> {
        let coin = self.clone();
        let tx: TezosOperation = try_fus!(deserialize(tx).map_err(|e| fomat!([e])));
        let fut = async move {
            coin.search_for_htlc_spend(&tx, search_from_block).await
        };
        let fut = Box::pin(fut);
        Box::new(fut.compat())
    }

    fn supported_secret_hash_algos(&self) -> &[SecretHashAlgo] {
        &[
            SecretHashAlgo::Sha256,
            SecretHashAlgo::Blake2b256,
        ]
    }
}

impl Transaction for TezosOperation {
    fn tx_hex(&self) -> Vec<u8> {
        serialize(self).take()
    }

    fn extract_secret(&self) -> Result<Vec<u8>, String> {
        match &self.contents[0] {
            TezosOperationEnum::Transaction(tx) => {
                match &tx.parameters {
                    Some(params) => {
                        let (path, args) = read_function_call(vec![], params.clone());
                        if path == vec![Or::R, Or::R, Or::L] {
                            let values = args.values_vec(vec![]);
                            match values.get(1) {
                                Some(val) => match val {
                                    TezosValue::Bytes { bytes } => Ok(bytes.0.clone()),
                                    _ => ERR!("The argument at index 1 must be TezosValue::Bytes, got {:?}", val),
                                },
                                None => ERR!("There's no argument at index 1"),
                            }
                        } else {
                            ERR!("Invalid function call")
                        }
                    },
                    None => ERR!("parameters are None"),
                }
            },
            TezosOperationEnum::BabylonTransaction(tx) => {
                match &tx.parameters {
                    Some(params) => {
                        let (_, args) = read_function_call(vec![], params.params.clone());
                        let values = args.values_vec(vec![]);
                        match values.get(1) {
                            Some(val) => match val {
                                TezosValue::Bytes { bytes } => Ok(bytes.0.clone()),
                                _ => ERR!("The argument at index 1 must be TezosValue::Bytes, got {:?}", val),
                            },
                            None => ERR!("There's no argument at index 1"),
                        }
                    },
                    None => ERR!("parameters are None"),
                }
            },
            _ => ERR!("Can't extract secret from non-Transaction operation"),
        }
    }

    fn tx_hash(&self) -> BytesJson {
        blake2b_256(&serialize(self).take()).to_vec().into()
    }
}

async fn withdraw_impl(coin: TezosCoin, req: WithdrawRequest) -> Result<TransactionDetails, String> {
    let mut operations = vec![];
    let _counter_lock = COUNTER_LOCK.lock().await;
    let mut counter = TezosUint(try_s!(coin.rpc_client.counter(&coin.my_address.to_string()).await) + BigUint::from(1u8));
    let head = try_s!(coin.rpc_client.block_header("head").await);
    let manager_key = try_s!(coin.rpc_client.manager_key(&coin.my_address.to_string()).await);
    if manager_key.is_none() {
        let my_pub: TezosPubkey = coin.get_pubkey().into();
        let reveal = Operation::reveal(Reveal {
            counter: counter.clone(),
            fee: BigUint::from(1269u32).into(),
            gas_limit: BigUint::from(10000u32).into(),
            public_key: my_pub,
            source: coin.my_address.clone(),
            storage_limit: BigUint::from(0u8).into(),
        });
        operations.push(reveal);
        counter = counter + TezosUint(BigUint::from(1u8));
    };
    let to_addr: TezosAddress = try_s!(req.to.parse());
    match &coin.coin_type {
        TezosCoinType::Tezos => operations.push(Operation::transaction(Tx{
            amount: try_s!(coin.big_decimal_to_big_uint(&req.amount)).into(),
            counter,
            destination: to_addr,
            fee: BigUint::from(1420u32).into(),
            gas_limit: BigUint::from(10600u32).into(),
            parameters: None,
            source: coin.my_address.clone(),
            storage_limit: BigUint::from(300u32).into(),
        })),
        TezosCoinType::ERC(addr) => {
            let amount = try_s!(coin.big_decimal_to_big_uint(&req.amount));
            let parameters = Some(mla_transfer_call(&coin.my_address, &to_addr, &amount));
            operations.push(Operation::transaction(Tx {
                amount: BigUint::from(0u8).into(),
                counter,
                destination: addr.clone(),
                fee: BigUint::from(100000u32).into(),
                gas_limit: BigUint::from(800000u32).into(),
                parameters,
                source: coin.my_address.clone(),
                storage_limit: BigUint::from(60000u32).into(),
            }));
        },
    };
    let forge_req = ForgeOperationsRequest {
        branch: head.hash.clone(),
        contents: operations.clone()
    };
    let mut tx_bytes = try_s!(coin.rpc_client.forge_operations(&head.chain_id, &head.hash, forge_req).await);
    let mut prefixed = vec![3u8];
    prefixed.append(&mut tx_bytes.0);
    let sig_hash = blake2b_256(&prefixed);
    let sig = try_s!(coin.priv_key.sign_message(&*sig_hash));
    let signature = TezosSignature {
        prefix: ED_SIG_PREFIX.to_vec(),
        data: sig,
    };
    let preapply_req = PreapplyOperationsRequest(vec![PreapplyOperation {
        branch: head.hash,
        contents: operations,
        protocol: head.protocol,
        signature: format!("{}", signature),
    }]);
    try_s!(coin.rpc_client.preapply_operations(preapply_req).await);
    prefixed.extend_from_slice(&signature.data);
    prefixed.remove(0);
    let op_hash = OpHash::from_op_bytes(&prefixed);
    let details = TransactionDetails {
        coin: coin.ticker.clone(),
        to: vec![req.to],
        from: vec![coin.my_address().into()],
        fee_details: None,
        tx_hex: prefixed.into(),
        block_height: 0,
        my_balance_change: 0.into(),
        total_amount: 0.into(),
        internal_id: vec![].into(),
        timestamp: 0,
        received_by_me: 0.into(),
        spent_by_me: 0.into(),
        tx_hash: op_hash.to_string()
    };
    Ok(details)
}

impl MmCoin for TezosCoin {
    fn is_asset_chain(&self) -> bool {
        false
    }

    fn check_i_have_enough_to_trade(&self, amount: &MmNumber, balance: &MmNumber, trade_info: TradeInfo) -> Box<dyn Future<Item=(), Error=String> + Send> {
        Box::new(futures01::future::ok(()))
    }

    fn can_i_spend_other_payment(&self) -> Box<dyn Future<Item=(), Error=String> + Send> {
        Box::new(futures01::future::ok(()))
    }

    fn withdraw(&self, req: WithdrawRequest) -> Box<dyn Future<Item=TransactionDetails, Error=String> + Send> {
        Box::new(Box::pin(withdraw_impl(self.clone(), req)).compat())
    }

    fn decimals(&self) -> u8 {
        self.decimals
    }

    fn process_history_loop(&self, ctx: MmArc) {
        unimplemented!()
    }

    fn tx_details_by_hash(&self, hash: &[u8]) -> Box<dyn Future<Item=TransactionDetails, Error=String> + Send> {
        Box::new(futures01::future::ok(TransactionDetails::default()))
    }

    fn history_sync_status(&self) -> HistorySyncState {
        unimplemented!()
    }

    /// Get fee to be paid per 1 swap transaction
    fn get_trade_fee(&self) -> Box<dyn Future<Item=TradeFee, Error=String> + Send> {
        Box::new(futures01::future::ok(TradeFee {
            coin: "XTZ".into(),
            amount: 0.into(),
        }))
    }

    fn required_confirmations(&self) -> u64 {
        self.required_confirmations.load(AtomicOrdering::Relaxed)
    }

    fn set_required_confirmations(&self, confirmations: u64) {
        self.required_confirmations.store(confirmations, AtomicOrdering::Relaxed);
    }
}

pub async fn tezos_coin_from_conf_and_request(
    ticker: &str,
    conf: &Json,
    req: &Json,
    priv_key: &[u8],
) -> Result<TezosCoin, String> {
    let mut urls: Vec<String> = try_s!(json::from_value(req["urls"].clone()));
    if urls.is_empty() {
        return ERR!("Enable request for Tezos coin protocol must have at least 1 node URL");
    }
    let rpc_client = try_s!(TezosRpcClient::new(urls));
    let priv_key = try_s!(EcPrivkey::new(CurveType::ED25519, priv_key));
    let addr_prefixes = AddressPrefixes {
        ed25519: try_s!(json::from_value(conf["ed25519_addr_prefix"].clone())),
        secp256k1: try_s!(json::from_value(conf["secp256k1_addr_prefix"].clone())),
        p256: try_s!(json::from_value(conf["p256_addr_prefix"].clone())),
        originated: [2, 90, 121],
    };
    let pubkey = priv_key.get_pubkey();
    let my_address = address_from_ec_pubkey(addr_prefixes.ed25519, &pubkey);
    let (decimals, coin_type) = match conf["protocol"]["token_type"].as_str() {
        Some("TEZOS") => {
            let decimals = conf["decimals"].as_u64().unwrap_or (6) as u8;
            (decimals, TezosCoinType::Tezos)
        },
        Some("MLA") => {
            let addr_str = try_s!(conf["protocol"]["contract_address"].as_str().ok_or("protocol.contract_address is not string"));
            let addr = try_s!(TezosAddress::from_str(addr_str));
            let storage: TezosMlaStorage = try_s!(rpc_client.get_storage(&addr.to_string()).await);
            if storage.is_paused {
                return ERR!("Contract {} is in paused state", addr);
            }
            (6, TezosCoinType::ERC(addr))
        },
        _ => return ERR!("Unsupported token type {:?}", conf["protocol"]["token_type"]),
    };

    let swap_contract_address = try_s!(req["swap_contract_address"].as_str().ok_or("swap_contract_address is not set or is not string"));
    let swap_contract_address: TezosAddress = try_s!(swap_contract_address.parse());
    if swap_contract_address.prefix != addr_prefixes.originated {
        return ERR!("Invalid swap_contract_address prefix, valid example: KT1WKyHJ8k4uti1Rjg2Jno1tu391dQWECRiB");
    }

    Ok(TezosCoin(Arc::new(TezosCoinImpl {
        addr_prefixes,
        coin_type,
        decimals,
        priv_key,
        my_address,
        required_confirmations: conf["required_confirmations"].as_u64().unwrap_or(1).into(),
        rpc_client,
        swap_contract_address,
        ticker: ticker.into(),
    })))
}

#[derive(Debug)]
struct TezosErcStorage {
    accounts: HashMap<BytesJson, TezosErcAccount>,
    version: u64,
    total_supply: BigUint,
    decimals: u8,
    name: String,
    symbol: String,
    owner: String,
}

#[derive(Debug)]
struct TezosMlaStorage {
    accounts: BigUint,
    owner: String,
    is_paused: bool,
    total_supply: BigUint,
}

impl TryFrom<TezosValue> for TezosMlaStorage {
    type Error = String;

    fn try_from(value: TezosValue) -> Result<Self, Self::Error> {
        let mut reader = TezosValueReader {
            inner: Some(value),
        };

        Ok(TezosMlaStorage {
            accounts: try_s!(try_s!(reader.read()).try_into()),
            owner: try_s!(try_s!(reader.read()).try_into()),
            is_paused: try_s!(try_s!(reader.read()).try_into()),
            total_supply: try_s!(try_s!(reader.read()).try_into()),
        })
    }
}

impl TryFrom<TezosValue> for BigUint {
    type Error = String;

    fn try_from(value: TezosValue) -> Result<Self, Self::Error> {
        match value {
            TezosValue::Int { int } => Ok(try_s!(int.to_biguint().ok_or(fomat!("Could not convert " (int) " to BigUint")))),
            _ => ERR!("BigUint can be constructed only from TezosValue::Int, got {:?}", value),
        }
    }
}

impl TryFrom<TezosValue> for bool {
    type Error = String;

    fn try_from(value: TezosValue) -> Result<Self, Self::Error> {
        match value {
            TezosValue::TezosPrim(TezosPrim::False)  => Ok(false),
            TezosValue::TezosPrim(TezosPrim::True)  => Ok(true),
            _ => ERR!("bool can be constructed only from Prim::False or Prim::True, got {:?}", value),
        }
    }
}

impl TryFrom<TezosValue> for u8 {
    type Error = String;

    fn try_from(value: TezosValue) -> Result<Self, Self::Error> {
        match value {
            TezosValue::Int { int } => Ok(try_s!(int.to_u8().ok_or(fomat!("Could not convert " (int) " to u8")))),
            _ => ERR!("u8 can be constructed only from TezosValue::Int, got {:?}", value),
        }
    }
}

impl TryFrom<TezosValue> for u64 {
    type Error = String;

    fn try_from(value: TezosValue) -> Result<Self, Self::Error> {
        match value {
            TezosValue::Int { int } => Ok(try_s!(int.to_u64().ok_or(fomat!("Could not convert " (int) " to u64")))),
            _ => ERR!("u64 can be constructed only from TezosValue::Int, got {:?}", value),
        }
    }
}

impl TryFrom<TezosValue> for String {
    type Error = String;

    fn try_from(value: TezosValue) -> Result<Self, Self::Error> {
        match value {
            TezosValue::String { string } => Ok(string),
            _ => ERR!("String can be constructed only from TezosValue::String, got {:?}", value),
        }
    }
}

macro_rules! impl_try_from_tezos_rpc_value_for_hash_map {
    ($key_type: ident, $value_type: ident) => {
        impl TryFrom<TezosValue> for HashMap<$key_type, $value_type> {
            type Error = String;

            fn try_from(value: TezosValue) -> Result<Self, Self::Error> {
                match value {
                    TezosValue::List (elems) => {
                        let mut res = HashMap::new();
                        for elem in elems {
                            match elem {
                                TezosValue::TezosPrim(TezosPrim::Elt((key, value))) => {
                                    res.insert(try_s!((*key).try_into()), try_s!((*value).try_into()));
                                },
                                _ => return ERR!("Unexpected item {:?} in list, must be TezosPrim::Elt", elem),
                            }
                        }
                        Ok(res)
                    },
                    _ => ERR!("HashMap can be constructed only from TezosValue::List, got {:?}", value),
                }
            }
        }
    };
}

impl_try_from_tezos_rpc_value_for_hash_map!(BytesJson, TezosErcAccount);
impl_try_from_tezos_rpc_value_for_hash_map!(BytesJson, BigUint);
impl_try_from_tezos_rpc_value_for_hash_map!(TezosAddress, BigUint);

impl TryFrom<TezosValue> for TezosErcAccount {
    type Error = String;

    fn try_from(value: TezosValue) -> Result<Self, Self::Error> {
        let mut reader = TezosValueReader {
            inner: Some(value),
        };

        Ok(TezosErcAccount {
            balance: try_s!(try_s!(reader.read()).try_into()),
            allowances: try_s!(try_s!(reader.read()).try_into()),
        })
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "prim", content = "args")]
pub enum TezosPrim {
    Pair ((Box<TezosValue>, Box<TezosValue>)),
    Elt ((Box<TezosValue>, Box<TezosValue>)),
    Right ([Box<TezosValue>; 1]),
    Left ([Box<TezosValue>; 1]),
    Some ([Box<TezosValue>; 1]),
    Unit,
    False,
    True,
    None,
}

impl Serialize for TezosInt {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error> where S: Serializer {
        s.serialize_str(&self.0.to_string())
    }
}

impl<'de> Deserialize<'de> for TezosInt {
    fn deserialize<D>(d: D) -> Result<TezosInt, D::Error> where D: Deserializer<'de> {
        struct BigIntStringVisitor;

        impl<'de> Visitor<'de> for BigIntStringVisitor {
            type Value = TezosInt;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string containing json data")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
                where
                    E: serde::de::Error,
            {

                BigInt::from_str(v).map_err(E::custom).map(|num| num.into())
            }
        }

        d.deserialize_any(BigIntStringVisitor)
    }
}

impl Serialize for TezosUint {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error> where S: Serializer {
        s.serialize_str(&self.0.to_string())
    }
}

impl<'de> Deserialize<'de> for TezosUint {
    fn deserialize<D>(d: D) -> Result<TezosUint, D::Error> where D: Deserializer<'de> {
        struct TezosUintStringVisitor;

        impl<'de> Visitor<'de> for TezosUintStringVisitor {
            type Value = TezosUint;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string containing json data")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
                where
                    E: serde::de::Error,
            {

                BigUint::from_str(v).map_err(E::custom).map(|num| num.into())
            }
        }

        d.deserialize_any(TezosUintStringVisitor)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(untagged)]
pub enum TezosValue {
    Bytes { bytes: BytesJson },
    Int { int: TezosInt },
    List (Vec<TezosValue>),
    TezosPrim (TezosPrim),
    String { string: String },
}

impl TezosValue {
    fn split_and_read_value(self) -> (TezosValue, Option<TezosValue>) {
        match self {
            TezosValue::TezosPrim(TezosPrim::Pair((left, right))) => (*left, Some(*right)),
            _ => (self, None),
        }
    }

    fn values_vec(self, mut values: Vec<TezosValue>) -> Vec<TezosValue> {
        let (cur, next) = self.split_and_read_value();
        values.push(cur);
        match next {
            Some(val) => val.values_vec(values),
            None => values,
        }
    }
}

fn read_function_call(mut path: Vec<Or>, value: TezosValue) -> (Vec<Or>, TezosValue) {
    match value {
        TezosValue::TezosPrim(TezosPrim::Left(val)) => {
            path.push(Or::L);
            read_function_call(path, *val[0].clone())
        },
        TezosValue::TezosPrim(TezosPrim::Right(val)) => {
            path.push(Or::R);
            read_function_call(path, *val[0].clone())
        },
        _ => (path, value)
    }
}

struct TezosValueReader {
    inner: Option<TezosValue>,
}

impl Serializable for TezosValue {
    fn serialize(&self, s: &mut Stream) {
        match self {
            TezosValue::String { string } => {
                let bytes = string.as_bytes();
                s.append(&1u8);
                s.append_slice(&(bytes.len() as u32).to_be_bytes());
                s.append_slice(&bytes);
            },
            TezosValue::Int { int } => {
                s.append(&0u8);
                s.append(int);
            },
            TezosValue::Bytes { bytes } => {
                s.append(&10u8);
                s.append_slice(&(bytes.len() as u32).to_be_bytes());
                s.append_slice(&bytes);
            },
            TezosValue::TezosPrim(TezosPrim::Pair((left, right))) => {
                s.append(&7u8);
                s.append(&7u8);
                s.append(left.as_ref());
                s.append(right.as_ref());
            },
            TezosValue::TezosPrim(TezosPrim::Left(value)) => {
                s.append(&5u8);
                s.append(&5u8);
                s.append(value[0].as_ref());
            },
            TezosValue::TezosPrim(TezosPrim::Right(value)) => {
                s.append(&5u8);
                s.append(&8u8);
                s.append(value[0].as_ref());
            },
            TezosValue::TezosPrim(TezosPrim::Unit) => {
                s.append(&3u8);
                s.append(&11u8);
            },
            TezosValue::TezosPrim(TezosPrim::Elt((key, value))) => {
                s.append(&7u8);
                s.append(&4u8);
                s.append(key.as_ref());
                s.append(value.as_ref());
            },
            TezosValue::TezosPrim(TezosPrim::Some(value)) => {
                s.append(&5u8);
                s.append(&9u8);
                s.append(value[0].as_ref());
            },
            TezosValue::TezosPrim(TezosPrim::None) => {
                s.append(&3u8);
                s.append(&6u8);
            },
            TezosValue::TezosPrim(TezosPrim::True) => {
                s.append(&3u8);
                s.append(&10u8);
            },
            TezosValue::TezosPrim(TezosPrim::False) => {
                s.append(&3u8);
                s.append(&3u8);
            },
            TezosValue::List(list_items) => {
                s.append(&2u8);
                let mut bytes = vec![];
                for item in list_items {
                    bytes.append(&mut serialize(item).take());
                }
                s.append_slice(&(bytes.len() as u32).to_be_bytes());
                s.append_slice(&bytes);
            },
        }
    }
}

impl TezosValueReader {
    fn read(&mut self) -> Result<TezosValue, String> {
        let val = self.inner.take();
        match val {
            Some(val) => {
                let (res, next) = val.split_and_read_value();
                self.inner = next;
                Ok(res)
            },
            None => ERR!("Inner value is None, reader ")
        }
    }
}

impl TryFrom<TezosValue> for TezosErcStorage {
    type Error = String;

    fn try_from(value: TezosValue) -> Result<Self, Self::Error> {
        let mut reader = TezosValueReader {
            inner: Some(value),
        };

        Ok(TezosErcStorage {
            accounts: try_s!(try_s!(reader.read()).try_into()),
            version: try_s!(try_s!(reader.read()).try_into()),
            total_supply: try_s!(try_s!(reader.read()).try_into()),
            decimals: try_s!(try_s!(reader.read()).try_into()),
            name: try_s!(try_s!(reader.read()).try_into()),
            symbol: try_s!(try_s!(reader.read()).try_into()),
            owner: try_s!(try_s!(reader.read()).try_into()),
        })
    }
}

impl TryFrom<TezosValue> for BytesJson {
    type Error = String;

    fn try_from(value: TezosValue) -> Result<Self, Self::Error> {
        match value {
            TezosValue::Bytes { bytes } => Ok(bytes),
            _ => ERR!("Bytes can be constructed only from TezosValue::Bytes, got {:?}", value),
        }
    }
}

#[derive(Debug, Default)]
struct TezosErcAccount {
    balance: BigUint,
    allowances: HashMap<TezosAddress, BigUint>,
}

#[derive(Debug, PartialEq)]
enum Or {
    L,
    R,
}

impl Into<TezosValue> for &str {
    fn into(self) -> TezosValue {
        TezosValue::String {
            string: self.into()
        }
    }
}

impl Into<TezosValue> for &TezosAddress {
    fn into(self) -> TezosValue {
        TezosValue::String {
            string: self.to_string()
        }
    }
}

impl Into<TezosValue> for &BigUint {
    fn into(self) -> TezosValue {
        TezosValue::Int {
            // BigUint::to_bigint always returns Some so unwrap is safe
            int: unwrap!(self.to_bigint()).into()
        }
    }
}

impl Into<TezosValue> for BigUint {
    fn into(self) -> TezosValue {
        TezosValue::Int {
            // BigUint::to_bigint always returns Some so unwrap is safe
            int: unwrap!(self.to_bigint()).into()
        }
    }
}

impl Into<TezosValue> for BytesJson {
    fn into(self) -> TezosValue {
        TezosValue::Bytes {
            bytes: self
        }
    }
}

impl Into<TezosValue> for TezosAddress {
    fn into(self) -> TezosValue {
        TezosValue::String {
            string: self.to_string()
        }
    }
}

impl Into<TezosValue> for DateTime<Utc> {
    fn into(self) -> TezosValue {
        TezosValue::String {
            string: self.to_rfc3339_opts(SecondsFormat::Secs, true)
        }
    }
}

fn mla_transfer_call(from: &TezosAddress, to: &TezosAddress, amount: &BigUint) -> TransactionParameters {
    TransactionParameters {
        entrypoint: "transfer".into(),
        value: tezos_func!( &[], from, to, amount),
    }
}

pub fn mla_mint_call(to: &TezosAddress, amount: &BigUint) -> TransactionParameters {
    TransactionParameters {
        entrypoint: "mint".into(),
        value: tezos_func!( &[], to, amount),
    }
}

pub fn sample_call(to: &TezosAddress) -> TransactionParameters {
    TransactionParameters {
        entrypoint: "default".into(),
        value: tezos_func!( &[], to),
    }
}

fn erc_approve_call(spender: &TezosAddress, amount: &BigUint) -> TransactionParameters {
    TransactionParameters {
        entrypoint: "approve".into(),
        value: tezos_func!(&[], spender, amount)
    }
}

fn init_tezos_swap_call(
    id: BytesJson,
    time_lock: u32,
    secret_hash: BytesJson,
    secret_hash_algo: SecretHashAlgo,
    receiver: TezosAddress,
) -> TransactionParameters {
    let time_lock = DateTime::from_utc(NaiveDateTime::from_timestamp(time_lock as i64, 0), Utc);
    TransactionParameters {
        entrypoint: "init_tezos_swap".into(),
        value: tezos_func!(&[], id, time_lock, secret_hash, secret_hash_algo, receiver)
    }
}

fn init_tezos_erc_swap_call(
    id: BytesJson,
    time_lock: u32,
    secret_hash: BytesJson,
    secret_hash_algo: SecretHashAlgo,
    receiver: TezosAddress,
    amount: BigUint,
    erc_addr: &TezosAddress,
) -> TransactionParameters {
    let time_lock = DateTime::from_utc(NaiveDateTime::from_timestamp(time_lock as i64, 0), Utc);
    TransactionParameters {
        entrypoint: "init_erc_swap".into(),
        value: tezos_func!(&[], id, time_lock, secret_hash, secret_hash_algo, receiver, amount, erc_addr)
    }
}

fn receiver_spends_call(
    id: BytesJson,
    secret: BytesJson,
    send_to: &TezosAddress,
) -> TransactionParameters {
    TransactionParameters {
        entrypoint: "receiver_spends".into(),
        value: tezos_func!(&[], id, secret, send_to)
    }
}

fn sender_refunds_call(
    id: BytesJson,
    send_to: &TezosAddress,
) -> TransactionParameters {
    TransactionParameters {
        entrypoint: "sender_refunds".into(),
        value: tezos_func!(&[], id, send_to)
    }
}

fn construct_function_call(func: &[Or], args: TezosValue) -> TezosValue {
    func.iter().rev().fold(args, |arg, or| match or {
        Or::L => TezosValue::TezosPrim(TezosPrim::Left([Box::new(arg)])),
        Or::R => TezosValue::TezosPrim(TezosPrim::Right([Box::new(arg)])),
    })
}

fn big_uint_to_zarith_bytes(mut num: BigUint) -> Vec<u8> {
    let mut bytes = vec![];
    loop {
        let remainder = &num % 128u8;
        num = num / 128u8;
        if num == BigUint::from(0u32) {
            // unwrap is safe because 0 <= remainder <= 127 so it always fits u8
            bytes.push(unwrap!(remainder.to_u8()));
            break;
        } else {
            // unwrap is safe because 0 <= remainder <= 127 so it always fits u8
            bytes.push(unwrap!(remainder.to_u8()) ^ 1u8 << 7);
        }
    }
    bytes
}

fn big_int_to_zarith_bytes(mut num: BigInt) -> Vec<u8> {
    let mut bytes = vec![];
    let mut divisor = 64u8;
    let zero = BigInt::from(0);
    let sign = if num < zero {
        num = -num;
        1u8
    } else {
        0u8
    };

    loop {
        // unwrap is safe because divisor is either 64 or 128 so num % divisor always fits u8
        let mut remainder = unwrap!((&num % divisor).to_u8());
        num = num / divisor;
        if divisor == 64 {
            remainder ^= sign << 6;
        }
        if num == zero {
            bytes.push(remainder);
            break;
        } else {
            bytes.push(remainder ^ (1u8 << 7));
        }
        divisor = 128;
    }
    bytes
}

/// http://tezos.gitlab.io/api/p2p.html#public-key-hash-21-bytes-8-bit-tag
#[derive(Clone, Debug, PartialEq)]
struct PubkeyHash {
    curve_type: CurveType,
    hash: H160,
}

impl Serializable for PubkeyHash {
    fn serialize(&self, s: &mut Stream) {
        match self.curve_type {
            CurveType::ED25519 => s.append(&0u8),
            CurveType::SECP256K1 => s.append(&1u8),
            CurveType::P256 => s.append(&2u8),
        };
        s.append(&self.hash);
    }
}

impl Deserializable for PubkeyHash {
    fn deserialize<T>(reader: &mut Reader<T>) -> Result<Self, serialization::Error>
        where Self: Sized, T: std::io::Read
    {
        let curve_tag: u8 = reader.read()?;
        let curve_type = match curve_tag {
            0 => CurveType::ED25519,
            1 => CurveType::SECP256K1,
            2 => CurveType::P256,
            _ => return Err(serialization::Error::MalformedData),
        };
        Ok(PubkeyHash {
            curve_type,
            hash: reader.read()?,
        })
    }
}

/// http://tezos.gitlab.io/api/p2p.html#contract-id-22-bytes-8-bit-tag
#[derive(Clone, Debug, PartialEq)]
enum ContractId {
    PubkeyHash(PubkeyHash),
    Originated(H160),
}

impl Serializable for ContractId {
    fn serialize(&self, s: &mut Stream) {
        match self {
            ContractId::PubkeyHash(hash) => {
                s.append(&0u8);
                s.append(hash);
            },
            ContractId::Originated(hash) => {
                s.append(&1u8);
                s.append(hash);
                s.append(&0u8);
            },
        }
    }
}

impl Deserializable for ContractId {
    fn deserialize<T>(reader: &mut Reader<T>) -> Result<Self, serialization::Error>
        where Self: Sized, T: std::io::Read
    {
        let tag: u8 = reader.read()?;
        match tag {
            0 => Ok(ContractId::PubkeyHash(reader.read()?)),
            1 => {
                let hash = reader.read()?;
                let _padding: u8 = reader.read()?;
                Ok(ContractId::Originated(hash))
            },
            _ => Err(serialization::Error::MalformedData)
        }
    }
}

impl Deserializable for TezosValue {
    fn deserialize<T>(reader: &mut Reader<T>) -> Result<Self, serialization::Error>
        where Self: Sized, T: std::io::Read
    {
        let tag: u8 = reader.read()?;
        match tag {
            0 => {
                Ok(TezosValue::Int {
                    int: reader.read()?
                })
            },
            1 => {
                let length: H32 = reader.read()?;
                let length = u32::from_be_bytes(length.take());
                let mut bytes = vec![0; length as usize];
                reader.read_slice(&mut bytes)?;
                Ok(TezosValue::String {
                    string: String::from_utf8(bytes).map_err(|_| serialization::Error::MalformedData)?
                })
            },
            2 => {
                let length: H32 = reader.read()?;
                let length = u32::from_be_bytes(length.take());
                let mut bytes = vec![0; length as usize];
                reader.read_slice(&mut bytes)?;
                let mut list_items = vec![];
                let mut list_reader = Reader::from_read(bytes.as_slice());
                while !list_reader.is_finished() {
                    let item = list_reader.read()?;
                    list_items.push(item);
                }
                Ok(TezosValue::List(list_items))
            },
            3 => {
                let sub_tag: u8 = reader.read()?;
                match sub_tag {
                    3 => Ok(TezosValue::TezosPrim(TezosPrim::False)),
                    6 => Ok(TezosValue::TezosPrim(TezosPrim::None)),
                    10 => Ok(TezosValue::TezosPrim(TezosPrim::True)),
                    11 => Ok(TezosValue::TezosPrim(TezosPrim::Unit)),
                    _ => return Err(serialization::Error::Custom(ERRL!("Unsupported tag {} and sub_tag {} combination", tag, sub_tag))),
                }
            },
            5 => {
                let sub_tag: u8 = reader.read()?;
                match sub_tag {
                    5 => Ok(TezosValue::TezosPrim(TezosPrim::Left([
                        Box::new(reader.read()?),
                    ]))),
                    8 => Ok(TezosValue::TezosPrim(TezosPrim::Right([
                        Box::new(reader.read()?),
                    ]))),
                    9 => Ok(TezosValue::TezosPrim(TezosPrim::Some([
                        Box::new(reader.read()?),
                    ]))),
                    _ => return Err(serialization::Error::Custom(ERRL!("Unsupported tag {} and sub_tag {} combination", tag, sub_tag))),
                }
            },
            7 => {
                let sub_tag: u8 = reader.read()?;
                match sub_tag {
                    4 => Ok(TezosValue::TezosPrim(TezosPrim::Elt((
                        Box::new(reader.read()?),
                        Box::new(reader.read()?),
                    )))),
                    7 => Ok(TezosValue::TezosPrim(TezosPrim::Pair((
                        Box::new(reader.read()?),
                        Box::new(reader.read()?),
                    )))),
                    _ => return Err(serialization::Error::Custom(ERRL!("Unsupported tag {} and sub_tag {} combination", tag, sub_tag))),
                }
            },
            10 => {
                let length: H32 = reader.read()?;
                let length = u32::from_be_bytes(length.take());
                let mut bytes = vec![0; length as usize];
                reader.read_slice(&mut bytes)?;
                Ok(TezosValue::Bytes {
                    bytes: bytes.into()
                })
            },
            _ => return Err(serialization::Error::Custom(ERRL!("Unsupported tag {}", tag))),
        }
    }
}

#[derive(Add, Clone, Debug, Deref, Display, PartialEq)]
pub struct TezosInt(pub BigInt);

impl Deserializable for TezosInt {
    fn deserialize<T>(reader: &mut Reader<T>) -> Result<Self, serialization::Error>
        where Self: Sized, T: std::io::Read
    {
        let mut bits_str = String::new();
        let mut sign = BigInt::from(1);
        let mut i = 0u32;
        let mut stop = false;
        loop {
            let mut byte: u8 = reader.read()?;
            if i == 0 && byte & (1u8 << 6) != 0 {
                sign = -sign;
                byte ^= 1u8 << 6;
            }

            if byte & (1u8 << 7) != 0 {
                byte ^= 1u8 << 7;
            } else {
                stop = true
            }
            if i == 0 {
                bits_str.insert_str(0, &format!("{:06b}", byte));
            } else {
                bits_str.insert_str(0, &format!("{:07b}", byte));
            }
            if stop { break; }
            i += 1;
        }
        let num = BigUint::from_str_radix(&bits_str, 2).map_err(|_| serialization::Error::MalformedData)?;
        Ok(TezosInt::from(sign * BigInt::from(num)))
    }
}

impl Serializable for TezosInt {
    fn serialize(&self, s: &mut Stream) {
        let bytes = big_int_to_zarith_bytes(self.0.clone());
        s.append_slice(&bytes);
    }
}

/// http://tezos.gitlab.io/api/p2p.html#transaction-tag-108
#[derive(Clone, Debug, PartialEq)]
pub struct TezosTransaction {
    source: ContractId,
    fee: TezosUint,
    counter: TezosUint,
    gas_limit: TezosUint,
    storage_limit: TezosUint,
    amount: TezosUint,
    destination: ContractId,
    parameters: Option<TezosValue>,
}

#[derive(Clone, Debug, PartialEq)]
struct BabylonTransactionParams {
    entrypoint: EntrypointId,
    params: TezosValue,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BabylonTransaction {
    source: PubkeyHash,
    fee: TezosUint,
    counter: TezosUint,
    gas_limit: TezosUint,
    storage_limit: TezosUint,
    amount: TezosUint,
    destination: ContractId,
    parameters: Option<BabylonTransactionParams>,
}

fn read_parameters<T>(reader: &mut Reader<T>) -> Result<Option<TezosValue>, serialization::Error>
    where T: std::io::Read {
    let has_parameters: u8 = reader.read()?;
    match has_parameters {
        0 => Ok(None),
        255 => {
            let len: H32 = reader.read()?;
            let len = u32::from_be_bytes(len.take()) as usize;
            let mut bytes = vec![0; len];
            reader.read_slice(&mut bytes)?;
            deserialize(&bytes[1..]).map(|res| Some(res))
        },
        _ => Err(serialization::Error::MalformedData),
    }
}

fn read_babylon_parameters<T>(reader: &mut Reader<T>) -> Result<Option<BabylonTransactionParams>, serialization::Error>
    where T: std::io::Read {
    let has_parameters: u8 = reader.read()?;
    match has_parameters {
        0 => Ok(None),
        255 => {
            let entrypoint: EntrypointId = reader.read()?;
            let len: H32 = reader.read()?;
            let len = u32::from_be_bytes(len.take()) as usize;
            let mut bytes = vec![0; len];
            reader.read_slice(&mut bytes)?;
            Ok(Some(BabylonTransactionParams {
                entrypoint,
                params: deserialize(bytes.as_slice())?
            }))
        },
        _ => Err(serialization::Error::MalformedData),
    }
}

impl Deserializable for TezosTransaction {
    fn deserialize<T>(reader: &mut Reader<T>) -> Result<Self, serialization::Error>
        where Self: Sized, T: std::io::Read
    {
        Ok(TezosTransaction {
            source: reader.read()?,
            fee: reader.read()?,
            counter: reader.read()?,
            gas_limit: reader.read()?,
            storage_limit: reader.read()?,
            amount: reader.read()?,
            destination: reader.read()?,
            parameters: read_parameters(reader)?,
        })
    }
}

impl Serializable for TezosTransaction {
    fn serialize(&self, s: &mut Stream) {
        s.append(&self.source);
        s.append(&self.fee);
        s.append(&self.counter);
        s.append(&self.gas_limit);
        s.append(&self.storage_limit);
        s.append(&self.amount);
        s.append(&self.destination);
        match &self.parameters {
            Some(params) => {
                s.append(&255u8);
                let bytes = serialize(params).take();
                let len = bytes.len() as u32 + 1;
                s.append_slice(&len.to_be_bytes());
                s.append(&0u8);
                s.append_slice(&bytes);
            },
            None => {
                s.append(&0u8);
            },
        }
    }
}

impl Serializable for BabylonTransaction {
    fn serialize(&self, s: &mut Stream) {
        s.append(&self.source);
        s.append(&self.fee);
        s.append(&self.counter);
        s.append(&self.gas_limit);
        s.append(&self.storage_limit);
        s.append(&self.amount);
        s.append(&self.destination);
        match &self.parameters {
            Some(params) => {
                s.append(&255u8);
                s.append(&params.entrypoint);
                let bytes = serialize(&params.params).take();
                let len = bytes.len() as u32;
                s.append_slice(&len.to_be_bytes());
                s.append_slice(&bytes);
            },
            None => {
                s.append(&0u8);
            },
        }
    }
}

impl Deserializable for BabylonTransaction {
    fn deserialize<T>(reader: &mut Reader<T>) -> Result<Self, serialization::Error>
        where Self: Sized, T: std::io::Read
    {
        Ok(BabylonTransaction {
            source: reader.read()?,
            fee: reader.read()?,
            counter: reader.read()?,
            gas_limit: reader.read()?,
            storage_limit: reader.read()?,
            amount: reader.read()?,
            destination: reader.read()?,
            parameters: read_babylon_parameters(reader)?,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum TezosOperationEnum {
    Transaction(TezosTransaction),
    BabylonTransaction(BabylonTransaction),
    Reveal(RevealOp),
}

#[derive(Clone, Debug, PartialEq)]
pub struct RevealOp {
    source: PubkeyHash,
    fee: TezosUint,
    counter: TezosUint,
    gas_limit: TezosUint,
    storage_limit: TezosUint,
    public_key: TezosPubkey,
}

impl Serializable for RevealOp {
    fn serialize(&self, s: &mut Stream) {
        s.append(&self.source)
            .append(&self.fee)
            .append(&self.counter)
            .append(&self.gas_limit)
            .append(&self.storage_limit)
            .append(&self.public_key);
    }
}

impl Deserializable for RevealOp {
    fn deserialize<T>(reader: &mut Reader<T>) -> Result<Self, serialization::Error>
        where Self: Sized, T: std::io::Read
    {
        Ok(RevealOp {
            source: reader.read()?,
            fee: reader.read()?,
            counter: reader.read()?,
            gas_limit: reader.read()?,
            storage_limit: reader.read()?,
            public_key: reader.read()?,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct TezosOperation {
    branch: H256,
    contents: Vec<TezosOperationEnum>,
    signature: Option<H512>,
}

impl TezosOperation {
    fn first_tx_destination(&self) -> Option<ContractId> {
        for op in self.contents.iter() {
            match op {
                TezosOperationEnum ::BabylonTransaction(t) => return Some(t.destination.clone()),
                _ => (),
            }
        }
        None
    }
}

impl Deserializable for TezosOperation {
    fn deserialize<T>(reader: &mut Reader<T>) -> Result<Self, serialization::Error>
        where Self: Sized, T: std::io::Read
    {
        let mut contents = vec![];
        let branch = reader.read()?;
        let mut buffer = vec![];
        reader.read_to_end(&mut buffer)?;

        // Tezos operation serialization doesn't contain the length of contents sequence
        // So we have to attempt to read the operation first and if this read fails
        // consider the bytes to represent the signature
        // So all bytes are read to temporary buffer and the buffer is returned if we fail to read from it
        // The implementation is very ineffective due to memory copying taking place on every loop
        // iteration, should consider refactoring the parity-bitcoin serialization crate to return
        // the bytes that was read in case of error
        let (error, bytes_left) = loop {
            let temp_buf = buffer.clone();
            let mut reader = Reader::from_read(temp_buf.as_slice());
            let tag: u8 = reader.read()?;
            let op = match tag {
                8 => {
                    let tx: TezosTransaction = match reader.read() {
                        Ok(t) => t,
                        Err(e) => break (e, buffer),
                    };
                    TezosOperationEnum::Transaction(tx)
                },
                107 => {
                    let tx: RevealOp = match reader.read() {
                        Ok(t) => t,
                        Err(e) => break (e, buffer),
                    };
                    TezosOperationEnum::Reveal(tx)
                },
                108 => {
                    let tx: BabylonTransaction = match reader.read() {
                        Ok(t) => t,
                        Err(e) => break (e, buffer),
                    };
                    TezosOperationEnum::BabylonTransaction(tx)
                },
                _ => break (serialization::Error::Custom(ERRL!("Unsupported tag {}", tag)), buffer),
            };
            contents.push(op);
            if reader.is_finished() {
                return Ok(TezosOperation {
                    branch,
                    contents,
                    signature: None,
                })
            }
            buffer.clear();
            reader.read_to_end(&mut buffer)?;
        };
        if bytes_left.len() != 64 {
            return Err(error);
        }
        let mut reader = Reader::from_read(bytes_left.as_slice());
        Ok(TezosOperation {
            branch,
            contents,
            signature: Some(reader.read()?),
        })
    }
}

impl Serializable for TezosOperation {
    fn serialize(&self, s: &mut Stream) {
        s.append(&self.branch);
        for op in self.contents.iter() {
            match op {
                TezosOperationEnum::Transaction(tx) => {
                    s.append(&8u8);
                    s.append(tx);
                },
                TezosOperationEnum::BabylonTransaction(tx) => {
                    s.append(&108u8);
                    s.append(tx);
                },
                TezosOperationEnum::Reveal(tx) => {
                    s.append(&107u8);
                    s.append(tx);
                },
            }
        }
        if let Some(sig) = &self.signature {
            s.append(sig);
        }
    }
}

impl TezosOperation {
    fn op_hash(&self) -> OpHash {
        OpHash::from_op_bytes(&serialize(self).take())
    }
}

#[derive(Add, Clone, Debug, Deref, Display, PartialEq)]
pub struct TezosUint(pub BigUint);

impl Deserializable for TezosUint {
    fn deserialize<T>(reader: &mut Reader<T>) -> Result<Self, serialization::Error>
        where Self: Sized, T: std::io::Read
    {
        let mut res = BigUint::from(0u8);
        let mut stop = false;
        let mut i = 0u32;
        loop {
            let mut byte: u8 = reader.read()?;
            if byte & 1u8 << 7 != 0 {
                byte ^= 1u8 << 7;
            } else {
                stop = true
            }
            res += byte * BigUint::from(128u8).pow(i);
            if stop { break; }
            i += 1;
        }
        Ok(TezosUint::from(res))
    }
}

impl Serializable for TezosUint {
    fn serialize(&self, s: &mut Stream) {
        let bytes = big_uint_to_zarith_bytes(self.0.clone());
        s.append_slice(&bytes);
    }
}

impl From<BigInt> for TezosInt {
    fn from(n: BigInt) -> TezosInt {
        TezosInt(n)
    }
}

impl From<BigUint> for TezosUint {
    fn from(n: BigUint) -> TezosUint {
        TezosUint(n)
    }
}

#[derive(Debug)]
enum TezosAtomicSwapState {
    Initialized,
    ReceiverSpent,
    SenderRefunded,
}

impl TryFrom<TezosValue> for TezosAtomicSwapState {
    type Error = String;

    fn try_from(value: TezosValue) -> Result<Self, Self::Error> {
        match value {
            TezosValue::TezosPrim(TezosPrim::Left(_)) => Ok(TezosAtomicSwapState::Initialized),
            TezosValue::TezosPrim(TezosPrim::Right(value)) => match *value[0] {
                TezosValue::TezosPrim(TezosPrim::Left(_)) => Ok(TezosAtomicSwapState::ReceiverSpent),
                TezosValue::TezosPrim(TezosPrim::Right(_)) => Ok(TezosAtomicSwapState::SenderRefunded),
                _ => ERR!("TezosAtomicSwapState can be constructed only from TezosPrim::Left or TezosPrim::Right, got {:?}", value),
            },
            _ => ERR!("TezosAtomicSwapState can be constructed only from TezosPrim::Left or TezosPrim::Right, got {:?}", value),
        }
    }
}

impl TryFrom<TezosValue> for SecretHashAlgo {
    type Error = String;

    fn try_from(value: TezosValue) -> Result<Self, Self::Error> {
        match value {
            TezosValue::TezosPrim(TezosPrim::Left(_)) => Ok(SecretHashAlgo::Sha256),
            TezosValue::TezosPrim(TezosPrim::Right(value)) => match *value[0] {
                TezosValue::TezosPrim(TezosPrim::Left(_)) => Ok(SecretHashAlgo::Sha512),
                TezosValue::TezosPrim(TezosPrim::Right(_)) => Ok(SecretHashAlgo::Blake2b256),
                _ => ERR!("SecretHashAlgo can be constructed only from TezosPrim::Left or TezosPrim::Right, got {:?}", value),
            },
            _ => ERR!("SecretHashAlgo can be constructed only from TezosPrim::Left or TezosPrim::Right, got {:?}", value),
        }
    }
}

impl Into<TezosValue> for SecretHashAlgo {
    fn into(self) -> TezosValue {
        let unit = [Box::new(TezosValue::TezosPrim(TezosPrim::Unit))];
        match self {
            SecretHashAlgo::Sha256 => TezosValue::TezosPrim(TezosPrim::Left(unit)),
            SecretHashAlgo::Sha512 => TezosValue::TezosPrim(TezosPrim::Right([Box::new(TezosValue::TezosPrim(TezosPrim::Left(unit)))])),
            SecretHashAlgo::Blake2b256 => TezosValue::TezosPrim(TezosPrim::Right([Box::new(TezosValue::TezosPrim(TezosPrim::Right(unit)))])),
            _ => unimplemented!(),
        }
    }
}

#[derive(Debug)]
struct TezosAtomicSwap {
    amount: BigUint,
    amount_nat: BigUint,
    contract_address: TezosOption<TezosAddress>,
    created_at: DateTime<Utc>,
    lock_time: DateTime<Utc>,
    receiver: TezosAddress,
    secret_hash: BytesJson,
    secret_hash_type: SecretHashAlgo,
    sender: TezosAddress,
    state: TezosAtomicSwapState,
    spent_at: TezosOption<DateTime<Utc>>,
    uuid: BytesJson,
}

impl TryFrom<TezosValue> for TezosAtomicSwap {
    type Error = String;

    fn try_from(value: TezosValue) -> Result<Self, Self::Error> {
        let mut reader = TezosValueReader {
            inner: Some(value),
        };

        Ok(TezosAtomicSwap {
            amount: try_s!(try_s!(reader.read()).try_into()),
            amount_nat: try_s!(try_s!(reader.read()).try_into()),
            contract_address: try_s!(try_s!(reader.read()).try_into()),
            created_at: try_s!(try_s!(reader.read()).try_into()),
            lock_time: try_s!(try_s!(reader.read()).try_into()),
            receiver: try_s!(try_s!(reader.read()).try_into()),
            secret_hash: try_s!(try_s!(reader.read()).try_into()),
            secret_hash_type: try_s!(try_s!(reader.read()).try_into()),
            sender: try_s!(try_s!(reader.read()).try_into()),
            spent_at: try_s!(try_s!(reader.read()).try_into()),
            state: try_s!(try_s!(reader.read()).try_into()),
            uuid: try_s!(try_s!(reader.read()).try_into()),
        })
    }
}

impl TryFrom<TezosValue> for ContractId {
    type Error = String;

    fn try_from(value: TezosValue) -> Result<Self, Self::Error> {
        match value {
            TezosValue::Bytes { bytes } => Ok(try_s!(deserialize(bytes.0.as_slice()).map_err(|e| ERRL!("{:?}", e)))),
            _ => ERR!("ContractId can be constructed only from TezosValue::Bytes, got {:?}", value),
        }
    }
}

impl TryFrom<TezosValue> for DateTime<Utc> {
    type Error = String;

    fn try_from(value: TezosValue) -> Result<Self, Self::Error> {
        match value {
            TezosValue::String { string } => Ok(try_s!(string.parse())),
            _ => ERR!("DateTime<Utc> can be constructed only from TezosValue::String, got {:?}", value),
        }
    }
}

impl TryFrom<TezosValue> for TezosAddress {
    type Error = String;

    fn try_from(value: TezosValue) -> Result<Self, Self::Error> {
        match value {
            TezosValue::String { string } => Ok(try_s!(string.parse())),
            _ => ERR!("TezosAddress can be constructed only from TezosValue::String, got {:?}", value),
        }
    }
}

#[derive(Debug)]
struct TezosOption<T>(Option<T>);

impl<T: TryFrom<TezosValue>> TryFrom<TezosValue> for TezosOption<T>
    where T::Error: fmt::Display
{
    type Error = String;

    fn try_from(value: TezosValue) -> Result<Self, Self::Error> {
        match value {
            TezosValue::TezosPrim(TezosPrim::None) => Ok(TezosOption(None)),
            TezosValue::TezosPrim(TezosPrim::Some(value)) => Ok(TezosOption(Some(try_s!(T::try_from((*value[0]).clone()))))),
            _ => ERR!("TezosOption can be constructed only from TezosPrim::None or TezosPrim::Some, got {:?}", value),
        }
    }
}

/// https://tezos.gitlab.io/protocols/005_babylon.html#transactions-now-have-an-entrypoint
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EntrypointId {
    Default,
    Root,
    Do,
    SetDelegate,
    RemoveDelegate,
    Named(String),
}

impl Serializable for EntrypointId {
    fn serialize(&self, s: &mut Stream) {
        match self {
            EntrypointId::Default => { s.append(&0u8); },
            EntrypointId::Root => { s.append(&1u8); },
            EntrypointId::Do => { s.append(&2u8); },
            EntrypointId::SetDelegate => { s.append(&3u8); },
            EntrypointId::RemoveDelegate => { s.append(&4u8); },
            EntrypointId::Named(name) => {
                s.append(&255u8);
                s.append(&(name.len() as u8));
                s.append_slice(name.as_bytes());
            },
        };
    }
}

impl Deserializable for EntrypointId {
    fn deserialize<T>(reader: &mut Reader<T>) -> Result<Self, serialization::Error>
        where Self: Sized, T: std::io::Read
    {
        let tag: u8 = reader.read()?;
        match tag {
            0 => Ok(EntrypointId::Default),
            1 => Ok(EntrypointId::Root),
            2 => Ok(EntrypointId::Do),
            3 => Ok(EntrypointId::SetDelegate),
            4 => Ok(EntrypointId::RemoveDelegate),
            255 => {
                let len: u8 = reader.read()?;
                let mut bytes = vec![0; len as usize];
                reader.read_slice(&mut bytes)?;
                let name = std::str::from_utf8(&bytes).map_err(|e| serialization::Error::Custom(ERRL!("Error {} parsing bytes as UTF8", e)))?;
                Ok(EntrypointId::Named(name.into()))
            },
            _ => Err(serialization::Error::Custom(ERRL!("Unsupported EntrypointId tag {}", tag))),
        }
    }
}

impl CryptoOps for TezosCoinImpl {
    fn get_pubkey(&self) -> EcPubkey {
        self.priv_key.get_pubkey()
    }

    fn sign_message(&self, msg: &[u8]) -> Result<Vec<u8>, String> {
        self.priv_key.sign_message(msg)
    }
}

impl CryptoOps for TezosCoin {
    fn get_pubkey(&self) -> EcPubkey {
        self.priv_key.get_pubkey()
    }

    fn sign_message(&self, msg: &[u8]) -> Result<Vec<u8>, String> {
        self.priv_key.sign_message(msg)
    }
}

lazy_static! {
    pub static ref COMMON_XTZ_CONFIG: Json = json!({
        "coin": "TEZOS",
        "name": "tezosbabylonnet",
        "ed25519_addr_prefix": TZ1_ADDR_PREFIX,
        "secp256k1_addr_prefix": TZ2_ADDR_PREFIX,
        "p256_addr_prefix": TZ3_ADDR_PREFIX,
        "protocol": {
          "platform": "TEZOS",
          "token_type": "TEZOS"
        },
        "mm2": 1
    });
}

pub async fn tezos_coin_for_test(priv_key: &[u8], node_url: &str, swap_contract: &str) -> TezosCoin {
    let req = json!({
        "method": "enable",
        "coin": "TEZOS",
        "urls": [
            node_url
        ],
        "mm2":1,
        "swap_contract_address": swap_contract,
    });
    unwrap!(tezos_coin_from_conf_and_request("TEZOS", &COMMON_XTZ_CONFIG, &req, priv_key).await)
}

pub fn tezos_mla_coin_for_test(priv_key: &[u8], node_url: &str, swap_contract: &str, token_contract: &str) -> TezosCoin {
    let conf = json!({
        "coin": "XTZ_MLA",
        "name": "tezos_managed_ledger_asset",
        "ed25519_addr_prefix": TZ1_ADDR_PREFIX,
        "secp256k1_addr_prefix": TZ2_ADDR_PREFIX,
        "p256_addr_prefix": TZ3_ADDR_PREFIX,
        "protocol": {
            "platform": "TEZOS",
            "token_type": "MLA",
            "contract_address": token_contract
        },
        "mm2": 1
    });
    let req = json!({
        "method": "enable",
        "coin": "XTZ_MLA",
        "urls": [
            node_url,
        ],
        "swap_contract_address": swap_contract,
        "mm2":1
    });
    let coin = unwrap!(block_on(tezos_coin_from_conf_and_request("XTZ_MLA", &conf, &req, priv_key)));
    coin
}

pub fn prepare_tezos_sandbox_network() -> (String, String) {
    // 1 of hardcoded sandbox bootstrap privkeys having a lot of tezos
    let priv_key: TezosSecret = unwrap!("edsk3RFgDiCt7tWB4bSUSXJgA5EQeXomgnMjF9fnDkeN96zsYxtbPC".parse());
    let coin = block_on(tezos_coin_for_test(&priv_key.data, "http://localhost:20000", "KT1WKyHJ8k4uti1Rjg2Jno1tu391dQWECRiB"));
    loop {
        let header = unwrap!(block_on(coin.rpc_client.block_header("head")));
        if header.protocol == "PsBabyM1eUXZseaJdmXFApDSBqj8YBfwELoxZHHW77EMcAbbwAS" {
            break;
        }
    }
    let swap_contract_script_str = r#"{"code":[{"prim":"parameter","args":[{"prim":"or","args":[{"prim":"mutez","annots":["%default"]},{"prim":"or","args":[{"prim":"pair","args":[{"prim":"bytes"},{"prim":"pair","args":[{"prim":"timestamp"},{"prim":"pair","args":[{"prim":"bytes"},{"prim":"pair","args":[{"prim":"or","args":[{"prim":"unit","annots":["%Sha256"]},{"prim":"or","args":[{"prim":"unit","annots":["%Sha512"]},{"prim":"unit","annots":["%Blake2b256"]}]}],"annots":[":secret_hash_algo"]},{"prim":"address"}]}]}]}],"annots":["%init_tezos_swap"]},{"prim":"or","args":[{"prim":"pair","args":[{"prim":"bytes"},{"prim":"pair","args":[{"prim":"timestamp"},{"prim":"pair","args":[{"prim":"bytes"},{"prim":"pair","args":[{"prim":"or","args":[{"prim":"unit","annots":["%Sha256"]},{"prim":"or","args":[{"prim":"unit","annots":["%Sha512"]},{"prim":"unit","annots":["%Blake2b256"]}]}],"annots":[":secret_hash_algo"]},{"prim":"pair","args":[{"prim":"address"},{"prim":"pair","args":[{"prim":"nat"},{"prim":"address"}]}]}]}]}]}],"annots":["%init_erc_swap"]},{"prim":"or","args":[{"prim":"pair","args":[{"prim":"bytes"},{"prim":"pair","args":[{"prim":"bytes"},{"prim":"key_hash"}]}],"annots":["%receiver_spends"]},{"prim":"pair","args":[{"prim":"bytes"},{"prim":"key_hash"}],"annots":["%sender_refunds"]}]}]}]}]}]},{"prim":"storage","args":[{"prim":"pair","args":[{"prim":"big_map","args":[{"prim":"bytes"},{"prim":"pair","args":[{"prim":"mutez","annots":["%amount"]},{"prim":"pair","args":[{"prim":"nat","annots":["%amount_nat"]},{"prim":"pair","args":[{"prim":"option","args":[{"prim":"address"}],"annots":["%contract_address"]},{"prim":"pair","args":[{"prim":"timestamp","annots":["%created_at"]},{"prim":"pair","args":[{"prim":"timestamp","annots":["%lock_time"]},{"prim":"pair","args":[{"prim":"address","annots":["%receiver"]},{"prim":"pair","args":[{"prim":"bytes","annots":["%secret_hash"]},{"prim":"pair","args":[{"prim":"or","args":[{"prim":"unit","annots":["%Sha256"]},{"prim":"or","args":[{"prim":"unit","annots":["%Sha512"]},{"prim":"unit","annots":["%Blake2b256"]}]}],"annots":[":secret_hash_algo","%secret_hash_type"]},{"prim":"pair","args":[{"prim":"address","annots":["%sender"]},{"prim":"pair","args":[{"prim":"option","args":[{"prim":"timestamp"}],"annots":["%spent_at"]},{"prim":"pair","args":[{"prim":"or","args":[{"prim":"unit","annots":["%Initialized"]},{"prim":"or","args":[{"prim":"unit","annots":["%ReceiverSpent"]},{"prim":"unit","annots":["%SenderRefunded"]}]}],"annots":[":swap_state","%state"]},{"prim":"bytes","annots":["%uuid"]}]}]}]}]}]}]}]}]}]}]}],"annots":[":atomic_swap"]}]},{"prim":"nat"}],"annots":[":storage"]}]},{"prim":"code","args":[[{"prim":"DUP"},{"prim":"DIP","args":[[{"prim":"CDR","annots":["@storage_slash_1"]}]]},{"prim":"CAR","annots":["@parameter_slash_2"]},{"prim":"DUP","annots":["@parameter"]},{"prim":"IF_LEFT","args":[[{"prim":"RENAME","annots":["@to_forward_slash_3"]},{"prim":"PUSH","args":[{"prim":"string"},{"string":"Not implemented"}]},{"prim":"FAILWITH"}],[{"prim":"IF_LEFT","args":[[{"prim":"RENAME","annots":["@_uuid_lock_time_secret_hash_secret_hash_type_receiver_slash_5"]},{"prim":"DIP","args":[{"int":"2"},[{"prim":"DUP","annots":["@storage"]}]]},{"prim":"DIG","args":[{"int":"2"}]},{"prim":"DIP","args":[[{"prim":"DUP"}]]},{"prim":"SWAP"},{"prim":"CAR","annots":["@uuid"]},{"prim":"DIP","args":[{"int":"2"},[{"prim":"DUP"}]]},{"prim":"DIG","args":[{"int":"2"}]},[{"prim":"CDR"},{"prim":"CDR"},{"prim":"CAR","annots":["@secret_hash"]}],{"prim":"DIP","args":[{"int":"3"},[{"prim":"DUP"}]]},{"prim":"DIG","args":[{"int":"3"}]},[{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CAR","annots":["@secret_hash_type"]}],{"prim":"DUP","annots":["@secret_hash_type"]},{"prim":"IF_LEFT","args":[[{"prim":"DROP"},{"prim":"PUSH","args":[{"prim":"nat"},{"int":"32"}]}],[{"prim":"IF_LEFT","args":[[{"prim":"DROP"},{"prim":"PUSH","args":[{"prim":"nat"},{"int":"64"}]}],[{"prim":"DROP"},{"prim":"PUSH","args":[{"prim":"nat"},{"int":"32"}]}]]}]]},{"prim":"RENAME","annots":["@expected_len"]},{"prim":"DUP","annots":["@expected_len"]},{"prim":"DIP","args":[{"int":"3"},[{"prim":"DUP","annots":["@secret_hash"]}]]},{"prim":"DIG","args":[{"int":"3"}]},{"prim":"SIZE"},{"prim":"COMPARE"},{"prim":"NEQ"},{"prim":"IF","args":[[{"prim":"DUP","annots":["@expected_len"]},{"prim":"PUSH","args":[{"prim":"string"},{"string":"Secret hash length must be "}]},{"prim":"PAIR"},{"prim":"FAILWITH"}],[{"prim":"UNIT"}]]},{"prim":"DROP"},{"prim":"PUSH","args":[{"prim":"mutez"},{"int":"0"}]},{"prim":"AMOUNT"},{"prim":"COMPARE"},{"prim":"LE"},{"prim":"IF","args":[[{"prim":"PUSH","args":[{"prim":"string"},{"string":"Transaction amount must be greater than zero"}]},{"prim":"FAILWITH"}],[{"prim":"UNIT"}]]},{"prim":"DROP"},{"prim":"DIP","args":[{"int":"4"},[{"prim":"DUP","annots":["@storage"]}]]},{"prim":"DIG","args":[{"int":"4"}]},{"prim":"CDR","annots":["%version"]},{"prim":"DIP","args":[{"int":"5"},[{"prim":"DUP","annots":["@storage"]}]]},{"prim":"DIG","args":[{"int":"5"}]},{"prim":"CAR","annots":["%swaps"]},{"prim":"DIP","args":[{"int":"6"},[{"prim":"DUP","annots":["@storage"]}]]},{"prim":"DIG","args":[{"int":"6"}]},{"prim":"CAR","annots":["%swaps"]},{"prim":"DIP","args":[{"int":"6"},[{"prim":"DUP","annots":["@uuid"]}]]},{"prim":"DIG","args":[{"int":"6"}]},{"prim":"GET"},{"prim":"IF_NONE","args":[[{"prim":"DIP","args":[{"int":"5"},[{"prim":"DUP","annots":["@uuid"]}]]},{"prim":"DIG","args":[{"int":"5"}]},{"prim":"PUSH","args":[{"prim":"or","args":[{"prim":"unit","annots":["%Initialized"]},{"prim":"or","args":[{"prim":"unit","annots":["%ReceiverSpent"]},{"prim":"unit","annots":["%SenderRefunded"]}]}],"annots":[":swap_state"]},{"prim":"Left","args":[{"prim":"Unit"}]}]},{"prim":"PAIR","annots":["%state","%uuid"]},{"prim":"NONE","args":[{"prim":"timestamp"}]},{"prim":"PAIR","annots":["%spent_at"]},{"prim":"SOURCE"},{"prim":"PAIR","annots":["%sender"]},{"prim":"DIP","args":[{"int":"4"},[{"prim":"DUP","annots":["@secret_hash_type"]}]]},{"prim":"DIG","args":[{"int":"4"}]},{"prim":"PAIR","annots":["%secret_hash_type"]},{"prim":"DIP","args":[{"int":"5"},[{"prim":"DUP","annots":["@secret_hash"]}]]},{"prim":"DIG","args":[{"int":"5"}]},{"prim":"PAIR","annots":["%secret_hash"]},{"prim":"DIP","args":[{"int":"8"},[{"prim":"DUP"}]]},{"prim":"DIG","args":[{"int":"8"}]},[{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR","annots":["@receiver"]}],{"prim":"PAIR","annots":["%receiver"]},{"prim":"DIP","args":[{"int":"8"},[{"prim":"DUP"}]]},{"prim":"DIG","args":[{"int":"8"}]},[{"prim":"CDR"},{"prim":"CAR","annots":["@lock_time"]}],{"prim":"PAIR","annots":["%lock_time"]},{"prim":"NOW"},{"prim":"PAIR","annots":["%created_at"]},{"prim":"NONE","args":[{"prim":"address"}]},{"prim":"PAIR","annots":["%contract_address"]},{"prim":"PUSH","args":[{"prim":"nat"},{"int":"0"}]},{"prim":"PAIR","annots":["%amount_nat"]},{"prim":"AMOUNT"},{"prim":"PAIR","annots":["%amount"]}],[{"prim":"PUSH","args":[{"prim":"string"},{"string":"Swap was initialized already"}]},{"prim":"FAILWITH"}]]},{"prim":"RENAME","annots":["@new_swap"]},{"prim":"DIP","args":[{"int":"6"},[{"prim":"DUP","annots":["@uuid"]}]]},{"prim":"DIG","args":[{"int":"6"}]},{"prim":"DIP","args":[[{"prim":"SOME"}]]},{"prim":"DIP","args":[{"int":"4"},[{"prim":"DROP","args":[{"int":"6"}]}]]},{"prim":"UPDATE","annots":["@new_swaps"]},{"prim":"PAIR","annots":["%swaps","%version"]},{"prim":"NIL","args":[{"prim":"operation"}]},{"prim":"PAIR"}],[{"prim":"IF_LEFT","args":[[{"prim":"RENAME","annots":["@_uuid_lock_time_secret_hash_secret_hash_type_receiver_amount_contract_addr_slash_16"]},{"prim":"DIP","args":[{"int":"2"},[{"prim":"DUP","annots":["@storage"]}]]},{"prim":"DIG","args":[{"int":"2"}]},{"prim":"DIP","args":[[{"prim":"DUP"}]]},{"prim":"SWAP"},{"prim":"CAR","annots":["@uuid"]},{"prim":"DIP","args":[{"int":"2"},[{"prim":"DUP"}]]},{"prim":"DIG","args":[{"int":"2"}]},[{"prim":"CDR"},{"prim":"CDR"},{"prim":"CAR","annots":["@secret_hash"]}],{"prim":"DIP","args":[{"int":"3"},[{"prim":"DUP"}]]},{"prim":"DIG","args":[{"int":"3"}]},[{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CAR","annots":["@secret_hash_type"]}],{"prim":"DIP","args":[{"int":"4"},[{"prim":"DUP"}]]},{"prim":"DIG","args":[{"int":"4"}]},[{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CAR","annots":["@amount"]}],{"prim":"DIP","args":[{"int":"5"},[{"prim":"DUP"}]]},{"prim":"DIG","args":[{"int":"5"}]},[{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR","annots":["@contract_addr"]}],{"prim":"PUSH","args":[{"prim":"mutez"},{"int":"0"}]},{"prim":"AMOUNT"},{"prim":"COMPARE"},{"prim":"GT"},{"prim":"IF","args":[[{"prim":"PUSH","args":[{"prim":"string"},{"string":"Tx amount must be zero"}]},{"prim":"FAILWITH"}],[{"prim":"UNIT"}]]},{"prim":"DROP"},{"prim":"DIP","args":[{"int":"2"},[{"prim":"DUP","annots":["@secret_hash_type"]}]]},{"prim":"DIG","args":[{"int":"2"}]},{"prim":"IF_LEFT","args":[[{"prim":"DROP"},{"prim":"PUSH","args":[{"prim":"nat"},{"int":"32"}]}],[{"prim":"IF_LEFT","args":[[{"prim":"DROP"},{"prim":"PUSH","args":[{"prim":"nat"},{"int":"64"}]}],[{"prim":"DROP"},{"prim":"PUSH","args":[{"prim":"nat"},{"int":"32"}]}]]}]]},{"prim":"RENAME","annots":["@expected_len"]},{"prim":"DUP","annots":["@expected_len"]},{"prim":"DIP","args":[{"int":"5"},[{"prim":"DUP","annots":["@secret_hash"]}]]},{"prim":"DIG","args":[{"int":"5"}]},{"prim":"SIZE"},{"prim":"COMPARE"},{"prim":"NEQ"},{"prim":"IF","args":[[{"prim":"DUP","annots":["@expected_len"]},{"prim":"PUSH","args":[{"prim":"string"},{"string":"Secret hash length must be "}]},{"prim":"PAIR"},{"prim":"FAILWITH"}],[{"prim":"UNIT"}]]},{"prim":"DROP"},{"prim":"PUSH","args":[{"prim":"nat"},{"int":"0"}]},{"prim":"DIP","args":[{"int":"3"},[{"prim":"DUP","annots":["@amount"]}]]},{"prim":"DIG","args":[{"int":"3"}]},{"prim":"COMPARE"},{"prim":"LE"},{"prim":"IF","args":[[{"prim":"PUSH","args":[{"prim":"string"},{"string":"ERC amount must be greater than zero"}]},{"prim":"FAILWITH"}],[{"prim":"UNIT"}]]},{"prim":"DROP"},{"prim":"DIP","args":[{"int":"6"},[{"prim":"DUP","annots":["@storage"]}]]},{"prim":"DIG","args":[{"int":"6"}]},{"prim":"CDR","annots":["%version"]},{"prim":"DIP","args":[{"int":"7"},[{"prim":"DUP","annots":["@storage"]}]]},{"prim":"DIG","args":[{"int":"7"}]},{"prim":"CAR","annots":["%swaps"]},{"prim":"DIP","args":[{"int":"8"},[{"prim":"DUP","annots":["@storage"]}]]},{"prim":"DIG","args":[{"int":"8"}]},{"prim":"CAR","annots":["%swaps"]},{"prim":"DIP","args":[{"int":"8"},[{"prim":"DUP","annots":["@uuid"]}]]},{"prim":"DIG","args":[{"int":"8"}]},{"prim":"GET"},{"prim":"IF_NONE","args":[[{"prim":"DIP","args":[{"int":"7"},[{"prim":"DUP","annots":["@uuid"]}]]},{"prim":"DIG","args":[{"int":"7"}]},{"prim":"PUSH","args":[{"prim":"or","args":[{"prim":"unit","annots":["%Initialized"]},{"prim":"or","args":[{"prim":"unit","annots":["%ReceiverSpent"]},{"prim":"unit","annots":["%SenderRefunded"]}]}],"annots":[":swap_state"]},{"prim":"Left","args":[{"prim":"Unit"}]}]},{"prim":"PAIR","annots":["%state","%uuid"]},{"prim":"NONE","args":[{"prim":"timestamp"}]},{"prim":"PAIR","annots":["%spent_at"]},{"prim":"SOURCE"},{"prim":"PAIR","annots":["%sender"]},{"prim":"DIP","args":[{"int":"6"},[{"prim":"DUP","annots":["@secret_hash_type"]}]]},{"prim":"DIG","args":[{"int":"6"}]},{"prim":"PAIR","annots":["%secret_hash_type"]},{"prim":"DIP","args":[{"int":"7"},[{"prim":"DUP","annots":["@secret_hash"]}]]},{"prim":"DIG","args":[{"int":"7"}]},{"prim":"PAIR","annots":["%secret_hash"]},{"prim":"DIP","args":[{"int":"10"},[{"prim":"DUP"}]]},{"prim":"DIG","args":[{"int":"10"}]},[{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CAR","annots":["@receiver"]}],{"prim":"PAIR","annots":["%receiver"]},{"prim":"DIP","args":[{"int":"10"},[{"prim":"DUP"}]]},{"prim":"DIG","args":[{"int":"10"}]},[{"prim":"CDR"},{"prim":"CAR","annots":["@lock_time"]}],{"prim":"PAIR","annots":["%lock_time"]},{"prim":"NOW"},{"prim":"PAIR","annots":["%created_at"]},{"prim":"DIP","args":[{"int":"4"},[{"prim":"DUP","annots":["@contract_addr"]}]]},{"prim":"DIG","args":[{"int":"4"}]},{"prim":"SOME"},{"prim":"PAIR","annots":["%contract_address"]},{"prim":"DIP","args":[{"int":"5"},[{"prim":"DUP","annots":["@amount"]}]]},{"prim":"DIG","args":[{"int":"5"}]},{"prim":"PAIR","annots":["%amount_nat"]},{"prim":"AMOUNT"},{"prim":"PAIR","annots":["%amount"]}],[{"prim":"PUSH","args":[{"prim":"string"},{"string":"Swap was initialized already"}]},{"prim":"FAILWITH"}]]},{"prim":"RENAME","annots":["@new_swap"]},{"prim":"DIP","args":[{"int":"8"},[{"prim":"DUP","annots":["@uuid"]}]]},{"prim":"DIG","args":[{"int":"8"}]},{"prim":"DIP","args":[[{"prim":"SOME"}]]},{"prim":"UPDATE","annots":["@new_swaps"]},{"prim":"PAIR","annots":["%swaps","%version"]},{"prim":"NIL","args":[{"prim":"operation"}]},{"prim":"DIP","args":[{"int":"3"},[{"prim":"DUP","annots":["@contract_addr"]}]]},{"prim":"DIG","args":[{"int":"3"}]},{"prim":"CONTRACT","args":[{"prim":"pair","args":[{"prim":"address"},{"prim":"pair","args":[{"prim":"address"},{"prim":"nat"}]}]}],"annots":["%transfer"]},{"prim":"IF_NONE","args":[[{"prim":"DIP","args":[{"int":"3"},[{"prim":"DUP","annots":["@contract_addr"]}]]},{"prim":"DIG","args":[{"int":"3"}]},{"prim":"PUSH","args":[{"prim":"string"},{"string":"Cannot recover erc contract from:"}]},{"prim":"PAIR"},{"prim":"FAILWITH"}],[{"prim":"DUP","annots":["@my_contract"]},{"prim":"PUSH","args":[{"prim":"mutez"},{"int":"0"}]},{"prim":"DIP","args":[{"int":"7"},[{"prim":"DUP","annots":["@amount"]}]]},{"prim":"DIG","args":[{"int":"7"}]},{"prim":"DIP","args":[{"int":"3"},[{"prim":"DROP"}]]},{"prim":"SELF"},{"prim":"ADDRESS","annots":["@my_address"]},{"prim":"PAIR"},{"prim":"SOURCE"},{"prim":"PAIR"},{"prim":"TRANSFER_TOKENS"}]]},{"prim":"DIP","args":[{"int":"3"},[{"prim":"DROP","args":[{"int":"8"}]}]]},{"prim":"RENAME","annots":["@op"]},{"prim":"CONS"},{"prim":"PAIR"}],[{"prim":"IF_LEFT","args":[[{"prim":"RENAME","annots":["@_uuid_secret_send_to_slash_32"]},{"prim":"DIP","args":[{"int":"2"},[{"prim":"DUP","annots":["@storage"]}]]},{"prim":"DIG","args":[{"int":"2"}]},{"prim":"DIP","args":[[{"prim":"DUP"}]]},{"prim":"SWAP"},{"prim":"CAR","annots":["@uuid"]},{"prim":"DIP","args":[{"int":"2"},[{"prim":"DUP"}]]},{"prim":"DIG","args":[{"int":"2"}]},[{"prim":"CDR"},{"prim":"CAR","annots":["@secret"]}],{"prim":"PUSH","args":[{"prim":"nat"},{"int":"32"}]},{"prim":"DIP","args":[[{"prim":"DUP","annots":["@secret"]}]]},{"prim":"SWAP"},{"prim":"SIZE"},{"prim":"COMPARE"},{"prim":"NEQ"},{"prim":"IF","args":[[{"prim":"PUSH","args":[{"prim":"string"},{"string":"Secret length must be 32"}]},{"prim":"FAILWITH"}],[{"prim":"UNIT"}]]},{"prim":"DROP"},{"prim":"PUSH","args":[{"prim":"mutez"},{"int":"0"}]},{"prim":"AMOUNT"},{"prim":"COMPARE"},{"prim":"GT"},{"prim":"IF","args":[[{"prim":"PUSH","args":[{"prim":"string"},{"string":"Tx amount must be zero"}]},{"prim":"FAILWITH"}],[{"prim":"UNIT"}]]},{"prim":"DROP"},{"prim":"DIP","args":[{"int":"2"},[{"prim":"DUP","annots":["@storage"]}]]},{"prim":"DIG","args":[{"int":"2"}]},{"prim":"CAR","annots":["%swaps"]},{"prim":"DIP","args":[{"int":"2"},[{"prim":"DUP","annots":["@uuid"]}]]},{"prim":"DIG","args":[{"int":"2"}]},{"prim":"GET"},{"prim":"IF_NONE","args":[[{"prim":"PUSH","args":[{"prim":"string"},{"string":"Swap was not initialized"}]},{"prim":"FAILWITH"}],[{"prim":"DUP","annots":["@swap"]},[{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CAR","annots":["%state"]}],{"prim":"IF_LEFT","args":[[{"prim":"RENAME","annots":["@__slash_38"]},{"prim":"DIP","args":[[{"prim":"DUP","annots":["@swap"]}]]},{"prim":"SWAP"},{"prim":"DIP","args":[[{"prim":"DROP"}]]}],[{"prim":"DROP"},{"prim":"PUSH","args":[{"prim":"string"},{"string":"Swap must be in initialized state"}]},{"prim":"FAILWITH"}]]},{"prim":"DIP","args":[[{"prim":"DROP"}]]}]]},{"prim":"RENAME","annots":["@swap"]},{"prim":"DUP","annots":["@swap"]},[{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CAR","annots":["%receiver"]}],{"prim":"SOURCE"},{"prim":"COMPARE"},{"prim":"NEQ"},{"prim":"IF","args":[[{"prim":"PUSH","args":[{"prim":"string"},{"string":"Tx must be sent from receiver address"}]},{"prim":"FAILWITH"}],[{"prim":"UNIT"}]]},{"prim":"DROP"},{"prim":"DUP","annots":["@swap"]},[{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CAR","annots":["%secret_hash"]}],{"prim":"DIP","args":[[{"prim":"DUP","annots":["@swap"]}]]},{"prim":"SWAP"},[{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CAR","annots":["%secret_hash_type"]}],{"prim":"IF_LEFT","args":[[{"prim":"DROP"},{"prim":"DIP","args":[{"int":"2"},[{"prim":"DUP","annots":["@secret"]}]]},{"prim":"DIG","args":[{"int":"2"}]},{"prim":"SHA256"}],[{"prim":"IF_LEFT","args":[[{"prim":"DROP"},{"prim":"DIP","args":[{"int":"2"},[{"prim":"DUP","annots":["@secret"]}]]},{"prim":"DIG","args":[{"int":"2"}]},{"prim":"SHA512"}],[{"prim":"DROP"},{"prim":"DIP","args":[{"int":"2"},[{"prim":"DUP","annots":["@secret"]}]]},{"prim":"DIG","args":[{"int":"2"}]},{"prim":"BLAKE2B"}]]}]]},{"prim":"RENAME","annots":["@hashed_secret"]},{"prim":"COMPARE"},{"prim":"NEQ"},{"prim":"IF","args":[[{"prim":"PUSH","args":[{"prim":"string"},{"string":"Invalid secret"}]},{"prim":"FAILWITH"}],[{"prim":"UNIT"}]]},{"prim":"DROP"},{"prim":"DUP","annots":["@swap"]},{"prim":"DUP"},{"prim":"CAR","annots":["%amount"]},{"prim":"SWAP"},{"prim":"CDR"},{"prim":"DUP"},{"prim":"CAR","annots":["%amount_nat"]},{"prim":"SWAP"},{"prim":"CDR"},{"prim":"DUP"},{"prim":"CAR","annots":["%contract_address"]},{"prim":"SWAP"},{"prim":"CDR"},{"prim":"DUP"},{"prim":"CAR","annots":["%created_at"]},{"prim":"SWAP"},{"prim":"CDR"},{"prim":"DUP"},{"prim":"CAR","annots":["%lock_time"]},{"prim":"SWAP"},{"prim":"CDR"},{"prim":"DUP"},{"prim":"CAR","annots":["%receiver"]},{"prim":"SWAP"},{"prim":"CDR"},{"prim":"DUP"},{"prim":"CAR","annots":["%secret_hash"]},{"prim":"SWAP"},{"prim":"CDR"},{"prim":"DUP"},{"prim":"CAR","annots":["%secret_hash_type"]},{"prim":"SWAP"},{"prim":"CDR"},{"prim":"DUP"},{"prim":"CAR","annots":["%sender"]},{"prim":"SWAP"},{"prim":"CDR"},{"prim":"DUP"},{"prim":"CAR","annots":["%spent_at"]},{"prim":"SWAP"},{"prim":"CDR"},{"prim":"CDR","annots":["%uuid"]},{"prim":"PUSH","args":[{"prim":"or","args":[{"prim":"unit","annots":["%Initialized"]},{"prim":"or","args":[{"prim":"unit","annots":["%ReceiverSpent"]},{"prim":"unit","annots":["%SenderRefunded"]}]}],"annots":[":swap_state"]},{"prim":"Right","args":[{"prim":"Left","args":[{"prim":"Unit"}]}]}]},{"prim":"PAIR","annots":["%state","%uuid"]},{"prim":"SWAP"},{"prim":"PAIR","annots":["%spent_at"]},{"prim":"SWAP"},{"prim":"PAIR","annots":["%sender"]},{"prim":"SWAP"},{"prim":"PAIR","annots":["%secret_hash_type"]},{"prim":"SWAP"},{"prim":"PAIR","annots":["%secret_hash"]},{"prim":"SWAP"},{"prim":"PAIR","annots":["%receiver"]},{"prim":"SWAP"},{"prim":"PAIR","annots":["%lock_time"]},{"prim":"SWAP"},{"prim":"PAIR","annots":["%created_at"]},{"prim":"SWAP"},{"prim":"PAIR","annots":["%contract_address"]},{"prim":"SWAP"},{"prim":"PAIR","annots":["%amount_nat"]},{"prim":"SWAP"},{"prim":"PAIR","annots":["@swap","%amount"]},{"prim":"DUP"},{"prim":"CAR","annots":["%amount"]},{"prim":"SWAP"},{"prim":"CDR"},{"prim":"DUP"},{"prim":"CAR","annots":["%amount_nat"]},{"prim":"SWAP"},{"prim":"CDR"},{"prim":"DUP"},{"prim":"CAR","annots":["%contract_address"]},{"prim":"SWAP"},{"prim":"CDR"},{"prim":"DUP"},{"prim":"CAR","annots":["%created_at"]},{"prim":"SWAP"},{"prim":"CDR"},{"prim":"DUP"},{"prim":"CAR","annots":["%lock_time"]},{"prim":"SWAP"},{"prim":"CDR"},{"prim":"DUP"},{"prim":"CAR","annots":["%receiver"]},{"prim":"SWAP"},{"prim":"CDR"},{"prim":"DUP"},{"prim":"CAR","annots":["%secret_hash"]},{"prim":"SWAP"},{"prim":"CDR"},{"prim":"DUP"},{"prim":"CAR","annots":["%secret_hash_type"]},{"prim":"SWAP"},{"prim":"CDR"},{"prim":"DUP"},{"prim":"CAR","annots":["%sender"]},{"prim":"SWAP"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"NOW","annots":["@timestamp"]},{"prim":"SOME"},{"prim":"PAIR","annots":["%spent_at"]},{"prim":"SWAP"},{"prim":"PAIR","annots":["%sender"]},{"prim":"SWAP"},{"prim":"PAIR","annots":["%secret_hash_type"]},{"prim":"SWAP"},{"prim":"PAIR","annots":["%secret_hash"]},{"prim":"SWAP"},{"prim":"PAIR","annots":["%receiver"]},{"prim":"SWAP"},{"prim":"PAIR","annots":["%lock_time"]},{"prim":"SWAP"},{"prim":"PAIR","annots":["%created_at"]},{"prim":"SWAP"},{"prim":"PAIR","annots":["%contract_address"]},{"prim":"SWAP"},{"prim":"PAIR","annots":["%amount_nat"]},{"prim":"SWAP"},{"prim":"PAIR","annots":["@swap","%amount"]},{"prim":"DIP","args":[{"int":"4"},[{"prim":"DUP","annots":["@storage"]}]]},{"prim":"DIG","args":[{"int":"4"}]},{"prim":"CDR","annots":["%version"]},{"prim":"DIP","args":[{"int":"5"},[{"prim":"DUP","annots":["@storage"]}]]},{"prim":"DIG","args":[{"int":"5"}]},{"prim":"CAR","annots":["%swaps"]},{"prim":"DIP","args":[{"int":"2"},[{"prim":"DUP","annots":["@swap"]}]]},{"prim":"DIG","args":[{"int":"2"}]},{"prim":"SOME"},{"prim":"DIP","args":[{"int":"6"},[{"prim":"DUP","annots":["@uuid"]}]]},{"prim":"DIG","args":[{"int":"6"}]},{"prim":"UPDATE","annots":["@new_swaps"]},{"prim":"PAIR","annots":["%swaps","%version"]},{"prim":"NIL","args":[{"prim":"operation"}]},{"prim":"DIP","args":[{"int":"2"},[{"prim":"DUP","annots":["@swap"]}]]},{"prim":"DIG","args":[{"int":"2"}]},[{"prim":"CDR"},{"prim":"CDR"},{"prim":"CAR","annots":["%contract_address"]}],{"prim":"IF_NONE","args":[[{"prim":"DIP","args":[{"int":"7"},[{"prim":"DUP"}]]},{"prim":"DIG","args":[{"int":"7"}]},[{"prim":"CDR"},{"prim":"CDR","annots":["@send_to"]}],{"prim":"IMPLICIT_ACCOUNT"},{"prim":"DIP","args":[{"int":"3"},[{"prim":"DUP","annots":["@swap"]}]]},{"prim":"DIG","args":[{"int":"3"}]},{"prim":"CAR","annots":["%amount"]},{"prim":"UNIT"},{"prim":"TRANSFER_TOKENS"}],[{"prim":"DUP","annots":["@contract_addr"]},{"prim":"CONTRACT","args":[{"prim":"pair","args":[{"prim":"address"},{"prim":"pair","args":[{"prim":"address"},{"prim":"nat"}]}]}],"annots":["%transfer"]},{"prim":"IF_NONE","args":[[{"prim":"DUP","annots":["@contract_addr"]},{"prim":"PUSH","args":[{"prim":"string"},{"string":"Cannot recover erc contract from:"}]},{"prim":"PAIR"},{"prim":"FAILWITH"}],[{"prim":"DUP","annots":["@my_contract"]},{"prim":"PUSH","args":[{"prim":"mutez"},{"int":"0"}]},{"prim":"DIP","args":[{"int":"6"},[{"prim":"DUP","annots":["@swap"]}]]},{"prim":"DIG","args":[{"int":"6"}]},[{"prim":"CDR"},{"prim":"CAR","annots":["%amount_nat"]}],{"prim":"DIP","args":[{"int":"7"},[{"prim":"DUP","annots":["@swap"]}]]},{"prim":"DIG","args":[{"int":"7"}]},{"prim":"DIP","args":[{"int":"4"},[{"prim":"DROP"}]]},[{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CAR","annots":["%receiver"]}],{"prim":"PAIR"},{"prim":"SELF"},{"prim":"ADDRESS","annots":["@my_address"]},{"prim":"PAIR"},{"prim":"TRANSFER_TOKENS"}]]},{"prim":"DIP","args":[[{"prim":"DROP"}]]}]]},{"prim":"DIP","args":[{"int":"3"},[{"prim":"DROP","args":[{"int":"6"}]}]]},{"prim":"RENAME","annots":["@op"]},{"prim":"CONS"},{"prim":"PAIR"}],[{"prim":"RENAME","annots":["@_uuid_send_to_slash_49"]},{"prim":"DIP","args":[{"int":"2"},[{"prim":"DUP","annots":["@storage"]}]]},{"prim":"DIG","args":[{"int":"2"}]},{"prim":"DIP","args":[[{"prim":"DUP"}]]},{"prim":"SWAP"},{"prim":"CAR","annots":["@uuid"]},{"prim":"PUSH","args":[{"prim":"mutez"},{"int":"0"}]},{"prim":"AMOUNT"},{"prim":"COMPARE"},{"prim":"GT"},{"prim":"IF","args":[[{"prim":"PUSH","args":[{"prim":"string"},{"string":"Tx amount must be zero"}]},{"prim":"FAILWITH"}],[{"prim":"UNIT"}]]},{"prim":"DROP"},{"prim":"DIP","args":[[{"prim":"DUP","annots":["@storage"]}]]},{"prim":"SWAP"},{"prim":"CAR","annots":["%swaps"]},{"prim":"DIP","args":[[{"prim":"DUP","annots":["@uuid"]}]]},{"prim":"SWAP"},{"prim":"GET"},{"prim":"IF_NONE","args":[[{"prim":"PUSH","args":[{"prim":"string"},{"string":"Swap was not initialized"}]},{"prim":"FAILWITH"}],[{"prim":"DUP","annots":["@swap"]},[{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CAR","annots":["%state"]}],{"prim":"IF_LEFT","args":[[{"prim":"RENAME","annots":["@__slash_54"]},{"prim":"DIP","args":[[{"prim":"DUP","annots":["@swap"]}]]},{"prim":"SWAP"},{"prim":"DIP","args":[[{"prim":"DROP"}]]}],[{"prim":"DROP"},{"prim":"PUSH","args":[{"prim":"string"},{"string":"Swap must be in initialized state"}]},{"prim":"FAILWITH"}]]},{"prim":"DIP","args":[[{"prim":"DROP"}]]}]]},{"prim":"RENAME","annots":["@swap"]},{"prim":"NOW","annots":["@timestamp"]},{"prim":"DIP","args":[[{"prim":"DUP","annots":["@swap"]}]]},{"prim":"SWAP"},[{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CAR","annots":["%lock_time"]}],{"prim":"DIP","args":[[{"prim":"DUP","annots":["@timestamp"]}]]},{"prim":"SWAP"},{"prim":"COMPARE"},{"prim":"LE"},{"prim":"IF","args":[[{"prim":"PUSH","args":[{"prim":"string"},{"string":"Too early to refund"}]},{"prim":"FAILWITH"}],[{"prim":"UNIT"}]]},{"prim":"DROP"},{"prim":"DIP","args":[[{"prim":"DUP","annots":["@swap"]}]]},{"prim":"SWAP"},[{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CAR","annots":["%sender"]}],{"prim":"SOURCE"},{"prim":"COMPARE"},{"prim":"NEQ"},{"prim":"IF","args":[[{"prim":"PUSH","args":[{"prim":"string"},{"string":"Tx must be sent from sender address"}]},{"prim":"FAILWITH"}],[{"prim":"UNIT"}]]},{"prim":"DROP"},{"prim":"DIP","args":[[{"prim":"DUP","annots":["@swap"]}]]},{"prim":"SWAP"},{"prim":"DUP"},{"prim":"CAR","annots":["%amount"]},{"prim":"SWAP"},{"prim":"CDR"},{"prim":"DUP"},{"prim":"CAR","annots":["%amount_nat"]},{"prim":"SWAP"},{"prim":"CDR"},{"prim":"DUP"},{"prim":"CAR","annots":["%contract_address"]},{"prim":"SWAP"},{"prim":"CDR"},{"prim":"DUP"},{"prim":"CAR","annots":["%created_at"]},{"prim":"SWAP"},{"prim":"CDR"},{"prim":"DUP"},{"prim":"CAR","annots":["%lock_time"]},{"prim":"SWAP"},{"prim":"CDR"},{"prim":"DUP"},{"prim":"CAR","annots":["%receiver"]},{"prim":"SWAP"},{"prim":"CDR"},{"prim":"DUP"},{"prim":"CAR","annots":["%secret_hash"]},{"prim":"SWAP"},{"prim":"CDR"},{"prim":"DUP"},{"prim":"CAR","annots":["%secret_hash_type"]},{"prim":"SWAP"},{"prim":"CDR"},{"prim":"DUP"},{"prim":"CAR","annots":["%sender"]},{"prim":"SWAP"},{"prim":"CDR"},{"prim":"DUP"},{"prim":"CAR","annots":["%spent_at"]},{"prim":"SWAP"},{"prim":"CDR"},{"prim":"CDR","annots":["%uuid"]},{"prim":"PUSH","args":[{"prim":"or","args":[{"prim":"unit","annots":["%Initialized"]},{"prim":"or","args":[{"prim":"unit","annots":["%ReceiverSpent"]},{"prim":"unit","annots":["%SenderRefunded"]}]}],"annots":[":swap_state"]},{"prim":"Right","args":[{"prim":"Right","args":[{"prim":"Unit"}]}]}]},{"prim":"PAIR","annots":["%state","%uuid"]},{"prim":"SWAP"},{"prim":"PAIR","annots":["%spent_at"]},{"prim":"SWAP"},{"prim":"PAIR","annots":["%sender"]},{"prim":"SWAP"},{"prim":"PAIR","annots":["%secret_hash_type"]},{"prim":"SWAP"},{"prim":"PAIR","annots":["%secret_hash"]},{"prim":"SWAP"},{"prim":"PAIR","annots":["%receiver"]},{"prim":"SWAP"},{"prim":"PAIR","annots":["%lock_time"]},{"prim":"SWAP"},{"prim":"PAIR","annots":["%created_at"]},{"prim":"SWAP"},{"prim":"PAIR","annots":["%contract_address"]},{"prim":"SWAP"},{"prim":"PAIR","annots":["%amount_nat"]},{"prim":"SWAP"},{"prim":"PAIR","annots":["@swap","%amount"]},{"prim":"DUP"},{"prim":"CAR","annots":["%amount"]},{"prim":"SWAP"},{"prim":"CDR"},{"prim":"DUP"},{"prim":"CAR","annots":["%amount_nat"]},{"prim":"SWAP"},{"prim":"CDR"},{"prim":"DUP"},{"prim":"CAR","annots":["%contract_address"]},{"prim":"SWAP"},{"prim":"CDR"},{"prim":"DUP"},{"prim":"CAR","annots":["%created_at"]},{"prim":"SWAP"},{"prim":"CDR"},{"prim":"DUP"},{"prim":"CAR","annots":["%lock_time"]},{"prim":"SWAP"},{"prim":"CDR"},{"prim":"DUP"},{"prim":"CAR","annots":["%receiver"]},{"prim":"SWAP"},{"prim":"CDR"},{"prim":"DUP"},{"prim":"CAR","annots":["%secret_hash"]},{"prim":"SWAP"},{"prim":"CDR"},{"prim":"DUP"},{"prim":"CAR","annots":["%secret_hash_type"]},{"prim":"SWAP"},{"prim":"CDR"},{"prim":"DUP"},{"prim":"CAR","annots":["%sender"]},{"prim":"SWAP"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"DIP","args":[{"int":"10"},[{"prim":"DUP","annots":["@timestamp"]}]]},{"prim":"DIG","args":[{"int":"10"}]},{"prim":"SOME"},{"prim":"PAIR","annots":["%spent_at"]},{"prim":"SWAP"},{"prim":"PAIR","annots":["%sender"]},{"prim":"SWAP"},{"prim":"PAIR","annots":["%secret_hash_type"]},{"prim":"SWAP"},{"prim":"PAIR","annots":["%secret_hash"]},{"prim":"SWAP"},{"prim":"PAIR","annots":["%receiver"]},{"prim":"SWAP"},{"prim":"PAIR","annots":["%lock_time"]},{"prim":"SWAP"},{"prim":"PAIR","annots":["%created_at"]},{"prim":"SWAP"},{"prim":"PAIR","annots":["%contract_address"]},{"prim":"SWAP"},{"prim":"PAIR","annots":["%amount_nat"]},{"prim":"SWAP"},{"prim":"PAIR","annots":["@swap","%amount"]},{"prim":"DIP","args":[{"int":"4"},[{"prim":"DUP","annots":["@storage"]}]]},{"prim":"DIG","args":[{"int":"4"}]},{"prim":"CDR","annots":["%version"]},{"prim":"DIP","args":[{"int":"5"},[{"prim":"DUP","annots":["@storage"]}]]},{"prim":"DIG","args":[{"int":"5"}]},{"prim":"CAR","annots":["%swaps"]},{"prim":"DIP","args":[{"int":"2"},[{"prim":"DUP","annots":["@swap"]}]]},{"prim":"DIG","args":[{"int":"2"}]},{"prim":"SOME"},{"prim":"DIP","args":[{"int":"6"},[{"prim":"DUP","annots":["@uuid"]}]]},{"prim":"DIG","args":[{"int":"6"}]},{"prim":"UPDATE","annots":["@new_swaps"]},{"prim":"PAIR","annots":["%swaps","%version"]},{"prim":"NIL","args":[{"prim":"operation"}]},{"prim":"DIP","args":[{"int":"2"},[{"prim":"DUP","annots":["@swap"]}]]},{"prim":"DIG","args":[{"int":"2"}]},[{"prim":"CDR"},{"prim":"CDR"},{"prim":"CAR","annots":["%contract_address"]}],{"prim":"IF_NONE","args":[[{"prim":"DIP","args":[{"int":"7"},[{"prim":"DUP"}]]},{"prim":"DIG","args":[{"int":"7"}]},{"prim":"CDR","annots":["@send_to"]},{"prim":"IMPLICIT_ACCOUNT"},{"prim":"DIP","args":[{"int":"3"},[{"prim":"DUP","annots":["@swap"]}]]},{"prim":"DIG","args":[{"int":"3"}]},{"prim":"CAR","annots":["%amount"]},{"prim":"UNIT"},{"prim":"TRANSFER_TOKENS"}],[{"prim":"DUP","annots":["@contract_addr"]},{"prim":"CONTRACT","args":[{"prim":"pair","args":[{"prim":"address"},{"prim":"pair","args":[{"prim":"address"},{"prim":"nat"}]}]}],"annots":["%transfer"]},{"prim":"IF_NONE","args":[[{"prim":"DUP","annots":["@contract_addr"]},{"prim":"PUSH","args":[{"prim":"string"},{"string":"Cannot recover erc contract from:"}]},{"prim":"PAIR"},{"prim":"FAILWITH"}],[{"prim":"DUP","annots":["@my_contract"]},{"prim":"PUSH","args":[{"prim":"mutez"},{"int":"0"}]},{"prim":"DIP","args":[{"int":"6"},[{"prim":"DUP","annots":["@swap"]}]]},{"prim":"DIG","args":[{"int":"6"}]},[{"prim":"CDR"},{"prim":"CAR","annots":["%amount_nat"]}],{"prim":"DIP","args":[{"int":"7"},[{"prim":"DUP","annots":["@swap"]}]]},{"prim":"DIG","args":[{"int":"7"}]},{"prim":"DIP","args":[{"int":"4"},[{"prim":"DROP"}]]},[{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CAR","annots":["%sender"]}],{"prim":"PAIR"},{"prim":"SELF"},{"prim":"ADDRESS","annots":["@my_address"]},{"prim":"PAIR"},{"prim":"TRANSFER_TOKENS"}]]},{"prim":"DIP","args":[[{"prim":"DROP"}]]}]]},{"prim":"DIP","args":[{"int":"3"},[{"prim":"DROP","args":[{"int":"6"}]}]]},{"prim":"RENAME","annots":["@op"]},{"prim":"CONS"},{"prim":"PAIR"}]]}]]}]]}]]},{"prim":"DIP","args":[[{"prim":"DROP","args":[{"int":"2"}]}]]}]]}],"storage":{"prim":"Pair","args":[[],{"int":"1"}]}}"#;
    let swap_script_json: Json = unwrap!(json::from_str(swap_contract_script_str));
    let mut operations = vec![];
    let counter = TezosUint(unwrap!(block_on(coin.rpc_client.counter(&coin.my_address().to_string()))) + BigUint::from(1u8));
    let head = unwrap!(block_on(coin.rpc_client.block_header("head")));
    let swap_orig = Operation::origination(Origination {
        counter: counter.clone(),
        fee: BigUint::from(32000u32).into(),
        gas_limit: BigUint::from(300000u32).into(),
        source: coin.my_address.clone(),
        storage_limit: BigUint::from(10000u32).into(),
        balance: BigUint::from(0u8).into(),
        script: swap_script_json,
    });
    operations.push(swap_orig);

    let managed_ledger_script_str = r#"{"code":[{"prim":"parameter","args":[{"prim":"or","args":[{"prim":"or","args":[{"prim":"or","args":[{"prim":"pair","args":[{"prim":"address","annots":[":from"]},{"prim":"pair","args":[{"prim":"address","annots":[":to"]},{"prim":"nat","annots":[":value"]}]}],"annots":["%transfer"]},{"prim":"pair","args":[{"prim":"address","annots":[":spender"]},{"prim":"nat","annots":[":value"]}],"annots":["%approve"]}]},{"prim":"or","args":[{"prim":"pair","args":[{"prim":"pair","args":[{"prim":"address","annots":[":owner"]},{"prim":"address","annots":[":spender"]}]},{"prim":"contract","args":[{"prim":"nat"}]}],"annots":["%getAllowance"]},{"prim":"or","args":[{"prim":"pair","args":[{"prim":"address","annots":[":owner"]},{"prim":"contract","args":[{"prim":"nat"}]}],"annots":["%getBalance"]},{"prim":"pair","args":[{"prim":"unit"},{"prim":"contract","args":[{"prim":"nat"}]}],"annots":["%getTotalSupply"]}]}]}]},{"prim":"or","args":[{"prim":"or","args":[{"prim":"bool","annots":["%setPause"]},{"prim":"address","annots":["%setAdministrator"]}]},{"prim":"or","args":[{"prim":"pair","args":[{"prim":"unit"},{"prim":"contract","args":[{"prim":"address"}]}],"annots":["%getAdministrator"]},{"prim":"or","args":[{"prim":"pair","args":[{"prim":"address","annots":[":to"]},{"prim":"nat","annots":[":value"]}],"annots":["%mint"]},{"prim":"pair","args":[{"prim":"address","annots":[":from"]},{"prim":"nat","annots":[":value"]}],"annots":["%burn"]}]}]}]}]}]},{"prim":"storage","args":[{"prim":"pair","args":[{"prim":"big_map","args":[{"prim":"address"},{"prim":"pair","args":[{"prim":"nat"},{"prim":"map","args":[{"prim":"address"},{"prim":"nat"}]}]}]},{"prim":"pair","args":[{"prim":"address"},{"prim":"pair","args":[{"prim":"bool"},{"prim":"nat"}]}]}]}]},{"prim":"code","args":[[{"prim":"CAST","args":[{"prim":"pair","args":[{"prim":"or","args":[{"prim":"or","args":[{"prim":"or","args":[{"prim":"pair","args":[{"prim":"address"},{"prim":"pair","args":[{"prim":"address"},{"prim":"nat"}]}]},{"prim":"pair","args":[{"prim":"address"},{"prim":"nat"}]}]},{"prim":"or","args":[{"prim":"pair","args":[{"prim":"pair","args":[{"prim":"address"},{"prim":"address"}]},{"prim":"contract","args":[{"prim":"nat"}]}]},{"prim":"or","args":[{"prim":"pair","args":[{"prim":"address"},{"prim":"contract","args":[{"prim":"nat"}]}]},{"prim":"pair","args":[{"prim":"unit"},{"prim":"contract","args":[{"prim":"nat"}]}]}]}]}]},{"prim":"or","args":[{"prim":"or","args":[{"prim":"bool"},{"prim":"address"}]},{"prim":"or","args":[{"prim":"pair","args":[{"prim":"unit"},{"prim":"contract","args":[{"prim":"address"}]}]},{"prim":"or","args":[{"prim":"pair","args":[{"prim":"address"},{"prim":"nat"}]},{"prim":"pair","args":[{"prim":"address"},{"prim":"nat"}]}]}]}]}]},{"prim":"pair","args":[{"prim":"big_map","args":[{"prim":"address"},{"prim":"pair","args":[{"prim":"nat"},{"prim":"map","args":[{"prim":"address"},{"prim":"nat"}]}]}]},{"prim":"pair","args":[{"prim":"address"},{"prim":"pair","args":[{"prim":"bool"},{"prim":"nat"}]}]}]}]}]},{"prim":"DUP"},{"prim":"CAR"},{"prim":"DIP","args":[[{"prim":"CDR"}]]},{"prim":"IF_LEFT","args":[[{"prim":"IF_LEFT","args":[[{"prim":"IF_LEFT","args":[[{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CAR"},{"prim":"IF","args":[[{"prim":"UNIT"},{"prim":"PUSH","args":[{"prim":"string"},{"string":"TokenOperationsArePaused"}]},{"prim":"PAIR"},{"prim":"FAILWITH"}],[]]}]]},{"prim":"DUP"},{"prim":"DUP"},{"prim":"CDR"},{"prim":"CAR"},{"prim":"DIP","args":[[{"prim":"CAR"}]]},{"prim":"COMPARE"},{"prim":"EQ"},{"prim":"IF","args":[[{"prim":"DROP"}],[{"prim":"DUP"},{"prim":"CAR"},{"prim":"SENDER"},{"prim":"COMPARE"},{"prim":"EQ"},{"prim":"IF","args":[[],[{"prim":"DUP"},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"DIP","args":[[{"prim":"DIP","args":[[{"prim":"DUP"}]]},{"prim":"CAR"},{"prim":"SENDER"},{"prim":"PAIR"},{"prim":"DUP"},{"prim":"DIP","args":[[{"prim":"CDR"},{"prim":"DIP","args":[[{"prim":"CAR"}]]},{"prim":"GET"},{"prim":"IF_NONE","args":[[{"prim":"EMPTY_MAP","args":[{"prim":"address"},{"prim":"nat"}]}],[{"prim":"CDR"}]]}]]},{"prim":"CAR"},{"prim":"GET"},{"prim":"IF_NONE","args":[[{"prim":"PUSH","args":[{"prim":"nat"},{"int":"0"}]}],[]]}]]},{"prim":"DUP"},{"prim":"CAR"},{"prim":"DIP","args":[[{"prim":"SENDER"},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"DIP","args":[{"int":"2"},[{"prim":"DUP"}]]},{"prim":"DIG","args":[{"int":"2"}]},{"prim":"SUB"},{"prim":"ISNAT"},{"prim":"IF_NONE","args":[[{"prim":"DIP","args":[[{"prim":"DUP"}]]},{"prim":"SWAP"},{"prim":"DIP","args":[[{"prim":"DUP"}]]},{"prim":"SWAP"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"PAIR"},{"prim":"PUSH","args":[{"prim":"string"},{"string":"NotEnoughAllowance"}]},{"prim":"PAIR"},{"prim":"FAILWITH"}],[]]}]]},{"prim":"PAIR"}]]},{"prim":"PAIR"},{"prim":"DIP","args":[[{"prim":"DROP"},{"prim":"DROP"}]]},{"prim":"DIP","args":[[{"prim":"DUP"}]]},{"prim":"SWAP"},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"CAR"}]]},{"prim":"SWAP"},{"prim":"DIP","args":[[{"prim":"CAR"}]]},{"prim":"GET"},{"prim":"IF_NONE","args":[[{"prim":"PUSH","args":[{"prim":"nat"},{"int":"0"}]},{"prim":"DIP","args":[[{"prim":"EMPTY_MAP","args":[{"prim":"address"},{"prim":"nat"}]}]]},{"prim":"PAIR"},{"prim":"EMPTY_MAP","args":[{"prim":"address"},{"prim":"nat"}]}],[{"prim":"DUP"},{"prim":"CDR"}]]},{"prim":"DIP","args":[{"int":"2"},[{"prim":"DUP"}]]},{"prim":"DIG","args":[{"int":"2"}]},{"prim":"CDR"},{"prim":"CDR"},{"prim":"DUP"},{"prim":"INT"},{"prim":"EQ"},{"prim":"IF","args":[[{"prim":"DROP"},{"prim":"NONE","args":[{"prim":"nat"}]}],[{"prim":"SOME"}]]},{"prim":"DIP","args":[{"int":"3"},[{"prim":"DUP"}]]},{"prim":"DIG","args":[{"int":"3"}]},{"prim":"CDR"},{"prim":"CAR"},{"prim":"UPDATE"},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"DIP","args":[[{"prim":"CAR"}]]},{"prim":"CDR"}]]},{"prim":"DIP","args":[[{"prim":"DROP"}]]},{"prim":"SWAP"},{"prim":"PAIR"},{"prim":"SWAP"},{"prim":"CAR"},{"prim":"DIP","args":[[{"prim":"SOME"}]]},{"prim":"DIP","args":[[{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"CAR"}]]}]]},{"prim":"UPDATE"},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"DIP","args":[[{"prim":"CDR"}]]},{"prim":"CAR"}]]},{"prim":"DIP","args":[[{"prim":"DROP"}]]},{"prim":"PAIR"}]]}]]},{"prim":"DIP","args":[[{"prim":"DUP"}]]},{"prim":"SWAP"},{"prim":"DIP","args":[[{"prim":"DUP"}]]},{"prim":"SWAP"},{"prim":"CDR"},{"prim":"CAR"},{"prim":"DIP","args":[[{"prim":"CAR"}]]},{"prim":"GET"},{"prim":"IF_NONE","args":[[{"prim":"DUP"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"INT"},{"prim":"EQ"},{"prim":"IF","args":[[{"prim":"NONE","args":[{"prim":"pair","args":[{"prim":"nat"},{"prim":"map","args":[{"prim":"address"},{"prim":"nat"}]}]}]}],[{"prim":"DUP"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"DIP","args":[[{"prim":"EMPTY_MAP","args":[{"prim":"address"},{"prim":"nat"}]}]]},{"prim":"PAIR"},{"prim":"SOME"}]]}],[{"prim":"DIP","args":[[{"prim":"DUP"}]]},{"prim":"SWAP"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"CAR"}]]},{"prim":"ADD"},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"DIP","args":[[{"prim":"CDR"}]]},{"prim":"CAR"}]]},{"prim":"DIP","args":[[{"prim":"DROP"}]]},{"prim":"PAIR"},{"prim":"SOME"}]]},{"prim":"SWAP"},{"prim":"DUP"},{"prim":"DIP","args":[[{"prim":"CDR"},{"prim":"CAR"},{"prim":"DIP","args":[[{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"CAR"}]]}]]},{"prim":"UPDATE"},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"DIP","args":[[{"prim":"CDR"}]]},{"prim":"CAR"}]]},{"prim":"DIP","args":[[{"prim":"DROP"}]]},{"prim":"PAIR"}]]},{"prim":"DUP"},{"prim":"DIP","args":[[{"prim":"CDR"},{"prim":"CDR"},{"prim":"INT"},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"}]]},{"prim":"ADD"},{"prim":"ISNAT"},{"prim":"IF_NONE","args":[[{"prim":"PUSH","args":[{"prim":"string"},{"string":"Internal: Negative total supply"}]},{"prim":"FAILWITH"}],[]]},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"CDR"}]]},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"DIP","args":[[{"prim":"CAR"}]]},{"prim":"CDR"}]]},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"DIP","args":[[{"prim":"CAR"}]]},{"prim":"CDR"}]]},{"prim":"DIP","args":[[{"prim":"DROP"}]]},{"prim":"SWAP"},{"prim":"PAIR"},{"prim":"SWAP"},{"prim":"PAIR"},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"DIP","args":[[{"prim":"CAR"}]]},{"prim":"CDR"}]]},{"prim":"DIP","args":[[{"prim":"DROP"}]]},{"prim":"SWAP"},{"prim":"PAIR"}]]},{"prim":"DIP","args":[[{"prim":"DUP"}]]},{"prim":"SWAP"},{"prim":"DIP","args":[[{"prim":"DUP"}]]},{"prim":"SWAP"},{"prim":"CAR"},{"prim":"DIP","args":[[{"prim":"CAR"}]]},{"prim":"GET"},{"prim":"IF_NONE","args":[[{"prim":"CDR"},{"prim":"CDR"},{"prim":"PUSH","args":[{"prim":"nat"},{"int":"0"}]},{"prim":"SWAP"},{"prim":"PAIR"},{"prim":"PUSH","args":[{"prim":"string"},{"string":"NotEnoughBalance"}]},{"prim":"PAIR"},{"prim":"FAILWITH"}],[]]},{"prim":"DUP"},{"prim":"CAR"},{"prim":"DIP","args":[{"int":"2"},[{"prim":"DUP"}]]},{"prim":"DIG","args":[{"int":"2"}]},{"prim":"CDR"},{"prim":"CDR"},{"prim":"SWAP"},{"prim":"SUB"},{"prim":"ISNAT"},{"prim":"IF_NONE","args":[[{"prim":"CAR"},{"prim":"DIP","args":[[{"prim":"DUP"}]]},{"prim":"SWAP"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"PAIR"},{"prim":"PUSH","args":[{"prim":"string"},{"string":"NotEnoughBalance"}]},{"prim":"PAIR"},{"prim":"FAILWITH"}],[]]},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"DIP","args":[[{"prim":"CDR"}]]},{"prim":"CAR"}]]},{"prim":"DIP","args":[[{"prim":"DROP"}]]},{"prim":"PAIR"},{"prim":"DIP","args":[[{"prim":"DUP"}]]},{"prim":"SWAP"},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"CAR"},{"prim":"INT"},{"prim":"EQ"},{"prim":"IF","args":[[{"prim":"DUP"},{"prim":"CDR"},{"prim":"SIZE"},{"prim":"INT"},{"prim":"EQ"},{"prim":"IF","args":[[{"prim":"DROP"},{"prim":"NONE","args":[{"prim":"pair","args":[{"prim":"nat"},{"prim":"map","args":[{"prim":"address"},{"prim":"nat"}]}]}]}],[{"prim":"SOME"}]]}],[{"prim":"SOME"}]]},{"prim":"SWAP"},{"prim":"CAR"},{"prim":"DIP","args":[[{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"CAR"}]]}]]},{"prim":"UPDATE"},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"DIP","args":[[{"prim":"CDR"}]]},{"prim":"CAR"}]]},{"prim":"DIP","args":[[{"prim":"DROP"}]]},{"prim":"PAIR"}]]},{"prim":"DUP"},{"prim":"DIP","args":[[{"prim":"CDR"},{"prim":"CDR"},{"prim":"NEG"},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"}]]},{"prim":"ADD"},{"prim":"ISNAT"},{"prim":"IF_NONE","args":[[{"prim":"PUSH","args":[{"prim":"string"},{"string":"Internal: Negative total supply"}]},{"prim":"FAILWITH"}],[]]},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"CDR"}]]},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"DIP","args":[[{"prim":"CAR"}]]},{"prim":"CDR"}]]},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"DIP","args":[[{"prim":"CAR"}]]},{"prim":"CDR"}]]},{"prim":"DIP","args":[[{"prim":"DROP"}]]},{"prim":"SWAP"},{"prim":"PAIR"},{"prim":"SWAP"},{"prim":"PAIR"},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"DIP","args":[[{"prim":"CAR"}]]},{"prim":"CDR"}]]},{"prim":"DIP","args":[[{"prim":"DROP"}]]},{"prim":"SWAP"},{"prim":"PAIR"}]]},{"prim":"DROP"}]]},{"prim":"NIL","args":[{"prim":"operation"}]},{"prim":"PAIR"}],[{"prim":"SENDER"},{"prim":"PAIR"},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CAR"},{"prim":"IF","args":[[{"prim":"UNIT"},{"prim":"PUSH","args":[{"prim":"string"},{"string":"TokenOperationsArePaused"}]},{"prim":"PAIR"},{"prim":"FAILWITH"}],[]]}]]},{"prim":"DIP","args":[[{"prim":"DUP"}]]},{"prim":"SWAP"},{"prim":"DIP","args":[[{"prim":"DUP"}]]},{"prim":"SWAP"},{"prim":"DUP"},{"prim":"DIP","args":[[{"prim":"CAR"},{"prim":"DIP","args":[[{"prim":"CAR"}]]},{"prim":"GET"},{"prim":"IF_NONE","args":[[{"prim":"EMPTY_MAP","args":[{"prim":"address"},{"prim":"nat"}]}],[{"prim":"CDR"}]]}]]},{"prim":"CDR"},{"prim":"CAR"},{"prim":"GET"},{"prim":"IF_NONE","args":[[{"prim":"PUSH","args":[{"prim":"nat"},{"int":"0"}]}],[]]},{"prim":"DUP"},{"prim":"INT"},{"prim":"EQ"},{"prim":"IF","args":[[{"prim":"DROP"}],[{"prim":"DIP","args":[[{"prim":"DUP"}]]},{"prim":"SWAP"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"INT"},{"prim":"EQ"},{"prim":"IF","args":[[{"prim":"DROP"}],[{"prim":"PUSH","args":[{"prim":"string"},{"string":"UnsafeAllowanceChange"}]},{"prim":"PAIR"},{"prim":"FAILWITH"}]]}]]},{"prim":"DIP","args":[[{"prim":"DUP"}]]},{"prim":"SWAP"},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"CAR"}]]},{"prim":"SWAP"},{"prim":"DIP","args":[[{"prim":"CAR"}]]},{"prim":"GET"},{"prim":"IF_NONE","args":[[{"prim":"PUSH","args":[{"prim":"nat"},{"int":"0"}]},{"prim":"DIP","args":[[{"prim":"EMPTY_MAP","args":[{"prim":"address"},{"prim":"nat"}]}]]},{"prim":"PAIR"},{"prim":"EMPTY_MAP","args":[{"prim":"address"},{"prim":"nat"}]}],[{"prim":"DUP"},{"prim":"CDR"}]]},{"prim":"DIP","args":[{"int":"2"},[{"prim":"DUP"}]]},{"prim":"DIG","args":[{"int":"2"}]},{"prim":"CDR"},{"prim":"CDR"},{"prim":"DUP"},{"prim":"INT"},{"prim":"EQ"},{"prim":"IF","args":[[{"prim":"DROP"},{"prim":"NONE","args":[{"prim":"nat"}]}],[{"prim":"SOME"}]]},{"prim":"DIP","args":[{"int":"3"},[{"prim":"DUP"}]]},{"prim":"DIG","args":[{"int":"3"}]},{"prim":"CDR"},{"prim":"CAR"},{"prim":"UPDATE"},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"DIP","args":[[{"prim":"CAR"}]]},{"prim":"CDR"}]]},{"prim":"DIP","args":[[{"prim":"DROP"}]]},{"prim":"SWAP"},{"prim":"PAIR"},{"prim":"SWAP"},{"prim":"CAR"},{"prim":"DIP","args":[[{"prim":"SOME"}]]},{"prim":"DIP","args":[[{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"CAR"}]]}]]},{"prim":"UPDATE"},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"DIP","args":[[{"prim":"CDR"}]]},{"prim":"CAR"}]]},{"prim":"DIP","args":[[{"prim":"DROP"}]]},{"prim":"PAIR"},{"prim":"NIL","args":[{"prim":"operation"}]},{"prim":"PAIR"}]]}],[{"prim":"IF_LEFT","args":[[{"prim":"DUP"},{"prim":"CAR"},{"prim":"DIP","args":[[{"prim":"CDR"}]]},{"prim":"DIP","args":[[{"prim":"DIP","args":[[{"prim":"DUP"}]]},{"prim":"SWAP"}]]},{"prim":"PAIR"},{"prim":"DUP"},{"prim":"CAR"},{"prim":"DIP","args":[[{"prim":"CDR"}]]},{"prim":"DUP"},{"prim":"DIP","args":[[{"prim":"CAR"},{"prim":"DIP","args":[[{"prim":"CAR"}]]},{"prim":"GET"},{"prim":"IF_NONE","args":[[{"prim":"EMPTY_MAP","args":[{"prim":"address"},{"prim":"nat"}]}],[{"prim":"CDR"}]]}]]},{"prim":"CDR"},{"prim":"GET"},{"prim":"IF_NONE","args":[[{"prim":"PUSH","args":[{"prim":"nat"},{"int":"0"}]}],[]]},{"prim":"DIP","args":[[{"prim":"AMOUNT"}]]},{"prim":"TRANSFER_TOKENS"},{"prim":"NIL","args":[{"prim":"operation"}]},{"prim":"SWAP"},{"prim":"CONS"},{"prim":"PAIR"}],[{"prim":"IF_LEFT","args":[[{"prim":"DUP"},{"prim":"CAR"},{"prim":"DIP","args":[[{"prim":"CDR"}]]},{"prim":"DIP","args":[[{"prim":"DIP","args":[[{"prim":"DUP"}]]},{"prim":"SWAP"}]]},{"prim":"PAIR"},{"prim":"DUP"},{"prim":"CAR"},{"prim":"DIP","args":[[{"prim":"CDR"}]]},{"prim":"DIP","args":[[{"prim":"CAR"}]]},{"prim":"GET"},{"prim":"IF_NONE","args":[[{"prim":"PUSH","args":[{"prim":"nat"},{"int":"0"}]}],[{"prim":"CAR"}]]},{"prim":"DIP","args":[[{"prim":"AMOUNT"}]]},{"prim":"TRANSFER_TOKENS"},{"prim":"NIL","args":[{"prim":"operation"}]},{"prim":"SWAP"},{"prim":"CONS"},{"prim":"PAIR"}],[{"prim":"DUP"},{"prim":"CAR"},{"prim":"DIP","args":[[{"prim":"CDR"}]]},{"prim":"DIP","args":[[{"prim":"DIP","args":[[{"prim":"DUP"}]]},{"prim":"SWAP"}]]},{"prim":"PAIR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"DIP","args":[[{"prim":"AMOUNT"}]]},{"prim":"TRANSFER_TOKENS"},{"prim":"NIL","args":[{"prim":"operation"}]},{"prim":"SWAP"},{"prim":"CONS"},{"prim":"PAIR"}]]}]]}]]}],[{"prim":"IF_LEFT","args":[[{"prim":"IF_LEFT","args":[[{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"CDR"},{"prim":"CAR"},{"prim":"SENDER"},{"prim":"COMPARE"},{"prim":"EQ"},{"prim":"IF","args":[[],[{"prim":"UNIT"},{"prim":"PUSH","args":[{"prim":"string"},{"string":"SenderIsNotAdmin"}]},{"prim":"PAIR"},{"prim":"FAILWITH"}]]}]]},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"CDR"}]]},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"DIP","args":[[{"prim":"CAR"}]]},{"prim":"CDR"}]]},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"DIP","args":[[{"prim":"CDR"}]]},{"prim":"CAR"}]]},{"prim":"DIP","args":[[{"prim":"DROP"}]]},{"prim":"PAIR"},{"prim":"SWAP"},{"prim":"PAIR"},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"DIP","args":[[{"prim":"CAR"}]]},{"prim":"CDR"}]]},{"prim":"DIP","args":[[{"prim":"DROP"}]]},{"prim":"SWAP"},{"prim":"PAIR"},{"prim":"NIL","args":[{"prim":"operation"}]},{"prim":"PAIR"}],[{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"CDR"},{"prim":"CAR"},{"prim":"SENDER"},{"prim":"COMPARE"},{"prim":"EQ"},{"prim":"IF","args":[[],[{"prim":"UNIT"},{"prim":"PUSH","args":[{"prim":"string"},{"string":"SenderIsNotAdmin"}]},{"prim":"PAIR"},{"prim":"FAILWITH"}]]}]]},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"CDR"}]]},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"DIP","args":[[{"prim":"CDR"}]]},{"prim":"CAR"}]]},{"prim":"DIP","args":[[{"prim":"DROP"}]]},{"prim":"PAIR"},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"DIP","args":[[{"prim":"CAR"}]]},{"prim":"CDR"}]]},{"prim":"DIP","args":[[{"prim":"DROP"}]]},{"prim":"SWAP"},{"prim":"PAIR"},{"prim":"NIL","args":[{"prim":"operation"}]},{"prim":"PAIR"}]]}],[{"prim":"IF_LEFT","args":[[{"prim":"DUP"},{"prim":"CAR"},{"prim":"DIP","args":[[{"prim":"CDR"}]]},{"prim":"DIP","args":[[{"prim":"DIP","args":[[{"prim":"DUP"}]]},{"prim":"SWAP"}]]},{"prim":"PAIR"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CAR"},{"prim":"DIP","args":[[{"prim":"AMOUNT"}]]},{"prim":"TRANSFER_TOKENS"},{"prim":"NIL","args":[{"prim":"operation"}]},{"prim":"SWAP"},{"prim":"CONS"},{"prim":"PAIR"}],[{"prim":"IF_LEFT","args":[[{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"CDR"},{"prim":"CAR"},{"prim":"SENDER"},{"prim":"COMPARE"},{"prim":"EQ"},{"prim":"IF","args":[[],[{"prim":"UNIT"},{"prim":"PUSH","args":[{"prim":"string"},{"string":"SenderIsNotAdmin"}]},{"prim":"PAIR"},{"prim":"FAILWITH"}]]}]]},{"prim":"DIP","args":[[{"prim":"DUP"}]]},{"prim":"SWAP"},{"prim":"DIP","args":[[{"prim":"DUP"}]]},{"prim":"SWAP"},{"prim":"CAR"},{"prim":"DIP","args":[[{"prim":"CAR"}]]},{"prim":"GET"},{"prim":"IF_NONE","args":[[{"prim":"DUP"},{"prim":"CDR"},{"prim":"INT"},{"prim":"EQ"},{"prim":"IF","args":[[{"prim":"NONE","args":[{"prim":"pair","args":[{"prim":"nat"},{"prim":"map","args":[{"prim":"address"},{"prim":"nat"}]}]}]}],[{"prim":"DUP"},{"prim":"CDR"},{"prim":"DIP","args":[[{"prim":"EMPTY_MAP","args":[{"prim":"address"},{"prim":"nat"}]}]]},{"prim":"PAIR"},{"prim":"SOME"}]]}],[{"prim":"DIP","args":[[{"prim":"DUP"}]]},{"prim":"SWAP"},{"prim":"CDR"},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"CAR"}]]},{"prim":"ADD"},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"DIP","args":[[{"prim":"CDR"}]]},{"prim":"CAR"}]]},{"prim":"DIP","args":[[{"prim":"DROP"}]]},{"prim":"PAIR"},{"prim":"SOME"}]]},{"prim":"SWAP"},{"prim":"DUP"},{"prim":"DIP","args":[[{"prim":"CAR"},{"prim":"DIP","args":[[{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"CAR"}]]}]]},{"prim":"UPDATE"},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"DIP","args":[[{"prim":"CDR"}]]},{"prim":"CAR"}]]},{"prim":"DIP","args":[[{"prim":"DROP"}]]},{"prim":"PAIR"}]]},{"prim":"DUP"},{"prim":"DIP","args":[[{"prim":"CDR"},{"prim":"INT"},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"}]]},{"prim":"ADD"},{"prim":"ISNAT"},{"prim":"IF_NONE","args":[[{"prim":"PUSH","args":[{"prim":"string"},{"string":"Internal: Negative total supply"}]},{"prim":"FAILWITH"}],[]]},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"CDR"}]]},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"DIP","args":[[{"prim":"CAR"}]]},{"prim":"CDR"}]]},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"DIP","args":[[{"prim":"CAR"}]]},{"prim":"CDR"}]]},{"prim":"DIP","args":[[{"prim":"DROP"}]]},{"prim":"SWAP"},{"prim":"PAIR"},{"prim":"SWAP"},{"prim":"PAIR"},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"DIP","args":[[{"prim":"CAR"}]]},{"prim":"CDR"}]]},{"prim":"DIP","args":[[{"prim":"DROP"}]]},{"prim":"SWAP"},{"prim":"PAIR"}]]},{"prim":"DROP"},{"prim":"NIL","args":[{"prim":"operation"}]},{"prim":"PAIR"}],[{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"CDR"},{"prim":"CAR"},{"prim":"SENDER"},{"prim":"COMPARE"},{"prim":"EQ"},{"prim":"IF","args":[[],[{"prim":"UNIT"},{"prim":"PUSH","args":[{"prim":"string"},{"string":"SenderIsNotAdmin"}]},{"prim":"PAIR"},{"prim":"FAILWITH"}]]}]]},{"prim":"DIP","args":[[{"prim":"DUP"}]]},{"prim":"SWAP"},{"prim":"DIP","args":[[{"prim":"DUP"}]]},{"prim":"SWAP"},{"prim":"CAR"},{"prim":"DIP","args":[[{"prim":"CAR"}]]},{"prim":"GET"},{"prim":"IF_NONE","args":[[{"prim":"CDR"},{"prim":"PUSH","args":[{"prim":"nat"},{"int":"0"}]},{"prim":"SWAP"},{"prim":"PAIR"},{"prim":"PUSH","args":[{"prim":"string"},{"string":"NotEnoughBalance"}]},{"prim":"PAIR"},{"prim":"FAILWITH"}],[]]},{"prim":"DUP"},{"prim":"CAR"},{"prim":"DIP","args":[{"int":"2"},[{"prim":"DUP"}]]},{"prim":"DIG","args":[{"int":"2"}]},{"prim":"CDR"},{"prim":"SWAP"},{"prim":"SUB"},{"prim":"ISNAT"},{"prim":"IF_NONE","args":[[{"prim":"CAR"},{"prim":"DIP","args":[[{"prim":"DUP"}]]},{"prim":"SWAP"},{"prim":"CDR"},{"prim":"PAIR"},{"prim":"PUSH","args":[{"prim":"string"},{"string":"NotEnoughBalance"}]},{"prim":"PAIR"},{"prim":"FAILWITH"}],[]]},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"DIP","args":[[{"prim":"CDR"}]]},{"prim":"CAR"}]]},{"prim":"DIP","args":[[{"prim":"DROP"}]]},{"prim":"PAIR"},{"prim":"DIP","args":[[{"prim":"DUP"}]]},{"prim":"SWAP"},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"CAR"},{"prim":"INT"},{"prim":"EQ"},{"prim":"IF","args":[[{"prim":"DUP"},{"prim":"CDR"},{"prim":"SIZE"},{"prim":"INT"},{"prim":"EQ"},{"prim":"IF","args":[[{"prim":"DROP"},{"prim":"NONE","args":[{"prim":"pair","args":[{"prim":"nat"},{"prim":"map","args":[{"prim":"address"},{"prim":"nat"}]}]}]}],[{"prim":"SOME"}]]}],[{"prim":"SOME"}]]},{"prim":"SWAP"},{"prim":"CAR"},{"prim":"DIP","args":[[{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"CAR"}]]}]]},{"prim":"UPDATE"},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"DIP","args":[[{"prim":"CDR"}]]},{"prim":"CAR"}]]},{"prim":"DIP","args":[[{"prim":"DROP"}]]},{"prim":"PAIR"}]]},{"prim":"DUP"},{"prim":"DIP","args":[[{"prim":"CDR"},{"prim":"NEG"},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"CDR"},{"prim":"CDR"},{"prim":"CDR"}]]},{"prim":"ADD"},{"prim":"ISNAT"},{"prim":"IF_NONE","args":[[{"prim":"PUSH","args":[{"prim":"string"},{"string":"Internal: Negative total supply"}]},{"prim":"FAILWITH"}],[]]},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"CDR"}]]},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"DIP","args":[[{"prim":"CAR"}]]},{"prim":"CDR"}]]},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"DIP","args":[[{"prim":"CAR"}]]},{"prim":"CDR"}]]},{"prim":"DIP","args":[[{"prim":"DROP"}]]},{"prim":"SWAP"},{"prim":"PAIR"},{"prim":"SWAP"},{"prim":"PAIR"},{"prim":"DIP","args":[[{"prim":"DUP"},{"prim":"DIP","args":[[{"prim":"CAR"}]]},{"prim":"CDR"}]]},{"prim":"DIP","args":[[{"prim":"DROP"}]]},{"prim":"SWAP"},{"prim":"PAIR"}]]},{"prim":"DROP"},{"prim":"NIL","args":[{"prim":"operation"}]},{"prim":"PAIR"}]]}]]}]]}]]}]]}],"storage":{"prim":"Pair","args":[[],{"prim":"Pair","args":[{"string":"tz1RZUEpGCVgDR9Q1GZD8bsp4WyWpNhu1MRY"},{"prim":"Pair","args":[{"prim":"False"},{"int":"0"}]}]}]}}"#;
    let managed_ledger_script_json: Json = unwrap!(json::from_str(managed_ledger_script_str));
    let swap_orig = Operation::origination(Origination {
        counter: TezosUint(counter.0 + BigUint::from(1u8)),
        fee: BigUint::from(21000u32).into(),
        gas_limit: BigUint::from(150000u32).into(),
        source: coin.my_address.clone(),
        storage_limit: BigUint::from(6000u32).into(),
        balance: BigUint::from(0u8).into(),
        script: managed_ledger_script_json,
    });
    operations.push(swap_orig);

    let forge_req = ForgeOperationsRequest {
        branch: head.hash.clone(),
        contents: operations.clone()
    };
    let mut tx_bytes = unwrap!(block_on(coin.rpc_client.forge_operations(&head.chain_id, &head.hash, forge_req)));
    let mut prefixed = vec![3u8];
    prefixed.append(&mut tx_bytes.0);
    let sig_hash = blake2b_256(&prefixed);
    let sig = unwrap!(coin.sign_message(&*sig_hash));
    let signature = TezosSignature {
        prefix: ED_SIG_PREFIX.to_vec(),
        data: sig,
    };
    let preapply_req = PreapplyOperationsRequest(vec![PreapplyOperation {
        branch: head.hash.clone(),
        contents: operations,
        protocol: head.protocol.clone(),
        signature: format!("{}", signature),
    }]);
    unwrap!(block_on(coin.rpc_client.preapply_operations(preapply_req)));
    prefixed.extend_from_slice(&signature.data);
    prefixed.remove(0);
    let hex_encoded = hex::encode(&prefixed);
    let hash = unwrap!(block_on(coin.rpc_client.inject_operation(&hex_encoded)));
    let op_hash: OpHash = unwrap!(hash.parse());
    let branch: TezosBlockHash = unwrap!(head.hash.parse());
    let result = unwrap!(block_on(coin.wait_for_operation_confirmation(
            op_hash,
            1,
            now_ms() / 1000 + 120,
            1,
            branch.data,
        )));
    (result[0].clone().metadata.operation_result.unwrap().originated_contracts.unwrap()[0].clone(),
    result[1].clone().metadata.operation_result.unwrap().originated_contracts.unwrap()[0].clone())
}