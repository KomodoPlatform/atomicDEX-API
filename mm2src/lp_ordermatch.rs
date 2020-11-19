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
//  lp_ordermatch.rs
//  marketmaker
//
#![allow(uncommon_codepoints)]
#![cfg_attr(not(feature = "native"), allow(dead_code))]

use async_trait::async_trait;
use bigdecimal::BigDecimal;
use blake2::digest::{Update, VariableOutput};
use blake2::VarBlake2b;
use coins::utxo::{compressed_pub_key_from_priv_raw, ChecksumType};
use coins::{lp_coinfindᵃ, BalanceTradeFeeUpdatedHandler, MmCoinEnum, TradeFee};
use common::executor::{spawn, Timer};
use common::mm_ctx::{from_ctx, MmArc, MmWeak};
use common::mm_number::{Fraction, MmNumber};
use common::{bits256, block_on, json_dir_entries, new_uuid, now_ms, remove_file, write};
use either::Either;
use futures::{compat::Future01CompatExt, lock::Mutex as AsyncMutex, StreamExt};
use gstuff::slurp;
use hash256_std_hasher::Hash256StdHasher;
use hash_db::{Hasher, EMPTY_PREFIX};
use http::Response;
use mm2_libp2p::{decode_signed, encode_and_sign, encode_message, pub_sub_topic, TopicPrefix, TOPIC_SEPARATOR};
#[cfg(test)] use mocktopus::macros::*;
use num_rational::BigRational;
use num_traits::identities::Zero;
use rpc::v1::types::H256 as H256Json;
use serde_json::{self as json, Value as Json};
use sp_trie::{delta_trie_root, generate_trie_proof, verify_trie_proof, DBValue, HashDBT, MemoryDB, Trie,
              TrieConfiguration, TrieDB, TrieDBMut, TrieHash, TrieMut};
use std::collections::hash_map::{Entry, HashMap, RawEntryMut};
use std::collections::{BTreeSet, HashSet};
use std::convert::TryInto;
use std::fmt;
use std::fs::DirEntry;
use std::path::PathBuf;
use std::sync::Arc;
use trie_db::NodeCodec as NodeCodecT;
use uuid::Uuid;

use crate::mm2::{lp_network::{broadcast_p2p_msg, request_one_peer, request_relays, subscribe_to_topic, P2PRequest,
                              PeerDecodedResponse},
                 lp_swap::{calc_max_maker_vol, check_balance_for_maker_swap, check_balance_for_taker_swap,
                           is_pubkey_banned, lp_atomic_locktime, run_maker_swap, run_taker_swap,
                           AtomicLocktimeVersion, MakerSwap, RunMakerSwapInput, RunTakerSwapInput,
                           SwapConfirmationsSettings, TakerSwap}};

#[path = "lp_ordermatch/new_protocol.rs"] mod new_protocol;
#[path = "lp_ordermatch/order_requests_tracker.rs"]
mod order_requests_tracker;
use order_requests_tracker::OrderRequestsTracker;

#[cfg(test)]
#[cfg(feature = "native")]
#[path = "ordermatch_tests.rs"]
mod ordermatch_tests;

pub const ORDERBOOK_PREFIX: TopicPrefix = "orbk";
const MIN_ORDER_KEEP_ALIVE_INTERVAL: u64 = 30;
const MAKER_ORDER_TIMEOUT: u64 = MIN_ORDER_KEEP_ALIVE_INTERVAL * 3;
const TAKER_ORDER_TIMEOUT: u64 = 30;
const ORDER_MATCH_TIMEOUT: u64 = 30;
const ORDERBOOK_REQUESTING_TIMEOUT: u64 = MIN_ORDER_KEEP_ALIVE_INTERVAL * 2;
const INACTIVE_ORDER_TIMEOUT: u64 = 240;
const MIN_TRADING_VOL: &str = "0.00777";

/// Alphabetically ordered orderbook pair
type AlbOrderedOrderbookPair = String;

impl From<(new_protocol::MakerOrderCreated, String)> for OrderbookItem {
    fn from(tuple: (new_protocol::MakerOrderCreated, String)) -> OrderbookItem {
        let (order, pubkey) = tuple;

        OrderbookItem {
            pubkey,
            base: order.base,
            rel: order.rel,
            price: order.price,
            max_volume: order.max_volume,
            min_volume: order.min_volume,
            uuid: order.uuid.into(),
            created_at: order.created_at,
        }
    }
}

async fn process_orders_keep_alive(
    ctx: MmArc,
    propagated_from_peer: String,
    from_pubkey: String,
    keep_alive: new_protocol::PubkeyKeepAlive,
) -> bool {
    let ordermatch_ctx = OrdermatchContext::from_ctx(&ctx).expect("from_ctx failed");
    let to_request = ordermatch_ctx
        .orderbook
        .lock()
        .await
        .process_keep_alive(&from_pubkey, keep_alive);

    let req = match to_request {
        Some(req) => req,
        // The message was processed, simply forward it
        None => return true,
    };

    let resp =
        request_one_peer::<SyncPubkeyOrderbookStateRes>(ctx.clone(), P2PRequest::Ordermatch(req), propagated_from_peer)
            .await;

    let response = match resp {
        Ok(Some(resp)) => resp,
        _ => return false,
    };

    let mut orderbook = ordermatch_ctx.orderbook.lock().await;
    for (pair, diff) in response.pair_orders_diff {
        let old_root = orderbook
            .pubkeys_state
            .get(&from_pubkey)
            .expect("Pubkey state always exists at this point")
            .trie_roots
            .get(&pair)
            .map(|val| *val)
            .unwrap_or(H64::default());
        let new_root = match diff {
            DeltaOrFullTrie::Delta(delta) => {
                let delta = delta.into_iter().map(|(uuid, order)| {
                    (
                        *uuid.as_bytes(),
                        order.map(|o| rmp_serde::to_vec(&o).expect("Serialization failed")),
                    )
                });
                delta_trie_root::<Layout, _, _, _, _, _>(&mut orderbook.memory_db, old_root, delta)
                    .expect("Pair trie should always exist")
            },
            DeltaOrFullTrie::FullTrie(values) => {
                orderbook.memory_db.remove_and_purge(&old_root, EMPTY_PREFIX);

                let mut new_root = H64::default();
                let trie_values: Vec<_> = values
                    .iter()
                    .map(|(uuid, order)| {
                        (
                            uuid.as_bytes().to_vec(),
                            rmp_serde::to_vec(&order).expect("Serialization failed"),
                        )
                    })
                    .collect();
                if let Err(e) = populate_trie::<Layout>(&mut orderbook.memory_db, &mut new_root, &trie_values) {
                    log!("Error " (e) " on trie population with values " [trie_values]);
                    return false;
                }
                for value in values {
                    orderbook.insert_or_update_order(value.1);
                }
                new_root
            },
        };
        orderbook
            .pubkeys_state
            .get_mut(&from_pubkey)
            .expect("Pubkey state always exists at this point")
            .trie_roots
            .insert(pair, new_root);
    }
    true
}

async fn process_maker_order_updated(
    ctx: MmArc,
    propagated_from_peer: String,
    from_pubkey: String,
    updated_msg: new_protocol::MakerOrderUpdated,
    serialized: Vec<u8>,
) -> bool {
    let ordermatch_ctx = OrdermatchContext::from_ctx(&ctx).expect("from_ctx failed");
    let uuid = updated_msg.uuid();
    let mut orderbook = ordermatch_ctx.orderbook.lock().await;
    match orderbook.find_order_by_uuid_and_pubkey(&uuid, &from_pubkey) {
        Some(mut order) => {
            order.apply_updated(&updated_msg);
            orderbook.insert_or_update_order_update_trie(order);
            true
        },
        None => {
            log!("Couldn't find an order " [uuid] ", ignoring, it will be synced upon pubkey keep alive");
            false
        },
    }
}

// TODO
// fn purge_pubkey_state_for_pair(pubkey_state: &OrderbookPubkeyState, base: &str, rel: &str) {
//     let s = pubkey_state.order_pairs_trie_state_history.
// }
//
fn verify_pubkey_orderbook(orderbook: &GetOrderbookPubkeyItem) -> Result<(), String> {
    let keys: Vec<(_, _)> = orderbook
        .orders
        .iter()
        .map(|(uuid, order)| {
            let order_bytes = rmp_serde::to_vec(&order).expect("Serialization should never fail");
            (uuid.as_bytes(), Some(order_bytes))
        })
        .collect();
    let (orders_root, proof) = &orderbook.pair_orders_trie_root;
    verify_trie_proof::<Layout, _, _, _>(orders_root, proof, &keys)
        .map_err(|e| ERRL!("Error on pair_orders_trie_root verification: {}", e))?;
    Ok(())
}

/// Request best asks and bids for the given `base` and `rel` coins from relays.
/// Set `asks_num` and/or `bids_num` to get corresponding number of best asks and bids or None to get all of the available orders.
///
/// # Safety
///
/// The function locks [`MmCtx::p2p_ctx`] and [`MmCtx::ordermatch_ctx`]
async fn request_and_fill_orderbook(
    ctx: &MmArc,
    base: &str,
    rel: &str,
    asks_num: Option<usize>,
    bids_num: Option<usize>,
) -> Result<(), String> {
    /*
    // The function converts the given Vec<OrderInitialMessage> to Iter<Item = OrderbookItem>.
    fn process_initial_messages(
        initial_msgs: Vec<new_protocol::OrderInitialMessage>,
    ) -> impl Iterator<Item = OrderbookItem> {
        initial_msgs.into_iter().filter_map(
            |new_protocol::OrderInitialMessage {
                 initial_message,
                 from_peer,
                 update_messages,
             }| match OrderbookItem::from_initial_msg(initial_message, update_messages, from_peer) {
                Ok(order) => Some(order),
                Err(e) => {
                    log!("Error on parse OrderbookItem from initial message: "[e]);
                    None
                },
            },
        )
    }

    let get_orderbook = OrdermatchRequest::GetOrderbook {
        base: base.to_string(),
        rel: rel.to_string(),
        asks_num,
        bids_num,
    };

    let responses =
        try_s!(request_relays::<new_protocol::Orderbook>(ctx.clone(), P2PRequest::Ordermatch(get_orderbook)).await);

    let mut asks = Vec::new();
    let mut bids = Vec::new();
    for (peer_id, peer_response) in responses {
        match peer_response {
            PeerDecodedResponse::Ok(orderbook) => {
                asks.extend(process_initial_messages(orderbook.asks));
                bids.extend(process_initial_messages(orderbook.bids));
            },
            PeerDecodedResponse::None => (),
            PeerDecodedResponse::Err(e) => log!("Received error from peer " [peer_id] ": " [e]),
        }
    }

    if let Some(n) = asks_num {
        // the best asks are with the lowest prices (from lowest to highest prices)
        asks.sort_by(|x, y| x.price.cmp(&y.price));
        // truncate excess asks
        asks.truncate(n)
    }
    if let Some(n) = bids_num {
        // the best bids are with the highest prices (from highest to lowest prices)
        bids.sort_by(|x, y| y.price.cmp(&x.price));
        // truncate excess bids
        bids.truncate(n)
    }

    let ordermatch_ctx = OrdermatchContext::from_ctx(&ctx).unwrap();
    let mut orderbook = ordermatch_ctx.orderbook.lock().await;

    for ask in asks {
        orderbook.insert_or_update_order(ask);
    }
    for bid in bids {
        orderbook.insert_or_update_order(bid);
    }

    orderbook
        .topics_subscribed_to
        .insert(orderbook_topic(base, rel), OrderbookRequestingState::Requested);


     */
    Ok(())
}

/// Insert or update an order `req`.
/// Note this function locks the [`OrdermatchContext::orderbook`] async mutex.
async fn insert_or_update_order(ctx: &MmArc, item: OrderbookItem) {
    let ordermatch_ctx = OrdermatchContext::from_ctx(&ctx).expect("from_ctx failed");
    let mut orderbook = ordermatch_ctx.orderbook.lock().await;
    orderbook.insert_or_update_order_update_trie(item)
}

async fn delete_order(ctx: &MmArc, pubkey: &str, uuid: Uuid) {
    let ordermatch_ctx = OrdermatchContext::from_ctx(&ctx).expect("from_ctx failed");

    let mut inactive = ordermatch_ctx.inactive_orders.lock().await;
    match inactive.get(&uuid) {
        // don't remove the order if the pubkey is not equal
        Some(order) if order.pubkey != pubkey => (),
        Some(_) => {
            inactive.remove(&uuid);
        },
        None => (),
    }

    let mut orderbook = ordermatch_ctx.orderbook.lock().await;
    match orderbook.order_set.get(&uuid) {
        // don't remove the order if the pubkey is not equal
        Some(order) if order.pubkey != pubkey => (),
        Some(_) => {
            orderbook.remove_order_trie_update(uuid);
        },
        None => (),
    }
}

async fn delete_my_order(ctx: &MmArc, uuid: Uuid) {
    let ordermatch_ctx: Arc<OrdermatchContext> = OrdermatchContext::from_ctx(&ctx).expect("from_ctx failed");
    let mut orderbook = ordermatch_ctx.orderbook.lock().await;
    orderbook.remove_order_trie_update(uuid);
}

/// Attempts to decode a message and process it returning whether the message is valid and worth rebroadcasting
pub async fn process_msg(ctx: MmArc, _topics: Vec<String>, from_peer: String, msg: &[u8]) -> bool {
    match decode_signed::<new_protocol::OrdermatchMessage>(msg) {
        Ok((message, _sig, pubkey)) => match message {
            new_protocol::OrdermatchMessage::MakerOrderCreated(created_msg) => {
                let order: OrderbookItem = (created_msg, hex::encode(pubkey.to_bytes().as_slice())).into();
                insert_or_update_order(&ctx, order).await;
                true
            },
            new_protocol::OrdermatchMessage::PubkeyKeepAlive(keep_alive) => {
                process_orders_keep_alive(ctx, from_peer, pubkey.to_hex(), keep_alive).await
            },
            new_protocol::OrdermatchMessage::TakerRequest(taker_request) => {
                let msg = TakerRequest::from_new_proto_and_pubkey(taker_request, pubkey.unprefixed().into());
                process_taker_request(ctx, msg).await;
                true
            },
            new_protocol::OrdermatchMessage::MakerReserved(maker_reserved) => {
                let msg = MakerReserved::from_new_proto_and_pubkey(maker_reserved, pubkey.unprefixed().into());
                process_maker_reserved(ctx, msg).await;
                true
            },
            new_protocol::OrdermatchMessage::TakerConnect(taker_connect) => {
                process_taker_connect(ctx, pubkey.unprefixed().into(), taker_connect.into()).await;
                true
            },
            new_protocol::OrdermatchMessage::MakerConnected(maker_connected) => {
                process_maker_connected(ctx, pubkey.unprefixed().into(), maker_connected.into()).await;
                true
            },
            new_protocol::OrdermatchMessage::MakerOrderCancelled(cancelled_msg) => {
                delete_order(&ctx, &pubkey.to_hex(), cancelled_msg.uuid.into()).await;
                true
            },
            new_protocol::OrdermatchMessage::MakerOrderUpdated(updated_msg) => {
                process_maker_order_updated(ctx, from_peer, pubkey.to_hex(), updated_msg, msg.to_owned()).await
            },
        },
        Err(e) => {
            println!("Error {} while decoding signed message", e);
            false
        },
    }
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum OrdermatchRequest {
    /// Get an orderbook for the given pair.
    GetOrderbook { base: String, rel: String },
    /// Sync specific pubkey orderbook state if our known Patricia trie state doesn't match the latest keep alive message
    SyncPubkeyOrderbookState {
        pubkey: String,
        /// Request using this condition
        trie_roots: HashMap<AlbOrderedOrderbookPair, H64>,
    },
}

#[derive(Debug)]
struct TryFromBytesError(String);

impl From<String> for TryFromBytesError {
    fn from(string: String) -> Self { TryFromBytesError(string) }
}

trait TryFromBytes {
    fn try_from_bytes(bytes: Vec<u8>) -> Result<Self, TryFromBytesError>
    where
        Self: Sized;
}

impl TryFromBytes for String {
    fn try_from_bytes(bytes: Vec<u8>) -> Result<Self, TryFromBytesError> {
        String::from_utf8(bytes).map_err(|e| ERRL!("{}", e).into())
    }
}

impl TryFromBytes for OrderbookItem {
    fn try_from_bytes(bytes: Vec<u8>) -> Result<Self, TryFromBytesError> {
        rmp_serde::from_read(bytes.as_slice()).map_err(|e| ERRL!("{}", e).into())
    }
}

impl TryFromBytes for H64 {
    fn try_from_bytes(bytes: Vec<u8>) -> Result<Self, TryFromBytesError> {
        bytes.try_into().map_err(|e| ERRL!("{:?}", e).into())
    }
}

impl TryFromBytes for Uuid {
    fn try_from_bytes(bytes: Vec<u8>) -> Result<Self, TryFromBytesError> {
        Uuid::from_slice(&bytes).map_err(|e| ERRL!("{}", e).into())
    }
}

pub async fn process_peer_request(ctx: MmArc, request: OrdermatchRequest) -> Result<Option<Vec<u8>>, String> {
    println!("Got ordermatch request {:?}", request);
    match request {
        OrdermatchRequest::GetOrderbook { base, rel } => process_get_orderbook_request(ctx, base, rel).await,
        OrdermatchRequest::SyncPubkeyOrderbookState { pubkey, trie_roots } => {
            let response = process_sync_pubkey_orderbook_state(ctx, pubkey, trie_roots).await;
            response.map(|res| res.map(|r| encode_message(&r).expect("Serialization failed")))
        },
    }
}

type TrieProof = Vec<Vec<u8>>;

#[derive(Debug, Deserialize, Serialize)]
struct GetOrderbookPubkeyItem {
    /// Timestamp of the latest keep alive message received.
    last_keep_alive: u64,
    /// First - root hash of the requested pair orders trie, where the key - `Uuid`, value - `OrderbookItem`.
    /// Second - proof for the given `orders` in the trie.
    pair_orders_trie_root: (H64, TrieProof),
    /// Requested orders.
    orders: Vec<(Uuid, OrderbookItem)>,
}

#[derive(Debug, Deserialize, Serialize)]
struct GetOrderbookRes {
    /// Asks and bids grouped by pubkey.
    pubkey_orders: HashMap<String, GetOrderbookPubkeyItem>,
}

async fn process_get_orderbook_request(ctx: MmArc, base: String, rel: String) -> Result<Option<Vec<u8>>, String> {
    fn get_pubkeys_orders(
        orderbook: &Orderbook,
        base: String,
        rel: String,
    ) -> HashMap<String, Vec<(Uuid, OrderbookItem)>> {
        let order_uuids = match orderbook.unordered.get(&(base, rel)) {
            Some(uuids) => uuids,
            None => return HashMap::new(),
        };

        let mut uuids_by_pubkey = HashMap::new();
        for uuid in order_uuids.iter() {
            let order = orderbook
                .order_set
                .get(uuid)
                .expect("Orderbook::ordered contains an uuid that is not in Orderbook::order_set");
            let uuids = uuids_by_pubkey
                .entry(order.pubkey.clone())
                .or_insert_with(|| Vec::new());
            uuids.push((uuid.clone(), order.clone()))
        }

        uuids_by_pubkey
    }

    let ordermatch_ctx = unwrap!(OrdermatchContext::from_ctx(&ctx));
    let orderbook = ordermatch_ctx.orderbook.lock().await;

    let orders_to_send = get_pubkeys_orders(&orderbook, base.clone(), rel.clone());

    let alb_pair = alb_ordered_pair(&base, &rel);
    let mut result = HashMap::new();
    for (pubkey, orders) in orders_to_send {
        let pubkey_state = orderbook.pubkeys_state.get(&pubkey).ok_or(ERRL!(
            "Orderbook::pubkeys_state is expected to contain the {:?} pubkey",
            pubkey
        ))?;

        let pair_orders_trie_root = pubkey_state.trie_roots.get(&alb_pair).ok_or(ERRL!(
            "OrderbookPubkeyState::order_pairs_trie_roots is expected to contain the {:?} base-rel pair",
            alb_pair
        ))?;

        let pair_orders_trie_root_proof = {
            let orders_keys = orders.iter().map(|(uuid, _orderbook_item)| uuid.as_bytes());
            try_s!(generate_trie_proof::<Layout, _, _, _>(
                &orderbook.memory_db,
                *pair_orders_trie_root,
                orders_keys
            ))
        };

        let item = GetOrderbookPubkeyItem {
            last_keep_alive: pubkey_state.last_keep_alive,
            pair_orders_trie_root: (*pair_orders_trie_root, pair_orders_trie_root_proof),
            orders,
        };
        result.insert(pubkey, item);
    }

    let response = GetOrderbookRes { pubkey_orders: result };
    let encoded = try_s!(encode_message(&response));
    Ok(Some(encoded))
}

#[derive(Debug, Deserialize, Serialize)]
enum DeltaOrFullTrie<Key: Eq + std::hash::Hash, Value> {
    Delta(HashMap<Key, Option<Value>>),
    FullTrie(Vec<(Key, Value)>),
}

#[derive(Debug)]
enum TrieDiffHistoryError {
    TrieDbError(Box<trie_db::TrieError<H64, sp_trie::Error>>),
    TryFromBytesError(TryFromBytesError),
}

impl std::fmt::Display for TrieDiffHistoryError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { write!(f, "({:?})", self) }
}

impl From<TryFromBytesError> for TrieDiffHistoryError {
    fn from(error: TryFromBytesError) -> TrieDiffHistoryError { TrieDiffHistoryError::TryFromBytesError(error) }
}

impl From<Box<trie_db::TrieError<H64, sp_trie::Error>>> for TrieDiffHistoryError {
    fn from(error: Box<trie_db::TrieError<H64, sp_trie::Error>>) -> TrieDiffHistoryError {
        TrieDiffHistoryError::TrieDbError(error)
    }
}

impl<Key: Clone + Eq + std::hash::Hash + TryFromBytes, Value: Clone + TryFromBytes> DeltaOrFullTrie<Key, Value> {
    fn from_history(
        history: &TrieDiffHistory<Key, Value>,
        from_hash: H64,
        trie_from_hash: H64,
        db: &MemoryDB<Blake2Hasher64>,
    ) -> Result<DeltaOrFullTrie<Key, Value>, TrieDiffHistoryError> {
        match history.get(&from_hash) {
            Some(delta) => {
                let mut current_delta = delta;
                let mut total_delta = HashMap::new();
                for (key, new_value) in &delta.delta {
                    total_delta.insert(key.clone(), new_value.clone());
                }
                while let Some(cur) = history.get(&current_delta.next_root) {
                    current_delta = cur;
                    for (key, new_value) in &current_delta.delta {
                        total_delta.insert(key.clone(), new_value.clone());
                    }
                }
                Ok(DeltaOrFullTrie::Delta(total_delta))
            },
            None => {
                let trie = TrieDB::<Layout>::new(db, &trie_from_hash)?;
                let trie: Result<Vec<_>, TrieDiffHistoryError> = trie
                    .iter()?
                    .map(|key_value| {
                        let (key, value) = key_value?;
                        Ok((TryFromBytes::try_from_bytes(key)?, TryFromBytes::try_from_bytes(value)?))
                    })
                    .collect();
                Ok(DeltaOrFullTrie::FullTrie(trie?))
            },
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct SyncPubkeyOrderbookStateRes {
    pair_orders_diff: HashMap<AlbOrderedOrderbookPair, DeltaOrFullTrie<Uuid, OrderbookItem>>,
}

async fn process_sync_pubkey_orderbook_state(
    ctx: MmArc,
    pubkey: String,
    trie_roots: HashMap<AlbOrderedOrderbookPair, H64>,
) -> Result<Option<SyncPubkeyOrderbookStateRes>, String> {
    let ordermatch_ctx = unwrap!(OrdermatchContext::from_ctx(&ctx));
    let orderbook = ordermatch_ctx.orderbook.lock().await;
    let pubkey_state = match orderbook.pubkeys_state.get(&pubkey) {
        Some(s) => s,
        None => return Ok(None),
    };

    let pair_orders_diff: Result<_, _> = trie_roots
        .into_iter()
        .map(|(pair, root)| {
            let pair_root = pubkey_state
                .trie_roots
                .get(&pair)
                .ok_or(ERRL!("No pair trie root for {}", pair))?;
            let delta = try_s!(DeltaOrFullTrie::from_history(
                &pubkey_state.order_pairs_trie_state_history,
                root,
                *pair_root,
                &orderbook.memory_db,
            ));
            Ok((pair, delta))
        })
        .collect();

    let pair_orders_diff = try_s!(pair_orders_diff);
    let result = SyncPubkeyOrderbookStateRes { pair_orders_diff };
    Ok(Some(result))
}

fn alb_ordered_pair(base: &str, rel: &str) -> AlbOrderedOrderbookPair {
    let (first, second) = if base < rel { (base, rel) } else { (rel, base) };
    let mut res = first.to_owned();
    res.push(':');
    res.push_str(second);
    res
}

fn orderbook_topic_from_base_rel(base: &str, rel: &str) -> String {
    pub_sub_topic(ORDERBOOK_PREFIX, &alb_ordered_pair(base, rel))
}

fn orderbook_topic_from_ordered_pair(pair: &str) -> String { pub_sub_topic(ORDERBOOK_PREFIX, pair) }

#[test]
fn test_alb_ordered_pair() {
    assert_eq!("BTC:KMD", alb_ordered_pair("KMD", "BTC"));
    assert_eq!("BTCH:KMD", alb_ordered_pair("KMD", "BTCH"));
    assert_eq!("KMD:QTUM", alb_ordered_pair("QTUM", "KMD"));
}

#[allow(dead_code)]
fn parse_orderbook_pair_from_topic(topic: &str) -> Option<(&str, &str)> {
    let mut split = topic.split(|maybe_sep| maybe_sep == TOPIC_SEPARATOR);
    match split.next() {
        Some(ORDERBOOK_PREFIX) => match split.next() {
            Some(maybe_pair) => {
                let colon = maybe_pair.find(|maybe_colon| maybe_colon == ':');
                match colon {
                    Some(index) => {
                        if index + 1 < maybe_pair.len() {
                            Some((&maybe_pair[..index], &maybe_pair[index + 1..]))
                        } else {
                            None
                        }
                    },
                    None => None,
                }
            },
            None => None,
        },
        _ => None,
    }
}

#[test]
fn test_parse_orderbook_pair_from_topic() {
    assert_eq!(Some(("BTC", "KMD")), parse_orderbook_pair_from_topic("orbk/BTC:KMD"));
    assert_eq!(None, parse_orderbook_pair_from_topic("orbk/BTC:"));
}

async fn maker_order_created_p2p_notify(ctx: MmArc, order: &MakerOrder) {
    let topic = orderbook_topic_from_base_rel(&order.base, &order.rel);
    let message = new_protocol::MakerOrderCreated {
        uuid: order.uuid.into(),
        base: order.base.clone(),
        rel: order.rel.clone(),
        price: order.price.to_ratio(),
        max_volume: order.available_amount().to_ratio(),
        min_volume: order.min_base_vol.to_ratio(),
        conf_settings: order.conf_settings.unwrap(),
        created_at: now_ms() / 1000,
    };

    let key_pair = ctx.secp256k1_key_pair.or(&&|| panic!());
    let to_broadcast = new_protocol::OrdermatchMessage::MakerOrderCreated(message.clone());
    let encoded_msg = encode_and_sign(&to_broadcast, &*key_pair.private().secret).unwrap();
    let order: OrderbookItem = (message, hex::encode(&**key_pair.public())).into();
    insert_or_update_order(&ctx, order).await;
    broadcast_p2p_msg(&ctx, vec![topic], encoded_msg);
}

async fn process_my_maker_order_updated(ctx: &MmArc, message: &new_protocol::MakerOrderUpdated, serialized: Vec<u8>) {
    let ordermatch_ctx = OrdermatchContext::from_ctx(&ctx).expect("from_ctx failed");
    let mut orderbook = ordermatch_ctx.orderbook.lock().await;

    let uuid = message.uuid();
    if let Some(mut order) = orderbook.find_order_by_uuid(&uuid) {
        order.apply_updated(message);
        orderbook.insert_or_update_order_update_trie(order);
    }
}

async fn maker_order_updated_p2p_notify(ctx: MmArc, base: &str, rel: &str, message: new_protocol::MakerOrderUpdated) {
    let msg: new_protocol::OrdermatchMessage = message.clone().into();
    let topic = orderbook_topic_from_base_rel(base, rel);
    let key_pair = ctx.secp256k1_key_pair.or(&&|| panic!());
    let encoded_msg = encode_and_sign(&msg, &*key_pair.private().secret).unwrap();
    process_my_maker_order_updated(&ctx, &message, encoded_msg.clone()).await;
    broadcast_p2p_msg(&ctx, vec![topic], encoded_msg);
}

async fn maker_order_cancelled_p2p_notify(ctx: MmArc, order: &MakerOrder) {
    let message = new_protocol::OrdermatchMessage::MakerOrderCancelled(new_protocol::MakerOrderCancelled {
        uuid: order.uuid.into(),
    });
    delete_my_order(&ctx, order.uuid).await;
    println!("maker_order_cancelled_p2p_notify called, message {:?}", message);
    broadcast_ordermatch_message(
        &ctx,
        vec![orderbook_topic_from_base_rel(&order.base, &order.rel)],
        message,
    );
}

pub struct BalanceUpdateOrdermatchHandler {
    ctx: MmArc,
}

impl BalanceUpdateOrdermatchHandler {
    pub fn new(ctx: MmArc) -> Self { BalanceUpdateOrdermatchHandler { ctx } }
}

#[async_trait]
impl BalanceTradeFeeUpdatedHandler for BalanceUpdateOrdermatchHandler {
    async fn balance_updated(&self, ticker: &str, new_balance: &BigDecimal, trade_fee: &TradeFee) {
        let new_volume = calc_max_maker_vol(&self.ctx, &new_balance, trade_fee, ticker);
        let ordermatch_ctx = unwrap!(OrdermatchContext::from_ctx(&self.ctx));
        let mut maker_orders = ordermatch_ctx.my_maker_orders.lock().await;
        *maker_orders = maker_orders
            .drain()
            .filter_map(|(uuid, order)| {
                if order.base == *ticker {
                    if new_volume < order.min_base_vol {
                        let ctx = self.ctx.clone();
                        delete_my_maker_order(&ctx, &order);
                        spawn(async move { maker_order_cancelled_p2p_notify(ctx, &order).await });
                        None
                    } else if new_volume < order.available_amount() {
                        let update_msg =
                            new_protocol::MakerOrderUpdated::new(order.uuid).with_new_max_volume(new_volume.to_ratio());
                        let base = order.base.to_owned();
                        let rel = order.rel.to_owned();
                        let ctx = self.ctx.clone();
                        spawn(async move { maker_order_updated_p2p_notify(ctx, &base, &rel, update_msg).await });
                        Some((uuid, order))
                    } else {
                        Some((uuid, order))
                    }
                } else {
                    Some((uuid, order))
                }
            })
            .collect();
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum TakerAction {
    Buy,
    Sell,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[cfg_attr(test, derive(Default))]
pub struct OrderConfirmationsSettings {
    pub base_confs: u64,
    pub base_nota: bool,
    pub rel_confs: u64,
    pub rel_nota: bool,
}

impl OrderConfirmationsSettings {
    pub fn reversed(&self) -> OrderConfirmationsSettings {
        OrderConfirmationsSettings {
            base_confs: self.rel_confs,
            base_nota: self.rel_nota,
            rel_confs: self.base_confs,
            rel_nota: self.base_nota,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TakerRequest {
    base: String,
    rel: String,
    base_amount: MmNumber,
    rel_amount: MmNumber,
    action: TakerAction,
    uuid: Uuid,
    sender_pubkey: H256Json,
    dest_pub_key: H256Json,
    #[serde(default)]
    match_by: MatchBy,
    conf_settings: Option<OrderConfirmationsSettings>,
}

impl TakerRequest {
    fn from_new_proto_and_pubkey(message: new_protocol::TakerRequest, sender_pubkey: H256Json) -> Self {
        let base_amount = MmNumber::from(message.base_amount);
        let rel_amount = MmNumber::from(message.rel_amount);

        TakerRequest {
            base: message.base,
            rel: message.rel,
            base_amount,
            rel_amount,
            action: message.action,
            uuid: message.uuid.into(),
            sender_pubkey,
            dest_pub_key: Default::default(),
            match_by: message.match_by.into(),
            conf_settings: Some(message.conf_settings),
        }
    }

    fn can_match_with_maker_pubkey(&self, maker_pubkey: &H256Json) -> bool {
        match &self.match_by {
            MatchBy::Pubkeys(pubkeys) => pubkeys.contains(maker_pubkey),
            _ => true,
        }
    }

    fn can_match_with_uuid(&self, uuid: &Uuid) -> bool {
        match &self.match_by {
            MatchBy::Orders(uuids) => uuids.contains(uuid),
            _ => true,
        }
    }
}

impl Into<new_protocol::OrdermatchMessage> for TakerRequest {
    fn into(self) -> new_protocol::OrdermatchMessage {
        new_protocol::OrdermatchMessage::TakerRequest(new_protocol::TakerRequest {
            base_amount: self.get_base_amount().to_ratio(),
            rel_amount: self.get_rel_amount().to_ratio(),
            base: self.base,
            rel: self.rel,
            action: self.action,
            uuid: self.uuid.into(),
            match_by: self.match_by.into(),
            conf_settings: self.conf_settings.unwrap(),
        })
    }
}

impl TakerRequest {
    fn get_base_amount(&self) -> &MmNumber { &self.base_amount }

    fn get_rel_amount(&self) -> &MmNumber { &self.rel_amount }
}

struct TakerRequestBuilder {
    base: String,
    rel: String,
    base_amount: MmNumber,
    rel_amount: MmNumber,
    sender_pubkey: H256Json,
    action: TakerAction,
    match_by: MatchBy,
    conf_settings: Option<OrderConfirmationsSettings>,
}

impl Default for TakerRequestBuilder {
    fn default() -> Self {
        TakerRequestBuilder {
            base: "".into(),
            rel: "".into(),
            base_amount: 0.into(),
            rel_amount: 0.into(),
            sender_pubkey: H256Json::default(),
            action: TakerAction::Buy,
            match_by: MatchBy::Any,
            conf_settings: None,
        }
    }
}

enum TakerRequestBuildError {
    BaseCoinEmpty,
    RelCoinEmpty,
    BaseEqualRel,
    /// Base amount too low with threshold
    BaseAmountTooLow {
        actual: MmNumber,
        threshold: MmNumber,
    },
    /// Rel amount too low with threshold
    RelAmountTooLow {
        actual: MmNumber,
        threshold: MmNumber,
    },
    SenderPubkeyIsZero,
    ConfsSettingsNotSet,
}

impl fmt::Display for TakerRequestBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TakerRequestBuildError::BaseCoinEmpty => write!(f, "Base coin can not be empty"),
            TakerRequestBuildError::RelCoinEmpty => write!(f, "Rel coin can not be empty"),
            TakerRequestBuildError::BaseEqualRel => write!(f, "Rel coin can not be same as base"),
            TakerRequestBuildError::BaseAmountTooLow { actual, threshold } => write!(
                f,
                "Base amount {} is too low, required: {}",
                actual.to_decimal(),
                threshold.to_decimal()
            ),
            TakerRequestBuildError::RelAmountTooLow { actual, threshold } => write!(
                f,
                "Rel amount {} is too low, required: {}",
                actual.to_decimal(),
                threshold.to_decimal()
            ),
            TakerRequestBuildError::SenderPubkeyIsZero => write!(f, "Sender pubkey can not be zero"),
            TakerRequestBuildError::ConfsSettingsNotSet => write!(f, "Confirmation settings must be set"),
        }
    }
}

impl TakerRequestBuilder {
    fn with_base_coin(mut self, ticker: String) -> Self {
        self.base = ticker;
        self
    }

    fn with_rel_coin(mut self, ticker: String) -> Self {
        self.rel = ticker;
        self
    }

    fn with_base_amount(mut self, vol: MmNumber) -> Self {
        self.base_amount = vol;
        self
    }

    fn with_rel_amount(mut self, vol: MmNumber) -> Self {
        self.rel_amount = vol;
        self
    }

    fn with_action(mut self, action: TakerAction) -> Self {
        self.action = action;
        self
    }

    fn with_match_by(mut self, match_by: MatchBy) -> Self {
        self.match_by = match_by;
        self
    }

    fn with_conf_settings(mut self, settings: OrderConfirmationsSettings) -> Self {
        self.conf_settings = Some(settings);
        self
    }

    fn with_sender_pubkey(mut self, sender_pubkey: H256Json) -> Self {
        self.sender_pubkey = sender_pubkey;
        self
    }

    /// Validate fields and build
    fn build(self) -> Result<TakerRequest, TakerRequestBuildError> {
        let min_vol = MmNumber::from(MIN_TRADING_VOL);

        if self.base.is_empty() {
            return Err(TakerRequestBuildError::BaseCoinEmpty);
        }

        if self.rel.is_empty() {
            return Err(TakerRequestBuildError::RelCoinEmpty);
        }

        if self.base == self.rel {
            return Err(TakerRequestBuildError::BaseEqualRel);
        }

        if self.base_amount < min_vol {
            return Err(TakerRequestBuildError::BaseAmountTooLow {
                actual: self.base_amount,
                threshold: min_vol,
            });
        }

        if self.rel_amount < min_vol {
            return Err(TakerRequestBuildError::RelAmountTooLow {
                actual: self.rel_amount,
                threshold: min_vol,
            });
        }

        if self.sender_pubkey == H256Json::default() {
            return Err(TakerRequestBuildError::SenderPubkeyIsZero);
        }

        if self.conf_settings.is_none() {
            return Err(TakerRequestBuildError::ConfsSettingsNotSet);
        }

        Ok(TakerRequest {
            base: self.base,
            rel: self.rel,
            base_amount: self.base_amount,
            rel_amount: self.rel_amount,
            action: self.action,
            uuid: new_uuid(),
            sender_pubkey: self.sender_pubkey,
            dest_pub_key: Default::default(),
            match_by: self.match_by,
            conf_settings: self.conf_settings,
        })
    }

    #[cfg(test)]
    /// skip validation for tests
    fn build_unchecked(self) -> TakerRequest {
        TakerRequest {
            base: self.base,
            rel: self.rel,
            base_amount: self.base_amount,
            rel_amount: self.rel_amount,
            action: self.action,
            uuid: new_uuid(),
            sender_pubkey: self.sender_pubkey,
            dest_pub_key: Default::default(),
            match_by: self.match_by,
            conf_settings: self.conf_settings,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", content = "data")]
pub enum MatchBy {
    Any,
    Orders(HashSet<Uuid>),
    Pubkeys(HashSet<H256Json>),
}

impl Default for MatchBy {
    fn default() -> Self { MatchBy::Any }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", content = "data")]
enum OrderType {
    FillOrKill,
    GoodTillCancelled,
}

impl Default for OrderType {
    fn default() -> Self { OrderType::GoodTillCancelled }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct TakerOrder {
    created_at: u64,
    request: TakerRequest,
    matches: HashMap<Uuid, TakerMatch>,
    order_type: OrderType,
}

/// Result of match_reserved function
#[derive(Debug, PartialEq)]
enum MatchReservedResult {
    /// Order and reserved message matched,
    Matched,
    /// Order and reserved didn't match
    NotMatched,
}

impl TakerOrder {
    fn is_cancellable(&self) -> bool { self.matches.is_empty() }

    fn match_reserved(&self, reserved: &MakerReserved) -> MatchReservedResult {
        match &self.request.match_by {
            MatchBy::Any => (),
            MatchBy::Orders(uuids) => {
                if !uuids.contains(&reserved.maker_order_uuid) {
                    return MatchReservedResult::NotMatched;
                }
            },
            MatchBy::Pubkeys(pubkeys) => {
                if !pubkeys.contains(&reserved.sender_pubkey) {
                    return MatchReservedResult::NotMatched;
                }
            },
        }

        let my_base_amount = self.request.get_base_amount();
        let my_rel_amount = self.request.get_rel_amount();
        let other_base_amount = reserved.get_base_amount();
        let other_rel_amount = reserved.get_rel_amount();

        match self.request.action {
            TakerAction::Buy => {
                if self.request.base == reserved.base
                    && self.request.rel == reserved.rel
                    && my_base_amount == other_base_amount
                    && other_rel_amount <= my_rel_amount
                {
                    MatchReservedResult::Matched
                } else {
                    MatchReservedResult::NotMatched
                }
            },
            TakerAction::Sell => {
                if self.request.base == reserved.rel
                    && self.request.rel == reserved.base
                    && my_base_amount == other_rel_amount
                    && my_rel_amount <= other_base_amount
                {
                    MatchReservedResult::Matched
                } else {
                    MatchReservedResult::NotMatched
                }
            },
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
/// Market maker order
/// The "action" is missing here because it's easier to always consider maker order as "sell"
/// So upon ordermatch with request we have only 2 combinations "sell":"sell" and "sell":"buy"
/// Adding "action" to maker order will just double possible combinations making order match more complex.
pub struct MakerOrder {
    pub max_base_vol: MmNumber,
    pub min_base_vol: MmNumber,
    pub price: MmNumber,
    pub created_at: u64,
    pub base: String,
    pub rel: String,
    matches: HashMap<Uuid, MakerMatch>,
    started_swaps: Vec<Uuid>,
    uuid: Uuid,
    conf_settings: Option<OrderConfirmationsSettings>,
}

struct MakerOrderBuilder {
    max_base_vol: MmNumber,
    min_base_vol: MmNumber,
    price: MmNumber,
    base: String,
    rel: String,
    conf_settings: Option<OrderConfirmationsSettings>,
}

impl Default for MakerOrderBuilder {
    fn default() -> MakerOrderBuilder {
        MakerOrderBuilder {
            base: "".into(),
            rel: "".into(),
            max_base_vol: 0.into(),
            min_base_vol: 0.into(),
            price: 0.into(),
            conf_settings: None,
        }
    }
}

enum MakerOrderBuildError {
    BaseCoinEmpty,
    RelCoinEmpty,
    BaseEqualRel,
    /// Max base vol too low with threshold
    MaxBaseVolTooLow {
        actual: MmNumber,
        threshold: MmNumber,
    },
    /// Min base vol too low with threshold
    MinBaseVolTooLow {
        actual: MmNumber,
        threshold: MmNumber,
    },
    /// Price too low with threshold
    PriceTooLow {
        actual: MmNumber,
        threshold: MmNumber,
    },
    /// Rel vol too low with threshold
    RelVolTooLow {
        actual: MmNumber,
        threshold: MmNumber,
    },
    ConfSettingsNotSet,
}

impl fmt::Display for MakerOrderBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MakerOrderBuildError::BaseCoinEmpty => write!(f, "Base coin can not be empty"),
            MakerOrderBuildError::RelCoinEmpty => write!(f, "Rel coin can not be empty"),
            MakerOrderBuildError::BaseEqualRel => write!(f, "Rel coin can not be same as base"),
            MakerOrderBuildError::MaxBaseVolTooLow { actual, threshold } => write!(
                f,
                "Max base vol {} is too low, required: {}",
                actual.to_decimal(),
                threshold.to_decimal()
            ),
            MakerOrderBuildError::MinBaseVolTooLow { actual, threshold } => write!(
                f,
                "Min base vol {} is too low, required: {}",
                actual.to_decimal(),
                threshold.to_decimal()
            ),
            MakerOrderBuildError::PriceTooLow { actual, threshold } => write!(
                f,
                "Price {} is too low, required: {}",
                actual.to_decimal(),
                threshold.to_decimal()
            ),
            MakerOrderBuildError::RelVolTooLow { actual, threshold } => write!(
                f,
                "Max rel vol {} is too low, required: {}",
                actual.to_decimal(),
                threshold.to_decimal()
            ),
            MakerOrderBuildError::ConfSettingsNotSet => write!(f, "Confirmation settings must be set"),
        }
    }
}

impl MakerOrderBuilder {
    fn with_base_coin(mut self, ticker: String) -> Self {
        self.base = ticker;
        self
    }

    fn with_rel_coin(mut self, ticker: String) -> Self {
        self.rel = ticker;
        self
    }

    fn with_max_base_vol(mut self, vol: MmNumber) -> Self {
        self.max_base_vol = vol;
        self
    }

    fn with_min_base_vol(mut self, vol: MmNumber) -> Self {
        self.min_base_vol = vol;
        self
    }

    fn with_price(mut self, price: MmNumber) -> Self {
        self.price = price;
        self
    }

    fn with_conf_settings(mut self, conf_settings: OrderConfirmationsSettings) -> Self {
        self.conf_settings = Some(conf_settings);
        self
    }

    /// Validate fields and build
    fn build(self) -> Result<MakerOrder, MakerOrderBuildError> {
        let min_price = MmNumber::from(BigRational::new(1.into(), 100_000_000.into()));
        let min_vol = MmNumber::from(MIN_TRADING_VOL);

        if self.base.is_empty() {
            return Err(MakerOrderBuildError::BaseCoinEmpty);
        }

        if self.rel.is_empty() {
            return Err(MakerOrderBuildError::RelCoinEmpty);
        }

        if self.base == self.rel {
            return Err(MakerOrderBuildError::BaseEqualRel);
        }

        if self.max_base_vol < min_vol {
            return Err(MakerOrderBuildError::MaxBaseVolTooLow {
                actual: self.max_base_vol,
                threshold: min_vol,
            });
        }

        if self.price < min_price {
            return Err(MakerOrderBuildError::PriceTooLow {
                actual: self.price,
                threshold: min_price,
            });
        }

        let rel_vol = &self.max_base_vol * &self.price;
        if rel_vol < min_vol {
            return Err(MakerOrderBuildError::RelVolTooLow {
                actual: rel_vol,
                threshold: min_vol,
            });
        }

        if self.min_base_vol < min_vol {
            return Err(MakerOrderBuildError::MinBaseVolTooLow {
                actual: self.min_base_vol,
                threshold: min_vol,
            });
        }

        if self.conf_settings.is_none() {
            return Err(MakerOrderBuildError::ConfSettingsNotSet);
        }

        Ok(MakerOrder {
            base: self.base,
            rel: self.rel,
            created_at: now_ms(),
            max_base_vol: self.max_base_vol,
            min_base_vol: self.min_base_vol,
            price: self.price,
            matches: HashMap::new(),
            started_swaps: Vec::new(),
            uuid: new_uuid(),
            conf_settings: self.conf_settings,
        })
    }

    #[cfg(test)]
    fn build_unchecked(self) -> MakerOrder {
        MakerOrder {
            base: self.base,
            rel: self.rel,
            created_at: now_ms(),
            max_base_vol: self.max_base_vol,
            min_base_vol: self.min_base_vol,
            price: self.price,
            matches: HashMap::new(),
            started_swaps: Vec::new(),
            uuid: new_uuid(),
            conf_settings: self.conf_settings,
        }
    }
}

#[allow(dead_code)]
fn zero_rat() -> BigRational { BigRational::zero() }

impl MakerOrder {
    fn available_amount(&self) -> MmNumber {
        let reserved: MmNumber = self.matches.iter().fold(
            MmNumber::from(BigRational::from_integer(0.into())),
            |reserved, (_, order_match)| &reserved + order_match.reserved.get_base_amount(),
        );
        &self.max_base_vol - &reserved
    }

    fn is_cancellable(&self) -> bool { !self.has_ongoing_matches() }

    fn has_ongoing_matches(&self) -> bool {
        for (_, order_match) in self.matches.iter() {
            // if there's at least 1 ongoing match the order is not cancellable
            if order_match.connected.is_none() && order_match.connect.is_none() {
                return true;
            }
        }
        false
    }

    fn match_with_request(&self, taker: &TakerRequest) -> OrderMatchResult {
        let taker_base_amount = taker.get_base_amount();
        let taker_rel_amount = taker.get_rel_amount();

        let zero = MmNumber::from(0);
        if taker_base_amount <= &zero || taker_rel_amount <= &zero {
            return OrderMatchResult::NotMatched;
        }

        match taker.action {
            TakerAction::Buy => {
                let taker_price = taker_rel_amount / taker_base_amount;
                if self.base == taker.base
                    && self.rel == taker.rel
                    && taker_base_amount <= &self.available_amount()
                    && taker_base_amount >= &self.min_base_vol
                    && taker_price >= self.price
                {
                    OrderMatchResult::Matched((taker_base_amount.clone(), taker_base_amount * &self.price))
                } else {
                    OrderMatchResult::NotMatched
                }
            },
            TakerAction::Sell => {
                let taker_price = taker_base_amount / taker_rel_amount;

                if self.base == taker.rel
                    && self.rel == taker.base
                    && taker_rel_amount <= &self.available_amount()
                    && taker_rel_amount >= &self.min_base_vol
                    && taker_price >= self.price
                {
                    OrderMatchResult::Matched((taker_base_amount / &self.price, taker_base_amount.clone()))
                } else {
                    OrderMatchResult::NotMatched
                }
            },
        }
    }
}

impl Into<MakerOrder> for TakerOrder {
    fn into(self) -> MakerOrder {
        match self.request.action {
            TakerAction::Sell => MakerOrder {
                price: (self.request.get_rel_amount() / self.request.get_base_amount()),
                max_base_vol: self.request.get_base_amount().clone(),
                min_base_vol: 0.into(),
                created_at: now_ms(),
                base: self.request.base,
                rel: self.request.rel,
                matches: HashMap::new(),
                started_swaps: Vec::new(),
                uuid: self.request.uuid,
                conf_settings: self.request.conf_settings,
            },
            // The "buy" taker order is recreated with reversed pair as Maker order is always considered as "sell"
            TakerAction::Buy => MakerOrder {
                price: (self.request.get_base_amount() / self.request.get_rel_amount()),
                max_base_vol: self.request.get_rel_amount().clone(),
                min_base_vol: 0.into(),
                created_at: now_ms(),
                base: self.request.rel,
                rel: self.request.base,
                matches: HashMap::new(),
                started_swaps: Vec::new(),
                uuid: self.request.uuid,
                conf_settings: self.request.conf_settings.map(|s| s.reversed()),
            },
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TakerConnect {
    taker_order_uuid: Uuid,
    maker_order_uuid: Uuid,
    sender_pubkey: H256Json,
    dest_pub_key: H256Json,
}

impl From<new_protocol::TakerConnect> for TakerConnect {
    fn from(message: new_protocol::TakerConnect) -> TakerConnect {
        TakerConnect {
            taker_order_uuid: message.taker_order_uuid.into(),
            maker_order_uuid: message.maker_order_uuid.into(),
            sender_pubkey: Default::default(),
            dest_pub_key: Default::default(),
        }
    }
}

impl Into<new_protocol::OrdermatchMessage> for TakerConnect {
    fn into(self) -> new_protocol::OrdermatchMessage {
        new_protocol::OrdermatchMessage::TakerConnect(new_protocol::TakerConnect {
            taker_order_uuid: self.taker_order_uuid.into(),
            maker_order_uuid: self.maker_order_uuid.into(),
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[cfg_attr(test, derive(Default))]
pub struct MakerReserved {
    base: String,
    rel: String,
    base_amount: MmNumber,
    rel_amount: MmNumber,
    taker_order_uuid: Uuid,
    maker_order_uuid: Uuid,
    sender_pubkey: H256Json,
    dest_pub_key: H256Json,
    conf_settings: Option<OrderConfirmationsSettings>,
}

impl MakerReserved {
    fn get_base_amount(&self) -> &MmNumber { &self.base_amount }

    fn get_rel_amount(&self) -> &MmNumber { &self.rel_amount }
}

impl MakerReserved {
    fn from_new_proto_and_pubkey(message: new_protocol::MakerReserved, sender_pubkey: H256Json) -> Self {
        let base_amount = MmNumber::from(message.base_amount);
        let rel_amount = MmNumber::from(message.rel_amount);

        MakerReserved {
            base: message.base,
            rel: message.rel,
            base_amount,
            rel_amount,
            taker_order_uuid: message.taker_order_uuid.into(),
            maker_order_uuid: message.maker_order_uuid.into(),
            sender_pubkey,
            dest_pub_key: Default::default(),
            conf_settings: Some(message.conf_settings),
        }
    }
}

impl Into<new_protocol::OrdermatchMessage> for MakerReserved {
    fn into(self) -> new_protocol::OrdermatchMessage {
        new_protocol::OrdermatchMessage::MakerReserved(new_protocol::MakerReserved {
            base_amount: self.get_base_amount().to_ratio(),
            rel_amount: self.get_rel_amount().to_ratio(),
            base: self.base,
            rel: self.rel,
            taker_order_uuid: self.taker_order_uuid.into(),
            maker_order_uuid: self.maker_order_uuid.into(),
            conf_settings: self.conf_settings.unwrap(),
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MakerConnected {
    taker_order_uuid: Uuid,
    maker_order_uuid: Uuid,
    method: String,
    sender_pubkey: H256Json,
    dest_pub_key: H256Json,
}

impl From<new_protocol::MakerConnected> for MakerConnected {
    fn from(message: new_protocol::MakerConnected) -> MakerConnected {
        MakerConnected {
            taker_order_uuid: message.taker_order_uuid.into(),
            maker_order_uuid: message.maker_order_uuid.into(),
            method: "".to_string(),
            sender_pubkey: Default::default(),
            dest_pub_key: Default::default(),
        }
    }
}

impl Into<new_protocol::OrdermatchMessage> for MakerConnected {
    fn into(self) -> new_protocol::OrdermatchMessage {
        new_protocol::OrdermatchMessage::MakerConnected(new_protocol::MakerConnected {
            taker_order_uuid: self.taker_order_uuid.into(),
            maker_order_uuid: self.maker_order_uuid.into(),
        })
    }
}

pub async fn broadcast_maker_orders_keep_alive_loop(ctx: MmArc) {
    let my_pubsecp = hex::encode(&**ctx.secp256k1_key_pair().public());
    while !ctx.is_stopping() {
        Timer::sleep(MIN_ORDER_KEEP_ALIVE_INTERVAL as f64).await;
        let ordermatch_ctx = OrdermatchContext::from_ctx(&ctx).expect("from_ctx failed");
        let orderbook = ordermatch_ctx.orderbook.lock().await;
        let state = match orderbook.pubkeys_state.get(&my_pubsecp) {
            Some(s) => s,
            None => continue,
        };

        let mut trie_roots = HashMap::new();
        let mut topics = HashSet::new();
        for (alb_pair, root) in state.trie_roots.clone() {
            if root == H64::default() && root == hashed_null_node::<Layout>() {
                continue;
            }
            topics.insert(orderbook_topic_from_ordered_pair(&alb_pair));
            trie_roots.insert(alb_pair, root);
        }

        let message = new_protocol::PubkeyKeepAlive {
            trie_roots,
            timestamp: now_ms() / 1000,
        };

        broadcast_ordermatch_message(&ctx, topics, message.into());
    }
}

fn broadcast_ordermatch_message(
    ctx: &MmArc,
    topics: impl IntoIterator<Item = String>,
    msg: new_protocol::OrdermatchMessage,
) {
    let key_pair = ctx.secp256k1_key_pair.or(&&|| panic!());
    let encoded_msg = encode_and_sign(&msg, &*key_pair.private().secret).unwrap();
    broadcast_p2p_msg(ctx, topics.into_iter().collect(), encoded_msg);
}

/// The order is ordered by [`OrderbookItem::price`] and [`OrderbookItem::uuid`].
#[derive(Eq, Ord, PartialEq, PartialOrd)]
struct OrderedByPriceOrder {
    price: MmNumber,
    uuid: Uuid,
}

#[derive(Clone, Debug, PartialEq)]
enum OrderbookRequestingState {
    /// The orderbook was requested from relays.
    Requested,
    /// We subscribed to a topic at `subscribed_at` time, but the orderbook was not requested.
    NotRequested { subscribed_at: u64 },
}

type H64 = [u8; 8];

#[derive(Debug, Eq, PartialEq)]
struct TrieDiff<Key, Value> {
    delta: Vec<(Key, Option<Value>)>,
    next_root: H64,
}

#[derive(Debug, Eq, PartialEq)]
struct TrieDiffHistory<Key, Value> {
    inner: HashMap<H64, TrieDiff<Key, Value>>,
}

impl<Key, Value> Default for TrieDiffHistory<Key, Value> {
    fn default() -> Self {
        TrieDiffHistory {
            inner: Default::default(),
        }
    }
}

impl<Key, Value> TrieDiffHistory<Key, Value> {
    fn insert_new_diff(&mut self, insert_at: H64, diff: TrieDiff<Key, Value>) {
        if insert_at == diff.next_root {
            // do nothing to avoid cycles in diff history
            return;
        }

        match self.inner.remove(&diff.next_root) {
            Some(mut diff) => {
                // we reached a state that was already reached previously
                // history can be cleaned up to this state hash
                while let Some(next_diff) = self.inner.remove(&diff.next_root) {
                    diff = next_diff;
                }
            },
            None => {
                self.inner.insert(insert_at, diff);
            },
        };
    }

    fn contains_key(&self, key: &H64) -> bool { self.inner.contains_key(key) }

    fn get(&self, key: &H64) -> Option<&TrieDiff<Key, Value>> { self.inner.get(key) }
}

#[derive(Default)]
struct OrderbookPubkeyState {
    /// Timestamp of the latest keep alive message received
    last_keep_alive: u64,
    /// The map storing historical data about specific pair subtrie changes
    /// Used to get diffs of orders of pair between specific root hashes
    order_pairs_trie_state_history: TrieDiffHistory<Uuid, OrderbookItem>,
    /// The known UUIDs owned by pubkey with alphabetically ordered pair to ease the lookup during pubkey orderbook requests
    orders_uuids: HashSet<(Uuid, AlbOrderedOrderbookPair)>,
    /// The map storing alphabetically ordered pair with trie root hash of orders owned by pubkey.
    trie_roots: HashMap<AlbOrderedOrderbookPair, H64>,
}

fn get_trie_mut<'a>(
    mem_db: &'a mut MemoryDB<Blake2Hasher64>,
    root: &'a mut H64,
) -> Result<TrieDBMut<'a, Layout>, String> {
    if *root == H64::default() {
        Ok(TrieDBMut::new(mem_db, root))
    } else {
        TrieDBMut::from_existing(mem_db, root).map_err(|e| ERRL!("{}", e))
    }
}

fn pubkey_state_mut<'a>(
    state: &'a mut HashMap<String, OrderbookPubkeyState>,
    from_pubkey: &str,
) -> &'a mut OrderbookPubkeyState {
    match state.raw_entry_mut().from_key(from_pubkey) {
        RawEntryMut::Occupied(e) => e.into_mut(),
        RawEntryMut::Vacant(e) => {
            let mut state: OrderbookPubkeyState = Default::default();
            state.last_keep_alive = now_ms() / 1000;
            e.insert(from_pubkey.to_string(), state).1
        },
    }
}

fn order_pair_root_mut<'a>(state: &'a mut HashMap<AlbOrderedOrderbookPair, H64>, pair: &str) -> &'a mut H64 {
    match state.raw_entry_mut().from_key(pair) {
        RawEntryMut::Occupied(e) => e.into_mut(),
        RawEntryMut::Vacant(e) => e.insert(pair.to_string(), Default::default()).1,
    }
}

fn populate_trie<'db, T: TrieConfiguration>(
    db: &'db mut dyn HashDBT<T::Hash, DBValue>,
    root: &'db mut TrieHash<T>,
    v: &[(Vec<u8>, Vec<u8>)],
) -> Result<TrieDBMut<'db, T>, String> {
    let mut t = TrieDBMut::<T>::new(db, root);
    for (key, val) in v {
        try_s!(t.insert(key, val));
    }
    Ok(t)
}

#[derive(Default)]
struct Orderbook {
    /// A map from (base, rel).
    ordered: HashMap<(String, String), BTreeSet<OrderedByPriceOrder>>,
    /// A map from (base, rel).
    unordered: HashMap<(String, String), HashSet<Uuid>>,
    order_set: HashMap<Uuid, OrderbookItem>,
    /// a map of orderbook states of known maker pubkeys
    pubkeys_state: HashMap<String, OrderbookPubkeyState>,
    topics_subscribed_to: HashMap<String, OrderbookRequestingState>,
    /// MemoryDB instance to store Patricia Tries data
    memory_db: MemoryDB<Blake2Hasher64>,
}

fn hashed_null_node<T: TrieConfiguration>() -> TrieHash<T> { <T::Codec as NodeCodecT>::hashed_null_node() }

impl Orderbook {
    fn find_order_by_uuid_and_pubkey(&self, uuid: &Uuid, from_pubkey: &str) -> Option<OrderbookItem> {
        self.order_set.get(uuid).and_then(|order| {
            if order.pubkey == from_pubkey {
                Some(order.clone())
            } else {
                None
            }
        })
    }

    fn find_order_by_uuid(&self, uuid: &Uuid) -> Option<OrderbookItem> {
        self.order_set.get(uuid).map(|order| order.clone())
    }

    fn insert_or_update_order_update_trie(&mut self, order: OrderbookItem) {
        let zero = BigRational::from_integer(0.into());
        if order.max_volume <= zero || order.price <= zero || order.min_volume < zero {
            self.remove_order_trie_update(order.uuid);
            return;
        } // else insert the order

        self.insert_or_update_order(order.clone());

        let pubkey_state = pubkey_state_mut(&mut self.pubkeys_state, &order.pubkey);

        let alb_ordered = alb_ordered_pair(&order.base, &order.rel);
        let pair_root = order_pair_root_mut(&mut pubkey_state.trie_roots, &alb_ordered);
        let prev_root = *pair_root;

        pubkey_state.orders_uuids.insert((order.uuid, alb_ordered.clone()));

        let mut pair_trie = match get_trie_mut(&mut self.memory_db, pair_root) {
            Ok(trie) => trie,
            Err(e) => {
                log!("Error getting "(e)" trie with root "[prev_root]);
                return;
            },
        };
        let order_bytes = rmp_serde::to_vec(&order).expect("Serialization should never fail");
        if let Err(e) = pair_trie.insert(order.uuid.as_bytes(), &order_bytes) {
            log!("Error " (e) " on insertion to trie. Key " (order.uuid) ", value " [order_bytes]);
            return;
        };
        drop(pair_trie);

        if prev_root != H64::default() {
            pubkey_state
                .order_pairs_trie_state_history
                .insert_new_diff(prev_root, TrieDiff {
                    delta: vec![(order.uuid, Some(order.clone()))],
                    next_root: *pair_root,
                });
        }
    }

    fn insert_or_update_order(&mut self, order: OrderbookItem) {
        log!("Inserting order "[order]);
        let zero = BigRational::from_integer(0.into());
        if order.max_volume <= zero || order.price <= zero || order.min_volume < zero {
            self.remove_order_trie_update(order.uuid);
            return;
        } // else insert the order

        let base_rel = (order.base.clone(), order.rel.clone());

        self.ordered
            .entry(base_rel.clone())
            .or_insert_with(BTreeSet::new)
            .insert(OrderedByPriceOrder {
                price: order.price.clone().into(),
                uuid: order.uuid,
            });

        self.unordered
            .entry(base_rel)
            .or_insert_with(HashSet::new)
            .insert(order.uuid);

        self.order_set.insert(order.uuid, order);
    }

    fn remove_order(&mut self, uuid: Uuid) -> Option<OrderbookItem> {
        let order = match self.order_set.remove(&uuid) {
            Some(order) => order,
            None => return None,
        };
        let base_rel = (order.base.clone(), order.rel.clone());

        // create an `order_to_delete` that allows to find and remove an element from `self.ordered` by hash
        let order_to_delete = OrderedByPriceOrder {
            price: order.price.clone().into(),
            uuid,
        };

        if let Some(orders) = self.ordered.get_mut(&base_rel) {
            orders.remove(&order_to_delete);
            if orders.is_empty() {
                self.ordered.remove(&base_rel);
            }
        }

        if let Some(orders) = self.unordered.get_mut(&base_rel) {
            // use the same uuid to remove an order
            orders.remove(&order_to_delete.uuid);
            if orders.is_empty() {
                self.unordered.remove(&base_rel);
            }
        };
        Some(order)
    }

    fn remove_order_trie_update(&mut self, uuid: Uuid) -> Option<OrderbookItem> {
        let order = match self.order_set.remove(&uuid) {
            Some(order) => order,
            None => return None,
        };
        let base_rel = (order.base.clone(), order.rel.clone());

        // create an `order_to_delete` that allows to find and remove an element from `self.ordered` by hash
        let order_to_delete = OrderedByPriceOrder {
            price: order.price.clone().into(),
            uuid,
        };

        if let Some(orders) = self.ordered.get_mut(&base_rel) {
            orders.remove(&order_to_delete);
            if orders.is_empty() {
                self.ordered.remove(&base_rel);
            }
        }

        if let Some(orders) = self.unordered.get_mut(&base_rel) {
            // use the same uuid to remove an order
            orders.remove(&order_to_delete.uuid);
            if orders.is_empty() {
                self.unordered.remove(&base_rel);
            }
        }

        let alb_ordered = alb_ordered_pair(&order.base, &order.rel);
        let pubkey_state = pubkey_state_mut(&mut self.pubkeys_state, &order.pubkey);
        let pair_state = order_pair_root_mut(&mut pubkey_state.trie_roots, &alb_ordered);
        let old_state = *pair_state;
        *pair_state = match delta_trie_root::<Layout, _, _, _, _, _>(&mut self.memory_db, *pair_state, vec![(
            *order.uuid.as_bytes(),
            None::<Vec<u8>>,
        )]) {
            Ok(root) => root,
            Err(_) => {
                log!("Failed to get existing trie with root "[pair_state]);
                return Some(order);
            },
        };

        pubkey_state
            .order_pairs_trie_state_history
            .insert_new_diff(old_state, TrieDiff {
                delta: vec![(uuid, None)],
                next_root: *pair_state,
            });
        Some(order)
    }

    fn is_subscribed_to(&self, topic: &str) -> bool { self.topics_subscribed_to.contains_key(topic) }

    fn process_keep_alive(
        &mut self,
        from_pubkey: &str,
        message: new_protocol::PubkeyKeepAlive,
    ) -> Option<OrdermatchRequest> {
        let pubkey_state = pubkey_state_mut(&mut self.pubkeys_state, from_pubkey);

        let mut trie_roots_to_request = HashMap::new();
        for (alb_pair, trie_root) in message.trie_roots {
            if !self
                .topics_subscribed_to
                .contains_key(&orderbook_topic_from_ordered_pair(&alb_pair))
            {
                continue;
            }

            let actual_trie_root = pubkey_state
                .trie_roots
                .entry(alb_pair.clone())
                .or_insert_with(|| H64::default());

            if *actual_trie_root != trie_root {
                trie_roots_to_request.insert(alb_pair, trie_root);
            }
        }

        if trie_roots_to_request.is_empty() {
            pubkey_state.last_keep_alive = message.timestamp;
            return None;
        }

        Some(OrdermatchRequest::SyncPubkeyOrderbookState {
            pubkey: from_pubkey.into(),
            trie_roots: trie_roots_to_request,
        })
    }
}

#[derive(Default)]
struct OrdermatchContext {
    pub my_maker_orders: AsyncMutex<HashMap<Uuid, MakerOrder>>,
    pub my_taker_orders: AsyncMutex<HashMap<Uuid, TakerOrder>>,
    pub my_cancelled_orders: AsyncMutex<HashMap<Uuid, MakerOrder>>,
    pub orderbook: AsyncMutex<Orderbook>,
    pub order_requests_tracker: AsyncMutex<OrderRequestsTracker>,
    pub inactive_orders: AsyncMutex<HashMap<Uuid, OrderbookItem>>,
}

#[cfg_attr(test, mockable)]
impl OrdermatchContext {
    /// Obtains a reference to this crate context, creating it if necessary.
    fn from_ctx(ctx: &MmArc) -> Result<Arc<OrdermatchContext>, String> {
        Ok(try_s!(from_ctx(&ctx.ordermatch_ctx, move || {
            Ok(OrdermatchContext::default())
        })))
    }

    /// Obtains a reference to this crate context, creating it if necessary.
    #[allow(dead_code)]
    fn from_ctx_weak(ctx_weak: &MmWeak) -> Result<Arc<OrdermatchContext>, String> {
        let ctx = try_s!(MmArc::from_weak(ctx_weak).ok_or("Context expired"));
        Self::from_ctx(&ctx)
    }
}

#[cfg_attr(test, mockable)]
fn lp_connect_start_bob(ctx: MmArc, maker_match: MakerMatch, maker_order: MakerOrder) {
    spawn(async move {
        // aka "maker_loop"
        let taker_coin = match lp_coinfindᵃ(&ctx, &maker_match.reserved.rel).await {
            Ok(Some(c)) => c,
            Ok(None) => {
                log!("Coin " (maker_match.reserved.rel) " is not found/enabled");
                return;
            },
            Err(e) => {
                log!("!lp_coinfind(" (maker_match.reserved.rel) "): " (e));
                return;
            },
        };

        let maker_coin = match lp_coinfindᵃ(&ctx, &maker_match.reserved.base).await {
            Ok(Some(c)) => c,
            Ok(None) => {
                log!("Coin " (maker_match.reserved.base) " is not found/enabled");
                return;
            },
            Err(e) => {
                log!("!lp_coinfind(" (maker_match.reserved.base) "): " (e));
                return;
            },
        };
        let mut alice = bits256::default();
        alice.bytes = maker_match.request.sender_pubkey.0;
        let maker_amount = maker_match.reserved.get_base_amount().to_decimal();
        let taker_amount = maker_match.reserved.get_rel_amount().to_decimal();
        let privkey = &ctx.secp256k1_key_pair().private().secret;
        let my_persistent_pub = unwrap!(compressed_pub_key_from_priv_raw(&privkey[..], ChecksumType::DSHA256));
        let uuid = maker_match.request.uuid;
        let my_conf_settings = choose_maker_confs_and_notas(
            maker_order.conf_settings,
            &maker_match.request,
            &maker_coin,
            &taker_coin,
        );
        // detect atomic lock time version implicitly by conf_settings existence in taker request
        let atomic_locktime_v = match maker_match.request.conf_settings {
            Some(_) => {
                let other_conf_settings =
                    choose_taker_confs_and_notas(&maker_match.request, &maker_match.reserved, &maker_coin, &taker_coin);
                AtomicLocktimeVersion::V2 {
                    my_conf_settings,
                    other_conf_settings,
                }
            },
            None => AtomicLocktimeVersion::V1,
        };
        let lock_time = lp_atomic_locktime(maker_coin.ticker(), taker_coin.ticker(), atomic_locktime_v);
        log!("Entering the maker_swap_loop " (maker_coin.ticker()) "/" (taker_coin.ticker()) " with uuid: " (uuid));
        let maker_swap = MakerSwap::new(
            ctx.clone(),
            alice,
            maker_amount,
            taker_amount,
            my_persistent_pub,
            uuid,
            my_conf_settings,
            maker_coin,
            taker_coin,
            lock_time,
        );
        run_maker_swap(RunMakerSwapInput::StartNew(maker_swap), ctx).await;
    });
}

fn lp_connected_alice(ctx: MmArc, taker_request: TakerRequest, taker_match: TakerMatch) {
    spawn(async move {
        // aka "taker_loop"
        let mut maker = bits256::default();
        maker.bytes = taker_match.reserved.sender_pubkey.0;
        let taker_coin = match lp_coinfindᵃ(&ctx, &taker_match.reserved.rel).await {
            Ok(Some(c)) => c,
            Ok(None) => {
                log!("Coin " (taker_match.reserved.rel) " is not found/enabled");
                return;
            },
            Err(e) => {
                log!("!lp_coinfind(" (taker_match.reserved.rel) "): " (e));
                return;
            },
        };

        let maker_coin = match lp_coinfindᵃ(&ctx, &taker_match.reserved.base).await {
            Ok(Some(c)) => c,
            Ok(None) => {
                log!("Coin " (taker_match.reserved.base) " is not found/enabled");
                return;
            },
            Err(e) => {
                log!("!lp_coinfind(" (taker_match.reserved.base) "): " (e));
                return;
            },
        };

        let privkey = &ctx.secp256k1_key_pair().private().secret;
        let my_persistent_pub = unwrap!(compressed_pub_key_from_priv_raw(&privkey[..], ChecksumType::DSHA256));
        let maker_amount = taker_match.reserved.get_base_amount().to_decimal();
        let taker_amount = taker_match.reserved.get_rel_amount().to_decimal();
        let uuid = taker_match.reserved.taker_order_uuid;

        let my_conf_settings =
            choose_taker_confs_and_notas(&taker_request, &taker_match.reserved, &maker_coin, &taker_coin);
        // detect atomic lock time version implicitly by conf_settings existence in maker reserved
        let atomic_locktime_v = match taker_match.reserved.conf_settings {
            Some(_) => {
                let other_conf_settings = choose_maker_confs_and_notas(
                    taker_match.reserved.conf_settings,
                    &taker_request,
                    &maker_coin,
                    &taker_coin,
                );
                AtomicLocktimeVersion::V2 {
                    my_conf_settings,
                    other_conf_settings,
                }
            },
            None => AtomicLocktimeVersion::V1,
        };
        let locktime = lp_atomic_locktime(maker_coin.ticker(), taker_coin.ticker(), atomic_locktime_v);
        log!("Entering the taker_swap_loop " (maker_coin.ticker()) "/" (taker_coin.ticker())  " with uuid: " (uuid));
        let taker_swap = TakerSwap::new(
            ctx.clone(),
            maker,
            maker_amount,
            taker_amount,
            my_persistent_pub,
            uuid,
            my_conf_settings,
            maker_coin,
            taker_coin,
            locktime,
        );
        run_taker_swap(RunTakerSwapInput::StartNew(taker_swap), ctx).await
    });
}

pub async fn lp_ordermatch_loop(ctx: MmArc) {
    let my_pubsecp = hex::encode(&**ctx.secp256k1_key_pair().public());
    loop {
        if ctx.is_stopping() {
            break;
        }
        let ordermatch_ctx = unwrap!(OrdermatchContext::from_ctx(&ctx));
        {
            let mut my_taker_orders = ordermatch_ctx.my_taker_orders.lock().await;
            let mut my_maker_orders = ordermatch_ctx.my_maker_orders.lock().await;
            let _my_cancelled_orders = ordermatch_ctx.my_cancelled_orders.lock().await;
            // transform the timed out and unmatched GTC taker orders to maker
            *my_taker_orders = my_taker_orders
                .drain()
                .filter_map(|(uuid, order)| {
                    if order.created_at + TAKER_ORDER_TIMEOUT * 1000 < now_ms() {
                        delete_my_taker_order(&ctx, &uuid);
                        if order.matches.is_empty() && order.order_type == OrderType::GoodTillCancelled {
                            let maker_order: MakerOrder = order.into();
                            my_maker_orders.insert(uuid, maker_order.clone());
                            save_my_maker_order(&ctx, &maker_order);
                            spawn({
                                let ctx = ctx.clone();
                                async move {
                                    maker_order_created_p2p_notify(ctx, &maker_order).await;
                                }
                            });
                        }
                        None
                    } else {
                        Some((uuid, order))
                    }
                })
                .collect();
            // remove timed out unfinished matches to unlock the reserved amount
            my_maker_orders.iter_mut().for_each(|(_, order)| {
                let old_len = order.matches.len();
                order.matches.retain(|_, order_match| {
                    order_match.last_updated + ORDER_MATCH_TIMEOUT * 1000 > now_ms() || order_match.connected.is_some()
                });
                if old_len != order.matches.len() {
                    save_my_maker_order(&ctx, order);
                }
            });
            *my_maker_orders = futures::stream::iter(my_maker_orders.drain())
                .filter_map(|(uuid, order)| {
                    let ctx = ctx.clone();
                    async move {
                        if order.available_amount() < order.min_base_vol && !order.has_ongoing_matches() {
                            delete_my_maker_order(&ctx, &order);
                            maker_order_cancelled_p2p_notify(ctx.clone(), &order).await;
                            None
                        } else {
                            Some((uuid, order))
                        }
                    }
                })
                .collect()
                .await;
        }

        {
            // remove "timed out" pubkeys states with their orders from orderbook
            let mut orderbook = ordermatch_ctx.orderbook.lock().await;
            let mut uuids_to_remove = vec![];
            let mut keys_to_remove = vec![];
            orderbook.pubkeys_state.retain(|pubkey, state| {
                let to_retain = pubkey == &my_pubsecp || state.last_keep_alive + MAKER_ORDER_TIMEOUT > now_ms() / 1000;
                if !to_retain {
                    for (uuid, _) in &state.orders_uuids {
                        uuids_to_remove.push(*uuid);
                    }
                    for (_, root) in &state.trie_roots {
                        keys_to_remove.push(*root);
                    }
                }
                to_retain
            });
            for uuid in uuids_to_remove {
                orderbook.remove_order(uuid);
            }

            for key in keys_to_remove {
                orderbook.memory_db.remove_and_purge(&key, EMPTY_PREFIX);
            }
            mm_gauge!(ctx.metrics, "orderbook.len", orderbook.order_set.len() as i64);
            // mm_gauge!(ctx.metrics, "inactive_orders.len", inactive.len() as i64);
        }

        {
            let my_maker_orders = ordermatch_ctx.my_maker_orders.lock().await;
            for (uuid, order) in my_maker_orders.iter() {
                if !ordermatch_ctx.orderbook.lock().await.order_set.contains_key(uuid) {
                    if let Ok(Some(_)) = lp_coinfindᵃ(&ctx, &order.base).await {
                        if let Ok(Some(_)) = lp_coinfindᵃ(&ctx, &order.rel).await {
                            maker_order_created_p2p_notify(ctx.clone(), order).await;
                        }
                    }
                }
            }
        }

        Timer::sleep(0.777).await;
    }
}

async fn process_maker_reserved(ctx: MmArc, reserved_msg: MakerReserved) {
    let ordermatch_ctx = unwrap!(OrdermatchContext::from_ctx(&ctx));
    let our_public_id = unwrap!(ctx.public_id());

    if is_pubkey_banned(&ctx, &reserved_msg.sender_pubkey) {
        log!("Sender pubkey " [reserved_msg.sender_pubkey] " is banned");
        return;
    }

    let mut my_taker_orders = ordermatch_ctx.my_taker_orders.lock().await;
    let my_order = match my_taker_orders.entry(reserved_msg.taker_order_uuid) {
        Entry::Vacant(_) => {
            log!("Our node doesn't have the order with uuid "(
                reserved_msg.taker_order_uuid
            ));
            return;
        },
        Entry::Occupied(entry) => entry.into_mut(),
    };

    // send "connect" message if reserved message targets our pubkey AND
    // reserved amounts match our order AND order is NOT reserved by someone else (empty matches)
    if my_order.match_reserved(&reserved_msg) == MatchReservedResult::Matched && my_order.matches.is_empty() {
        let connect = TakerConnect {
            sender_pubkey: H256Json::from(our_public_id.bytes),
            dest_pub_key: reserved_msg.sender_pubkey.clone(),
            taker_order_uuid: reserved_msg.taker_order_uuid,
            maker_order_uuid: reserved_msg.maker_order_uuid,
        };
        let topic = orderbook_topic_from_base_rel(&my_order.request.base, &my_order.request.rel);
        broadcast_ordermatch_message(&ctx, vec![topic], connect.clone().into());
        let taker_match = TakerMatch {
            reserved: reserved_msg,
            connect,
            connected: None,
            last_updated: now_ms(),
        };
        my_order
            .matches
            .insert(taker_match.reserved.maker_order_uuid, taker_match);
        save_my_taker_order(&ctx, &my_order);
    }
}

async fn process_maker_connected(ctx: MmArc, from_pubkey: H256Json, connected: MakerConnected) {
    let ordermatch_ctx = unwrap!(OrdermatchContext::from_ctx(&ctx));
    let _our_public_id = unwrap!(ctx.public_id());

    let mut my_taker_orders = ordermatch_ctx.my_taker_orders.lock().await;
    let my_order_entry = match my_taker_orders.entry(connected.taker_order_uuid) {
        Entry::Occupied(e) => e,
        Entry::Vacant(_) => {
            log!("Our node doesn't have the order with uuid "(connected.taker_order_uuid));
            return;
        },
    };
    let order_match = match my_order_entry.get().matches.get(&connected.maker_order_uuid) {
        Some(o) => o,
        None => {
            log!("Our node doesn't have the match with uuid "(connected.maker_order_uuid));
            return;
        },
    };

    if order_match.reserved.sender_pubkey != from_pubkey {
        log!("Connected message sender pubkey != reserved message sender pubkey");
        return;
    }
    // alice
    lp_connected_alice(ctx.clone(), my_order_entry.get().request.clone(), order_match.clone());
    // remove the matched order immediately
    delete_my_taker_order(&ctx, &my_order_entry.get().request.uuid);
    my_order_entry.remove();
}

async fn process_taker_request(ctx: MmArc, taker_request: TakerRequest) {
    log!({"Processing request {:?}", taker_request});

    if is_pubkey_banned(&ctx, &taker_request.sender_pubkey) {
        log!("Sender pubkey " [taker_request.sender_pubkey] " is banned");
        return;
    }

    let our_public_id: H256Json = unwrap!(ctx.public_id()).bytes.into();
    if our_public_id == taker_request.dest_pub_key {
        log!("Skip the request originating from our pubkey");
        return;
    }

    if !taker_request.can_match_with_maker_pubkey(&our_public_id) {
        return;
    }

    let ordermatch_ctx = unwrap!(OrdermatchContext::from_ctx(&ctx));
    let mut my_orders = ordermatch_ctx.my_maker_orders.lock().await;
    let filtered = my_orders
        .iter_mut()
        .filter(|(uuid, _)| taker_request.can_match_with_uuid(uuid));

    for (uuid, order) in filtered {
        if let OrderMatchResult::Matched((base_amount, rel_amount)) = order.match_with_request(&taker_request) {
            let base_coin = match lp_coinfindᵃ(&ctx, &order.base).await {
                Ok(Some(c)) => c,
                _ => return, // attempt to match with deactivated coin
            };
            let rel_coin = match lp_coinfindᵃ(&ctx, &order.rel).await {
                Ok(Some(c)) => c,
                _ => return, // attempt to match with deactivated coin
            };

            if !order.matches.contains_key(&taker_request.uuid) {
                let reserved = MakerReserved {
                    dest_pub_key: taker_request.sender_pubkey.clone(),
                    sender_pubkey: our_public_id,
                    base: order.base.clone(),
                    base_amount: base_amount.clone(),
                    rel_amount: rel_amount.clone(),
                    rel: order.rel.clone(),
                    taker_order_uuid: taker_request.uuid,
                    maker_order_uuid: *uuid,
                    conf_settings: order.conf_settings.or_else(|| {
                        Some(OrderConfirmationsSettings {
                            base_confs: base_coin.required_confirmations(),
                            base_nota: base_coin.requires_notarization(),
                            rel_confs: rel_coin.required_confirmations(),
                            rel_nota: rel_coin.requires_notarization(),
                        })
                    }),
                };
                let topic = orderbook_topic_from_base_rel(&order.base, &order.rel);
                log!({"Request matched sending reserved {:?}", reserved});
                broadcast_ordermatch_message(&ctx, vec![topic], reserved.clone().into());
                let maker_match = MakerMatch {
                    request: taker_request,
                    reserved,
                    connect: None,
                    connected: None,
                    last_updated: now_ms(),
                };
                order.matches.insert(maker_match.request.uuid, maker_match);
                save_my_maker_order(&ctx, &order);
            }
            return;
        }
    }
}

async fn process_taker_connect(ctx: MmArc, sender_pubkey: H256Json, connect_msg: TakerConnect) {
    let ordermatch_ctx = unwrap!(OrdermatchContext::from_ctx(&ctx));
    let our_public_id = unwrap!(ctx.public_id());

    let mut maker_orders = ordermatch_ctx.my_maker_orders.lock().await;
    let my_order = match maker_orders.get_mut(&connect_msg.maker_order_uuid) {
        Some(o) => o,
        None => {
            log!("Our node doesn't have the order with uuid "(
                connect_msg.maker_order_uuid
            ));
            return;
        },
    };
    let order_match = match my_order.matches.get_mut(&connect_msg.taker_order_uuid) {
        Some(o) => o,
        None => {
            log!("Our node doesn't have the match with uuid "(
                connect_msg.taker_order_uuid
            ));
            return;
        },
    };
    if order_match.request.sender_pubkey != sender_pubkey {
        log!("Connect message sender pubkey != request message sender pubkey");
        return;
    }

    if order_match.connected.is_none() && order_match.connect.is_none() {
        let connected = MakerConnected {
            sender_pubkey: our_public_id.bytes.into(),
            dest_pub_key: connect_msg.sender_pubkey.clone(),
            taker_order_uuid: connect_msg.taker_order_uuid,
            maker_order_uuid: connect_msg.maker_order_uuid,
            method: "connected".into(),
        };
        let topic = orderbook_topic_from_base_rel(&my_order.base, &my_order.rel);
        broadcast_ordermatch_message(&ctx, vec![topic], connected.clone().into());
        order_match.connect = Some(connect_msg);
        order_match.connected = Some(connected);
        my_order.started_swaps.push(order_match.request.uuid);
        lp_connect_start_bob(ctx.clone(), order_match.clone(), my_order.clone());

        // If volume is less order will be cancelled a bit later
        if my_order.available_amount() >= my_order.min_base_vol {
            let updated_msg = new_protocol::MakerOrderUpdated::new(my_order.uuid)
                .with_new_max_volume(my_order.available_amount().into());
            maker_order_updated_p2p_notify(ctx.clone(), &my_order.base, &my_order.rel, updated_msg).await;
        }
        save_my_maker_order(&ctx, &my_order);
    }
}

#[derive(Deserialize, Debug)]
pub struct AutoBuyInput {
    base: String,
    rel: String,
    price: MmNumber,
    volume: MmNumber,
    timeout: Option<u32>,
    /// Not used. Deprecated.
    duration: Option<u32>,
    // TODO: remove this field on API refactoring, method should be separated from params
    method: String,
    gui: Option<String>,
    #[serde(rename = "destpubkey")]
    #[serde(default)]
    dest_pub_key: H256Json,
    #[serde(default)]
    match_by: MatchBy,
    #[serde(default)]
    order_type: OrderType,
    base_confs: Option<u64>,
    base_nota: Option<bool>,
    rel_confs: Option<u64>,
    rel_nota: Option<bool>,
}

pub async fn buy(ctx: MmArc, req: Json) -> Result<Response<Vec<u8>>, String> {
    let input: AutoBuyInput = try_s!(json::from_value(req));
    if input.base == input.rel {
        return ERR!("Base and rel must be different coins");
    }
    let rel_coin = try_s!(lp_coinfindᵃ(&ctx, &input.rel).await);
    let rel_coin = try_s!(rel_coin.ok_or("Rel coin is not found or inactive"));
    let base_coin = try_s!(lp_coinfindᵃ(&ctx, &input.base).await);
    let base_coin: MmCoinEnum = try_s!(base_coin.ok_or("Base coin is not found or inactive"));
    if base_coin.wallet_only() {
        return ERR!("Base coin is wallet only");
    }
    if rel_coin.wallet_only() {
        return ERR!("Rel coin is wallet only");
    }
    let my_amount = &input.volume * &input.price;
    try_s!(check_balance_for_taker_swap(&ctx, &rel_coin, &base_coin, my_amount, None).await);
    try_s!(base_coin.can_i_spend_other_payment().compat().await);
    let res = try_s!(lp_auto_buy(&ctx, &base_coin, &rel_coin, input).await).into_bytes();
    Ok(try_s!(Response::builder().body(res)))
}

pub async fn sell(ctx: MmArc, req: Json) -> Result<Response<Vec<u8>>, String> {
    let input: AutoBuyInput = try_s!(json::from_value(req));
    if input.base == input.rel {
        return ERR!("Base and rel must be different coins");
    }
    let base_coin = try_s!(lp_coinfindᵃ(&ctx, &input.base).await);
    let base_coin = try_s!(base_coin.ok_or("Base coin is not found or inactive"));
    let rel_coin = try_s!(lp_coinfindᵃ(&ctx, &input.rel).await);
    let rel_coin = try_s!(rel_coin.ok_or("Rel coin is not found or inactive"));
    if base_coin.wallet_only() {
        return ERR!("Base coin is wallet only");
    }
    if rel_coin.wallet_only() {
        return ERR!("Rel coin is wallet only");
    }
    try_s!(check_balance_for_taker_swap(&ctx, &base_coin, &rel_coin, input.volume.clone(), None).await);
    try_s!(rel_coin.can_i_spend_other_payment().compat().await);
    let res = try_s!(lp_auto_buy(&ctx, &base_coin, &rel_coin, input).await).into_bytes();
    Ok(try_s!(Response::builder().body(res)))
}

/// Created when maker order is matched with taker request
#[derive(Clone, Debug, Deserialize, Serialize)]
struct MakerMatch {
    request: TakerRequest,
    reserved: MakerReserved,
    connect: Option<TakerConnect>,
    connected: Option<MakerConnected>,
    last_updated: u64,
}

/// Created upon taker request broadcast
#[derive(Clone, Debug, Deserialize, Serialize)]
struct TakerMatch {
    reserved: MakerReserved,
    connect: TakerConnect,
    connected: Option<MakerConnected>,
    last_updated: u64,
}

pub async fn lp_auto_buy(
    ctx: &MmArc,
    base_coin: &MmCoinEnum,
    rel_coin: &MmCoinEnum,
    input: AutoBuyInput,
) -> Result<String, String> {
    if input.price < MmNumber::from(BigRational::new(1.into(), 100_000_000.into())) {
        return ERR!("Price is too low, minimum is 0.00000001");
    }

    let action = match Some(input.method.as_ref()) {
        Some("buy") => TakerAction::Buy,
        Some("sell") => TakerAction::Sell,
        _ => return ERR!("Auto buy must be called only from buy/sell RPC methods"),
    };
    let request_orderbook = false;
    try_s!(subscribe_to_orderbook_topic(&ctx, &input.base, &input.rel, request_orderbook).await);
    let ordermatch_ctx = try_s!(OrdermatchContext::from_ctx(&ctx));
    let mut my_taker_orders = ordermatch_ctx.my_taker_orders.lock().await;
    let our_public_id = try_s!(ctx.public_id());
    let rel_volume = &input.volume * &input.price;
    let conf_settings = OrderConfirmationsSettings {
        base_confs: input.base_confs.unwrap_or_else(|| base_coin.required_confirmations()),
        base_nota: input.base_nota.unwrap_or_else(|| base_coin.requires_notarization()),
        rel_confs: input.rel_confs.unwrap_or_else(|| rel_coin.required_confirmations()),
        rel_nota: input.rel_nota.unwrap_or_else(|| rel_coin.requires_notarization()),
    };
    let request_builder = TakerRequestBuilder::default()
        .with_base_coin(input.base.clone())
        .with_rel_coin(input.rel.clone())
        .with_base_amount(input.volume)
        .with_rel_amount(rel_volume)
        .with_action(action)
        .with_match_by(input.match_by)
        .with_conf_settings(conf_settings)
        .with_sender_pubkey(H256Json::from(our_public_id.bytes));
    let request = try_s!(request_builder.build());
    broadcast_ordermatch_message(
        &ctx,
        vec![orderbook_topic_from_base_rel(&input.base, &input.rel)],
        request.clone().into(),
    );
    let result = json!({ "result": request }).to_string();
    let order = TakerOrder {
        created_at: now_ms(),
        matches: HashMap::new(),
        request,
        order_type: input.order_type,
    };
    save_my_taker_order(ctx, &order);
    my_taker_orders.insert(order.request.uuid, order);
    drop(my_taker_orders);
    Ok(result)
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
struct OrderbookItem {
    pubkey: String,
    base: String,
    rel: String,
    price: BigRational,
    max_volume: BigRational,
    min_volume: BigRational,
    uuid: Uuid,
    created_at: u64,
}

/// Concrete implementation of Hasher using Blake2b 64-bit hashes
#[derive(Debug)]
pub struct Blake2Hasher64;

impl Hasher for Blake2Hasher64 {
    type Out = [u8; 8];
    type StdHasher = Hash256StdHasher;
    const LENGTH: usize = 8;

    fn hash(x: &[u8]) -> Self::Out {
        let mut hasher = VarBlake2b::new(8).expect("8 is valid VarBlake2b output_size");
        hasher.update(x);
        let mut res: [u8; 8] = Default::default();
        hasher.finalize_variable(|hash| res.copy_from_slice(hash));
        res
    }
}

type Layout = sp_trie::Layout<Blake2Hasher64>;

impl OrderbookItem {
    fn from_initial_msg(initial_message: Vec<u8>, from_peer: String) -> Result<OrderbookItem, String> {
        let (message, _sig, init_pubkey) = try_s!(decode_signed::<new_protocol::OrdermatchMessage>(&initial_message));
        let order = match message {
            new_protocol::OrdermatchMessage::MakerOrderCreated(order) => order,
            msg => return ERR!("Expected MakerOrderCreated, found {:?}", msg),
        };

        let req: OrderbookItem = (order, hex::encode(init_pubkey.to_bytes().as_slice())).into();
        Ok(req)
    }

    fn apply_updated(&mut self, msg: &new_protocol::MakerOrderUpdated) {
        if let Some(new_price) = msg.new_price() {
            self.price = new_price.into();
        }

        if let Some(new_max_volume) = msg.new_max_volume() {
            self.max_volume = new_max_volume.into();
        }

        if let Some(new_min_volume) = msg.new_min_volume() {
            self.min_volume = new_min_volume.into();
        }
    }
}

fn get_true() -> bool { true }

fn min_volume() -> MmNumber { MmNumber::from(MIN_TRADING_VOL) }

#[derive(Deserialize)]
struct SetPriceReq {
    base: String,
    rel: String,
    price: MmNumber,
    #[serde(default)]
    max: bool,
    #[serde(default)]
    volume: MmNumber,
    #[serde(default = "min_volume")]
    min_volume: MmNumber,
    #[serde(default = "get_true")]
    cancel_previous: bool,
    base_confs: Option<u64>,
    base_nota: Option<bool>,
    rel_confs: Option<u64>,
    rel_nota: Option<bool>,
}

#[derive(Serialize)]
struct SetPriceResult<'a> {
    base: &'a str,
    rel: &'a str,
    price: BigDecimal,
    price_rat: &'a MmNumber,
    max_base_vol: BigDecimal,
    max_base_vol_rat: &'a MmNumber,
    min_base_vol: BigDecimal,
    min_base_vol_rat: &'a MmNumber,
    created_at: u64,
    matches: &'a HashMap<Uuid, MakerMatch>,
    started_swaps: &'a [Uuid],
    uuid: Uuid,
    conf_settings: &'a Option<OrderConfirmationsSettings>,
}

impl<'a> From<&'a MakerOrder> for SetPriceResult<'a> {
    fn from(order: &'a MakerOrder) -> SetPriceResult<'a> {
        SetPriceResult {
            base: &order.base,
            rel: &order.rel,
            price: order.price.to_decimal(),
            price_rat: &order.price,
            max_base_vol: order.max_base_vol.to_decimal(),
            max_base_vol_rat: &order.max_base_vol,
            min_base_vol: order.min_base_vol.to_decimal(),
            min_base_vol_rat: &order.min_base_vol,
            created_at: order.created_at,
            matches: &order.matches,
            started_swaps: &order.started_swaps,
            uuid: order.uuid,
            conf_settings: &order.conf_settings,
        }
    }
}

pub async fn set_price(ctx: MmArc, req: Json) -> Result<Response<Vec<u8>>, String> {
    let req: SetPriceReq = try_s!(json::from_value(req));

    let base_coin: MmCoinEnum = match try_s!(lp_coinfindᵃ(&ctx, &req.base).await) {
        Some(coin) => coin,
        None => return ERR!("Base coin {} is not found", req.base),
    };

    let rel_coin: MmCoinEnum = match try_s!(lp_coinfindᵃ(&ctx, &req.rel).await) {
        Some(coin) => coin,
        None => return ERR!("Rel coin {} is not found", req.rel),
    };

    if base_coin.wallet_only() {
        return ERR!("Base coin is wallet only");
    }
    if rel_coin.wallet_only() {
        return ERR!("Rel coin is wallet only");
    }

    let my_balance = try_s!(base_coin.my_balance().compat().await);
    let volume = if req.max {
        // use entire balance deducting the locked amount and trade fee if it's paid with base coin,
        // skipping "check_balance_for_maker_swap"
        let trade_fee = try_s!(base_coin.get_trade_fee().compat().await);
        calc_max_maker_vol(&ctx, &my_balance, &trade_fee, base_coin.ticker())
    } else {
        try_s!(check_balance_for_maker_swap(&ctx, &base_coin, req.volume.clone(), None).await);
        req.volume.clone()
    };
    try_s!(rel_coin.can_i_spend_other_payment().compat().await);

    let conf_settings = OrderConfirmationsSettings {
        base_confs: req.base_confs.unwrap_or_else(|| base_coin.required_confirmations()),
        base_nota: req.base_nota.unwrap_or_else(|| base_coin.requires_notarization()),
        rel_confs: req.rel_confs.unwrap_or_else(|| rel_coin.required_confirmations()),
        rel_nota: req.rel_nota.unwrap_or_else(|| rel_coin.requires_notarization()),
    };
    let builder = MakerOrderBuilder::default()
        .with_base_coin(req.base)
        .with_rel_coin(req.rel)
        .with_max_base_vol(volume)
        .with_min_base_vol(req.min_volume)
        .with_price(req.price)
        .with_conf_settings(conf_settings);

    let new_order = try_s!(builder.build());
    let request_orderbook = false;
    try_s!(subscribe_to_orderbook_topic(&ctx, &new_order.base, &new_order.rel, request_orderbook).await);
    save_my_maker_order(&ctx, &new_order);
    maker_order_created_p2p_notify(ctx.clone(), &new_order).await;

    let ordermatch_ctx = try_s!(OrdermatchContext::from_ctx(&ctx));
    let mut my_orders = ordermatch_ctx.my_maker_orders.lock().await;
    if req.cancel_previous {
        let mut cancelled = vec![];
        // remove the previous orders if there're some to allow multiple setprice call per pair
        // it's common use case now as `autoprice` doesn't work with new ordermatching and
        // MM2 users request the coins price from aggregators by their own scripts issuing
        // repetitive setprice calls with new price
        *my_orders = my_orders
            .drain()
            .filter_map(|(uuid, order)| {
                let to_delete = order.base == new_order.base && order.rel == new_order.rel;
                if to_delete {
                    delete_my_maker_order(&ctx, &order);
                    cancelled.push(order);
                    None
                } else {
                    Some((uuid, order))
                }
            })
            .collect();
        for order in cancelled {
            maker_order_cancelled_p2p_notify(ctx.clone(), &order).await;
        }
    }
    let rpc_result = SetPriceResult::from(&new_order);
    let res = try_s!(json::to_vec(&json!({ "result": rpc_result })));
    my_orders.insert(new_order.uuid, new_order);
    Ok(try_s!(Response::builder().body(res)))
}

/// Result of match_order_and_request function
#[derive(Debug, PartialEq)]
enum OrderMatchResult {
    /// Order and request matched, contains base and rel resulting amounts
    Matched((MmNumber, MmNumber)),
    /// Orders didn't match
    NotMatched,
}

#[derive(Deserialize)]
struct OrderStatusReq {
    uuid: Uuid,
}

pub async fn order_status(ctx: MmArc, req: Json) -> Result<Response<Vec<u8>>, String> {
    let req: OrderStatusReq = try_s!(json::from_value(req));

    let ordermatch_ctx = try_s!(OrdermatchContext::from_ctx(&ctx));
    let maker_orders = ordermatch_ctx.my_maker_orders.lock().await;
    if let Some(order) = maker_orders.get(&req.uuid) {
        let res = json!({
            "type": "Maker",
            "order": MakerOrderForRpc::from(order),
        });
        return Response::builder()
            .body(json::to_vec(&res).expect("Serialization failed"))
            .map_err(|e| ERRL!("{}", e));
    }

    let taker_orders = ordermatch_ctx.my_taker_orders.lock().await;
    if let Some(order) = taker_orders.get(&req.uuid) {
        let res = json!({
            "type": "Taker",
            "order": TakerOrderForRpc::from(order),
        });
        return Response::builder()
            .body(json::to_vec(&res).expect("Serialization failed"))
            .map_err(|e| ERRL!("{}", e));
    }

    let res = json!({
        "error": format!("Order with uuid {} is not found", req.uuid),
    });
    Response::builder()
        .status(404)
        .body(json::to_vec(&res).expect("Serialization failed"))
        .map_err(|e| ERRL!("{}", e))
}

#[derive(Deserialize)]
struct CancelOrderReq {
    uuid: Uuid,
}

pub async fn cancel_order(ctx: MmArc, req: Json) -> Result<Response<Vec<u8>>, String> {
    let req: CancelOrderReq = try_s!(json::from_value(req));

    let ordermatch_ctx = try_s!(OrdermatchContext::from_ctx(&ctx));
    let mut maker_orders = ordermatch_ctx.my_maker_orders.lock().await;
    match maker_orders.entry(req.uuid) {
        Entry::Occupied(order) => {
            if !order.get().is_cancellable() {
                return ERR!("Order {} is being matched now, can't cancel", req.uuid);
            }
            let order = order.remove();
            maker_order_cancelled_p2p_notify(ctx, &order).await;
            let res = json!({
                "result": "success"
            });
            return Response::builder()
                .body(json::to_vec(&res).expect("Serialization failed"))
                .map_err(|e| ERRL!("{}", e));
        },
        // look for taker order with provided uuid
        Entry::Vacant(_) => (),
    }

    let mut taker_orders = ordermatch_ctx.my_taker_orders.lock().await;
    match taker_orders.entry(req.uuid) {
        Entry::Occupied(order) => {
            if !order.get().is_cancellable() {
                return ERR!("Order {} is being matched now, can't cancel", req.uuid);
            }
            let order = order.remove();
            delete_my_taker_order(&ctx, &order.request.uuid);
            let res = json!({
                "result": "success"
            });
            return Response::builder()
                .body(json::to_vec(&res).expect("Serialization failed"))
                .map_err(|e| ERRL!("{}", e));
        },
        // error is returned
        Entry::Vacant(_) => (),
    }

    let res = json!({
        "error": format!("Order with uuid {} is not found", req.uuid),
    });
    Response::builder()
        .status(404)
        .body(json::to_vec(&res).expect("Serialization failed"))
        .map_err(|e| ERRL!("{}", e))
}

#[derive(Serialize)]
struct MakerOrderForRpc<'a> {
    #[serde(flatten)]
    order: SetPriceResult<'a>,
    cancellable: bool,
    available_amount: BigDecimal,
}

impl<'a> From<&'a MakerOrder> for MakerOrderForRpc<'a> {
    fn from(order: &'a MakerOrder) -> MakerOrderForRpc {
        MakerOrderForRpc {
            order: order.into(),
            cancellable: order.is_cancellable(),
            available_amount: order.available_amount().into(),
        }
    }
}

#[derive(Serialize)]
struct TakerOrderForRpc<'a> {
    #[serde(flatten)]
    order: &'a TakerOrder,
    cancellable: bool,
}

impl<'a> From<&'a TakerOrder> for TakerOrderForRpc<'a> {
    fn from(order: &'a TakerOrder) -> TakerOrderForRpc {
        TakerOrderForRpc {
            order,
            cancellable: order.is_cancellable(),
        }
    }
}

pub async fn my_orders(ctx: MmArc) -> Result<Response<Vec<u8>>, String> {
    let ordermatch_ctx = try_s!(OrdermatchContext::from_ctx(&ctx));
    let maker_orders = ordermatch_ctx.my_maker_orders.lock().await;
    let taker_orders = ordermatch_ctx.my_taker_orders.lock().await;
    let maker_orders_for_rpc: HashMap<_, _> = maker_orders
        .iter()
        .map(|(uuid, order)| (uuid, MakerOrderForRpc::from(order)))
        .collect();
    let taker_orders_for_rpc: HashMap<_, _> = taker_orders
        .iter()
        .map(|(uuid, order)| (uuid, TakerOrderForRpc::from(order)))
        .collect();
    let res = json!({
        "result": {
            "maker_orders": maker_orders_for_rpc,
            "taker_orders": taker_orders_for_rpc,
        }
    });
    Response::builder()
        .body(json::to_vec(&res).expect("Serialization failed"))
        .map_err(|e| ERRL!("{}", e))
}

pub fn my_maker_orders_dir(ctx: &MmArc) -> PathBuf { ctx.dbdir().join("ORDERS").join("MY").join("MAKER") }

fn my_taker_orders_dir(ctx: &MmArc) -> PathBuf { ctx.dbdir().join("ORDERS").join("MY").join("TAKER") }

pub fn my_maker_order_file_path(ctx: &MmArc, uuid: &Uuid) -> PathBuf {
    my_maker_orders_dir(ctx).join(format!("{}.json", uuid))
}

fn my_taker_order_file_path(ctx: &MmArc, uuid: &Uuid) -> PathBuf {
    my_taker_orders_dir(ctx).join(format!("{}.json", uuid))
}

fn save_my_maker_order(ctx: &MmArc, order: &MakerOrder) {
    let path = my_maker_order_file_path(ctx, &order.uuid);
    let content = unwrap!(json::to_vec(order));
    unwrap!(write(&path, &content));
}

fn save_my_taker_order(ctx: &MmArc, order: &TakerOrder) {
    let path = my_taker_order_file_path(ctx, &order.request.uuid);
    let content = unwrap!(json::to_vec(order));
    unwrap!(write(&path, &content));
}

#[cfg_attr(test, mockable)]
fn delete_my_maker_order(ctx: &MmArc, order: &MakerOrder) {
    let path = my_maker_order_file_path(ctx, &order.uuid);
    match remove_file(&path) {
        Ok(_) => (),
        Err(e) => log!("Warning, could not remove order file " (path.display()) ", error " (e)),
    }
}

#[cfg_attr(test, mockable)]
fn delete_my_taker_order(ctx: &MmArc, uuid: &Uuid) {
    let path = my_taker_order_file_path(ctx, uuid);
    match remove_file(&path) {
        Ok(_) => (),
        Err(e) => log!("Warning, could not remove order file " (path.display()) ", error " (e)),
    }
}

pub async fn orders_kick_start(ctx: &MmArc) -> Result<HashSet<String>, String> {
    let mut coins = HashSet::new();
    let ordermatch_ctx = try_s!(OrdermatchContext::from_ctx(ctx));
    let mut maker_orders = ordermatch_ctx.my_maker_orders.lock().await;
    let maker_entries = try_s!(json_dir_entries(&my_maker_orders_dir(&ctx)));

    maker_entries.iter().for_each(|entry| {
        if let Ok(order) = json::from_slice::<MakerOrder>(&slurp(&entry.path())) {
            coins.insert(order.base.clone());
            coins.insert(order.rel.clone());
            maker_orders.insert(order.uuid, order);
        }
    });

    let mut taker_orders = ordermatch_ctx.my_taker_orders.lock().await;
    let taker_entries: Vec<DirEntry> = try_s!(json_dir_entries(&my_taker_orders_dir(&ctx)));

    taker_entries.iter().for_each(|entry| {
        if let Ok(order) = json::from_slice::<TakerOrder>(&slurp(&entry.path())) {
            coins.insert(order.request.base.clone());
            coins.insert(order.request.rel.clone());
            taker_orders.insert(order.request.uuid, order);
        }
    });
    Ok(coins)
}

#[derive(Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum CancelBy {
    /// All orders of current node
    All,
    /// All orders of specific pair
    Pair { base: String, rel: String },
    /// All orders using the coin ticker as base or rel
    Coin { ticker: String },
}

pub async fn cancel_orders_by(ctx: &MmArc, cancel_by: CancelBy) -> Result<(Vec<Uuid>, Vec<Uuid>), String> {
    let mut cancelled = vec![];
    let mut cancelled_maker_orders = vec![];
    let mut currently_matching = vec![];

    let ordermatch_ctx = try_s!(OrdermatchContext::from_ctx(ctx));
    let mut maker_orders = ordermatch_ctx.my_maker_orders.lock().await;
    let mut taker_orders = ordermatch_ctx.my_taker_orders.lock().await;

    macro_rules! cancel_maker_if_true {
        ($e: expr, $uuid: ident, $order: ident) => {
            if $e {
                if $order.is_cancellable() {
                    delete_my_maker_order(&ctx, &$order);
                    cancelled_maker_orders.push($order);
                    cancelled.push($uuid);
                    None
                } else {
                    currently_matching.push($uuid);
                    Some(($uuid, $order))
                }
            } else {
                Some(($uuid, $order))
            }
        };
    }

    macro_rules! cancel_taker_if_true {
        ($e: expr, $uuid: ident, $order: ident) => {
            if $e {
                if $order.is_cancellable() {
                    delete_my_taker_order(&ctx, &$order.request.uuid);
                    cancelled.push($uuid);
                    None
                } else {
                    currently_matching.push($uuid);
                    Some(($uuid, $order))
                }
            } else {
                Some(($uuid, $order))
            }
        };
    }

    match cancel_by {
        CancelBy::All => {
            *maker_orders = maker_orders
                .drain()
                .filter_map(|(uuid, order)| cancel_maker_if_true!(true, uuid, order))
                .collect();
            *taker_orders = taker_orders
                .drain()
                .filter_map(|(uuid, order)| cancel_taker_if_true!(true, uuid, order))
                .collect();
        },
        CancelBy::Pair { base, rel } => {
            *maker_orders = maker_orders
                .drain()
                .filter_map(|(uuid, order)| cancel_maker_if_true!(order.base == base && order.rel == rel, uuid, order))
                .collect();
            *taker_orders = taker_orders
                .drain()
                .filter_map(|(uuid, order)| {
                    cancel_taker_if_true!(order.request.base == base && order.request.rel == rel, uuid, order)
                })
                .collect();
        },
        CancelBy::Coin { ticker } => {
            *maker_orders = maker_orders
                .drain()
                .filter_map(|(uuid, order)| {
                    cancel_maker_if_true!(order.base == ticker || order.rel == ticker, uuid, order)
                })
                .collect();
            *taker_orders = taker_orders
                .drain()
                .filter_map(|(uuid, order)| {
                    cancel_taker_if_true!(order.request.base == ticker || order.request.rel == ticker, uuid, order)
                })
                .collect();
        },
    };
    for order in cancelled_maker_orders {
        maker_order_cancelled_p2p_notify(ctx.clone(), &order).await;
    }
    Ok((cancelled, currently_matching))
}

pub async fn cancel_all_orders(ctx: MmArc, req: Json) -> Result<Response<Vec<u8>>, String> {
    let cancel_by: CancelBy = try_s!(json::from_value(req["cancel_by"].clone()));

    let (cancelled, currently_matching) = try_s!(cancel_orders_by(&ctx, cancel_by).await);

    let res = json!({
        "result": {
            "cancelled": cancelled,
            "currently_matching": currently_matching,
        }
    });
    Response::builder()
        .body(json::to_vec(&res).expect("Serialization failed"))
        .map_err(|e| ERRL!("{}", e))
}

/// Subscribe to an orderbook topic (see [`orderbook_topic`]).
/// If the `request_orderbook` is true and the orderbook for the given pair of coins is not requested yet (or is not filled up yet),
/// request and fill the orderbook.
///
/// # Safety
///
/// The function locks [`MmCtx::p2p_ctx`] and [`MmCtx::ordermatch_ctx`]
async fn subscribe_to_orderbook_topic(
    ctx: &MmArc,
    base: &str,
    rel: &str,
    request_orderbook: bool,
) -> Result<(), String> {
    const ASKS_NUMBER: Option<usize> = Some(20);
    const BIDS_NUMBER: Option<usize> = Some(20);

    let current_timestamp = now_ms() / 1000;
    let topic = orderbook_topic_from_base_rel(base, rel);
    let is_orderbook_filled = {
        let ordermatch_ctx = try_s!(OrdermatchContext::from_ctx(ctx));
        let mut orderbook = ordermatch_ctx.orderbook.lock().await;

        match orderbook.topics_subscribed_to.entry(topic.clone()) {
            Entry::Vacant(e) => {
                // we weren't subscribed to the topic yet
                e.insert(OrderbookRequestingState::NotRequested {
                    subscribed_at: current_timestamp,
                });
                subscribe_to_topic(&ctx, topic.clone()).await;
                // orderbook is not filled
                false
            },
            Entry::Occupied(e) => match e.get() {
                OrderbookRequestingState::Requested => {
                    // We are subscribed to the topic and the orderbook was requested already
                    true
                },
                OrderbookRequestingState::NotRequested { subscribed_at }
                    if *subscribed_at + ORDERBOOK_REQUESTING_TIMEOUT < current_timestamp =>
                {
                    // We are subscribed to the topic. Also we didn't request the orderbook,
                    // but enough time has passed for the orderbook to fill by OrdermatchMessage::MakerOrderKeepAlive messages.
                    true
                }
                OrderbookRequestingState::NotRequested { .. } => {
                    // We are subscribed to the topic. Also we didn't request the orderbook,
                    // and the orderbook has not filled up yet.
                    false
                },
            },
        }
    };

    if !is_orderbook_filled && request_orderbook {
        try_s!(request_and_fill_orderbook(&ctx, base, rel, ASKS_NUMBER, BIDS_NUMBER).await);
    }

    Ok(())
}

#[derive(Serialize)]
pub struct OrderbookEntry {
    coin: String,
    address: String,
    price: BigDecimal,
    price_rat: BigRational,
    price_fraction: Fraction,
    #[serde(rename = "maxvolume")]
    max_volume: BigDecimal,
    max_volume_rat: BigRational,
    max_volume_fraction: Fraction,
    min_volume: BigDecimal,
    min_volume_rat: BigRational,
    min_volume_fraction: Fraction,
    pubkey: String,
    age: i64,
    zcredits: u64,
    uuid: Uuid,
    is_mine: bool,
}

#[derive(Serialize)]
pub struct OrderbookResponse {
    #[serde(rename = "askdepth")]
    ask_depth: u32,
    asks: Vec<OrderbookEntry>,
    base: String,
    #[serde(rename = "biddepth")]
    bid_depth: u32,
    bids: Vec<OrderbookEntry>,
    netid: u16,
    #[serde(rename = "numasks")]
    num_asks: usize,
    #[serde(rename = "numbids")]
    num_bids: usize,
    rel: String,
    timestamp: u64,
}

#[derive(Deserialize)]
struct OrderbookReq {
    base: String,
    rel: String,
}

pub async fn orderbook(ctx: MmArc, req: Json) -> Result<Response<Vec<u8>>, String> {
    let req: OrderbookReq = try_s!(json::from_value(req));
    if req.base == req.rel {
        return ERR!("Base and rel must be different coins");
    }
    let rel_coin = try_s!(lp_coinfindᵃ(&ctx, &req.rel).await);
    let rel_coin = try_s!(rel_coin.ok_or("Rel coin is not found or inactive"));
    let base_coin = try_s!(lp_coinfindᵃ(&ctx, &req.base).await);
    let base_coin: MmCoinEnum = try_s!(base_coin.ok_or("Base coin is not found or inactive"));
    // TODO change this to `true` when orderbook request is reimplemented using tries
    let request_orderbook = false;
    try_s!(subscribe_to_orderbook_topic(&ctx, &req.base, &req.rel, request_orderbook).await);
    let ordermatch_ctx: Arc<OrdermatchContext> = try_s!(OrdermatchContext::from_ctx(&ctx));
    let orderbook = ordermatch_ctx.orderbook.lock().await;
    let my_pubsecp = hex::encode(&**ctx.secp256k1_key_pair().public());

    let asks = match orderbook.unordered.get(&(req.base.clone(), req.rel.clone())) {
        Some(uuids) => {
            let mut orderbook_entries = Vec::new();
            for uuid in uuids {
                let ask = orderbook.order_set.get(uuid).ok_or(ERRL!(
                    "Orderbook::unordered contains {:?} uuid that is not in Orderbook::order_set",
                    uuid
                ))?;
                let price_mm: MmNumber = ask.price.clone().into();
                let max_vol_mm: MmNumber = ask.max_volume.clone().into();
                let min_vol_mm: MmNumber = ask.min_volume.clone().into();

                orderbook_entries.push(OrderbookEntry {
                    coin: req.base.clone(),
                    address: try_s!(base_coin.address_from_pubkey_str(&ask.pubkey)),
                    price: price_mm.to_decimal(),
                    price_rat: price_mm.to_ratio(),
                    price_fraction: price_mm.to_fraction(),
                    max_volume: max_vol_mm.to_decimal(),
                    max_volume_rat: max_vol_mm.to_ratio(),
                    max_volume_fraction: max_vol_mm.to_fraction(),
                    min_volume: min_vol_mm.to_decimal(),
                    min_volume_rat: min_vol_mm.to_ratio(),
                    min_volume_fraction: min_vol_mm.to_fraction(),
                    pubkey: ask.pubkey.clone(),
                    age: (now_ms() as i64 / 1000),
                    zcredits: 0,
                    uuid: *uuid,
                    is_mine: my_pubsecp == ask.pubkey,
                })
            }
            orderbook_entries
        },
        None => Vec::new(),
    };

    let bids = match orderbook.unordered.get(&(req.rel.clone(), req.base.clone())) {
        Some(uuids) => {
            let mut orderbook_entries = vec![];
            for uuid in uuids {
                let bid = orderbook.order_set.get(uuid).ok_or(ERRL!(
                    "Orderbook::unordered contains {:?} uuid that is not in Orderbook::order_set",
                    uuid
                ))?;
                let price_mm = &MmNumber::from(1i32) / &bid.price.clone().into();
                let max_vol_mm: MmNumber = bid.max_volume.clone().into();
                let min_vol_mm: MmNumber = bid.min_volume.clone().into();
                orderbook_entries.push(OrderbookEntry {
                    coin: req.rel.clone(),
                    address: try_s!(rel_coin.address_from_pubkey_str(&bid.pubkey)),
                    // NB: 1/x can not be represented as a decimal and introduces a rounding error
                    // cf. https://github.com/KomodoPlatform/atomicDEX-API/issues/495#issuecomment-516365682
                    price: price_mm.to_decimal(),
                    price_rat: price_mm.to_ratio(),
                    price_fraction: price_mm.to_fraction(),
                    max_volume: max_vol_mm.to_decimal(),
                    max_volume_rat: max_vol_mm.to_ratio(),
                    max_volume_fraction: max_vol_mm.to_fraction(),
                    min_volume: min_vol_mm.to_decimal(),
                    min_volume_rat: min_vol_mm.to_ratio(),
                    min_volume_fraction: min_vol_mm.to_fraction(),
                    pubkey: bid.pubkey.clone(),
                    age: (now_ms() as i64 / 1000),
                    zcredits: 0,
                    uuid: *uuid,
                    is_mine: my_pubsecp == bid.pubkey,
                })
            }
            orderbook_entries
        },
        None => vec![],
    };
    let response = OrderbookResponse {
        num_asks: asks.len(),
        num_bids: bids.len(),
        ask_depth: 0,
        asks,
        base: req.base,
        bid_depth: 0,
        bids,
        netid: ctx.netid(),
        rel: req.rel,
        timestamp: now_ms() / 1000,
    };
    let responseʲ = try_s!(json::to_vec(&response));
    Ok(try_s!(Response::builder().body(responseʲ)))
}

fn choose_maker_confs_and_notas(
    maker_confs: Option<OrderConfirmationsSettings>,
    taker_req: &TakerRequest,
    maker_coin: &MmCoinEnum,
    taker_coin: &MmCoinEnum,
) -> SwapConfirmationsSettings {
    let maker_settings = maker_confs.unwrap_or(OrderConfirmationsSettings {
        base_confs: maker_coin.required_confirmations(),
        base_nota: maker_coin.requires_notarization(),
        rel_confs: taker_coin.required_confirmations(),
        rel_nota: taker_coin.requires_notarization(),
    });

    let (maker_coin_confs, maker_coin_nota, taker_coin_confs, taker_coin_nota) = match taker_req.conf_settings {
        Some(taker_settings) => match taker_req.action {
            TakerAction::Sell => {
                let maker_coin_confs = if taker_settings.rel_confs < maker_settings.base_confs {
                    taker_settings.rel_confs
                } else {
                    maker_settings.base_confs
                };
                let maker_coin_nota = if !taker_settings.rel_nota {
                    taker_settings.rel_nota
                } else {
                    maker_settings.base_nota
                };
                (
                    maker_coin_confs,
                    maker_coin_nota,
                    maker_settings.rel_confs,
                    maker_settings.rel_nota,
                )
            },
            TakerAction::Buy => {
                let maker_coin_confs = if taker_settings.base_confs < maker_settings.base_confs {
                    taker_settings.base_confs
                } else {
                    maker_settings.base_confs
                };
                let maker_coin_nota = if !taker_settings.base_nota {
                    taker_settings.base_nota
                } else {
                    maker_settings.base_nota
                };
                (
                    maker_coin_confs,
                    maker_coin_nota,
                    maker_settings.rel_confs,
                    maker_settings.rel_nota,
                )
            },
        },
        None => (
            maker_settings.base_confs,
            maker_settings.base_nota,
            maker_settings.rel_confs,
            maker_settings.rel_nota,
        ),
    };

    SwapConfirmationsSettings {
        maker_coin_confs,
        maker_coin_nota,
        taker_coin_confs,
        taker_coin_nota,
    }
}

fn choose_taker_confs_and_notas(
    taker_req: &TakerRequest,
    maker_reserved: &MakerReserved,
    maker_coin: &MmCoinEnum,
    taker_coin: &MmCoinEnum,
) -> SwapConfirmationsSettings {
    let (mut taker_coin_confs, mut taker_coin_nota, maker_coin_confs, maker_coin_nota) = match taker_req.action {
        TakerAction::Buy => match taker_req.conf_settings {
            Some(s) => (s.rel_confs, s.rel_nota, s.base_confs, s.base_nota),
            None => (
                taker_coin.required_confirmations(),
                taker_coin.requires_notarization(),
                maker_coin.required_confirmations(),
                maker_coin.requires_notarization(),
            ),
        },
        TakerAction::Sell => match taker_req.conf_settings {
            Some(s) => (s.base_confs, s.base_nota, s.rel_confs, s.rel_nota),
            None => (
                taker_coin.required_confirmations(),
                taker_coin.requires_notarization(),
                maker_coin.required_confirmations(),
                maker_coin.requires_notarization(),
            ),
        },
    };
    if let Some(settings_from_maker) = maker_reserved.conf_settings {
        if settings_from_maker.rel_confs < taker_coin_confs {
            taker_coin_confs = settings_from_maker.rel_confs;
        }
        if !settings_from_maker.rel_nota {
            taker_coin_nota = settings_from_maker.rel_nota;
        }
    }
    SwapConfirmationsSettings {
        maker_coin_confs,
        maker_coin_nota,
        taker_coin_confs,
        taker_coin_nota,
    }
}
