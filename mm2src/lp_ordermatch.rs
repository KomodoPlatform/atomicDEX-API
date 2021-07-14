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

use async_trait::async_trait;
use best_orders::BestOrdersAction;
use bigdecimal::BigDecimal;
use blake2::digest::{Update, VariableOutput};
use blake2::VarBlake2b;
use coins::utxo::{compressed_pub_key_from_priv_raw, ChecksumType};
use coins::{find_pair, lp_coinfind, BalanceTradeFeeUpdatedHandler, FeeApproxStage, MmCoinEnum};
use common::executor::{spawn, Timer};
use common::log::error;
use common::mm_ctx::{from_ctx, MmArc, MmWeak};
use common::mm_number::{Fraction, MmNumber};
use common::{bits256, json_dir_entries, log, new_uuid, now_ms, remove_file, write};
use derive_more::Display;
use futures::{compat::Future01CompatExt, lock::Mutex as AsyncMutex, StreamExt, TryFutureExt};
use gstuff::slurp;
use hash256_std_hasher::Hash256StdHasher;
use hash_db::{Hasher, EMPTY_PREFIX};
use http::Response;
use mm2_libp2p::{decode_signed, encode_and_sign, encode_message, pub_sub_topic, TopicPrefix, TOPIC_SEPARATOR};
#[cfg(test)] use mocktopus::macros::*;
use num_rational::BigRational;
use num_traits::identities::Zero;
use order_requests_tracker::OrderRequestsTracker;
use rpc::v1::types::H256 as H256Json;
use serde_json::{self as json, Value as Json};
use sp_trie::{delta_trie_root, DBValue, HashDBT, MemoryDB, Trie, TrieConfiguration, TrieDB, TrieDBMut, TrieHash,
              TrieMut};
use std::collections::hash_map::{Entry, HashMap, RawEntryMut};
use std::collections::{BTreeSet, HashSet};
use std::convert::TryInto;
use std::fmt;
use std::fs::DirEntry;
use std::path::PathBuf;
use std::sync::Arc;
use trie_db::NodeCodec as NodeCodecT;
use uuid::Uuid;

use crate::mm2::lp_network::{broadcast_p2p_msg, request_any_relay, request_one_peer, subscribe_to_topic, P2PRequest};
use crate::mm2::lp_swap::{calc_max_maker_vol, check_balance_for_maker_swap, check_balance_for_taker_swap,
                          check_other_coin_balance_for_swap, insert_new_swap_to_db, is_pubkey_banned,
                          lp_atomic_locktime, run_maker_swap, run_taker_swap, AtomicLocktimeVersion,
                          CheckBalanceError, MakerSwap, RunMakerSwapInput, RunTakerSwapInput,
                          SwapConfirmationsSettings, TakerSwap};

pub use best_orders::best_orders_rpc;
use common::mm_error::MmError;
pub use orderbook_depth::orderbook_depth_rpc;
pub use orderbook_rpc::orderbook_rpc;

#[path = "lp_ordermatch/best_orders.rs"] mod best_orders;
#[path = "lp_ordermatch/new_protocol.rs"] mod new_protocol;
#[path = "lp_ordermatch/order_requests_tracker.rs"]
mod order_requests_tracker;
#[path = "lp_ordermatch/orderbook_depth.rs"] mod orderbook_depth;
#[path = "lp_ordermatch/orderbook_rpc.rs"] mod orderbook_rpc;
#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "ordermatch_tests.rs"]
mod ordermatch_tests;

pub const ORDERBOOK_PREFIX: TopicPrefix = "orbk";
const MIN_ORDER_KEEP_ALIVE_INTERVAL: u64 = 30;
const MAKER_ORDER_TIMEOUT: u64 = MIN_ORDER_KEEP_ALIVE_INTERVAL * 3;
const TAKER_ORDER_TIMEOUT: u64 = 30;
const ORDER_MATCH_TIMEOUT: u64 = 30;
const ORDERBOOK_REQUESTING_TIMEOUT: u64 = MIN_ORDER_KEEP_ALIVE_INTERVAL * 2;
const MAX_ORDERS_NUMBER_IN_ORDERBOOK_RESPONSE: usize = 1000;

/// Alphabetically ordered orderbook pair
type AlbOrderedOrderbookPair = String;
type PubkeyOrders = Vec<(Uuid, OrderbookItem)>;

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

fn process_pubkey_full_trie(
    orderbook: &mut Orderbook,
    pubkey: &str,
    alb_pair: &str,
    new_trie_orders: PubkeyOrders,
) -> H64 {
    remove_and_purge_pubkey_pair_orders(orderbook, pubkey, alb_pair);

    for (_uuid, order) in new_trie_orders {
        orderbook.insert_or_update_order_update_trie(order);
    }

    let new_root = pubkey_state_mut(&mut orderbook.pubkeys_state, pubkey)
        .trie_roots
        .get(alb_pair)
        .copied()
        .unwrap_or_else(H64::default);
    new_root
}

fn process_trie_delta(
    orderbook: &mut Orderbook,
    pubkey: &str,
    alb_pair: &str,
    delta_orders: HashMap<Uuid, Option<OrderbookItem>>,
) -> H64 {
    for (uuid, order) in delta_orders {
        match order {
            Some(order) => orderbook.insert_or_update_order_update_trie(order),
            None => {
                orderbook.remove_order_trie_update(uuid);
            },
        }
    }

    let new_root = match orderbook.pubkeys_state.get(pubkey) {
        Some(pubkey_state) => pubkey_state
            .trie_roots
            .get(alb_pair)
            .copied()
            .unwrap_or_else(H64::default),
        None => H64::default(),
    };
    new_root
}

async fn process_orders_keep_alive(
    ctx: MmArc,
    propagated_from_peer: String,
    from_pubkey: String,
    keep_alive: new_protocol::PubkeyKeepAlive,
    i_am_relay: bool,
) -> bool {
    let ordermatch_ctx = OrdermatchContext::from_ctx(&ctx).expect("from_ctx failed");
    let to_request = ordermatch_ctx
        .orderbook
        .lock()
        .await
        .process_keep_alive(&from_pubkey, keep_alive, i_am_relay);

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
        let _new_root = match diff {
            DeltaOrFullTrie::Delta(delta) => process_trie_delta(&mut orderbook, &from_pubkey, &pair, delta),
            DeltaOrFullTrie::FullTrie(values) => process_pubkey_full_trie(&mut orderbook, &from_pubkey, &pair, values),
        };
    }
    true
}

async fn process_maker_order_updated(
    ctx: MmArc,
    from_pubkey: String,
    updated_msg: new_protocol::MakerOrderUpdated,
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
            log::warn!(
                "Couldn't find an order {}, ignoring, it will be synced upon pubkey keep alive",
                uuid
            );
            false
        },
    }
}

// fn verify_pubkey_orderbook(orderbook: &GetOrderbookPubkeyItem) -> Result<(), String> {
//     let keys: Vec<(_, _)> = orderbook
//         .orders
//         .iter()
//         .map(|(uuid, order)| {
//             let order_bytes = rmp_serde::to_vec(&order).expect("Serialization should never fail");
//             (uuid.as_bytes(), Some(order_bytes))
//         })
//         .collect();
//     let (orders_root, proof) = &orderbook.pair_orders_trie_root;
//     verify_trie_proof::<Layout, _, _, _>(orders_root, proof, &keys)
//         .map_err(|e| ERRL!("Error on pair_orders_trie_root verification: {}", e))?;
//     Ok(())
// }

/// Request best asks and bids for the given `base` and `rel` coins from relays.
/// Set `asks_num` and/or `bids_num` to get corresponding number of best asks and bids or None to get all of the available orders.
///
/// # Safety
///
/// The function locks [`MmCtx::p2p_ctx`] and [`MmCtx::ordermatch_ctx`]
async fn request_and_fill_orderbook(ctx: &MmArc, base: &str, rel: &str) -> Result<(), String> {
    let request = OrdermatchRequest::GetOrderbook {
        base: base.to_string(),
        rel: rel.to_string(),
    };

    let response = try_s!(request_any_relay::<GetOrderbookRes>(ctx.clone(), P2PRequest::Ordermatch(request)).await);
    let pubkey_orders = match response {
        Some((GetOrderbookRes { pubkey_orders }, _peer_id)) => pubkey_orders,
        None => return Ok(()),
    };

    let ordermatch_ctx = OrdermatchContext::from_ctx(&ctx).unwrap();
    let mut orderbook = ordermatch_ctx.orderbook.lock().await;

    let alb_pair = alb_ordered_pair(base, rel);
    for (pubkey, GetOrderbookPubkeyItem { orders, .. }) in pubkey_orders {
        let pubkey_bytes = match hex::decode(&pubkey) {
            Ok(b) => b,
            Err(e) => {
                log::warn!("Error {} decoding pubkey {}", e, pubkey);
                continue;
            },
        };
        if is_pubkey_banned(ctx, &pubkey_bytes[1..].into()) {
            log::warn!("Pubkey {} is banned", pubkey);
            continue;
        }
        let _new_root = process_pubkey_full_trie(&mut orderbook, &pubkey, &alb_pair, orders);
    }

    let topic = orderbook_topic_from_base_rel(base, rel);
    orderbook
        .topics_subscribed_to
        .insert(topic, OrderbookRequestingState::Requested);

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

fn remove_and_purge_pubkey_pair_orders(orderbook: &mut Orderbook, pubkey: &str, alb_pair: &str) {
    let pubkey_state = match orderbook.pubkeys_state.get_mut(pubkey) {
        Some(state) => state,
        None => return,
    };
    let pair_root = match pubkey_state.trie_roots.remove(alb_pair) {
        Some(root) => root,
        None => return,
    };
    pubkey_state.order_pairs_trie_state_history.remove(alb_pair);

    let mut orders_to_remove = Vec::with_capacity(pubkey_state.orders_uuids.len());
    pubkey_state.orders_uuids.retain(|(uuid, alb)| {
        if alb == alb_pair {
            orders_to_remove.push(*uuid);
            false
        } else {
            true
        }
    });

    for order in orders_to_remove {
        orderbook.remove_order(order);
    }

    if orderbook.memory_db.remove_and_purge(&pair_root, EMPTY_PREFIX).is_none()
        && pair_root != H64::default()
        && pair_root != hashed_null_node::<Layout>()
    {
        log::warn!("Warning: couldn't find {:?} hash root in memory_db", pair_root);
    }
}

/// Attempts to decode a message and process it returning whether the message is valid and worth rebroadcasting
pub async fn process_msg(ctx: MmArc, _topics: Vec<String>, from_peer: String, msg: &[u8], i_am_relay: bool) -> bool {
    match decode_signed::<new_protocol::OrdermatchMessage>(msg) {
        Ok((message, _sig, pubkey)) => {
            if is_pubkey_banned(&ctx, &pubkey.unprefixed().into()) {
                log::warn!("Pubkey {} is banned", pubkey.to_hex());
                return false;
            }
            match message {
                new_protocol::OrdermatchMessage::MakerOrderCreated(created_msg) => {
                    let order: OrderbookItem = (created_msg, hex::encode(pubkey.to_bytes().as_slice())).into();
                    insert_or_update_order(&ctx, order).await;
                    true
                },
                new_protocol::OrdermatchMessage::PubkeyKeepAlive(keep_alive) => {
                    process_orders_keep_alive(ctx, from_peer, pubkey.to_hex(), keep_alive, i_am_relay).await
                },
                new_protocol::OrdermatchMessage::TakerRequest(taker_request) => {
                    let msg = TakerRequest::from_new_proto_and_pubkey(taker_request, pubkey.unprefixed().into());
                    process_taker_request(ctx, pubkey.unprefixed().into(), msg).await;
                    true
                },
                new_protocol::OrdermatchMessage::MakerReserved(maker_reserved) => {
                    let msg = MakerReserved::from_new_proto_and_pubkey(maker_reserved, pubkey.unprefixed().into());
                    process_maker_reserved(ctx, pubkey.unprefixed().into(), msg).await;
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
                    process_maker_order_updated(ctx, pubkey.to_hex(), updated_msg).await
                },
            }
        },
        Err(e) => {
            log::error!("Error {} while decoding signed message", e);
            false
        },
    }
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum OrdermatchRequest {
    /// Get an orderbook for the given pair.
    GetOrderbook {
        base: String,
        rel: String,
    },
    /// Sync specific pubkey orderbook state if our known Patricia trie state doesn't match the latest keep alive message
    SyncPubkeyOrderbookState {
        pubkey: String,
        /// Request using this condition
        trie_roots: HashMap<AlbOrderedOrderbookPair, H64>,
    },
    BestOrders {
        coin: String,
        action: BestOrdersAction,
        volume: BigRational,
    },
    OrderbookDepth {
        pairs: Vec<(String, String)>,
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
    log::debug!("Got ordermatch request {:?}", request);
    match request {
        OrdermatchRequest::GetOrderbook { base, rel } => process_get_orderbook_request(ctx, base, rel).await,
        OrdermatchRequest::SyncPubkeyOrderbookState { pubkey, trie_roots } => {
            let response = process_sync_pubkey_orderbook_state(ctx, pubkey, trie_roots).await;
            response.map(|res| res.map(|r| encode_message(&r).expect("Serialization failed")))
        },
        OrdermatchRequest::BestOrders { coin, action, volume } => {
            best_orders::process_best_orders_p2p_request(ctx, coin, action, volume).await
        },
        OrdermatchRequest::OrderbookDepth { pairs } => {
            orderbook_depth::process_orderbook_depth_p2p_request(ctx, pairs).await
        },
    }
}

type TrieProof = Vec<Vec<u8>>;

#[derive(Debug, Deserialize, Serialize)]
struct GetOrderbookPubkeyItem {
    /// Timestamp of the latest keep alive message received.
    last_keep_alive: u64,
    /// last signed OrdermatchMessage payload
    last_signed_pubkey_payload: Vec<u8>,
    /// Requested orders.
    orders: PubkeyOrders,
}

#[derive(Debug, Deserialize, Serialize)]
struct GetOrderbookRes {
    /// Asks and bids grouped by pubkey.
    pubkey_orders: HashMap<String, GetOrderbookPubkeyItem>,
}

async fn process_get_orderbook_request(ctx: MmArc, base: String, rel: String) -> Result<Option<Vec<u8>>, String> {
    fn get_pubkeys_orders(orderbook: &Orderbook, base: String, rel: String) -> (usize, HashMap<String, PubkeyOrders>) {
        let asks = orderbook.unordered.get(&(base.clone(), rel.clone()));
        let bids = orderbook.unordered.get(&(rel, base));

        let asks_num = asks.map(|x| x.len()).unwrap_or(0);
        let bids_num = bids.map(|x| x.len()).unwrap_or(0);
        let total_orders_number = asks_num + bids_num;

        // flatten Option(asks) and Option(bids) to avoid cloning
        let orders = asks.iter().chain(bids.iter()).copied().flatten();

        let mut uuids_by_pubkey = HashMap::new();
        for uuid in orders {
            let order = orderbook
                .order_set
                .get(uuid)
                .expect("Orderbook::ordered contains an uuid that is not in Orderbook::order_set");
            let uuids = uuids_by_pubkey.entry(order.pubkey.clone()).or_insert_with(Vec::new);
            uuids.push((*uuid, order.clone()))
        }

        (total_orders_number, uuids_by_pubkey)
    }

    let ordermatch_ctx = OrdermatchContext::from_ctx(&ctx).unwrap();
    let orderbook = ordermatch_ctx.orderbook.lock().await;

    let (total_orders_number, orders) = get_pubkeys_orders(&orderbook, base, rel);
    if total_orders_number > MAX_ORDERS_NUMBER_IN_ORDERBOOK_RESPONSE {
        return ERR!("Orderbook too large");
    }

    let orders_to_send: Result<HashMap<_, _>, String> = orders
        .into_iter()
        .map(|(pubkey, orders)| {
            let pubkey_state = orderbook.pubkeys_state.get(&pubkey).ok_or(ERRL!(
                "Orderbook::pubkeys_state is expected to contain the {:?} pubkey",
                pubkey
            ))?;

            let item = GetOrderbookPubkeyItem {
                last_keep_alive: pubkey_state.last_keep_alive,
                orders,
                // TODO save last signed payload to pubkey state
                last_signed_pubkey_payload: vec![],
            };

            Ok((pubkey, item))
        })
        .collect();

    let pubkey_orders = orders_to_send?;
    let response = GetOrderbookRes { pubkey_orders };
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

fn get_full_trie<Key, Value>(
    trie_root: &H64,
    db: &MemoryDB<Blake2Hasher64>,
) -> Result<Vec<(Key, Value)>, TrieDiffHistoryError>
where
    Key: Clone + Eq + std::hash::Hash + TryFromBytes,
    Value: Clone + TryFromBytes,
{
    let trie = TrieDB::<Layout>::new(db, trie_root)?;
    let trie: Result<Vec<_>, TrieDiffHistoryError> = trie
        .iter()?
        .map(|key_value| {
            let (key, value) = key_value?;
            Ok((TryFromBytes::try_from_bytes(key)?, TryFromBytes::try_from_bytes(value)?))
        })
        .collect();
    trie
}

impl<Key: Clone + Eq + std::hash::Hash + TryFromBytes, Value: Clone + TryFromBytes> DeltaOrFullTrie<Key, Value> {
    fn from_history(
        history: &TrieDiffHistory<Key, Value>,
        from_hash: H64,
        actual_trie_root: H64,
        db: &MemoryDB<Blake2Hasher64>,
    ) -> Result<DeltaOrFullTrie<Key, Value>, TrieDiffHistoryError> {
        if let Some(delta) = history.get(&from_hash) {
            let mut current_delta = delta;
            let mut total_delta = HashMap::new();
            total_delta.extend(delta.delta.iter().cloned());
            while let Some(cur) = history.get(&current_delta.next_root) {
                current_delta = cur;
                total_delta.extend(current_delta.delta.iter().cloned());
            }
            if current_delta.next_root == actual_trie_root {
                return Ok(DeltaOrFullTrie::Delta(total_delta));
            }

            log::warn!(
                "History started from {:?} ends with not up-to-date trie root {:?}",
                from_hash,
                actual_trie_root
            );
        }

        let trie = get_full_trie(&actual_trie_root, db)?;
        Ok(DeltaOrFullTrie::FullTrie(trie))
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct SyncPubkeyOrderbookStateRes {
    /// last signed OrdermatchMessage payload from pubkey
    last_signed_pubkey_payload: Vec<u8>,
    pair_orders_diff: HashMap<AlbOrderedOrderbookPair, DeltaOrFullTrie<Uuid, OrderbookItem>>,
}

async fn process_sync_pubkey_orderbook_state(
    ctx: MmArc,
    pubkey: String,
    trie_roots: HashMap<AlbOrderedOrderbookPair, H64>,
) -> Result<Option<SyncPubkeyOrderbookStateRes>, String> {
    let ordermatch_ctx = OrdermatchContext::from_ctx(&ctx).unwrap();
    let orderbook = ordermatch_ctx.orderbook.lock().await;
    let pubkey_state = match orderbook.pubkeys_state.get(&pubkey) {
        Some(s) => s,
        None => return Ok(None),
    };

    let pair_orders_diff: Result<_, _> = trie_roots
        .into_iter()
        .map(|(pair, root)| {
            let actual_pair_root = pubkey_state
                .trie_roots
                .get(&pair)
                .ok_or(ERRL!("No pair trie root for {}", pair))?;

            let delta_result = match pubkey_state.order_pairs_trie_state_history.get(&pair) {
                Some(history) => DeltaOrFullTrie::from_history(history, root, *actual_pair_root, &orderbook.memory_db),
                None => get_full_trie(actual_pair_root, &orderbook.memory_db).map(DeltaOrFullTrie::FullTrie),
            };

            let delta = try_s!(delta_result);
            Ok((pair, delta))
        })
        .collect();

    let pair_orders_diff = try_s!(pair_orders_diff);
    let last_signed_pubkey_payload = vec![];
    let result = SyncPubkeyOrderbookStateRes {
        last_signed_pubkey_payload,
        pair_orders_diff,
    };
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
        timestamp: now_ms() / 1000,
        pair_trie_root: H64::default(),
    };

    let key_pair = ctx.secp256k1_key_pair.or(&&|| panic!());
    let to_broadcast = new_protocol::OrdermatchMessage::MakerOrderCreated(message.clone());
    let encoded_msg = encode_and_sign(&to_broadcast, &*key_pair.private().secret).unwrap();
    let order: OrderbookItem = (message, hex::encode(&**key_pair.public())).into();
    insert_or_update_order(&ctx, order).await;
    broadcast_p2p_msg(&ctx, vec![topic], encoded_msg);
}

async fn process_my_maker_order_updated(ctx: &MmArc, message: &new_protocol::MakerOrderUpdated) {
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
    process_my_maker_order_updated(&ctx, &message).await;
    broadcast_p2p_msg(&ctx, vec![topic], encoded_msg);
}

async fn maker_order_cancelled_p2p_notify(ctx: MmArc, order: &MakerOrder) {
    let message = new_protocol::OrdermatchMessage::MakerOrderCancelled(new_protocol::MakerOrderCancelled {
        uuid: order.uuid.into(),
        timestamp: now_ms() / 1000,
        pair_trie_root: H64::default(),
    });
    delete_my_order(&ctx, order.uuid).await;
    log::debug!("maker_order_cancelled_p2p_notify called, message {:?}", message);
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
    async fn balance_updated(&self, coin: &MmCoinEnum, new_balance: &BigDecimal) {
        // Get the max maker available volume to check if the wallet balances are sufficient for the issued maker orders.
        // Note although the maker orders are issued already, but they are not matched yet, so pass the `OrderIssue` stage.
        let new_volume = match calc_max_maker_vol(&self.ctx, coin, new_balance, FeeApproxStage::OrderIssue).await {
            Ok(v) => v,
            Err(e) if e.get_inner().not_sufficient_balance() => MmNumber::from(0),
            Err(e) => {
                log::warn!("Couldn't handle the 'balance_updated' event: {}", e);
                return;
            },
        };
        let ordermatch_ctx = OrdermatchContext::from_ctx(&self.ctx).unwrap();
        let mut maker_orders = ordermatch_ctx.my_maker_orders.lock().await;
        *maker_orders = maker_orders
            .drain()
            .filter_map(|(uuid, order)| {
                if order.base == coin.ticker() {
                    if new_volume < order.min_base_vol {
                        let ctx = self.ctx.clone();
                        delete_my_maker_order(&ctx, &order, MakerOrderCancellationReason::InsufficientBalance);
                        spawn(async move { maker_order_cancelled_p2p_notify(ctx, &order).await });
                        None
                    } else if new_volume < order.available_amount() {
                        let mut update_msg = new_protocol::MakerOrderUpdated::new(order.uuid);
                        update_msg.with_new_max_volume(new_volume.to_ratio());
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

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
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
    pub base: String,
    pub rel: String,
    pub base_amount: MmNumber,
    pub rel_amount: MmNumber,
    pub action: TakerAction,
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

impl From<TakerRequest> for new_protocol::OrdermatchMessage {
    fn from(taker_request: TakerRequest) -> Self {
        new_protocol::OrdermatchMessage::TakerRequest(new_protocol::TakerRequest {
            base_amount: taker_request.get_base_amount().to_ratio(),
            rel_amount: taker_request.get_rel_amount().to_ratio(),
            base: taker_request.base,
            rel: taker_request.rel,
            action: taker_request.action,
            uuid: taker_request.uuid.into(),
            match_by: taker_request.match_by.into(),
            conf_settings: taker_request.conf_settings.unwrap(),
        })
    }
}

impl TakerRequest {
    fn get_base_amount(&self) -> &MmNumber { &self.base_amount }

    fn get_rel_amount(&self) -> &MmNumber { &self.rel_amount }
}

pub struct TakerOrderBuilder<'a> {
    base_coin: &'a MmCoinEnum,
    rel_coin: &'a MmCoinEnum,
    base_amount: MmNumber,
    rel_amount: MmNumber,
    sender_pubkey: H256Json,
    action: TakerAction,
    match_by: MatchBy,
    order_type: OrderType,
    conf_settings: Option<OrderConfirmationsSettings>,
    min_volume: Option<MmNumber>,
    timeout: u64,
    save_in_history: bool,
}

pub enum TakerOrderBuildError {
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
    /// Min volume too low with threshold
    MinVolumeTooLow {
        actual: MmNumber,
        threshold: MmNumber,
    },
    /// Max vol below min base vol
    MaxBaseVolBelowMinBaseVol {
        max: MmNumber,
        min: MmNumber,
    },
    SenderPubkeyIsZero,
    ConfsSettingsNotSet,
}

impl fmt::Display for TakerOrderBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TakerOrderBuildError::BaseEqualRel => write!(f, "Rel coin can not be same as base"),
            TakerOrderBuildError::BaseAmountTooLow { actual, threshold } => write!(
                f,
                "Base amount {} is too low, required: {}",
                actual.to_decimal(),
                threshold.to_decimal()
            ),
            TakerOrderBuildError::RelAmountTooLow { actual, threshold } => write!(
                f,
                "Rel amount {} is too low, required: {}",
                actual.to_decimal(),
                threshold.to_decimal()
            ),
            TakerOrderBuildError::MinVolumeTooLow { actual, threshold } => write!(
                f,
                "Min volume {} is too low, required: {}",
                actual.to_decimal(),
                threshold.to_decimal()
            ),
            TakerOrderBuildError::MaxBaseVolBelowMinBaseVol { min, max } => write!(
                f,
                "Max base vol {} is below min base vol: {}",
                max.to_decimal(),
                min.to_decimal()
            ),
            TakerOrderBuildError::SenderPubkeyIsZero => write!(f, "Sender pubkey can not be zero"),
            TakerOrderBuildError::ConfsSettingsNotSet => write!(f, "Confirmation settings must be set"),
        }
    }
}

impl<'a> TakerOrderBuilder<'a> {
    pub fn new(base_coin: &'a MmCoinEnum, rel_coin: &'a MmCoinEnum) -> TakerOrderBuilder<'a> {
        TakerOrderBuilder {
            base_coin,
            rel_coin,
            base_amount: MmNumber::from(0),
            rel_amount: MmNumber::from(0),
            sender_pubkey: H256Json::default(),
            action: TakerAction::Buy,
            match_by: MatchBy::Any,
            conf_settings: None,
            min_volume: None,
            order_type: OrderType::GoodTillCancelled,
            timeout: TAKER_ORDER_TIMEOUT,
            save_in_history: true,
        }
    }

    pub fn with_base_amount(mut self, vol: MmNumber) -> Self {
        self.base_amount = vol;
        self
    }

    pub fn with_rel_amount(mut self, vol: MmNumber) -> Self {
        self.rel_amount = vol;
        self
    }

    pub fn with_min_volume(mut self, vol: Option<MmNumber>) -> Self {
        self.min_volume = vol;
        self
    }

    pub fn with_action(mut self, action: TakerAction) -> Self {
        self.action = action;
        self
    }

    pub fn with_match_by(mut self, match_by: MatchBy) -> Self {
        self.match_by = match_by;
        self
    }

    fn with_order_type(mut self, order_type: OrderType) -> Self {
        self.order_type = order_type;
        self
    }

    pub fn with_conf_settings(mut self, settings: OrderConfirmationsSettings) -> Self {
        self.conf_settings = Some(settings);
        self
    }

    pub fn with_sender_pubkey(mut self, sender_pubkey: H256Json) -> Self {
        self.sender_pubkey = sender_pubkey;
        self
    }

    pub fn with_timeout(mut self, timeout: u64) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn with_save_in_history(mut self, save_in_history: bool) -> Self {
        self.save_in_history = save_in_history;
        self
    }

    /// Validate fields and build
    pub fn build(self) -> Result<TakerOrder, TakerOrderBuildError> {
        let min_base_amount = self.base_coin.min_trading_vol();
        let min_rel_amount = self.rel_coin.min_trading_vol();

        if self.base_coin.ticker() == self.rel_coin.ticker() {
            return Err(TakerOrderBuildError::BaseEqualRel);
        }

        if self.base_amount < min_base_amount {
            return Err(TakerOrderBuildError::BaseAmountTooLow {
                actual: self.base_amount,
                threshold: min_base_amount,
            });
        }

        if self.rel_amount < min_rel_amount {
            return Err(TakerOrderBuildError::RelAmountTooLow {
                actual: self.rel_amount,
                threshold: min_rel_amount,
            });
        }

        if self.sender_pubkey == H256Json::default() {
            return Err(TakerOrderBuildError::SenderPubkeyIsZero);
        }

        if self.conf_settings.is_none() {
            return Err(TakerOrderBuildError::ConfsSettingsNotSet);
        }

        let price = &self.rel_amount / &self.base_amount;
        let base_min_by_rel = &min_rel_amount / &price;
        let base_min_vol_threshold = min_base_amount.max(base_min_by_rel);

        let min_volume = self.min_volume.unwrap_or_else(|| base_min_vol_threshold.clone());

        if min_volume < base_min_vol_threshold {
            return Err(TakerOrderBuildError::MinVolumeTooLow {
                actual: min_volume,
                threshold: base_min_vol_threshold,
            });
        }

        if self.base_amount < min_volume {
            return Err(TakerOrderBuildError::MaxBaseVolBelowMinBaseVol {
                max: self.base_amount,
                min: min_volume,
            });
        }

        Ok(TakerOrder {
            created_at: now_ms(),
            request: TakerRequest {
                base: self.base_coin.ticker().to_owned(),
                rel: self.rel_coin.ticker().to_owned(),
                base_amount: self.base_amount,
                rel_amount: self.rel_amount,
                action: self.action,
                uuid: new_uuid(),
                sender_pubkey: self.sender_pubkey,
                dest_pub_key: Default::default(),
                match_by: self.match_by,
                conf_settings: self.conf_settings,
            },
            matches: Default::default(),
            min_volume,
            order_type: self.order_type,
            timeout: self.timeout,
            save_in_history: self.save_in_history,
        })
    }

    #[cfg(test)]
    /// skip validation for tests
    fn build_unchecked(self) -> TakerOrder {
        TakerOrder {
            created_at: now_ms(),
            request: TakerRequest {
                base: self.base_coin.ticker().to_owned(),
                rel: self.rel_coin.ticker().to_owned(),
                base_amount: self.base_amount,
                rel_amount: self.rel_amount,
                action: self.action,
                uuid: new_uuid(),
                sender_pubkey: self.sender_pubkey,
                dest_pub_key: Default::default(),
                match_by: self.match_by,
                conf_settings: self.conf_settings,
            },
            matches: HashMap::new(),
            min_volume: Default::default(),
            order_type: Default::default(),
            timeout: self.timeout,
            save_in_history: false,
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

#[derive(Copy, Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", content = "data")]
enum OrderType {
    FillOrKill,
    GoodTillCancelled,
}

impl Default for OrderType {
    fn default() -> Self { OrderType::GoodTillCancelled }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TakerOrder {
    pub created_at: u64,
    pub request: TakerRequest,
    matches: HashMap<Uuid, TakerMatch>,
    min_volume: MmNumber,
    order_type: OrderType,
    timeout: u64,
    #[serde(default = "get_true")]
    save_in_history: bool,
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
    pub updated_at: Option<u64>,
    pub base: String,
    pub rel: String,
    matches: HashMap<Uuid, MakerMatch>,
    started_swaps: Vec<Uuid>,
    uuid: Uuid,
    conf_settings: Option<OrderConfirmationsSettings>,
    #[serde(skip_serializing_if = "Option::is_none")]
    changes_history: Option<Vec<HistoricalOrder>>,
    #[serde(default = "get_true")]
    save_in_history: bool,
}

pub struct MakerOrderBuilder<'a> {
    max_base_vol: MmNumber,
    min_base_vol: Option<MmNumber>,
    price: MmNumber,
    base_coin: &'a MmCoinEnum,
    rel_coin: &'a MmCoinEnum,
    conf_settings: Option<OrderConfirmationsSettings>,
    save_in_history: bool,
}

pub enum MakerOrderBuildError {
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
    MaxBaseVolBelowMinBaseVol {
        min: MmNumber,
        max: MmNumber,
    },
}

impl fmt::Display for MakerOrderBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
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
            MakerOrderBuildError::MaxBaseVolBelowMinBaseVol { min, max } => write!(
                f,
                "Max base vol {} is below min base vol: {}",
                max.to_decimal(),
                min.to_decimal()
            ),
        }
    }
}

fn validate_price(price: MmNumber) -> Result<(), MakerOrderBuildError> {
    let min_price = MmNumber::from(BigRational::new(1.into(), 100_000_000.into()));

    if price < min_price {
        return Err(MakerOrderBuildError::PriceTooLow {
            actual: price,
            threshold: min_price,
        });
    }

    Ok(())
}

fn validate_and_get_min_vol(
    min_base_amount: MmNumber,
    min_rel_amount: MmNumber,
    min_base_vol: Option<MmNumber>,
    price: MmNumber,
) -> Result<MmNumber, MakerOrderBuildError> {
    let base_min_by_rel = min_rel_amount / price;
    let base_min_vol_threshold = min_base_amount.max(base_min_by_rel);
    let actual_min_base_vol = min_base_vol.unwrap_or_else(|| base_min_vol_threshold.clone());

    if actual_min_base_vol < base_min_vol_threshold {
        return Err(MakerOrderBuildError::MinBaseVolTooLow {
            actual: actual_min_base_vol,
            threshold: base_min_vol_threshold,
        });
    }

    Ok(actual_min_base_vol)
}

fn validate_max_vol(
    min_base_amount: MmNumber,
    min_rel_amount: MmNumber,
    max_base_vol: MmNumber,
    min_base_vol: Option<MmNumber>,
    price: MmNumber,
) -> Result<(), MakerOrderBuildError> {
    if let Some(min) = min_base_vol {
        if max_base_vol < min {
            return Err(MakerOrderBuildError::MaxBaseVolBelowMinBaseVol { min, max: max_base_vol });
        }
    }

    if max_base_vol < min_base_amount {
        return Err(MakerOrderBuildError::MaxBaseVolTooLow {
            actual: max_base_vol,
            threshold: min_base_amount,
        });
    }

    let rel_vol = max_base_vol * price;
    if rel_vol < min_rel_amount {
        return Err(MakerOrderBuildError::RelVolTooLow {
            actual: rel_vol,
            threshold: min_rel_amount,
        });
    }

    Ok(())
}

impl<'a> MakerOrderBuilder<'a> {
    pub fn new(base_coin: &'a MmCoinEnum, rel_coin: &'a MmCoinEnum) -> MakerOrderBuilder<'a> {
        MakerOrderBuilder {
            base_coin,
            rel_coin,
            max_base_vol: 0.into(),
            min_base_vol: None,
            price: 0.into(),
            conf_settings: None,
            save_in_history: true,
        }
    }

    pub fn with_max_base_vol(mut self, vol: MmNumber) -> Self {
        self.max_base_vol = vol;
        self
    }

    pub fn with_min_base_vol(mut self, vol: Option<MmNumber>) -> Self {
        self.min_base_vol = vol;
        self
    }

    pub fn with_price(mut self, price: MmNumber) -> Self {
        self.price = price;
        self
    }

    pub fn with_conf_settings(mut self, conf_settings: OrderConfirmationsSettings) -> Self {
        self.conf_settings = Some(conf_settings);
        self
    }

    pub fn with_save_in_history(mut self, save_in_history: bool) -> Self {
        self.save_in_history = save_in_history;
        self
    }

    /// Build MakerOrder
    pub fn build(self) -> Result<MakerOrder, MakerOrderBuildError> {
        if self.base_coin.ticker() == self.rel_coin.ticker() {
            return Err(MakerOrderBuildError::BaseEqualRel);
        }

        if self.conf_settings.is_none() {
            return Err(MakerOrderBuildError::ConfSettingsNotSet);
        }

        let min_base_amount = self.base_coin.min_trading_vol();
        let min_rel_amount = self.rel_coin.min_trading_vol();

        validate_price(self.price.clone())?;

        let actual_min_base_vol = validate_and_get_min_vol(
            min_base_amount.clone(),
            min_rel_amount.clone(),
            self.min_base_vol.clone(),
            self.price.clone(),
        )?;

        validate_max_vol(
            min_base_amount,
            min_rel_amount,
            self.max_base_vol.clone(),
            self.min_base_vol.clone(),
            self.price.clone(),
        )?;

        Ok(MakerOrder {
            base: self.base_coin.ticker().to_owned(),
            rel: self.rel_coin.ticker().to_owned(),
            created_at: now_ms(),
            updated_at: Some(now_ms()),
            max_base_vol: self.max_base_vol,
            min_base_vol: actual_min_base_vol,
            price: self.price,
            matches: HashMap::new(),
            started_swaps: Vec::new(),
            uuid: new_uuid(),
            conf_settings: self.conf_settings,
            changes_history: None,
            save_in_history: self.save_in_history,
        })
    }

    #[cfg(test)]
    fn build_unchecked(self) -> MakerOrder {
        MakerOrder {
            base: self.base_coin.ticker().to_owned(),
            rel: self.rel_coin.ticker().to_owned(),
            created_at: now_ms(),
            updated_at: Some(now_ms()),
            max_base_vol: self.max_base_vol,
            min_base_vol: self.min_base_vol.unwrap_or(self.base_coin.min_trading_vol()),
            price: self.price,
            matches: HashMap::new(),
            started_swaps: Vec::new(),
            uuid: new_uuid(),
            conf_settings: self.conf_settings,
            changes_history: None,
            save_in_history: false,
        }
    }
}

#[allow(dead_code)]
fn zero_rat() -> BigRational { BigRational::zero() }

impl MakerOrder {
    fn available_amount(&self) -> MmNumber { &self.max_base_vol - &self.reserved_amount() }

    fn reserved_amount(&self) -> MmNumber {
        self.matches.iter().fold(
            MmNumber::from(BigRational::from_integer(0.into())),
            |reserved, (_, order_match)| &reserved + order_match.reserved.get_base_amount(),
        )
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

    fn apply_updated(&mut self, msg: &new_protocol::MakerOrderUpdated) {
        if let Some(new_price) = msg.new_price() {
            self.price = new_price;
        }

        if let Some(new_max_volume) = msg.new_max_volume() {
            self.max_base_vol = new_max_volume;
        }

        if let Some(new_min_volume) = msg.new_min_volume() {
            self.min_base_vol = new_min_volume;
        }

        if let Some(conf_settings) = msg.new_conf_settings() {
            self.conf_settings = conf_settings.into();
        }

        self.updated_at = Some(now_ms());
    }

    async fn check_balance(
        &self,
        ctx: &MmArc,
        base: &MmCoinEnum,
        rel: &MmCoinEnum,
    ) -> Result<(), MmError<CheckBalanceError>> {
        check_balance_for_maker_swap(
            ctx,
            base,
            rel,
            self.available_amount(),
            None,
            None,
            FeeApproxStage::OrderIssue,
        )
        .await
    }
}

impl From<TakerOrder> for MakerOrder {
    fn from(taker_order: TakerOrder) -> Self {
        match taker_order.request.action {
            TakerAction::Sell => MakerOrder {
                price: (taker_order.request.get_rel_amount() / taker_order.request.get_base_amount()),
                max_base_vol: taker_order.request.get_base_amount().clone(),
                min_base_vol: taker_order.min_volume,
                created_at: now_ms(),
                updated_at: Some(now_ms()),
                base: taker_order.request.base,
                rel: taker_order.request.rel,
                matches: HashMap::new(),
                started_swaps: Vec::new(),
                uuid: taker_order.request.uuid,
                conf_settings: taker_order.request.conf_settings,
                changes_history: None,
                save_in_history: taker_order.save_in_history,
            },
            // The "buy" taker order is recreated with reversed pair as Maker order is always considered as "sell"
            TakerAction::Buy => {
                let price = taker_order.request.get_base_amount() / taker_order.request.get_rel_amount();
                let min_base_vol = &taker_order.min_volume / &price;
                MakerOrder {
                    price,
                    max_base_vol: taker_order.request.get_rel_amount().clone(),
                    min_base_vol,
                    created_at: now_ms(),
                    updated_at: Some(now_ms()),
                    base: taker_order.request.rel,
                    rel: taker_order.request.base,
                    matches: HashMap::new(),
                    started_swaps: Vec::new(),
                    uuid: taker_order.request.uuid,
                    conf_settings: taker_order.request.conf_settings.map(|s| s.reversed()),
                    changes_history: None,
                    save_in_history: taker_order.save_in_history,
                }
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

impl From<TakerConnect> for new_protocol::OrdermatchMessage {
    fn from(taker_connect: TakerConnect) -> Self {
        new_protocol::OrdermatchMessage::TakerConnect(new_protocol::TakerConnect {
            taker_order_uuid: taker_connect.taker_order_uuid.into(),
            maker_order_uuid: taker_connect.maker_order_uuid.into(),
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

impl From<MakerReserved> for new_protocol::OrdermatchMessage {
    fn from(maker_reserved: MakerReserved) -> Self {
        new_protocol::OrdermatchMessage::MakerReserved(new_protocol::MakerReserved {
            base_amount: maker_reserved.get_base_amount().to_ratio(),
            rel_amount: maker_reserved.get_rel_amount().to_ratio(),
            base: maker_reserved.base,
            rel: maker_reserved.rel,
            taker_order_uuid: maker_reserved.taker_order_uuid.into(),
            maker_order_uuid: maker_reserved.maker_order_uuid.into(),
            conf_settings: maker_reserved.conf_settings.unwrap(),
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

impl From<MakerConnected> for new_protocol::OrdermatchMessage {
    fn from(maker_connected: MakerConnected) -> Self {
        new_protocol::OrdermatchMessage::MakerConnected(new_protocol::MakerConnected {
            taker_order_uuid: maker_connected.taker_order_uuid.into(),
            maker_order_uuid: maker_connected.maker_order_uuid.into(),
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
        for (alb_pair, root) in state.trie_roots.iter() {
            if *root == H64::default() && *root == hashed_null_node::<Layout>() {
                continue;
            }
            topics.insert(orderbook_topic_from_ordered_pair(alb_pair));
            trie_roots.insert(alb_pair.clone(), *root);
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
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct OrderedByPriceOrder {
    price: MmNumber,
    uuid: Uuid,
}

#[derive(Clone, Debug, PartialEq)]
enum OrderbookRequestingState {
    /// The orderbook was requested from relays.
    #[allow(dead_code)]
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

    #[allow(dead_code)]
    fn remove_key(&mut self, key: &H64) { self.inner.remove(key); }

    #[allow(dead_code)]
    fn contains_key(&self, key: &H64) -> bool { self.inner.contains_key(key) }

    fn get(&self, key: &H64) -> Option<&TrieDiff<Key, Value>> { self.inner.get(key) }
}

type TrieOrderHistory = TrieDiffHistory<Uuid, OrderbookItem>;

#[derive(Default)]
struct OrderbookPubkeyState {
    /// Timestamp of the latest keep alive message received
    last_keep_alive: u64,
    /// The map storing historical data about specific pair subtrie changes
    /// Used to get diffs of orders of pair between specific root hashes
    order_pairs_trie_state_history: HashMap<AlbOrderedOrderbookPair, TrieOrderHistory>,
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
            let state = OrderbookPubkeyState {
                last_keep_alive: now_ms() / 1000,
                ..OrderbookPubkeyState::default()
            };
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

fn pair_history_mut<'a>(
    state: &'a mut HashMap<AlbOrderedOrderbookPair, TrieOrderHistory>,
    pair: &str,
) -> &'a mut TrieOrderHistory {
    match state.raw_entry_mut().from_key(pair) {
        RawEntryMut::Occupied(e) => e.into_mut(),
        RawEntryMut::Vacant(e) => e.insert(pair.to_owned(), Default::default()).1,
    }
}

#[allow(dead_code)]
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

/// `parity_util_mem::malloc_size` crushes for some reason on wasm32
#[cfg(target_arch = "wasm32")]
fn collect_orderbook_metrics(_ctx: &MmArc, _orderbook: &Orderbook) {}

#[cfg(not(target_arch = "wasm32"))]
fn collect_orderbook_metrics(ctx: &MmArc, orderbook: &Orderbook) {
    use parity_util_mem::malloc_size;

    fn history_committed_changes(history: &HashMap<AlbOrderedOrderbookPair, TrieOrderHistory>) -> i64 {
        let total = history
            .iter()
            .fold(0usize, |total, (_alb_pair, history)| total + history.inner.len());
        total as i64
    }

    let memory_db_size = malloc_size(&orderbook.memory_db);
    mm_gauge!(ctx.metrics, "orderbook.len", orderbook.order_set.len() as i64);
    mm_gauge!(ctx.metrics, "orderbook.memory_db", memory_db_size as i64);
    // mm_gauge!(ctx.metrics, "inactive_orders.len", inactive.len() as i64);

    // TODO remove metrics below after testing
    for (pubkey, pubkey_state) in orderbook.pubkeys_state.iter() {
        mm_gauge!(ctx.metrics, "orders_uuids", pubkey_state.orders_uuids.len() as i64, "pubkey" => pubkey.clone());
        mm_gauge!(ctx.metrics, "history.commited_changes", history_committed_changes(&pubkey_state.order_pairs_trie_state_history), "pubkey" => pubkey.clone());
    }
}

#[derive(Default)]
struct Orderbook {
    /// A map from (base, rel).
    ordered: HashMap<(String, String), BTreeSet<OrderedByPriceOrder>>,
    /// A map from base ticker to the set of another tickers to track the existing pairs
    pairs_existing_for_base: HashMap<String, HashSet<String>>,
    /// A map from rel ticker to the set of another tickers to track the existing pairs
    pairs_existing_for_rel: HashMap<String, HashSet<String>>,
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

    fn find_order_by_uuid(&self, uuid: &Uuid) -> Option<OrderbookItem> { self.order_set.get(uuid).cloned() }

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
                log::error!("Error getting {} trie with root {:?}", e, prev_root);
                return;
            },
        };
        let order_bytes = rmp_serde::to_vec(&order).expect("Serialization should never fail");
        if let Err(e) = pair_trie.insert(order.uuid.as_bytes(), &order_bytes) {
            log::error!(
                "Error {} on insertion to trie. Key {}, value {:?}",
                e,
                order.uuid,
                order_bytes
            );
            return;
        };
        drop(pair_trie);

        if prev_root != H64::default() {
            let history = pair_history_mut(&mut pubkey_state.order_pairs_trie_state_history, &alb_ordered);
            history.insert_new_diff(prev_root, TrieDiff {
                delta: vec![(order.uuid, Some(order.clone()))],
                next_root: *pair_root,
            });
        }
    }

    fn insert_or_update_order(&mut self, order: OrderbookItem) {
        log::debug!("Inserting order {:?}", order);
        let zero = BigRational::from_integer(0.into());
        if order.max_volume <= zero || order.price <= zero || order.min_volume < zero {
            self.remove_order_trie_update(order.uuid);
            return;
        } // else insert the order

        let base_rel = (order.base.clone(), order.rel.clone());

        let ordered = self.ordered.entry(base_rel.clone()).or_insert_with(BTreeSet::new);

        // have to clone to drop immutable ordered borrow
        let existing = ordered
            .iter()
            .find(|maybe_existing| maybe_existing.uuid == order.uuid)
            .cloned();

        if let Some(exists) = existing {
            ordered.remove(&exists);
        }
        ordered.insert(OrderedByPriceOrder {
            uuid: order.uuid,
            price: order.price.clone().into(),
        });

        self.pairs_existing_for_base
            .entry(order.base.clone())
            .or_insert_with(HashSet::new)
            .insert(order.rel.clone());

        self.pairs_existing_for_rel
            .entry(order.rel.clone())
            .or_insert_with(HashSet::new)
            .insert(order.base.clone());

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

        let to_remove = &(uuid, alb_ordered.clone());
        pubkey_state.orders_uuids.remove(to_remove);

        *pair_state = match delta_trie_root::<Layout, _, _, _, _, _>(&mut self.memory_db, *pair_state, vec![(
            *order.uuid.as_bytes(),
            None::<Vec<u8>>,
        )]) {
            Ok(root) => root,
            Err(_) => {
                log::error!("Failed to get existing trie with root {:?}", pair_state);
                return Some(order);
            },
        };

        let history = pair_history_mut(&mut pubkey_state.order_pairs_trie_state_history, &alb_ordered);
        history.insert_new_diff(old_state, TrieDiff {
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
        i_am_relay: bool,
    ) -> Option<OrdermatchRequest> {
        let pubkey_state = pubkey_state_mut(&mut self.pubkeys_state, from_pubkey);

        let mut trie_roots_to_request = HashMap::new();
        for (alb_pair, trie_root) in message.trie_roots {
            let subscribed = self
                .topics_subscribed_to
                .contains_key(&orderbook_topic_from_ordered_pair(&alb_pair));
            if !subscribed && !i_am_relay {
                continue;
            }

            if trie_root == H64::default() || trie_root == hashed_null_node::<Layout>() {
                log::warn!(
                    "Received zero or hashed_null_node pair {} trie root from pub {}",
                    alb_pair,
                    from_pubkey
                );

                continue;
            }
            let actual_trie_root = order_pair_root_mut(&mut pubkey_state.trie_roots, &alb_pair);
            if *actual_trie_root != trie_root {
                trie_roots_to_request.insert(alb_pair, trie_root);
            }
        }

        if trie_roots_to_request.is_empty() {
            pubkey_state.last_keep_alive = message.timestamp;
            return None;
        }

        Some(OrdermatchRequest::SyncPubkeyOrderbookState {
            pubkey: from_pubkey.to_owned(),
            trie_roots: trie_roots_to_request,
        })
    }

    fn orderbook_item_with_proof(&self, order: OrderbookItem) -> OrderbookItemWithProof {
        OrderbookItemWithProof {
            order,
            last_message_payload: vec![],
            proof: vec![],
        }
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
        let taker_coin = match lp_coinfind(&ctx, &maker_match.reserved.rel).await {
            Ok(Some(c)) => c,
            Ok(None) => {
                log::error!("Coin {} is not found/enabled", maker_match.reserved.rel);
                return;
            },
            Err(e) => {
                log::error!("!lp_coinfind({}): {}", maker_match.reserved.rel, e);
                return;
            },
        };

        let maker_coin = match lp_coinfind(&ctx, &maker_match.reserved.base).await {
            Ok(Some(c)) => c,
            Ok(None) => {
                log::error!("Coin {} is not found/enabled", maker_match.reserved.base);
                return;
            },
            Err(e) => {
                log::error!("!lp_coinfind({}): {}", maker_match.reserved.base, e);
                return;
            },
        };
        let alice = bits256::from(maker_match.request.sender_pubkey.0);
        let maker_amount = maker_match.reserved.get_base_amount().to_decimal();
        let taker_amount = maker_match.reserved.get_rel_amount().to_decimal();
        let privkey = &ctx.secp256k1_key_pair().private().secret;
        let my_persistent_pub = compressed_pub_key_from_priv_raw(&privkey[..], ChecksumType::DSHA256).unwrap();
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
        log_tag!(
            ctx,
            "";
            fmt = "Entering the maker_swap_loop {}/{} with uuid: {}",
            maker_coin.ticker(),
            taker_coin.ticker(),
            uuid
        );

        let now = now_ms() / 1000;
        if let Err(e) = insert_new_swap_to_db(
            &ctx,
            maker_coin.ticker(),
            taker_coin.ticker(),
            &uuid.to_string(),
            &now.to_string(),
        ) {
            error!("Error {} on new swap insertion", e);
        }
        let maker_swap = MakerSwap::new(
            ctx.clone(),
            alice,
            maker_amount,
            taker_amount,
            my_persistent_pub,
            uuid,
            Some(maker_order.uuid),
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
        let maker = bits256::from(taker_match.reserved.sender_pubkey.0);
        let taker_coin = match lp_coinfind(&ctx, &taker_match.reserved.rel).await {
            Ok(Some(c)) => c,
            Ok(None) => {
                log::error!("Coin {} is not found/enabled", taker_match.reserved.rel);
                return;
            },
            Err(e) => {
                log::error!("!lp_coinfind({}): {}", taker_match.reserved.rel, e);
                return;
            },
        };

        let maker_coin = match lp_coinfind(&ctx, &taker_match.reserved.base).await {
            Ok(Some(c)) => c,
            Ok(None) => {
                log::error!("Coin {} is not found/enabled", taker_match.reserved.base);
                return;
            },
            Err(e) => {
                log::error!("!lp_coinfind({}): {}", taker_match.reserved.base, e);
                return;
            },
        };

        let privkey = &ctx.secp256k1_key_pair().private().secret;
        let my_persistent_pub = compressed_pub_key_from_priv_raw(&privkey[..], ChecksumType::DSHA256).unwrap();
        let maker_amount = taker_match.reserved.get_base_amount().clone();
        let taker_amount = taker_match.reserved.get_rel_amount().clone();
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
        log_tag!(
            ctx,
            "";
            fmt = "Entering the taker_swap_loop {}/{} with uuid: {}",
            maker_coin.ticker(),
            taker_coin.ticker(),
            uuid
        );
        let now = now_ms() / 1000;
        if let Err(e) = insert_new_swap_to_db(
            &ctx,
            taker_coin.ticker(),
            maker_coin.ticker(),
            &uuid.to_string(),
            &now.to_string(),
        ) {
            error!("Error {} on new swap insertion", e);
        }
        let taker_swap = TakerSwap::new(
            ctx.clone(),
            maker,
            maker_amount,
            taker_amount,
            my_persistent_pub,
            uuid,
            Some(uuid),
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
    let maker_order_timeout = ctx.conf["maker_order_timeout"].as_u64().unwrap_or(MAKER_ORDER_TIMEOUT);
    loop {
        if ctx.is_stopping() {
            break;
        }
        let ordermatch_ctx = OrdermatchContext::from_ctx(&ctx).unwrap();
        {
            let mut my_taker_orders = ordermatch_ctx.my_taker_orders.lock().await;
            let mut my_maker_orders = ordermatch_ctx.my_maker_orders.lock().await;
            let _my_cancelled_orders = ordermatch_ctx.my_cancelled_orders.lock().await;
            // transform the timed out and unmatched GTC taker orders to maker
            *my_taker_orders = my_taker_orders
                .drain()
                .filter_map(|(uuid, order)| {
                    if order.created_at + order.timeout * 1000 < now_ms() {
                        if order.matches.is_empty() && order.order_type == OrderType::GoodTillCancelled {
                            delete_my_taker_order(&ctx, &order, TakerOrderCancellationReason::ToMaker);
                            let maker_order: MakerOrder = order.into();
                            my_maker_orders.insert(uuid, maker_order.clone());
                            save_my_maker_order(&ctx, &maker_order);
                            if maker_order.save_in_history {
                                if let Err(e) = update_was_taker_in_db(&ctx, uuid) {
                                    error!("Error {} on order update", e);
                                }
                            }
                            spawn({
                                let ctx = ctx.clone();
                                async move {
                                    maker_order_created_p2p_notify(ctx, &maker_order).await;
                                }
                            });
                        } else {
                            delete_my_taker_order(&ctx, &order, TakerOrderCancellationReason::TimedOut);
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
                            if order.matches.is_empty() {
                                delete_my_maker_order(&ctx, &order, MakerOrderCancellationReason::InsufficientBalance);
                            } else {
                                delete_my_maker_order(&ctx, &order, MakerOrderCancellationReason::Fulfilled);
                            }
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
                let to_retain = pubkey == &my_pubsecp || state.last_keep_alive + maker_order_timeout > now_ms() / 1000;
                if !to_retain {
                    for (uuid, _) in &state.orders_uuids {
                        uuids_to_remove.push(*uuid);
                    }
                    for root in state.trie_roots.values() {
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

            collect_orderbook_metrics(&ctx, &orderbook);
        }

        {
            let mut my_maker_orders = ordermatch_ctx.my_maker_orders.lock().await;
            let mut to_cancel = vec![];

            for (uuid, order) in my_maker_orders.iter() {
                if !ordermatch_ctx.orderbook.lock().await.order_set.contains_key(uuid) {
                    if let Ok(Some((base, rel))) = find_pair(&ctx, &order.base, &order.rel).await {
                        if let Err(e) = order.check_balance(&ctx, &base, &rel).await {
                            log::info!("Error {} on balance check to kickstart order {}, cancelling", e, uuid);
                            to_cancel.push(*uuid);
                            continue;
                        }

                        let topic = orderbook_topic_from_base_rel(&order.base, &order.rel);
                        if !ordermatch_ctx.orderbook.lock().await.is_subscribed_to(&topic) {
                            let request_orderbook = false;
                            if let Err(e) =
                                subscribe_to_orderbook_topic(&ctx, &order.base, &order.rel, request_orderbook).await
                            {
                                log::error!("Error {} on subscribing to orderbook topic {}", e, topic);
                            }
                        }
                        maker_order_created_p2p_notify(ctx.clone(), order).await;
                    }
                }
            }

            for uuid in to_cancel {
                if let Some(order) = my_maker_orders.remove(&uuid) {
                    delete_my_maker_order(&ctx, &order, MakerOrderCancellationReason::InsufficientBalance);
                }
            }
        }

        Timer::sleep(0.777).await;
    }
}

async fn process_maker_reserved(ctx: MmArc, from_pubkey: H256Json, reserved_msg: MakerReserved) {
    let ordermatch_ctx = OrdermatchContext::from_ctx(&ctx).unwrap();
    let our_public_id = ctx.public_id().unwrap();
    if our_public_id.bytes == from_pubkey.0 {
        log::warn!("Skip maker reserved from our pubkey");
        return;
    }

    let mut my_taker_orders = ordermatch_ctx.my_taker_orders.lock().await;
    let my_order = match my_taker_orders.entry(reserved_msg.taker_order_uuid) {
        Entry::Vacant(_) => return,
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
    let ordermatch_ctx = OrdermatchContext::from_ctx(&ctx).unwrap();
    let our_public_id = ctx.public_id().unwrap();
    if our_public_id.bytes == from_pubkey.0 {
        log::warn!("Skip maker connected from our pubkey");
        return;
    }

    let mut my_taker_orders = ordermatch_ctx.my_taker_orders.lock().await;
    let my_order_entry = match my_taker_orders.entry(connected.taker_order_uuid) {
        Entry::Occupied(e) => e,
        Entry::Vacant(_) => return,
    };
    let order_match = match my_order_entry.get().matches.get(&connected.maker_order_uuid) {
        Some(o) => o,
        None => {
            log::warn!(
                "Our node doesn't have the match with uuid {}",
                connected.maker_order_uuid
            );
            return;
        },
    };

    if order_match.reserved.sender_pubkey != from_pubkey {
        log::error!("Connected message sender pubkey != reserved message sender pubkey");
        return;
    }
    // alice
    lp_connected_alice(ctx.clone(), my_order_entry.get().request.clone(), order_match.clone());
    // remove the matched order immediately
    delete_my_taker_order(&ctx, &my_order_entry.get(), TakerOrderCancellationReason::Fulfilled);
    my_order_entry.remove();
}

async fn process_taker_request(ctx: MmArc, from_pubkey: H256Json, taker_request: TakerRequest) {
    let our_public_id: H256Json = ctx.public_id().unwrap().bytes.into();
    if our_public_id == from_pubkey {
        log::warn!("Skip the request originating from our pubkey");
        return;
    }
    log::debug!("Processing request {:?}", taker_request);

    if !taker_request.can_match_with_maker_pubkey(&our_public_id) {
        return;
    }

    let ordermatch_ctx = OrdermatchContext::from_ctx(&ctx).unwrap();
    let mut my_orders = ordermatch_ctx.my_maker_orders.lock().await;
    let filtered = my_orders
        .iter_mut()
        .filter(|(uuid, _)| taker_request.can_match_with_uuid(uuid));

    for (uuid, order) in filtered {
        if let OrderMatchResult::Matched((base_amount, rel_amount)) = order.match_with_request(&taker_request) {
            let base_coin = match lp_coinfind(&ctx, &order.base).await {
                Ok(Some(c)) => c,
                _ => return, // attempt to match with deactivated coin
            };
            let rel_coin = match lp_coinfind(&ctx, &order.rel).await {
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
                log::debug!("Request matched sending reserved {:?}", reserved);
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
    let ordermatch_ctx = OrdermatchContext::from_ctx(&ctx).unwrap();
    let our_public_id = ctx.public_id().unwrap();
    if our_public_id.bytes == sender_pubkey.0 {
        log::warn!("Skip taker connect from our pubkey");
        return;
    }

    let mut maker_orders = ordermatch_ctx.my_maker_orders.lock().await;
    let my_order = match maker_orders.get_mut(&connect_msg.maker_order_uuid) {
        Some(o) => o,
        None => return,
    };
    let order_match = match my_order.matches.get_mut(&connect_msg.taker_order_uuid) {
        Some(o) => o,
        None => {
            log::warn!(
                "Our node doesn't have the match with uuid {}",
                connect_msg.taker_order_uuid
            );
            return;
        },
    };
    if order_match.request.sender_pubkey != sender_pubkey {
        log::warn!("Connect message sender pubkey != request message sender pubkey");
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
            let mut updated_msg = new_protocol::MakerOrderUpdated::new(my_order.uuid);
            updated_msg.with_new_max_volume(my_order.available_amount().into());
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
    timeout: Option<u64>,
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
    min_volume: Option<MmNumber>,
    #[serde(default = "get_true")]
    save_in_history: bool,
}

pub async fn buy(ctx: MmArc, req: Json) -> Result<Response<Vec<u8>>, String> {
    let input: AutoBuyInput = try_s!(json::from_value(req));
    if input.base == input.rel {
        return ERR!("Base and rel must be different coins");
    }
    let rel_coin = try_s!(lp_coinfind(&ctx, &input.rel).await);
    let rel_coin = try_s!(rel_coin.ok_or("Rel coin is not found or inactive"));
    let base_coin = try_s!(lp_coinfind(&ctx, &input.base).await);
    let base_coin: MmCoinEnum = try_s!(base_coin.ok_or("Base coin is not found or inactive"));
    if base_coin.wallet_only(&ctx) {
        return ERR!("Base coin {} is wallet only", input.base);
    }
    if rel_coin.wallet_only(&ctx) {
        return ERR!("Rel coin {} is wallet only", input.rel);
    }
    let my_amount = &input.volume * &input.price;
    try_s!(
        check_balance_for_taker_swap(
            &ctx,
            &rel_coin,
            &base_coin,
            my_amount,
            None,
            None,
            FeeApproxStage::OrderIssue
        )
        .await
    );
    let res = try_s!(lp_auto_buy(&ctx, &base_coin, &rel_coin, input).await).into_bytes();
    Ok(try_s!(Response::builder().body(res)))
}

pub async fn sell(ctx: MmArc, req: Json) -> Result<Response<Vec<u8>>, String> {
    let input: AutoBuyInput = try_s!(json::from_value(req));
    if input.base == input.rel {
        return ERR!("Base and rel must be different coins");
    }
    let base_coin = try_s!(lp_coinfind(&ctx, &input.base).await);
    let base_coin = try_s!(base_coin.ok_or("Base coin is not found or inactive"));
    let rel_coin = try_s!(lp_coinfind(&ctx, &input.rel).await);
    let rel_coin = try_s!(rel_coin.ok_or("Rel coin is not found or inactive"));
    if base_coin.wallet_only(&ctx) {
        return ERR!("Base coin {} is wallet only", input.base);
    }
    if rel_coin.wallet_only(&ctx) {
        return ERR!("Rel coin {} is wallet only", input.rel);
    }
    try_s!(
        check_balance_for_taker_swap(
            &ctx,
            &base_coin,
            &rel_coin,
            input.volume.clone(),
            None,
            None,
            FeeApproxStage::OrderIssue
        )
        .await
    );
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

impl<'a> From<&'a TakerRequest> for TakerRequestForRpc<'a> {
    fn from(request: &'a TakerRequest) -> TakerRequestForRpc<'a> {
        TakerRequestForRpc {
            base: &request.base,
            rel: &request.rel,
            base_amount: request.base_amount.to_decimal(),
            base_amount_rat: request.base_amount.to_ratio(),
            rel_amount: request.rel_amount.to_decimal(),
            rel_amount_rat: request.rel_amount.to_ratio(),
            action: &request.action,
            uuid: &request.uuid,
            method: "request".to_string(),
            sender_pubkey: &request.sender_pubkey,
            dest_pub_key: &request.dest_pub_key,
            match_by: &request.match_by,
            conf_settings: &request.conf_settings,
        }
    }
}

construct_detailed!(DetailedMinVolume, min_volume);

#[derive(Serialize)]
struct LpautobuyResult<'a> {
    #[serde(flatten)]
    request: TakerRequestForRpc<'a>,
    order_type: OrderType,
    #[serde(flatten)]
    min_volume: DetailedMinVolume,
}

#[derive(Clone, Debug, Serialize)]
pub struct TakerRequestForRpc<'a> {
    base: &'a str,
    rel: &'a str,
    base_amount: BigDecimal,
    base_amount_rat: BigRational,
    rel_amount: BigDecimal,
    rel_amount_rat: BigRational,
    action: &'a TakerAction,
    uuid: &'a Uuid,
    method: String,
    sender_pubkey: &'a H256Json,
    dest_pub_key: &'a H256Json,
    match_by: &'a MatchBy,
    conf_settings: &'a Option<OrderConfirmationsSettings>,
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
    let mut order_builder = TakerOrderBuilder::new(base_coin, rel_coin)
        .with_base_amount(input.volume)
        .with_rel_amount(rel_volume)
        .with_action(action)
        .with_match_by(input.match_by)
        .with_min_volume(input.min_volume)
        .with_order_type(input.order_type)
        .with_conf_settings(conf_settings)
        .with_sender_pubkey(H256Json::from(our_public_id.bytes))
        .with_save_in_history(input.save_in_history);
    if let Some(timeout) = input.timeout {
        order_builder = order_builder.with_timeout(timeout);
    }
    let order = try_s!(order_builder.build());
    broadcast_ordermatch_message(
        &ctx,
        vec![orderbook_topic_from_base_rel(&input.base, &input.rel)],
        order.request.clone().into(),
    );

    let result = json!({ "result": LpautobuyResult {
        request: (&order.request).into(),
        order_type: order.order_type,
        min_volume: order.min_volume.clone().into(),
    } });
    save_my_new_taker_order(ctx, &order);
    my_taker_orders.insert(order.request.uuid, order);
    Ok(result.to_string())
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

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
struct OrderbookItemWithProof {
    /// Orderbook item
    order: OrderbookItem,
    /// Last pubkey message payload that contains most recent pair trie root
    last_message_payload: Vec<u8>,
    /// Proof confirming that orderbook item is in the pair trie
    proof: TrieProof,
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

    fn as_rpc_entry_ask(&self, address: String, is_mine: bool) -> RpcOrderbookEntry {
        let price_mm = MmNumber::from(self.price.clone());
        let max_vol_mm = MmNumber::from(self.max_volume.clone());
        let min_vol_mm = MmNumber::from(self.min_volume.clone());

        let base_max_volume = max_vol_mm.clone().into();
        let base_min_volume = min_vol_mm.clone().into();
        let rel_max_volume = (&max_vol_mm * &price_mm).into();
        let rel_min_volume = (&min_vol_mm * &price_mm).into();

        RpcOrderbookEntry {
            coin: self.base.clone(),
            address,
            price: price_mm.to_decimal(),
            price_rat: price_mm.to_ratio(),
            price_fraction: price_mm.to_fraction(),
            max_volume: max_vol_mm.to_decimal(),
            max_volume_rat: max_vol_mm.to_ratio(),
            max_volume_fraction: max_vol_mm.to_fraction(),
            min_volume: min_vol_mm.to_decimal(),
            min_volume_rat: min_vol_mm.to_ratio(),
            min_volume_fraction: min_vol_mm.to_fraction(),
            pubkey: self.pubkey.clone(),
            age: (now_ms() as i64 / 1000),
            zcredits: 0,
            uuid: self.uuid,
            is_mine,
            base_max_volume,
            base_min_volume,
            rel_max_volume,
            rel_min_volume,
        }
    }

    fn as_rpc_entry_bid(&self, address: String, is_mine: bool) -> RpcOrderbookEntry {
        let price_mm = MmNumber::from(1i32) / self.price.clone().into();
        let max_vol_mm = MmNumber::from(self.max_volume.clone());
        let min_vol_mm = MmNumber::from(self.min_volume.clone());

        let base_max_volume = (&max_vol_mm / &price_mm).into();
        let base_min_volume = (&min_vol_mm / &price_mm).into();
        let rel_max_volume = max_vol_mm.clone().into();
        let rel_min_volume = min_vol_mm.clone().into();

        RpcOrderbookEntry {
            coin: self.base.clone(),
            address,
            price: price_mm.to_decimal(),
            price_rat: price_mm.to_ratio(),
            price_fraction: price_mm.to_fraction(),
            max_volume: max_vol_mm.to_decimal(),
            max_volume_rat: max_vol_mm.to_ratio(),
            max_volume_fraction: max_vol_mm.to_fraction(),
            min_volume: min_vol_mm.to_decimal(),
            min_volume_rat: min_vol_mm.to_ratio(),
            min_volume_fraction: min_vol_mm.to_fraction(),
            pubkey: self.pubkey.clone(),
            age: (now_ms() as i64 / 1000),
            zcredits: 0,
            uuid: self.uuid,
            is_mine,
            base_max_volume,
            base_min_volume,
            rel_max_volume,
            rel_min_volume,
        }
    }
}

fn get_true() -> bool { true }

#[derive(Deserialize)]
struct SetPriceReq {
    base: String,
    rel: String,
    price: MmNumber,
    #[serde(default)]
    max: bool,
    #[serde(default)]
    volume: MmNumber,
    min_volume: Option<MmNumber>,
    #[serde(default = "get_true")]
    cancel_previous: bool,
    base_confs: Option<u64>,
    base_nota: Option<bool>,
    rel_confs: Option<u64>,
    rel_nota: Option<bool>,
    #[serde(default = "get_true")]
    save_in_history: bool,
}

#[derive(Deserialize)]
struct MakerOrderUpdateReq {
    uuid: Uuid,
    new_price: Option<MmNumber>,
    max: Option<bool>,
    volume_delta: Option<MmNumber>,
    min_volume: Option<MmNumber>,
    base_confs: Option<u64>,
    base_nota: Option<bool>,
    rel_confs: Option<u64>,
    rel_nota: Option<bool>,
}

#[derive(Debug, Serialize)]
pub struct MakerReservedForRpc<'a> {
    base: &'a str,
    rel: &'a str,
    base_amount: BigDecimal,
    base_amount_rat: BigRational,
    rel_amount: BigDecimal,
    rel_amount_rat: BigRational,
    taker_order_uuid: &'a Uuid,
    maker_order_uuid: &'a Uuid,
    sender_pubkey: &'a H256Json,
    dest_pub_key: &'a H256Json,
    conf_settings: &'a Option<OrderConfirmationsSettings>,
    method: String,
}

#[derive(Debug, Serialize)]
pub struct TakerConnectForRpc<'a> {
    taker_order_uuid: &'a Uuid,
    maker_order_uuid: &'a Uuid,
    method: String,
    sender_pubkey: &'a H256Json,
    dest_pub_key: &'a H256Json,
}

impl<'a> From<&'a TakerConnect> for TakerConnectForRpc<'a> {
    fn from(connect: &'a TakerConnect) -> TakerConnectForRpc {
        TakerConnectForRpc {
            taker_order_uuid: &connect.taker_order_uuid,
            maker_order_uuid: &connect.maker_order_uuid,
            method: "connect".to_string(),
            sender_pubkey: &connect.sender_pubkey,
            dest_pub_key: &connect.dest_pub_key,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct MakerConnectedForRpc<'a> {
    taker_order_uuid: &'a Uuid,
    maker_order_uuid: &'a Uuid,
    method: String,
    sender_pubkey: &'a H256Json,
    dest_pub_key: &'a H256Json,
}

impl<'a> From<&'a MakerConnected> for MakerConnectedForRpc<'a> {
    fn from(connected: &'a MakerConnected) -> MakerConnectedForRpc {
        MakerConnectedForRpc {
            taker_order_uuid: &connected.taker_order_uuid,
            maker_order_uuid: &connected.maker_order_uuid,
            method: "connected".to_string(),
            sender_pubkey: &connected.sender_pubkey,
            dest_pub_key: &connected.dest_pub_key,
        }
    }
}

impl<'a> From<&'a MakerReserved> for MakerReservedForRpc<'a> {
    fn from(reserved: &MakerReserved) -> MakerReservedForRpc {
        MakerReservedForRpc {
            base: &reserved.base,
            rel: &reserved.rel,
            base_amount: reserved.base_amount.to_decimal(),
            base_amount_rat: reserved.base_amount.to_ratio(),
            rel_amount: reserved.rel_amount.to_decimal(),
            rel_amount_rat: reserved.rel_amount.to_ratio(),
            taker_order_uuid: &reserved.taker_order_uuid,
            maker_order_uuid: &reserved.maker_order_uuid,
            sender_pubkey: &reserved.sender_pubkey,
            dest_pub_key: &reserved.dest_pub_key,
            conf_settings: &reserved.conf_settings,
            method: "reserved".to_string(),
        }
    }
}

#[derive(Debug, Serialize)]
struct MakerMatchForRpc<'a> {
    request: TakerRequestForRpc<'a>,
    reserved: MakerReservedForRpc<'a>,
    connect: Option<TakerConnectForRpc<'a>>,
    connected: Option<MakerConnectedForRpc<'a>>,
    last_updated: u64,
}

impl<'a> From<&'a MakerMatch> for MakerMatchForRpc<'a> {
    fn from(maker_match: &'a MakerMatch) -> MakerMatchForRpc {
        MakerMatchForRpc {
            request: (&maker_match.request).into(),
            reserved: (&maker_match.reserved).into(),
            connect: maker_match.connect.as_ref().map(Into::into),
            connected: maker_match.connected.as_ref().map(Into::into),
            last_updated: maker_match.last_updated,
        }
    }
}

#[derive(Serialize)]
struct MakerOrderForRpc<'a> {
    base: &'a str,
    rel: &'a str,
    price: BigDecimal,
    price_rat: &'a MmNumber,
    max_base_vol: BigDecimal,
    max_base_vol_rat: &'a MmNumber,
    min_base_vol: BigDecimal,
    min_base_vol_rat: &'a MmNumber,
    created_at: u64,
    updated_at: Option<u64>,
    matches: HashMap<Uuid, MakerMatchForRpc<'a>>,
    started_swaps: &'a [Uuid],
    uuid: Uuid,
    conf_settings: &'a Option<OrderConfirmationsSettings>,
    #[serde(skip_serializing_if = "Option::is_none")]
    changes_history: &'a Option<Vec<HistoricalOrder>>,
}

impl<'a> From<&'a MakerOrder> for MakerOrderForRpc<'a> {
    fn from(order: &'a MakerOrder) -> MakerOrderForRpc<'a> {
        MakerOrderForRpc {
            base: &order.base,
            rel: &order.rel,
            price: order.price.to_decimal(),
            price_rat: &order.price,
            max_base_vol: order.max_base_vol.to_decimal(),
            max_base_vol_rat: &order.max_base_vol,
            min_base_vol: order.min_base_vol.to_decimal(),
            min_base_vol_rat: &order.min_base_vol,
            created_at: order.created_at,
            updated_at: order.updated_at,
            matches: order
                .matches
                .iter()
                .map(|(uuid, order_match)| (*uuid, order_match.into()))
                .collect(),
            started_swaps: &order.started_swaps,
            uuid: order.uuid,
            conf_settings: &order.conf_settings,
            changes_history: &order.changes_history,
        }
    }
}

/// Cancels the orders in case of error on different checks
/// https://github.com/KomodoPlatform/atomicDEX-API/issues/794
async fn cancel_orders_on_error<T, E>(ctx: &MmArc, req: &SetPriceReq, error: E) -> Result<T, E> {
    if req.cancel_previous {
        let ordermatch_ctx = OrdermatchContext::from_ctx(&ctx).unwrap();
        let mut my_orders = ordermatch_ctx.my_maker_orders.lock().await;

        let mut cancelled = vec![];
        // remove the previous orders if there're some to allow multiple setprice call per pair
        // it's common use case now as `autoprice` doesn't work with new ordermatching and
        // MM2 users request the coins price from aggregators by their own scripts issuing
        // repetitive setprice calls with new price
        *my_orders = my_orders
            .drain()
            .filter_map(|(uuid, order)| {
                let to_delete = order.base == req.base && order.rel == req.rel;
                if to_delete {
                    delete_my_maker_order(&ctx, &order, MakerOrderCancellationReason::Cancelled);
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
    Err(error)
}

async fn get_max_volume(ctx: &MmArc, my_coin: &MmCoinEnum, other_coin: &MmCoinEnum) -> Result<MmNumber, String> {
    let my_balance = try_s!(my_coin.my_spendable_balance().compat().await);
    // first check if `rel_coin` balance is sufficient
    let other_coin_trade_fee = try_s!(
        other_coin
            .get_receiver_trade_fee(FeeApproxStage::OrderIssue)
            .compat()
            .await
    );
    try_s!(check_other_coin_balance_for_swap(&ctx, &other_coin, None, other_coin_trade_fee).await);
    // calculate max maker volume
    // note the `calc_max_maker_vol` returns [`CheckBalanceError::NotSufficientBalance`] error if the balance of `base_coin` is not sufficient
    Ok(try_s!(
        calc_max_maker_vol(&ctx, &my_coin, &my_balance, FeeApproxStage::OrderIssue).await
    ))
}

pub async fn set_price(ctx: MmArc, req: Json) -> Result<Response<Vec<u8>>, String> {
    let req: SetPriceReq = try_s!(json::from_value(req));

    let base_coin: MmCoinEnum = match try_s!(lp_coinfind(&ctx, &req.base).await) {
        Some(coin) => coin,
        None => return ERR!("Base coin {} is not found", req.base),
    };

    let rel_coin: MmCoinEnum = match try_s!(lp_coinfind(&ctx, &req.rel).await) {
        Some(coin) => coin,
        None => return ERR!("Rel coin {} is not found", req.rel),
    };

    if base_coin.wallet_only(&ctx) {
        return ERR!("Base coin {} is wallet only", req.base);
    }
    if rel_coin.wallet_only(&ctx) {
        return ERR!("Rel coin {} is wallet only", req.rel);
    }

    let volume = if req.max {
        try_s!(
            get_max_volume(&ctx, &base_coin, &rel_coin)
                .or_else(|e| cancel_orders_on_error(&ctx, &req, e))
                .await
        )
    } else {
        try_s!(
            check_balance_for_maker_swap(
                &ctx,
                &base_coin,
                &rel_coin,
                req.volume.clone(),
                None,
                None,
                FeeApproxStage::OrderIssue
            )
            .or_else(|e| cancel_orders_on_error(&ctx, &req, e))
            .await
        );
        req.volume.clone()
    };

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
                let to_delete = order.base == req.base && order.rel == req.rel;
                if to_delete {
                    delete_my_maker_order(&ctx, &order, MakerOrderCancellationReason::Cancelled);
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

    let conf_settings = OrderConfirmationsSettings {
        base_confs: req.base_confs.unwrap_or_else(|| base_coin.required_confirmations()),
        base_nota: req.base_nota.unwrap_or_else(|| base_coin.requires_notarization()),
        rel_confs: req.rel_confs.unwrap_or_else(|| rel_coin.required_confirmations()),
        rel_nota: req.rel_nota.unwrap_or_else(|| rel_coin.requires_notarization()),
    };
    let builder = MakerOrderBuilder::new(&base_coin, &rel_coin)
        .with_max_base_vol(volume)
        .with_min_base_vol(req.min_volume)
        .with_price(req.price)
        .with_conf_settings(conf_settings)
        .with_save_in_history(req.save_in_history);

    let new_order = try_s!(builder.build());

    let request_orderbook = false;
    try_s!(subscribe_to_orderbook_topic(&ctx, &new_order.base, &new_order.rel, request_orderbook).await);
    save_my_new_maker_order(&ctx, &new_order);
    maker_order_created_p2p_notify(ctx.clone(), &new_order).await;
    let rpc_result = MakerOrderForRpc::from(&new_order);
    let res = try_s!(json::to_vec(&json!({ "result": rpc_result })));
    my_orders.insert(new_order.uuid, new_order);
    Ok(try_s!(Response::builder().body(res)))
}

pub async fn update_maker_order(ctx: MmArc, req: Json) -> Result<Response<Vec<u8>>, String> {
    let req: MakerOrderUpdateReq = try_s!(json::from_value(req));

    let ordermatch_ctx = try_s!(OrdermatchContext::from_ctx(&ctx));
    let my_maker_orders = ordermatch_ctx.my_maker_orders.lock().await;

    let (base_coin, rel_coin, original_price, original_volume, updated_conf_settings, matches, reserved_amount) =
        match my_maker_orders.get(&req.uuid) {
            Some(order) => {
                if order.has_ongoing_matches() {
                    return ERR!("Can't update an order that has ongoing matches");
                }
                let base = order.base.as_str();
                let base_coin: MmCoinEnum = match try_s!(lp_coinfind(&ctx, base).await) {
                    Some(coin) => coin,
                    None => return ERR!("Base coin {} has been removed from config", base),
                };

                let rel = order.rel.as_str();
                let rel_coin: MmCoinEnum = match try_s!(lp_coinfind(&ctx, rel).await) {
                    Some(coin) => coin,
                    None => return ERR!("Rel coin {} has been removed from config", rel),
                };

                let original_conf_settings = order.conf_settings.unwrap();

                let updated_conf_settings = OrderConfirmationsSettings {
                    base_confs: req.base_confs.unwrap_or(original_conf_settings.base_confs),
                    base_nota: req.base_nota.unwrap_or(original_conf_settings.base_nota),
                    rel_confs: req.rel_confs.unwrap_or(original_conf_settings.rel_confs),
                    rel_nota: req.rel_nota.unwrap_or(original_conf_settings.rel_nota),
                };

                (
                    base_coin,
                    rel_coin,
                    order.price.clone(),
                    order.max_base_vol.clone(),
                    updated_conf_settings,
                    order.matches.clone(),
                    order.reserved_amount(),
                )
            },
            None => return ERR!("There is no order with UUID {}", req.uuid),
        };

    drop(my_maker_orders);

    let mut update_msg = new_protocol::MakerOrderUpdated::new(req.uuid);
    update_msg.with_new_conf_settings(updated_conf_settings);

    // Validate and Add new_price to update_msg if new_price is found in the request
    let new_price = match req.new_price {
        Some(new_price) => {
            try_s!(validate_price(new_price.clone()));
            update_msg.with_new_price(new_price.clone().into());
            new_price
        },
        None => original_price,
    };

    let min_base_amount = base_coin.min_trading_vol();
    let min_rel_amount = rel_coin.min_trading_vol();

    // Add min_volume to update_msg if min_volume is found in the request
    if let Some(min_volume) = req.min_volume.clone() {
        // Validate and Ca lculate Minimum Volume
        let actual_min_vol = try_s!(validate_and_get_min_vol(
            min_base_amount.clone(),
            min_rel_amount.clone(),
            Some(min_volume),
            new_price.clone()
        ));
        update_msg.with_new_min_volume(actual_min_vol.into());
    }

    // Calculate order volume and add to update_msg if new_volume is found in the request
    let new_volume = if req.max.unwrap_or(false) {
        let max_volume = try_s!(get_max_volume(&ctx, &base_coin, &rel_coin).await) + reserved_amount.clone();
        update_msg.with_new_max_volume(max_volume.clone().into());
        max_volume
    } else if Option::is_some(&req.volume_delta) {
        let volume = original_volume + req.volume_delta.unwrap();
        if volume <= MmNumber::from("0") {
            return ERR!("New volume {} should be more than zero", volume);
        }
        try_s!(
            check_balance_for_maker_swap(
                &ctx,
                &base_coin,
                &rel_coin,
                volume.clone(),
                None,
                None,
                FeeApproxStage::OrderIssue
            )
            .await
        );
        update_msg.with_new_max_volume(volume.clone().into());
        volume
    } else {
        original_volume
    };

    if new_volume <= reserved_amount {
        return ERR!(
            "New volume {} should be more than reserved amount for order matches {}",
            new_volume,
            reserved_amount
        );
    }

    // Validate Order Volume
    try_s!(validate_max_vol(
        min_base_amount.clone(),
        min_rel_amount.clone(),
        new_volume.clone() - reserved_amount.clone(),
        req.min_volume.clone(),
        new_price
    ));

    let mut my_maker_orders = ordermatch_ctx.my_maker_orders.lock().await;
    let (rpc_result, base, rel) = match my_maker_orders.get_mut(&req.uuid) {
        None => return ERR!("Order with UUID: {} has been deleted", req.uuid),
        Some(order) => {
            if order.matches.len() != matches.len() || !order.matches.keys().all(|k| matches.contains_key(k)) {
                return ERR!("Order {} is being matched now, can't update", req.uuid);
            }

            let new_change = HistoricalOrder::build(&update_msg, &order);
            order.apply_updated(&update_msg);
            order.changes_history.get_or_insert(Vec::new()).push(new_change);
            save_maker_order_on_update(&ctx, &order);
            update_msg.with_new_max_volume((new_volume - reserved_amount).into());
            (MakerOrderForRpc::from(&*order), order.base.as_str(), order.rel.as_str())
        },
    };
    let res = try_s!(json::to_vec(&json!({ "result": rpc_result })));
    maker_order_updated_p2p_notify(ctx.clone(), base, rel, update_msg).await;

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

#[derive(Serialize)]
struct OrderForRpcWithCancellationReason<'a> {
    #[serde(flatten)]
    order: OrderForRpc<'a>,
    cancellation_reason: &'a str,
}

#[cfg(target_arch = "wasm32")]
pub async fn order_status(_ctx: MmArc, _req: Json) -> Result<Response<Vec<u8>>, String> {
    let res = json!({
        "error": format!("'order_status' is only supported in native mode"),
    });
    Response::builder()
        .status(404)
        .body(json::to_vec(&res).expect("Serialization failed"))
        .map_err(|e| ERRL!("{}", e))
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn order_status(ctx: MmArc, req: Json) -> Result<Response<Vec<u8>>, String> {
    use crate::mm2::database::my_orders::select_status_by_uuid;

    let req: OrderStatusReq = try_s!(json::from_value(req));

    let ordermatch_ctx = try_s!(OrdermatchContext::from_ctx(&ctx));
    let maker_orders = ordermatch_ctx.my_maker_orders.lock().await;
    if let Some(order) = maker_orders.get(&req.uuid) {
        let res = json!({
            "type": "Maker",
            "order": MakerOrderForMyOrdersRpc::from(order),
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

    let order_path = my_orders_history_dir(&ctx).join(req.uuid.to_string() + ".json");

    if let Ok(order) = json::from_slice::<Order>(&slurp(&order_path)) {
        let cancellation_reason = &try_s!(select_status_by_uuid(&ctx.sqlite_connection(), &req.uuid));
        let res = json!(OrderForRpcWithCancellationReason {
            order: OrderForRpc::from(&order),
            cancellation_reason,
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

#[derive(Display)]
enum MakerOrderCancellationReason {
    Fulfilled,
    InsufficientBalance,
    Cancelled,
}

#[derive(Display)]
enum TakerOrderCancellationReason {
    Fulfilled,
    ToMaker,
    TimedOut,
    Cancelled,
}

#[derive(Debug, Deserialize)]
pub struct MyOrdersFilter {
    pub order_type: Option<String>,
    pub initial_action: Option<String>,
    pub base: Option<String>,
    pub rel: Option<String>,
    pub from_price: Option<MmNumber>,
    pub to_price: Option<MmNumber>,
    pub from_volume: Option<MmNumber>,
    pub to_volume: Option<MmNumber>,
    pub from_timestamp: Option<u64>,
    pub to_timestamp: Option<u64>,
    pub was_taker: Option<bool>,
    pub status: Option<String>,
    #[serde(default)]
    pub include_details: bool,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(tag = "type", content = "order")]
enum Order {
    Maker(MakerOrder),
    Taker(TakerOrder),
}

impl<'a> From<&'a Order> for OrderForRpc<'a> {
    fn from(order: &'a Order) -> OrderForRpc {
        match order {
            Order::Maker(o) => OrderForRpc::Maker(MakerOrderForRpc::from(o)),
            Order::Taker(o) => OrderForRpc::Taker(TakerOrderForRpc::from(o)),
        }
    }
}

#[derive(Serialize)]
struct UuidParseError {
    uuid: String,
    warning: String,
}

#[cfg(target_arch = "wasm32")]
pub async fn orders_history_by_filter(_ctx: MmArc, _req: Json) -> Result<Response<Vec<u8>>, String> {
    let res = json!({
        "error": format!("'orders_history_by_filter' is only supported in native mode yet"),
    });
    Response::builder()
        .status(404)
        .body(json::to_vec(&res).expect("Serialization failed"))
        .map_err(|e| ERRL!("{}", e))
}

/// Returns *all* uuids of swaps, which match the selected filter.
#[cfg(not(target_arch = "wasm32"))]
pub async fn orders_history_by_filter(ctx: MmArc, req: Json) -> Result<Response<Vec<u8>>, String> {
    use crate::mm2::database::my_orders::select_orders_by_filter;

    let filter: MyOrdersFilter = try_s!(json::from_value(req));
    let db_result = try_s!(select_orders_by_filter(&ctx.sqlite_connection(), &filter, None));
    let mut warnings = vec![];

    let rpc_orders = if filter.include_details {
        let mut vec = Vec::with_capacity(db_result.result.len());
        for order in db_result.result.iter() {
            let uuid = match Uuid::parse_str(order.uuid.as_str()) {
                Ok(uuid) => uuid,
                Err(e) => {
                    let warning = format!(
                        "Order details for Uuid {} were skipped because uuid could not be parsed",
                        order.uuid
                    );
                    log::warn!("{}, error {}", warning, e);
                    warnings.push(UuidParseError {
                        uuid: order.uuid.clone(),
                        warning,
                    });
                    continue;
                },
            };
            let order_path = my_orders_history_dir(&ctx).join(order.uuid.clone() + ".json");
            let content = slurp(&order_path);
            if let Ok(order) = json::from_slice::<Order>(&content) {
                vec.push(order);
                continue;
            }

            let ordermatch_ctx = try_s!(OrdermatchContext::from_ctx(&ctx));
            if order.order_type == *"Maker" {
                let maker_orders = ordermatch_ctx.my_maker_orders.lock().await;
                if let Some(maker_order) = maker_orders.get(&uuid) {
                    vec.push(Order::Maker(maker_order.to_owned()));
                }
                continue;
            }

            let taker_orders = ordermatch_ctx.my_taker_orders.lock().await;
            if let Some(taker_order) = taker_orders.get(&uuid) {
                vec.push(Order::Taker(taker_order.to_owned()));
            }
        }
        vec
    } else {
        vec![]
    };

    let details: Vec<_> = rpc_orders.iter().map(OrderForRpc::from).collect();

    let json = json!({
    "result": {
        "orders": db_result.result,
        "details": details,
        "found_records": db_result.total_count,
        "warnings": warnings,
    }});

    let res = try_s!(json::to_vec(&json));

    Ok(try_s!(Response::builder().body(res)))
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
            delete_my_maker_order(&ctx, &order, MakerOrderCancellationReason::Cancelled);
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
            delete_my_taker_order(&ctx, &order, TakerOrderCancellationReason::Cancelled);
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
struct MakerOrderForMyOrdersRpc<'a> {
    #[serde(flatten)]
    order: MakerOrderForRpc<'a>,
    cancellable: bool,
    available_amount: BigDecimal,
}

impl<'a> From<&'a MakerOrder> for MakerOrderForMyOrdersRpc<'a> {
    fn from(order: &'a MakerOrder) -> MakerOrderForMyOrdersRpc {
        MakerOrderForMyOrdersRpc {
            order: order.into(),
            cancellable: order.is_cancellable(),
            available_amount: order.available_amount().into(),
        }
    }
}

#[derive(Serialize)]
struct TakerMatchForRpc<'a> {
    reserved: MakerReservedForRpc<'a>,
    connect: TakerConnectForRpc<'a>,
    connected: Option<MakerConnectedForRpc<'a>>,
    last_updated: u64,
}

impl<'a> From<&'a TakerMatch> for TakerMatchForRpc<'a> {
    fn from(taker_match: &'a TakerMatch) -> TakerMatchForRpc {
        TakerMatchForRpc {
            reserved: (&taker_match.reserved).into(),
            connect: (&taker_match.connect).into(),
            connected: taker_match.connected.as_ref().map(|connected| connected.into()),
            last_updated: 0,
        }
    }
}

#[derive(Serialize)]
struct TakerOrderForRpc<'a> {
    created_at: u64,
    request: TakerRequestForRpc<'a>,
    matches: HashMap<Uuid, TakerMatchForRpc<'a>>,
    order_type: &'a OrderType,
    cancellable: bool,
}

impl<'a> From<&'a TakerOrder> for TakerOrderForRpc<'a> {
    fn from(order: &'a TakerOrder) -> TakerOrderForRpc {
        TakerOrderForRpc {
            created_at: order.created_at,
            request: (&order.request).into(),
            matches: order
                .matches
                .iter()
                .map(|(uuid, taker_match)| (*uuid, taker_match.into()))
                .collect(),
            cancellable: order.is_cancellable(),
            order_type: &order.order_type,
        }
    }
}

#[derive(Serialize)]
#[serde(tag = "type", content = "order")]
enum OrderForRpc<'a> {
    Maker(MakerOrderForRpc<'a>),
    Taker(TakerOrderForRpc<'a>),
}

pub async fn my_orders(ctx: MmArc) -> Result<Response<Vec<u8>>, String> {
    let ordermatch_ctx = try_s!(OrdermatchContext::from_ctx(&ctx));
    let maker_orders = ordermatch_ctx.my_maker_orders.lock().await;
    let taker_orders = ordermatch_ctx.my_taker_orders.lock().await;
    let maker_orders_for_rpc: HashMap<_, _> = maker_orders
        .iter()
        .map(|(uuid, order)| (uuid, MakerOrderForMyOrdersRpc::from(order)))
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

#[cfg(not(target_arch = "wasm32"))]
fn insert_maker_order_to_db(ctx: &MmArc, uuid: Uuid, order: &MakerOrder) -> Result<(), String> {
    crate::mm2::database::my_orders::insert_maker_order(ctx, uuid, order).map_err(|e| ERRL!("{}", e))
}

#[cfg(target_arch = "wasm32")]
fn insert_maker_order_to_db(_ctx: &MmArc, _uuid: Uuid, _order: &MakerOrder) -> Result<(), String> { Ok(()) }

#[cfg(not(target_arch = "wasm32"))]
fn insert_taker_order_to_db(ctx: &MmArc, uuid: Uuid, order: &TakerOrder) -> Result<(), String> {
    crate::mm2::database::my_orders::insert_taker_order(ctx, uuid, order).map_err(|e| ERRL!("{}", e))
}

#[cfg(target_arch = "wasm32")]
fn insert_taker_order_to_db(_ctx: &MmArc, _uuid: Uuid, _order: &TakerOrder) -> Result<(), String> { Ok(()) }

#[cfg(not(target_arch = "wasm32"))]
fn update_maker_order_in_db(ctx: &MmArc, uuid: Uuid, order: &MakerOrder) -> Result<(), String> {
    crate::mm2::database::my_orders::update_maker_order(ctx, uuid, order).map_err(|e| ERRL!("{}", e))
}

#[cfg(target_arch = "wasm32")]
fn update_maker_order_in_db(_ctx: &MmArc, _uuid: Uuid, _order: &MakerOrder) -> Result<(), String> { Ok(()) }

#[cfg(not(target_arch = "wasm32"))]
fn update_was_taker_in_db(ctx: &MmArc, uuid: Uuid) -> Result<(), String> {
    crate::mm2::database::my_orders::update_was_taker(ctx, uuid).map_err(|e| ERRL!("{}", e))
}

#[cfg(target_arch = "wasm32")]
fn update_was_taker_in_db(_ctx: &MmArc, _uuid: Uuid) -> Result<(), String> { Ok(()) }

#[cfg(not(target_arch = "wasm32"))]
fn update_order_status_in_db(ctx: &MmArc, uuid: Uuid, status: String) -> Result<(), String> {
    crate::mm2::database::my_orders::update_order_status(ctx, uuid, status).map_err(|e| ERRL!("{}", e))
}

#[cfg(target_arch = "wasm32")]
fn update_order_status_in_db(_ctx: &MmArc, _uuid: Uuid, _status: String) -> Result<(), String> { Ok(()) }

pub fn my_maker_orders_dir(ctx: &MmArc) -> PathBuf { ctx.dbdir().join("ORDERS").join("MY").join("MAKER") }

fn my_taker_orders_dir(ctx: &MmArc) -> PathBuf { ctx.dbdir().join("ORDERS").join("MY").join("TAKER") }

fn my_orders_history_dir(ctx: &MmArc) -> PathBuf { ctx.dbdir().join("ORDERS").join("MY").join("HISTORY") }

pub fn my_maker_order_file_path(ctx: &MmArc, uuid: &Uuid) -> PathBuf {
    my_maker_orders_dir(ctx).join(format!("{}.json", uuid))
}

fn my_taker_order_file_path(ctx: &MmArc, uuid: &Uuid) -> PathBuf {
    my_taker_orders_dir(ctx).join(format!("{}.json", uuid))
}

fn my_order_history_file_path(ctx: &MmArc, uuid: &Uuid) -> PathBuf {
    my_orders_history_dir(ctx).join(format!("{}.json", uuid))
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct HistoricalOrder {
    #[serde(skip_serializing_if = "Option::is_none")]
    max_base_vol: Option<MmNumber>,
    #[serde(skip_serializing_if = "Option::is_none")]
    min_base_vol: Option<MmNumber>,
    #[serde(skip_serializing_if = "Option::is_none")]
    price: Option<MmNumber>,
    #[serde(skip_serializing_if = "Option::is_none")]
    updated_at: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    conf_settings: Option<OrderConfirmationsSettings>,
}

impl HistoricalOrder {
    fn build(new_order: &new_protocol::MakerOrderUpdated, old_order: &MakerOrder) -> HistoricalOrder {
        HistoricalOrder {
            max_base_vol: if new_order.new_max_volume().is_some() {
                Some(old_order.max_base_vol.clone())
            } else {
                None
            },
            min_base_vol: if new_order.new_min_volume().is_some() {
                Some(old_order.min_base_vol.clone())
            } else {
                None
            },
            price: if new_order.new_price().is_some() {
                Some(old_order.price.clone())
            } else {
                None
            },
            updated_at: old_order.updated_at,
            conf_settings: if let Some(settings) = new_order.new_conf_settings() {
                if Some(settings) == old_order.conf_settings {
                    None
                } else {
                    old_order.conf_settings
                }
            } else {
                None
            },
        }
    }
}

fn save_my_maker_order(ctx: &MmArc, order: &MakerOrder) {
    let path = my_maker_order_file_path(ctx, &order.uuid);
    let content = json::to_vec(order).unwrap();
    write(&path, &content).unwrap();
}

fn save_my_new_maker_order(ctx: &MmArc, order: &MakerOrder) {
    save_my_maker_order(ctx, order);

    if order.save_in_history {
        if let Err(e) = insert_maker_order_to_db(&ctx, order.uuid, &order) {
            error!("Error {} on new order insertion", e);
        }
    }
}

fn save_maker_order_on_update(ctx: &MmArc, order: &MakerOrder) {
    save_my_maker_order(ctx, order);

    if order.save_in_history {
        if let Err(e) = update_maker_order_in_db(&ctx, order.uuid, &order) {
            error!("Error {} on order update", e);
        }
    }
}

fn save_my_taker_order(ctx: &MmArc, order: &TakerOrder) {
    let path = my_taker_order_file_path(ctx, &order.request.uuid);
    let content = json::to_vec(order).unwrap();
    write(&path, &content).unwrap();
}

fn save_my_new_taker_order(ctx: &MmArc, order: &TakerOrder) {
    save_my_taker_order(ctx, order);

    if order.save_in_history {
        if let Err(e) = insert_taker_order_to_db(&ctx, order.request.uuid, &order) {
            error!("Error {} on new order insertion", e);
        }
    }
}

fn save_my_order_in_history(ctx: &MmArc, order: &Order) {
    let path = match order {
        Order::Maker(o) => my_order_history_file_path(ctx, &o.uuid),
        Order::Taker(o) => my_order_history_file_path(ctx, &o.request.uuid),
    };
    let content = json::to_vec(order).unwrap();
    write(&path, &content).unwrap();
}

#[cfg_attr(test, mockable)]
fn delete_my_maker_order(ctx: &MmArc, order: &MakerOrder, reason: MakerOrderCancellationReason) {
    let path = my_maker_order_file_path(ctx, &order.uuid);
    match remove_file(&path) {
        Ok(_) => (),
        Err(e) => log::warn!("Could not remove order file {}, error {}", path.display(), e),
    }

    if order.save_in_history {
        save_my_order_in_history(ctx, &Order::Maker(order.clone()));

        if let Err(e) = update_order_status_in_db(ctx, order.uuid, reason.to_string()) {
            error!("Error {} on order update", e);
        }
    }
}

#[cfg_attr(test, mockable)]
fn delete_my_taker_order(ctx: &MmArc, order: &TakerOrder, reason: TakerOrderCancellationReason) {
    let path = my_taker_order_file_path(ctx, &order.request.uuid);
    match remove_file(&path) {
        Ok(_) => (),
        Err(e) => log::warn!("Could not remove order file {}, error {}", path.display(), e),
    }

    match reason {
        TakerOrderCancellationReason::ToMaker => (),
        _ => {
            if order.save_in_history {
                save_my_order_in_history(ctx, &Order::Taker(order.clone()));
            }
        },
    }

    if order.save_in_history {
        if let Err(e) = update_order_status_in_db(ctx, order.request.uuid, reason.to_string()) {
            error!("Error {} on order update", e);
        }
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
                    delete_my_maker_order(&ctx, &$order, MakerOrderCancellationReason::Cancelled);
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
                    delete_my_taker_order(&ctx, &$order, TakerOrderCancellationReason::Cancelled);
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
pub(self) async fn subscribe_to_orderbook_topic(
    ctx: &MmArc,
    base: &str,
    rel: &str,
    request_orderbook: bool,
) -> Result<(), String> {
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
                    // but enough time has passed for the orderbook to fill by OrdermatchRequest::SyncPubkeyOrderbookState.
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
        try_s!(request_and_fill_orderbook(&ctx, base, rel).await);
    }

    Ok(())
}

construct_detailed!(DetailedBaseMaxVolume, base_max_volume);
construct_detailed!(DetailedBaseMinVolume, base_min_volume);
construct_detailed!(DetailedRelMaxVolume, rel_max_volume);
construct_detailed!(DetailedRelMinVolume, rel_min_volume);

#[derive(Debug, Serialize)]
pub struct RpcOrderbookEntry {
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
    #[serde(flatten)]
    base_max_volume: DetailedBaseMaxVolume,
    #[serde(flatten)]
    base_min_volume: DetailedBaseMinVolume,
    #[serde(flatten)]
    rel_max_volume: DetailedRelMaxVolume,
    #[serde(flatten)]
    rel_min_volume: DetailedRelMinVolume,
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
