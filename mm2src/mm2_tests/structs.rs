#![allow(dead_code, unused_variables)]

/// The helper structs used in testing of RPC responses, these should be separated from actual MM2 code to ensure
/// backwards compatibility
/// Use `#[serde(deny_unknown_fields)]` for all structs for tests to fail in case of adding new fields to the response
use bigdecimal::BigDecimal;
use common::mm_number::{Fraction, MmNumber};
use num_rational::BigRational;
use rpc::v1::types::H256 as H256Json;
use serde_json::Value as Json;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RpcSuccessResponse<T> {
    pub mmrpc: String,
    pub result: T,
    pub id: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RpcErrorResponse<E> {
    pub mmrpc: String,
    /// The legacy error description
    pub error: String,
    pub error_path: String,
    pub error_trace: String,
    pub error_type: String,
    pub error_data: Option<E>,
    pub id: Option<usize>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(tag = "type", content = "data")]
pub enum OrderType {
    FillOrKill,
    GoodTillCancelled,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderConfirmationsSettings {
    pub base_confs: u64,
    pub base_nota: bool,
    pub rel_confs: u64,
    pub rel_nota: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HistoricalOrder {
    max_base_vol: Option<MmNumber>,
    min_base_vol: Option<MmNumber>,
    price: Option<MmNumber>,
    updated_at: Option<u64>,
    conf_settings: Option<OrderConfirmationsSettings>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub enum TakerAction {
    Buy,
    Sell,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(tag = "type", content = "data")]
pub enum MatchBy {
    Any,
    Orders(HashSet<Uuid>),
    Pubkeys(HashSet<H256Json>),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BuyOrSellRpcRes {
    pub base: String,
    pub rel: String,
    pub base_amount: BigDecimal,
    pub base_amount_rat: BigRational,
    pub rel_amount: BigDecimal,
    pub rel_amount_rat: BigRational,
    pub min_volume: BigDecimal,
    pub min_volume_rat: BigRational,
    pub min_volume_fraction: Fraction,
    pub action: TakerAction,
    pub uuid: Uuid,
    pub method: String,
    pub sender_pubkey: H256Json,
    pub dest_pub_key: H256Json,
    pub match_by: MatchBy,
    pub conf_settings: OrderConfirmationsSettings,
    pub order_type: OrderType,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BuyOrSellRpcResult {
    pub result: BuyOrSellRpcRes,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TakerRequest {
    base: String,
    rel: String,
    base_amount: BigDecimal,
    base_amount_rat: BigRational,
    rel_amount: BigDecimal,
    rel_amount_rat: BigRational,
    action: TakerAction,
    uuid: Uuid,
    method: String,
    sender_pubkey: H256Json,
    dest_pub_key: H256Json,
    match_by: MatchBy,
    conf_settings: OrderConfirmationsSettings,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MakerReserved {
    base: String,
    rel: String,
    base_amount: BigDecimal,
    base_amount_rat: BigRational,
    rel_amount: BigDecimal,
    rel_amount_rat: BigRational,
    taker_order_uuid: Uuid,
    maker_order_uuid: Uuid,
    method: String,
    sender_pubkey: H256Json,
    dest_pub_key: H256Json,
    conf_settings: OrderConfirmationsSettings,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TakerConnect {
    taker_order_uuid: Uuid,
    maker_order_uuid: Uuid,
    method: String,
    sender_pubkey: H256Json,
    dest_pub_key: H256Json,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MakerConnected {
    taker_order_uuid: Uuid,
    maker_order_uuid: Uuid,
    method: String,
    sender_pubkey: H256Json,
    dest_pub_key: H256Json,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MakerMatch {
    request: TakerRequest,
    reserved: MakerReserved,
    connect: Option<TakerConnect>,
    connected: Option<MakerConnected>,
    last_updated: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MakerOrderRpcResult {
    pub max_base_vol: BigDecimal,
    pub max_base_vol_rat: BigRational,
    pub min_base_vol: BigDecimal,
    pub min_base_vol_rat: BigRational,
    pub price: BigDecimal,
    pub price_rat: BigRational,
    pub created_at: u64,
    pub updated_at: Option<u64>,
    pub base: String,
    pub rel: String,
    pub matches: HashMap<Uuid, MakerMatch>,
    pub started_swaps: Vec<Uuid>,
    pub uuid: Uuid,
    pub conf_settings: Option<OrderConfirmationsSettings>,
    changes_history: Option<Vec<HistoricalOrder>>,
    pub cancellable: bool,
    pub available_amount: BigDecimal,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetPriceResult {
    pub max_base_vol: BigDecimal,
    pub max_base_vol_rat: BigRational,
    pub min_base_vol: BigDecimal,
    pub min_base_vol_rat: BigRational,
    pub price: BigDecimal,
    pub price_rat: BigRational,
    pub created_at: u64,
    pub updated_at: Option<u64>,
    pub base: String,
    pub rel: String,
    pub matches: HashMap<Uuid, MakerMatch>,
    pub started_swaps: Vec<Uuid>,
    pub uuid: Uuid,
    pub conf_settings: Option<OrderConfirmationsSettings>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetPriceResponse {
    pub result: SetPriceResult,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TakerMatch {
    reserved: MakerReserved,
    connect: TakerConnect,
    connected: Option<MakerConnected>,
    last_updated: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TakerOrderRpcResult {
    created_at: u64,
    request: TakerRequest,
    matches: HashMap<Uuid, TakerMatch>,
    order_type: OrderType,
    pub cancellable: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MyOrdersRpc {
    pub maker_orders: HashMap<Uuid, MakerOrderRpcResult>,
    pub taker_orders: HashMap<Uuid, TakerOrderRpcResult>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MyOrdersRpcResult {
    pub result: MyOrdersRpc,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderbookEntry {
    pub coin: String,
    pub address: String,
    pub price: BigDecimal,
    pub price_rat: BigRational,
    pub price_fraction: Fraction,
    #[serde(rename = "maxvolume")]
    pub max_volume: BigDecimal,
    pub max_volume_rat: BigRational,
    pub max_volume_fraction: Fraction,
    pub base_max_volume: BigDecimal,
    pub base_max_volume_rat: BigRational,
    pub base_max_volume_fraction: Fraction,
    pub base_min_volume: BigDecimal,
    pub base_min_volume_rat: BigRational,
    pub base_min_volume_fraction: Fraction,
    pub rel_max_volume: BigDecimal,
    pub rel_max_volume_rat: BigRational,
    pub rel_max_volume_fraction: Fraction,
    pub rel_min_volume: BigDecimal,
    pub rel_min_volume_rat: BigRational,
    pub rel_min_volume_fraction: Fraction,
    pub min_volume: BigDecimal,
    pub min_volume_rat: BigRational,
    pub min_volume_fraction: Fraction,
    pub pubkey: String,
    pub age: i64,
    pub zcredits: u64,
    pub uuid: Uuid,
    pub is_mine: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderbookEntryAggregate {
    pub coin: String,
    pub address: String,
    pub price: BigDecimal,
    pub price_rat: BigRational,
    pub price_fraction: Fraction,
    #[serde(rename = "maxvolume")]
    pub max_volume: BigDecimal,
    pub max_volume_rat: BigRational,
    pub max_volume_fraction: Fraction,
    pub base_max_volume: BigDecimal,
    pub base_max_volume_rat: BigRational,
    pub base_max_volume_fraction: Fraction,
    pub base_min_volume: BigDecimal,
    pub base_min_volume_rat: BigRational,
    pub base_min_volume_fraction: Fraction,
    pub rel_max_volume: BigDecimal,
    pub rel_max_volume_rat: BigRational,
    pub rel_max_volume_fraction: Fraction,
    pub rel_min_volume: BigDecimal,
    pub rel_min_volume_rat: BigRational,
    pub rel_min_volume_fraction: Fraction,
    pub min_volume: BigDecimal,
    pub min_volume_rat: BigRational,
    pub min_volume_fraction: Fraction,
    pub pubkey: String,
    pub age: i64,
    pub zcredits: u64,
    pub uuid: Uuid,
    pub is_mine: bool,
    pub base_max_volume_aggr: BigDecimal,
    pub base_max_volume_aggr_rat: BigRational,
    pub base_max_volume_aggr_fraction: Fraction,
    pub rel_max_volume_aggr: BigDecimal,
    pub rel_max_volume_aggr_rat: BigRational,
    pub rel_max_volume_aggr_fraction: Fraction,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BestOrdersResponse {
    pub result: HashMap<String, Vec<OrderbookEntry>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderbookResponse {
    pub base: String,
    pub rel: String,
    #[serde(rename = "askdepth")]
    pub ask_depth: usize,
    #[serde(rename = "biddepth")]
    pub bid_depth: usize,
    #[serde(rename = "numasks")]
    num_asks: usize,
    #[serde(rename = "numbids")]
    num_bids: usize,
    pub netid: u16,
    timestamp: u64,
    pub total_asks_base_vol: BigDecimal,
    pub total_asks_base_vol_rat: BigRational,
    pub total_asks_base_vol_fraction: Fraction,
    pub total_asks_rel_vol: BigDecimal,
    pub total_asks_rel_vol_rat: BigRational,
    pub total_asks_rel_vol_fraction: Fraction,
    pub total_bids_base_vol: BigDecimal,
    pub total_bids_base_vol_rat: BigRational,
    pub total_bids_base_vol_fraction: Fraction,
    pub total_bids_rel_vol: BigDecimal,
    pub total_bids_rel_vol_rat: BigRational,
    pub total_bids_rel_vol_fraction: Fraction,
    pub asks: Vec<OrderbookEntryAggregate>,
    pub bids: Vec<OrderbookEntryAggregate>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PairDepth {
    pub asks: usize,
    pub bids: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PairWithDepth {
    pub pair: (String, String),
    pub depth: PairDepth,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderbookDepthResponse {
    pub result: Vec<PairWithDepth>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnableElectrumResponse {
    pub coin: String,
    pub address: String,
    pub balance: BigDecimal,
    pub unspendable_balance: BigDecimal,
    pub required_confirmations: u64,
    pub mature_confirmations: Option<u64>,
    pub requires_notarization: bool,
    pub result: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TradeFeeForTest {
    pub coin: String,
    pub amount: BigDecimal,
    pub amount_rat: BigRational,
    pub amount_fraction: Fraction,
    pub paid_from_trading_vol: bool,
}

impl TradeFeeForTest {
    pub fn new(coin: &str, amount: &'static str, paid_from_trading_vol: bool) -> TradeFeeForTest {
        let amount_mm = MmNumber::from(amount);
        TradeFeeForTest {
            coin: coin.into(),
            amount: amount_mm.to_decimal(),
            amount_rat: amount_mm.to_ratio(),
            amount_fraction: amount_mm.to_fraction(),
            paid_from_trading_vol,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TotalTradeFeeForTest {
    pub coin: String,
    pub amount: BigDecimal,
    pub amount_rat: BigRational,
    pub amount_fraction: Fraction,
    pub required_balance: BigDecimal,
    pub required_balance_rat: BigRational,
    pub required_balance_fraction: Fraction,
}

impl TotalTradeFeeForTest {
    pub fn new(coin: &str, amount: &'static str, required_balance: &'static str) -> TotalTradeFeeForTest {
        let amount_mm = MmNumber::from(amount);
        let required_mm = MmNumber::from(required_balance);
        TotalTradeFeeForTest {
            coin: coin.into(),
            amount: amount_mm.to_decimal(),
            amount_rat: amount_mm.to_ratio(),
            amount_fraction: amount_mm.to_fraction(),
            required_balance: required_mm.to_decimal(),
            required_balance_rat: required_mm.to_ratio(),
            required_balance_fraction: required_mm.to_fraction(),
        }
    }
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TakerPreimage {
    pub base_coin_fee: TradeFeeForTest,
    pub rel_coin_fee: TradeFeeForTest,
    pub taker_fee: TradeFeeForTest,
    pub fee_to_send_taker_fee: TradeFeeForTest,
    // the order of fees is not deterministic
    pub total_fees: Vec<TotalTradeFeeForTest>,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MakerPreimage {
    pub base_coin_fee: TradeFeeForTest,
    pub rel_coin_fee: TradeFeeForTest,
    pub volume: Option<BigDecimal>,
    pub volume_rat: Option<BigRational>,
    pub volume_fraction: Option<Fraction>,
    // the order of fees is not deterministic
    pub total_fees: Vec<TotalTradeFeeForTest>,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
#[serde(untagged)]
pub enum TradePreimageResult {
    TakerPreimage(TakerPreimage),
    MakerPreimage(MakerPreimage),
}

impl TradePreimageResult {
    pub fn sort_total_fees(&mut self) {
        match self {
            TradePreimageResult::MakerPreimage(ref mut preimage) => {
                preimage.total_fees.sort_by(|fee1, fee2| fee1.coin.cmp(&fee2.coin))
            },
            TradePreimageResult::TakerPreimage(ref mut preimage) => {
                preimage.total_fees.sort_by(|fee1, fee2| fee1.coin.cmp(&fee2.coin))
            },
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradePreimageResponse {
    pub result: TradePreimageResult,
}

impl TradePreimageResponse {
    pub fn sort_total_fees(&mut self) { self.result.sort_total_fees() }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaxTakerVolResponse {
    pub result: Fraction,
    pub coin: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawTransactionResult {
    /// Raw bytes of signed transaction in hexadecimal string, this should be sent as is to send_raw_transaction RPC to broadcast the transaction
    pub tx_hex: String,
}

pub mod raw_transaction_error {
    #[derive(Debug, Deserialize, PartialEq)]
    #[serde(deny_unknown_fields)]
    pub struct InvalidCoin {
        pub coin: String,
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransactionDetails {
    pub tx_hex: String,
    pub tx_hash: String,
    pub from: Vec<String>,
    pub to: Vec<String>,
    pub total_amount: BigDecimal,
    pub spent_by_me: BigDecimal,
    pub received_by_me: BigDecimal,
    pub my_balance_change: BigDecimal,
    pub block_height: u64,
    pub timestamp: u64,
    pub fee_details: Json,
    pub coin: String,
    pub internal_id: String,
}

pub mod withdraw_error {
    use bigdecimal::BigDecimal;

    #[derive(Debug, Deserialize, PartialEq)]
    #[serde(deny_unknown_fields)]
    pub struct NotSufficientBalance {
        pub coin: String,
        pub available: BigDecimal,
        pub required: BigDecimal,
    }

    #[derive(Debug, Deserialize, PartialEq)]
    #[serde(deny_unknown_fields)]
    pub struct AmountTooLow {
        pub amount: BigDecimal,
        pub threshold: BigDecimal,
    }
}

pub mod trade_preimage_error {
    use bigdecimal::BigDecimal;

    #[derive(Debug, Deserialize, PartialEq)]
    #[serde(deny_unknown_fields)]
    pub struct NotSufficientBalance {
        pub coin: String,
        pub available: BigDecimal,
        pub required: BigDecimal,
        pub locked_by_swaps: Option<BigDecimal>,
    }

    #[derive(Debug, Deserialize, PartialEq)]
    #[serde(deny_unknown_fields)]
    pub struct VolumeTooLow {
        pub coin: String,
        pub volume: BigDecimal,
        pub threshold: BigDecimal,
    }

    #[derive(Debug, Deserialize, PartialEq)]
    #[serde(deny_unknown_fields)]
    pub struct NoSuchCoin {
        pub coin: String,
    }

    #[derive(Debug, Deserialize, PartialEq)]
    #[serde(deny_unknown_fields)]
    pub struct InvalidParam {
        pub param: String,
        pub reason: String,
    }

    #[derive(Debug, Deserialize, PartialEq)]
    #[serde(deny_unknown_fields)]
    pub struct PriceTooLow {
        pub price: BigDecimal,
        pub threshold: BigDecimal,
    }
}
