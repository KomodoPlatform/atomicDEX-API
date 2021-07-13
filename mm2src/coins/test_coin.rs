use super::{CoinBalance, HistorySyncState, MarketCoinOps, MmCoin, SwapOps, TradeFee, TransactionEnum, TransactionFut};
use crate::{BalanceFut, FeeApproxStage, FoundSwapTxSpend, NegotiateSwapContractAddrErr, RawTransactionFut,
            RawTransactionRequest, TradePreimageFut, TradePreimageValue, ValidateAddressResult, WithdrawFut,
            WithdrawRequest};
use bigdecimal::BigDecimal;
use common::mm_ctx::MmArc;
use common::mm_error::MmError;
use common::mm_number::MmNumber;
use futures01::Future;
use mocktopus::macros::*;
use rpc::v1::types::Bytes as BytesJson;
use serde_json::Value as Json;

/// Dummy coin struct used in tests which functions are unimplemented but then mocked
/// in specific test to emulate the required behaviour
#[derive(Clone, Debug)]
pub struct TestCoin {
    ticker: String,
}

impl Default for TestCoin {
    fn default() -> Self { TestCoin { ticker: "test".into() } }
}

impl TestCoin {
    pub fn new(ticker: &str) -> TestCoin { TestCoin { ticker: ticker.into() } }
}

#[mockable]
#[allow(clippy::forget_ref, clippy::forget_copy, clippy::cast_ref_to_mut)]
impl MarketCoinOps for TestCoin {
    fn ticker(&self) -> &str { &self.ticker }

    fn my_address(&self) -> Result<String, String> { unimplemented!() }

    fn my_balance(&self) -> BalanceFut<CoinBalance> { unimplemented!() }

    fn base_coin_balance(&self) -> BalanceFut<BigDecimal> { unimplemented!() }

    /// Receives raw transaction bytes in hexadecimal format as input and returns tx hash in hexadecimal format
    fn send_raw_tx(&self, tx: &str) -> Box<dyn Future<Item = String, Error = String> + Send> { unimplemented!() }

    fn wait_for_confirmations(
        &self,
        tx: &[u8],
        confirmations: u64,
        requires_nota: bool,
        wait_until: u64,
        check_every: u64,
    ) -> Box<dyn Future<Item = (), Error = String> + Send> {
        unimplemented!()
    }

    fn wait_for_tx_spend(
        &self,
        transaction: &[u8],
        wait_until: u64,
        from_block: u64,
        swap_contract_address: &Option<BytesJson>,
    ) -> TransactionFut {
        unimplemented!()
    }

    fn tx_enum_from_bytes(&self, bytes: &[u8]) -> Result<TransactionEnum, String> { unimplemented!() }

    fn current_block(&self) -> Box<dyn Future<Item = u64, Error = String> + Send> { unimplemented!() }

    fn address_from_pubkey_str(&self, pubkey: &str) -> Result<String, String> { unimplemented!() }

    fn display_priv_key(&self) -> String { unimplemented!() }

    fn min_tx_amount(&self) -> BigDecimal { unimplemented!() }

    fn min_trading_vol(&self) -> MmNumber { MmNumber::from("0.00777") }
}

#[mockable]
#[allow(clippy::forget_ref, clippy::forget_copy, clippy::cast_ref_to_mut)]
impl SwapOps for TestCoin {
    fn send_taker_fee(&self, fee_addr: &[u8], amount: BigDecimal) -> TransactionFut { unimplemented!() }

    fn send_maker_payment(
        &self,
        time_lock: u32,
        taker_pub: &[u8],
        secret_hash: &[u8],
        amount: BigDecimal,
        swap_contract_address: &Option<BytesJson>,
    ) -> TransactionFut {
        unimplemented!()
    }

    fn send_taker_payment(
        &self,
        time_lock: u32,
        maker_pub: &[u8],
        secret_hash: &[u8],
        amount: BigDecimal,
        swap_contract_address: &Option<BytesJson>,
    ) -> TransactionFut {
        unimplemented!()
    }

    fn send_maker_spends_taker_payment(
        &self,
        taker_payment_tx: &[u8],
        time_lock: u32,
        taker_pub: &[u8],
        secret: &[u8],
        swap_contract_address: &Option<BytesJson>,
    ) -> TransactionFut {
        unimplemented!()
    }

    fn send_taker_spends_maker_payment(
        &self,
        maker_payment_tx: &[u8],
        time_lock: u32,
        maker_pub: &[u8],
        secret: &[u8],
        swap_contract_address: &Option<BytesJson>,
    ) -> TransactionFut {
        unimplemented!()
    }

    fn send_taker_refunds_payment(
        &self,
        taker_payment_tx: &[u8],
        time_lock: u32,
        maker_pub: &[u8],
        secret_hash: &[u8],
        swap_contract_address: &Option<BytesJson>,
    ) -> TransactionFut {
        unimplemented!()
    }

    fn send_maker_refunds_payment(
        &self,
        maker_payment_tx: &[u8],
        time_lock: u32,
        taker_pub: &[u8],
        secret_hash: &[u8],
        swap_contract_address: &Option<BytesJson>,
    ) -> TransactionFut {
        unimplemented!()
    }

    fn validate_fee(
        &self,
        fee_tx: &TransactionEnum,
        expected_sender: &[u8],
        fee_addr: &[u8],
        amount: &BigDecimal,
        min_block_number: u64,
    ) -> Box<dyn Future<Item = (), Error = String> + Send> {
        unimplemented!()
    }

    fn validate_maker_payment(
        &self,
        payment_tx: &[u8],
        time_lock: u32,
        maker_pub: &[u8],
        priv_bn_hash: &[u8],
        amount: BigDecimal,
        swap_contract_address: &Option<BytesJson>,
    ) -> Box<dyn Future<Item = (), Error = String> + Send> {
        unimplemented!()
    }

    fn validate_taker_payment(
        &self,
        payment_tx: &[u8],
        time_lock: u32,
        taker_pub: &[u8],
        priv_bn_hash: &[u8],
        amount: BigDecimal,
        swap_contract_address: &Option<BytesJson>,
    ) -> Box<dyn Future<Item = (), Error = String> + Send> {
        unimplemented!()
    }

    fn check_if_my_payment_sent(
        &self,
        time_lock: u32,
        other_pub: &[u8],
        secret_hash: &[u8],
        search_from_block: u64,
        swap_contract_address: &Option<BytesJson>,
    ) -> Box<dyn Future<Item = Option<TransactionEnum>, Error = String> + Send> {
        unimplemented!()
    }

    fn search_for_swap_tx_spend_my(
        &self,
        time_lock: u32,
        other_pub: &[u8],
        secret_hash: &[u8],
        tx: &[u8],
        search_from_block: u64,
        swap_contract_address: &Option<BytesJson>,
    ) -> Result<Option<FoundSwapTxSpend>, String> {
        unimplemented!()
    }

    fn search_for_swap_tx_spend_other(
        &self,
        time_lock: u32,
        other_pub: &[u8],
        secret_hash: &[u8],
        tx: &[u8],
        search_from_block: u64,
        swap_contract_address: &Option<BytesJson>,
    ) -> Result<Option<FoundSwapTxSpend>, String> {
        unimplemented!()
    }

    fn extract_secret(&self, secret_hash: &[u8], spend_tx: &[u8]) -> Result<Vec<u8>, String> { unimplemented!() }

    fn negotiate_swap_contract_addr(
        &self,
        other_side_address: Option<&[u8]>,
    ) -> Result<Option<BytesJson>, MmError<NegotiateSwapContractAddrErr>> {
        unimplemented!()
    }
}

#[mockable]
#[allow(clippy::forget_ref, clippy::forget_copy, clippy::cast_ref_to_mut)]
impl MmCoin for TestCoin {
    fn is_asset_chain(&self) -> bool { unimplemented!() }

    fn withdraw(&self, req: WithdrawRequest) -> WithdrawFut { unimplemented!() }

    fn get_raw_transaction(&self, _req: RawTransactionRequest) -> RawTransactionFut { unimplemented!() }

    fn decimals(&self) -> u8 { unimplemented!() }

    fn convert_to_address(&self, from: &str, to_address_format: Json) -> Result<String, String> { unimplemented!() }

    fn validate_address(&self, address: &str) -> ValidateAddressResult { unimplemented!() }

    fn process_history_loop(&self, ctx: MmArc) -> Box<dyn Future<Item = (), Error = ()> + Send> { unimplemented!() }

    fn history_sync_status(&self) -> HistorySyncState { unimplemented!() }

    /// Get fee to be paid per 1 swap transaction
    fn get_trade_fee(&self) -> Box<dyn Future<Item = TradeFee, Error = String> + Send> { unimplemented!() }

    fn get_sender_trade_fee(&self, value: TradePreimageValue, stage: FeeApproxStage) -> TradePreimageFut<TradeFee> {
        unimplemented!()
    }

    fn get_receiver_trade_fee(&self, stage: FeeApproxStage) -> TradePreimageFut<TradeFee> { unimplemented!() }

    fn get_fee_to_send_taker_fee(
        &self,
        dex_fee_amount: BigDecimal,
        stage: FeeApproxStage,
    ) -> TradePreimageFut<TradeFee> {
        unimplemented!()
    }

    fn required_confirmations(&self) -> u64 { 1 }

    fn requires_notarization(&self) -> bool { false }

    fn set_required_confirmations(&self, _confirmations: u64) { unimplemented!() }

    fn set_requires_notarization(&self, _requires_nota: bool) { unimplemented!() }

    fn swap_contract_address(&self) -> Option<BytesJson> { unimplemented!() }

    fn mature_confirmations(&self) -> Option<u32> { unimplemented!() }
}
