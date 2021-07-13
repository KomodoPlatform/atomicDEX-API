use super::*;
use crate::{eth, CanRefundHtlc, CoinBalance, NegotiateSwapContractAddrErr, SwapOps, TradePreimageValue,
            ValidateAddressResult, WithdrawFut};
use common::mm_metrics::MetricsArc;
use common::mm_number::MmNumber;
use ethereum_types::H160;
use futures::{FutureExt, TryFutureExt};
use serialization::CoinVariant;

pub const QTUM_STANDARD_DUST: u64 = 1000;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "format")]
pub enum QtumAddressFormat {
    /// Standard Qtum/UTXO address format.
    #[serde(rename = "wallet")]
    Wallet,
    /// Contract address format. The same as used in ETH/ERC20.
    /// Note starts with "0x" prefix.
    #[serde(rename = "contract")]
    Contract,
}

#[async_trait]
pub trait QtumBasedCoin: AsRef<UtxoCoinFields> + UtxoCommonOps + MarketCoinOps {
    async fn qtum_balance(&self) -> BalanceResult<CoinBalance> {
        let balance = self
            .as_ref()
            .rpc_client
            .display_balance(self.as_ref().my_address.clone(), self.as_ref().decimals)
            .compat()
            .await?;

        let unspendable = utxo_common::my_unspendable_balance(self, &balance).await?;
        let spendable = &balance - &unspendable;
        Ok(CoinBalance { spendable, unspendable })
    }

    fn convert_to_address(&self, from: &str, to_address_format: Json) -> Result<String, String> {
        let to_address_format: QtumAddressFormat =
            json::from_value(to_address_format).map_err(|e| ERRL!("Error on parse Qtum address format {:?}", e))?;
        let from_address = try_s!(self.utxo_address_from_any_format(from));
        match to_address_format {
            QtumAddressFormat::Wallet => Ok(from_address.to_string()),
            QtumAddressFormat::Contract => Ok(display_as_contract_address(from_address)),
        }
    }

    /// Try to parse address from either wallet (UTXO) format or contract format.
    fn utxo_address_from_any_format(&self, from: &str) -> Result<Address, String> {
        let utxo_err = match Address::from_str(from) {
            Ok(addr) => {
                let is_p2pkh = addr.prefix == self.as_ref().conf.pub_addr_prefix
                    && addr.t_addr_prefix == self.as_ref().conf.pub_t_addr_prefix;
                if is_p2pkh {
                    return Ok(addr);
                }
                "Address has invalid prefixes".to_string()
            },
            Err(e) => e.to_string(),
        };
        let utxo_segwit_err = match Address::from_segwitaddress(
            from,
            self.as_ref().conf.checksum_type,
            self.as_ref().my_address.prefix,
            self.as_ref().my_address.t_addr_prefix,
        ) {
            Ok(addr) => {
                let is_segwit =
                    addr.hrp.is_some() && addr.hrp == self.as_ref().conf.bech32_hrp && self.as_ref().conf.segwit;
                if is_segwit {
                    return Ok(addr);
                }
                "Address has invalid hrp".to_string()
            },
            Err(e) => e,
        };
        let contract_err = match contract_addr_from_str(from) {
            Ok(contract_addr) => return Ok(self.utxo_addr_from_contract_addr(contract_addr)),
            Err(e) => e,
        };
        ERR!(
            "error on parse wallet address: {:?}, {:?}, error on parse contract address: {:?}",
            utxo_err,
            utxo_segwit_err,
            contract_err,
        )
    }

    fn utxo_addr_from_contract_addr(&self, address: H160) -> Address {
        let utxo = self.as_ref();
        Address {
            prefix: utxo.conf.pub_addr_prefix,
            t_addr_prefix: utxo.conf.pub_t_addr_prefix,
            hash: address.0.into(),
            checksum_type: utxo.conf.checksum_type,
            hrp: utxo.conf.bech32_hrp.clone(),
            addr_format: utxo.my_address.addr_format.clone(),
        }
    }

    fn my_addr_as_contract_addr(&self) -> H160 { contract_addr_from_utxo_addr(self.as_ref().my_address.clone()) }

    fn utxo_address_from_contract_addr(&self, address: H160) -> Address {
        let utxo = self.as_ref();
        Address {
            prefix: utxo.conf.pub_addr_prefix,
            t_addr_prefix: utxo.conf.pub_t_addr_prefix,
            hash: address.0.into(),
            checksum_type: utxo.conf.checksum_type,
            hrp: utxo.conf.bech32_hrp.clone(),
            addr_format: utxo.my_address.addr_format.clone(),
        }
    }

    fn contract_address_from_raw_pubkey(&self, pubkey: &[u8]) -> Result<H160, String> {
        let utxo = self.as_ref();
        let qtum_address = try_s!(utxo_common::address_from_raw_pubkey(
            pubkey,
            utxo.conf.pub_addr_prefix,
            utxo.conf.pub_t_addr_prefix,
            utxo.conf.checksum_type,
            utxo.conf.bech32_hrp.clone(),
            utxo.my_address.addr_format.clone()
        ));
        Ok(qtum::contract_addr_from_utxo_addr(qtum_address))
    }

    fn is_qtum_unspent_mature(&self, output: &RpcTransaction) -> bool {
        let is_qrc20_coinbase = output.vout.iter().any(|x| x.is_empty());
        let is_coinbase = output.is_coinbase() || is_qrc20_coinbase;
        !is_coinbase || output.confirmations >= self.as_ref().conf.mature_confirmations
    }
}

#[derive(Clone, Debug)]
pub struct QtumCoin {
    utxo_arc: UtxoArc,
}

impl AsRef<UtxoCoinFields> for QtumCoin {
    fn as_ref(&self) -> &UtxoCoinFields { &self.utxo_arc }
}

impl From<UtxoArc> for QtumCoin {
    fn from(coin: UtxoArc) -> QtumCoin { QtumCoin { utxo_arc: coin } }
}

impl From<QtumCoin> for UtxoArc {
    fn from(coin: QtumCoin) -> Self { coin.utxo_arc }
}

pub async fn qtum_coin_from_conf_and_request(
    ctx: &MmArc,
    ticker: &str,
    conf: &Json,
    req: &Json,
    priv_key: &[u8],
) -> Result<QtumCoin, String> {
    let coin: QtumCoin = try_s!(utxo_common::utxo_arc_from_conf_and_request(ctx, ticker, conf, req, priv_key).await);
    Ok(coin)
}

impl QtumBasedCoin for QtumCoin {}

#[async_trait]
#[cfg_attr(test, mockable)]
impl UtxoCommonOps for QtumCoin {
    async fn get_tx_fee(&self) -> Result<ActualTxFee, JsonRpcError> { utxo_common::get_tx_fee(&self.utxo_arc).await }

    async fn get_htlc_spend_fee(&self) -> UtxoRpcResult<u64> { utxo_common::get_htlc_spend_fee(self).await }

    fn addresses_from_script(&self, script: &Script) -> Result<Vec<Address>, String> {
        utxo_common::addresses_from_script(&self.utxo_arc, script)
    }

    fn denominate_satoshis(&self, satoshi: i64) -> f64 { utxo_common::denominate_satoshis(&self.utxo_arc, satoshi) }

    fn my_public_key(&self) -> &Public { self.utxo_arc.key_pair.public() }

    fn address_from_str(&self, address: &str) -> Result<Address, String> {
        utxo_common::checked_address_from_str(&self.utxo_arc, address)
    }

    async fn get_current_mtp(&self) -> UtxoRpcResult<u32> {
        utxo_common::get_current_mtp(&self.utxo_arc, CoinVariant::Qtum).await
    }

    fn is_unspent_mature(&self, output: &RpcTransaction) -> bool { self.is_qtum_unspent_mature(output) }

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
        my_script_pub: Bytes,
    ) -> UtxoRpcResult<(TransactionInputSigner, AdditionalTxData)> {
        utxo_common::calc_interest_if_required(self, unsigned, data, my_script_pub).await
    }

    async fn calc_interest_of_tx(
        &self,
        _tx: &UtxoTx,
        _input_transactions: &mut HistoryUtxoTxMap,
    ) -> UtxoRpcResult<u64> {
        MmError::err(UtxoRpcError::Internal(
            "QTUM coin doesn't support transaction rewards".to_owned(),
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
        redeem_script: Bytes,
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
        let fut = async move { utxo_common::get_verbose_transaction_from_cache_or_rpc(&selfi.utxo_arc, txid).await };
        Box::new(fut.boxed().compat())
    }

    async fn cache_transaction_if_possible(&self, tx: &RpcTransaction) -> Result<(), String> {
        utxo_common::cache_transaction_if_possible(&self.utxo_arc, tx).await
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
        utxo_common::p2sh_tx_locktime(self, &self.utxo_arc.conf.ticker, htlc_locktime).await
    }
}

#[async_trait]
impl UtxoStandardOps for QtumCoin {
    async fn tx_details_by_hash(
        &self,
        hash: &[u8],
        input_transactions: &mut HistoryUtxoTxMap,
    ) -> Result<TransactionDetails, String> {
        utxo_common::tx_details_by_hash(self, hash, input_transactions).await
    }

    async fn request_tx_history(&self, metrics: MetricsArc) -> RequestTxHistoryResult {
        utxo_common::request_tx_history(self, metrics).await
    }

    async fn update_kmd_rewards(
        &self,
        tx_details: &mut TransactionDetails,
        input_transactions: &mut HistoryUtxoTxMap,
    ) -> UtxoRpcResult<()> {
        utxo_common::update_kmd_rewards(self, tx_details, input_transactions).await
    }
}

impl SwapOps for QtumCoin {
    fn send_taker_fee(&self, fee_addr: &[u8], amount: BigDecimal) -> TransactionFut {
        utxo_common::send_taker_fee(self.clone(), fee_addr, amount)
    }

    fn send_maker_payment(
        &self,
        time_lock: u32,
        taker_pub: &[u8],
        secret_hash: &[u8],
        amount: BigDecimal,
        _swap_contract_address: &Option<BytesJson>,
    ) -> TransactionFut {
        utxo_common::send_maker_payment(self.clone(), time_lock, taker_pub, secret_hash, amount)
    }

    fn send_taker_payment(
        &self,
        time_lock: u32,
        maker_pub: &[u8],
        secret_hash: &[u8],
        amount: BigDecimal,
        _swap_contract_address: &Option<BytesJson>,
    ) -> TransactionFut {
        utxo_common::send_taker_payment(self.clone(), time_lock, maker_pub, secret_hash, amount)
    }

    fn send_maker_spends_taker_payment(
        &self,
        taker_payment_tx: &[u8],
        time_lock: u32,
        taker_pub: &[u8],
        secret: &[u8],
        _swap_contract_address: &Option<BytesJson>,
    ) -> TransactionFut {
        utxo_common::send_maker_spends_taker_payment(self.clone(), taker_payment_tx, time_lock, taker_pub, secret)
    }

    fn send_taker_spends_maker_payment(
        &self,
        maker_payment_tx: &[u8],
        time_lock: u32,
        maker_pub: &[u8],
        secret: &[u8],
        _swap_contract_address: &Option<BytesJson>,
    ) -> TransactionFut {
        utxo_common::send_taker_spends_maker_payment(self.clone(), maker_payment_tx, time_lock, maker_pub, secret)
    }

    fn send_taker_refunds_payment(
        &self,
        taker_payment_tx: &[u8],
        time_lock: u32,
        maker_pub: &[u8],
        secret_hash: &[u8],
        _swap_contract_address: &Option<BytesJson>,
    ) -> TransactionFut {
        utxo_common::send_taker_refunds_payment(self.clone(), taker_payment_tx, time_lock, maker_pub, secret_hash)
    }

    fn send_maker_refunds_payment(
        &self,
        maker_payment_tx: &[u8],
        time_lock: u32,
        taker_pub: &[u8],
        secret_hash: &[u8],
        _swap_contract_address: &Option<BytesJson>,
    ) -> TransactionFut {
        utxo_common::send_maker_refunds_payment(self.clone(), maker_payment_tx, time_lock, taker_pub, secret_hash)
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
        utxo_common::validate_fee(
            self.clone(),
            tx,
            utxo_common::DEFAULT_FEE_VOUT,
            expected_sender,
            amount,
            min_block_number,
            fee_addr,
        )
    }

    fn validate_maker_payment(
        &self,
        payment_tx: &[u8],
        time_lock: u32,
        maker_pub: &[u8],
        priv_bn_hash: &[u8],
        amount: BigDecimal,
        _swap_contract_address: &Option<BytesJson>,
    ) -> Box<dyn Future<Item = (), Error = String> + Send> {
        utxo_common::validate_maker_payment(self, payment_tx, time_lock, maker_pub, priv_bn_hash, amount)
    }

    fn validate_taker_payment(
        &self,
        payment_tx: &[u8],
        time_lock: u32,
        taker_pub: &[u8],
        priv_bn_hash: &[u8],
        amount: BigDecimal,
        _swap_contract_address: &Option<BytesJson>,
    ) -> Box<dyn Future<Item = (), Error = String> + Send> {
        utxo_common::validate_taker_payment(self, payment_tx, time_lock, taker_pub, priv_bn_hash, amount)
    }

    fn check_if_my_payment_sent(
        &self,
        time_lock: u32,
        other_pub: &[u8],
        secret_hash: &[u8],
        _search_from_block: u64,
        _swap_contract_address: &Option<BytesJson>,
    ) -> Box<dyn Future<Item = Option<TransactionEnum>, Error = String> + Send> {
        utxo_common::check_if_my_payment_sent(self.clone(), time_lock, other_pub, secret_hash)
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
            &self.utxo_arc,
            time_lock,
            other_pub,
            secret_hash,
            tx,
            utxo_common::DEFAULT_SWAP_VOUT,
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
            &self.utxo_arc,
            time_lock,
            other_pub,
            secret_hash,
            tx,
            utxo_common::DEFAULT_SWAP_VOUT,
            search_from_block,
        )
    }

    fn extract_secret(&self, secret_hash: &[u8], spend_tx: &[u8]) -> Result<Vec<u8>, String> {
        utxo_common::extract_secret(secret_hash, spend_tx)
    }

    fn can_refund_htlc(&self, locktime: u64) -> Box<dyn Future<Item = CanRefundHtlc, Error = String> + Send + '_> {
        Box::new(
            utxo_common::can_refund_htlc(self, locktime)
                .boxed()
                .map_err(|e| ERRL!("{}", e))
                .compat(),
        )
    }

    fn negotiate_swap_contract_addr(
        &self,
        _other_side_address: Option<&[u8]>,
    ) -> Result<Option<BytesJson>, MmError<NegotiateSwapContractAddrErr>> {
        Ok(None)
    }
}

impl MarketCoinOps for QtumCoin {
    fn ticker(&self) -> &str { &self.utxo_arc.conf.ticker }

    fn my_address(&self) -> Result<String, String> { utxo_common::my_address(self) }

    fn my_balance(&self) -> BalanceFut<CoinBalance> {
        let selfi = self.clone();
        let fut = async move { selfi.qtum_balance().await };
        Box::new(fut.boxed().compat())
    }

    fn base_coin_balance(&self) -> BalanceFut<BigDecimal> { utxo_common::base_coin_balance(self) }

    fn send_raw_tx(&self, tx: &str) -> Box<dyn Future<Item = String, Error = String> + Send> {
        utxo_common::send_raw_tx(&self.utxo_arc, tx)
    }

    fn wait_for_confirmations(
        &self,
        tx: &[u8],
        confirmations: u64,
        requires_nota: bool,
        wait_until: u64,
        check_every: u64,
    ) -> Box<dyn Future<Item = (), Error = String> + Send> {
        utxo_common::wait_for_confirmations(
            &self.utxo_arc,
            tx,
            confirmations,
            requires_nota,
            wait_until,
            check_every,
        )
    }

    fn wait_for_tx_spend(
        &self,
        transaction: &[u8],
        wait_until: u64,
        from_block: u64,
        _swap_contract_address: &Option<BytesJson>,
    ) -> TransactionFut {
        utxo_common::wait_for_output_spend(
            &self.utxo_arc,
            transaction,
            utxo_common::DEFAULT_SWAP_VOUT,
            from_block,
            wait_until,
        )
    }

    fn tx_enum_from_bytes(&self, bytes: &[u8]) -> Result<TransactionEnum, String> {
        utxo_common::tx_enum_from_bytes(self.as_ref(), bytes)
    }

    fn current_block(&self) -> Box<dyn Future<Item = u64, Error = String> + Send> {
        utxo_common::current_block(&self.utxo_arc)
    }

    fn address_from_pubkey_str(&self, pubkey: &str) -> Result<String, String> {
        utxo_common::address_from_pubkey_str(self, pubkey)
    }

    fn display_priv_key(&self) -> String { utxo_common::display_priv_key(&self.utxo_arc) }

    fn min_tx_amount(&self) -> BigDecimal { utxo_common::min_tx_amount(self.as_ref()) }

    fn min_trading_vol(&self) -> MmNumber { utxo_common::min_trading_vol(self.as_ref()) }
}

impl MmCoin for QtumCoin {
    fn is_asset_chain(&self) -> bool { utxo_common::is_asset_chain(&self.utxo_arc) }

    fn withdraw(&self, req: WithdrawRequest) -> WithdrawFut {
        Box::new(utxo_common::withdraw(self.clone(), req).boxed().compat())
    }

    fn get_raw_transaction(&self, req: RawTransactionRequest) -> RawTransactionFut {
        Box::new(utxo_common::get_raw_transaction(self.clone(), req).boxed().compat())
    }

    fn decimals(&self) -> u8 { utxo_common::decimals(&self.utxo_arc) }

    /// Check if the `to_address_format` is standard and if the `from` address is standard UTXO address.
    fn convert_to_address(&self, from: &str, to_address_format: Json) -> Result<String, String> {
        QtumBasedCoin::convert_to_address(self, from, to_address_format)
    }

    fn validate_address(&self, address: &str) -> ValidateAddressResult { utxo_common::validate_address(self, address) }

    fn process_history_loop(&self, ctx: MmArc) -> Box<dyn Future<Item = (), Error = ()> + Send> {
        Box::new(
            utxo_common::process_history_loop(self.clone(), ctx)
                .map(|_| Ok(()))
                .boxed()
                .compat(),
        )
    }

    fn history_sync_status(&self) -> HistorySyncState { utxo_common::history_sync_status(&self.utxo_arc) }

    fn get_trade_fee(&self) -> Box<dyn Future<Item = TradeFee, Error = String> + Send> {
        utxo_common::get_trade_fee(self.clone())
    }

    fn get_sender_trade_fee(&self, value: TradePreimageValue, stage: FeeApproxStage) -> TradePreimageFut<TradeFee> {
        utxo_common::get_sender_trade_fee(self.clone(), value, stage)
    }

    fn get_receiver_trade_fee(&self, _stage: FeeApproxStage) -> TradePreimageFut<TradeFee> {
        utxo_common::get_receiver_trade_fee(self.clone())
    }

    fn get_fee_to_send_taker_fee(
        &self,
        dex_fee_amount: BigDecimal,
        stage: FeeApproxStage,
    ) -> TradePreimageFut<TradeFee> {
        utxo_common::get_fee_to_send_taker_fee(self.clone(), dex_fee_amount, stage)
    }

    fn required_confirmations(&self) -> u64 { utxo_common::required_confirmations(&self.utxo_arc) }

    fn requires_notarization(&self) -> bool { utxo_common::requires_notarization(&self.utxo_arc) }

    fn set_required_confirmations(&self, confirmations: u64) {
        utxo_common::set_required_confirmations(&self.utxo_arc, confirmations)
    }

    fn set_requires_notarization(&self, requires_nota: bool) {
        utxo_common::set_requires_notarization(&self.utxo_arc, requires_nota)
    }

    fn swap_contract_address(&self) -> Option<BytesJson> { utxo_common::swap_contract_address() }

    fn mature_confirmations(&self) -> Option<u32> { Some(self.utxo_arc.conf.mature_confirmations) }
}

/// Parse contract address (H160) from string.
/// Qtum Contract addresses have another checksum verification algorithm, because of this do not use [`eth::valid_addr_from_str`].
pub fn contract_addr_from_str(addr: &str) -> Result<H160, String> { eth::addr_from_str(addr) }

pub fn contract_addr_from_utxo_addr(address: Address) -> H160 { address.hash.take().into() }

pub fn display_as_contract_address(address: Address) -> String {
    let address = qtum::contract_addr_from_utxo_addr(address);
    format!("{:#02x}", address)
}
