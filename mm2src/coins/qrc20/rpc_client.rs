use super::*;
use std::collections::HashMap;

#[derive(Debug, Deserialize, PartialEq)]
pub struct TokenInfo {
    pub name: String,
    pub decimals: u8,
    pub total_supply: f64,
    pub symbol: String,
}

#[derive(Debug, Deserialize)]
pub struct ExecutionResult {
    pub output: BytesJson,
}

#[derive(Debug, Deserialize)]
pub struct ContractCallResult {
    address: H160Json,
    #[serde(rename = "executionResult")]
    pub execution_result: ExecutionResult,
}

#[derive(Debug, Deserialize)]
pub struct TxHistoryItem {
    pub tx_hash: H256Json,
    pub height: u64,
    pub log_index: u64,
}

#[derive(Debug, Deserialize)]
pub struct ContractCreateResult {
    /// The transaction id.
    pub txid: H256Json,
    /// QTUM address of the sender.
    pub sender: String,
    /// ripemd-160 hash of the sender.
    pub hash160: H160Json,
    /// Expected contract address.
    pub address: H160Json,
}

#[derive(Debug, Deserialize)]
pub struct SendToContractResult {
    /// The transaction id.
    pub txid: H256Json,
    /// QTUM address of the sender.
    pub sender: String,
    /// ripemd-160 hash of the sender.
    pub hash160: H160Json,
}

#[derive(Debug, Deserialize)]
pub struct AddressPurpose {
    purpose: String,
}

pub type AddressesByLabelResult = HashMap<String, AddressPurpose>;

/// The structure is the same as Qtum Core RPC gettransactionreceipt returned data.
/// https://docs.qtum.site/en/Qtum-RPC-API/#gettransactionreceipt
#[derive(Debug, Deserialize, PartialEq, Serialize)]
pub struct TxReceipt {
    /// Hash of the block this transaction was included within.
    #[serde(rename = "blockHash")]
    pub block_hash: H256Json,
    /// Number of the block this transaction was included within.
    #[serde(rename = "blockNumber")]
    pub block_number: u64,
    /// Transaction hash.
    #[serde(rename = "transactionHash")]
    pub transaction_hash: H256Json,
    /// Index within the block.
    #[serde(rename = "transactionIndex")]
    pub transaction_index: u64,
    /// Index within the outputs.
    #[serde(rename = "outputIndex")]
    pub output_index: u64,
    /// 20 bytes，the sender address of this tx.
    pub from: String,
    /// 20 bytes，the receiver address of this tx. if this  address is created by a contract, return null.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to: Option<String>,
    /// The total amount of gas used after execution of the current transaction.
    #[serde(rename = "cumulativeGasUsed")]
    pub cumulative_gas_used: u64,
    /// The gas cost alone to execute the current transaction.
    #[serde(rename = "gasUsed")]
    pub gas_used: u64,
    /// Contract address created, or `None` if not a deployment.
    #[serde(rename = "contractAddress")]
    pub contract_address: Option<String>,
    /// Logs generated within this transaction.
    pub log: Vec<LogEntry>,
    /// Whether corresponding contract call (specified in UTXO outputs[output_index]) was failed.
    /// If None or Some("None") - completed, else failed.
    pub excepted: Option<String>,
    #[serde(rename = "exceptedMessage")]
    pub excepted_message: Option<String>,
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
pub struct LogEntry {
    /// Contract address.
    pub address: String,
    /// Vector of 0x-prefixed hex strings with length of 64.
    pub topics: Vec<String>,
    /// In other words the data means a transaction value.
    pub data: String,
}

impl LogEntry {
    pub fn parse_address(&self) -> Result<H160, String> {
        if self.address.starts_with("0x") {
            qrc20_addr_from_str(&self.address)
        } else {
            let address = format!("0x{}", self.address);
            qrc20_addr_from_str(&address)
        }
    }
}

#[derive(Clone, Debug)]
pub enum TopicFilter {
    Match(String),
    Skip,
}

impl From<&str> for TopicFilter {
    fn from(topic: &str) -> Self { TopicFilter::Match(topic.to_string()) }
}

/// Qrc20 specific RPC ops
pub trait Qrc20RpcOps {
    /// This can be used to get the basic information(name, decimals, total_supply, symbol) of a QRC20 token.
    /// https://github.com/qtumproject/qtum-electrumx-server/blob/master/docs/qrc20-integration.md#blockchaintokenget_infotoken_address
    fn blockchain_token_get_info(&self, token_addr: &H160Json) -> RpcRes<TokenInfo>;

    fn blockchain_contract_call(&self, contract_addr: &H160Json, data: BytesJson) -> RpcRes<ContractCallResult>;

    /// This can be used to retrieve QRC20 token transfer history, params are the same as blockchain.contract.event.subscribe,
    /// and it returns a list of map{tx_hash, height, log_index}, where log_index is the position for this event log in its transaction.
    /// https://github.com/qtumproject/qtum-electrumx-server/blob/master/docs/qrc20-integration.md#blockchaincontracteventget_historyhash160-contract_addr-topic
    fn blockchain_contract_event_get_history(
        &self,
        address: &H160Json,
        contract_addr: &H160Json,
        topic: &str,
    ) -> RpcRes<Vec<TxHistoryItem>>;

    /// This can be used to get eventlogs in the transaction, the returned data is the same as Qtum Core RPC gettransactionreceipt.
    /// from the eventlogs, we can get QRC20 Token transafer informations(from, to, amount).
    /// https://github.com/qtumproject/qtum-electrumx-server/blob/master/docs/qrc20-integration.md#blochchaintransactionget_receipttxid
    fn blochchain_transaction_get_receipt(&self, hash: &H256Json) -> RpcRes<Vec<TxReceipt>>;
}

pub trait Qrc20NativeOps {
    /// https://docs.qtum.site/en/Qtum-RPC-API/#callcontract
    fn call_contract(&self, contract_addr: &H160Json, data: BytesJson) -> RpcRes<ContractCallResult>;

    /// Similar to [`Qrc20RpcOps::blochchain_transaction_get_receipt`]
    /// https://docs.qtum.site/en/Qtum-RPC-API/#gettransactionreceipt
    fn get_transaction_receipt(&self, hash: &H256Json) -> RpcRes<Vec<TxReceipt>>;

    /// Returns the list of addresses assigned the specified label.
    /// https://docs.qtum.site/en/Qtum-RPC-API/#getaddressesbylabel
    fn get_addresses_by_label(&self, label: &str) -> RpcRes<AddressesByLabelResult>;

    /// This can be used to retrieve QRC20 transaction history.
    /// https://docs.qtum.site/en/Qtum-RPC-API/#searchlogs
    fn search_logs(
        &self,
        from_block: u64,
        to_block: Option<u64>,
        addresses: Vec<H160Json>,
        topics: Vec<TopicFilter>,
    ) -> RpcRes<Vec<TxReceipt>>;
}

/// QRC20 Native RPC operations that may change the wallet state.
pub trait Qrc20NativeWalletOps {
    /// Create contract with bytecode and specified sender.
    /// https://docs.qtum.site/en/Qtum-RPC-API/#createcontract
    fn create_contract(
        &self,
        bytecode: &BytesJson,
        gas_limit: u64,
        gas_price: BigDecimal,
        sender: &str,
    ) -> RpcRes<ContractCreateResult>;

    /// Send data to a contract.
    /// https://docs.qtum.site/en/Qtum-RPC-API/#sendtocontract
    fn send_to_contract(
        &self,
        contract_addr: H160Json,
        bytecode: &BytesJson,
        qtum_amount: u64,
        gas_limit: u64,
        gas_price: BigDecimal,
        from_addr: &str,
    ) -> RpcRes<SendToContractResult>;

    /// Send `transfer` contract call to the `token_addr`.
    /// This method uses [`Qrc20NativeWallerOps::send_to_contract`] to send the encoded contract call params.
    /// Note qtum_amount = 0, gas_limit = QRC20_GAS_LIMIT_DEFAULT, gas_price = QRC20_GAS_PRICE_DEFAULT will be used.
    fn transfer_tokens(
        &self,
        token_addr: &H160,
        from_addr: &str,
        to_addr: H160,
        amount: U256,
        decimals: u8,
    ) -> Box<dyn Future<Item = SendToContractResult, Error = String> + Send> {
        let token_addr = qrc20_addr_into_rpc_format(token_addr);
        let qtum_amount = 0;
        let gas_price = big_decimal_from_sat(QRC20_GAS_PRICE_DEFAULT as i64, decimals);

        let function = try_fus!(eth::ERC20_CONTRACT.function("transfer"));
        let params = try_fus!(function.encode_input(&[Token::Address(to_addr), Token::Uint(amount)]));
        Box::new(
            self.send_to_contract(
                token_addr,
                &params.into(),
                qtum_amount,
                QRC20_GAS_LIMIT_DEFAULT,
                gas_price,
                from_addr,
            )
            .map_err(|e| ERRL!("{}", e)),
        )
    }
}

impl Qrc20NativeOps for NativeClient {
    fn call_contract(&self, contract_addr: &H160Json, data: BytesJson) -> RpcRes<ContractCallResult> {
        rpc_func!(self, "callcontract", contract_addr, data)
    }

    fn get_transaction_receipt(&self, hash: &H256Json) -> RpcRes<Vec<TxReceipt>> {
        rpc_func!(self, "gettransactionreceipt", hash)
    }

    fn get_addresses_by_label(&self, label: &str) -> RpcRes<AddressesByLabelResult> {
        rpc_func!(self, "getaddressesbylabel", label)
    }

    fn search_logs(
        &self,
        from_block: u64,
        to_block: Option<u64>,
        addresses: Vec<H160Json>,
        topics: Vec<TopicFilter>,
    ) -> RpcRes<Vec<TxReceipt>> {
        let to_block = to_block.map(|x| x as i64).unwrap_or(-1);
        let addr_block = json!({ "addresses": addresses });
        let topics: Vec<Json> = topics
            .into_iter()
            .map(|t| match t {
                TopicFilter::Match(s) => Json::String(s),
                TopicFilter::Skip => Json::Null,
            })
            .collect();
        let topic_block = json!({
            "topics": topics,
        });
        rpc_func!(self, "searchlogs", from_block, to_block, addr_block, topic_block)
    }
}

impl Qrc20NativeWalletOps for NativeClient {
    fn create_contract(
        &self,
        bytecode: &BytesJson,
        gas_limit: u64,
        gas_price: BigDecimal,
        sender: &str,
    ) -> RpcRes<ContractCreateResult> {
        rpc_func!(self, "createcontract", bytecode, gas_limit, gas_price, sender)
    }

    fn send_to_contract(
        &self,
        contract_addr: H160Json,
        bytecode: &BytesJson,
        qtum_amount: u64,
        gas_limit: u64,
        gas_price: BigDecimal,
        sender: &str,
    ) -> RpcRes<SendToContractResult> {
        rpc_func!(
            self,
            "sendtocontract",
            contract_addr,
            bytecode,
            qtum_amount,
            gas_limit,
            gas_price,
            sender
        )
    }
}

impl Qrc20RpcOps for ElectrumClient {
    fn blockchain_token_get_info(&self, token_addr: &H160Json) -> RpcRes<TokenInfo> {
        rpc_func!(self, "blockchain.token.get_info", token_addr)
    }

    fn blockchain_contract_call(&self, contract_addr: &H160Json, data: BytesJson) -> RpcRes<ContractCallResult> {
        let sender = "";
        rpc_func!(self, "blockchain.contract.call", contract_addr, data, sender)
    }

    fn blockchain_contract_event_get_history(
        &self,
        address: &H160Json,
        contract_addr: &H160Json,
        topic: &str,
    ) -> RpcRes<Vec<TxHistoryItem>> {
        rpc_func!(
            self,
            "blockchain.contract.event.get_history",
            address,
            contract_addr,
            topic
        )
    }

    fn blochchain_transaction_get_receipt(&self, hash: &H256Json) -> RpcRes<Vec<TxReceipt>> {
        rpc_func!(self, "blochchain.transaction.get_receipt", hash)
    }
}
