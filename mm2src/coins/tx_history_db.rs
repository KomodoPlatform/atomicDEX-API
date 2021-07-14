use crate::{TransactionDetails, TxHistoryError, TxHistoryResult};
use async_trait::async_trait;
use common::indexed_db::{DbIdentifier, DbInstance, DbTransactionError, DbUpgrader, IndexedDb, IndexedDbBuilder,
                         InitDbError, InitDbResult, OnUpgradeResult, TableSignature};
use common::mm_error::prelude::*;

const DB_NAME: &str = "tx_history";
const DB_VERSION: u32 = 1;

impl From<InitDbError> for TxHistoryError {
    fn from(e: InitDbError) -> Self {
        match &e {
            InitDbError::NotSupported(_) => TxHistoryError::NotSupported(e.to_string()),
            InitDbError::EmptyTableList
            | InitDbError::DbIsOpenAlready { .. }
            | InitDbError::InvalidVersion(_)
            | InitDbError::OpeningError(_)
            | InitDbError::TypeMismatch { .. }
            | InitDbError::UnexpectedState(_)
            | InitDbError::UpgradingError { .. } => TxHistoryError::InternalError(e.to_string()),
        }
    }
}

impl From<DbTransactionError> for TxHistoryError {
    fn from(e: DbTransactionError) -> Self {
        match e {
            DbTransactionError::ErrorSerializingItem(_) => TxHistoryError::ErrorSerializing(e.to_string()),
            DbTransactionError::ErrorDeserializingItem(_) => TxHistoryError::ErrorDeserializing(e.to_string()),
            DbTransactionError::ErrorUploadingItem(_) => TxHistoryError::ErrorSaving(e.to_string()),
            DbTransactionError::ErrorGettingItems(_) => TxHistoryError::ErrorLoading(e.to_string()),
            DbTransactionError::ErrorDeletingItems(_) => TxHistoryError::ErrorClearing(e.to_string()),
            DbTransactionError::NoSuchTable { .. }
            | DbTransactionError::ErrorCreatingTransaction(_)
            | DbTransactionError::ErrorOpeningTable { .. }
            | DbTransactionError::ErrorSerializingIndex { .. }
            | DbTransactionError::UnexpectedState(_)
            | DbTransactionError::TransactionAborted
            | DbTransactionError::NoSuchIndex { .. }
            | DbTransactionError::InvalidIndex { .. } => TxHistoryError::InternalError(e.to_string()),
        }
    }
}

pub struct TxHistoryDb {
    inner: IndexedDb,
}

#[async_trait]
impl DbInstance for TxHistoryDb {
    fn db_name() -> &'static str { DB_NAME }

    async fn init(db_id: DbIdentifier) -> InitDbResult<Self> {
        let inner = IndexedDbBuilder::new(db_id)
            .with_version(DB_VERSION)
            .with_table::<TxHistoryTable>()
            .build()
            .await?;
        Ok(TxHistoryDb { inner })
    }
}

impl TxHistoryDb {
    pub async fn load_history(&self, ticker: &str, wallet_address: &str) -> TxHistoryResult<Vec<TransactionDetails>> {
        let history_id = HistoryId::new(ticker, wallet_address);

        let transaction = self.inner.transaction().await?;
        let table = transaction.table::<TxHistoryTable>().await?;

        let items = table.get_items("history_id", history_id.as_str()).await?;
        if items.len() > 1 {
            let error = format!(
                "Expected only one item by the 'history_id' index, found {}",
                items.len()
            );
            return MmError::err(TxHistoryError::InternalError(error));
        }

        let mut item_iter = items.into_iter();
        match item_iter.next() {
            Some((_item_id, TxHistoryTable { txs, .. })) => Ok(txs),
            None => Ok(Vec::new()),
        }
    }

    pub async fn save_history(
        &self,
        ticker: &str,
        wallet_address: &str,
        txs: Vec<TransactionDetails>,
    ) -> TxHistoryResult<()> {
        let history_id = HistoryId::new(ticker, wallet_address);
        let history_id_value = history_id.to_string();
        let tx_history_item = TxHistoryTable { history_id, txs };

        let transaction = self.inner.transaction().await?;
        let table = transaction.table::<TxHistoryTable>().await?;

        // First, check if the coin's tx history exists already.
        let ids = table.get_item_ids("history_id", &history_id_value).await?;
        match ids.len() {
            // The history doesn't exist, add the new `tx_history_item`.
            0 => {
                table.add_item(&tx_history_item).await?;
            },
            // The history exists already, replace it with the actual `tx_history_item`.
            1 => {
                let item_id = ids[0];
                table.replace_item(item_id, &tx_history_item).await?;
            },
            unexpected_len => {
                let error = format!(
                    "Expected only one item by the 'history_id' index, found {}",
                    unexpected_len
                );
                return MmError::err(TxHistoryError::InternalError(error));
            },
        }

        transaction.wait_for_complete().await?;
        Ok(())
    }

    pub async fn clear(&self, ticker: &str, wallet_address: &str) -> TxHistoryResult<()> {
        let history_id = HistoryId::new(ticker, wallet_address);

        let transaction = self.inner.transaction().await?;
        let table = transaction.table::<TxHistoryTable>().await?;

        // First, check if the coin's tx history exists.
        let ids = table.get_item_ids("history_id", history_id.as_str()).await?;
        match ids.len() {
            // The history doesn't exist, we don't need to do anything.
            0 => (),
            1 => {
                let item_id = ids[0];
                table.delete_item(item_id).await?;
            },
            unexpected_len => {
                let error = format!(
                    "Expected only one item by the 'history_id' index, found {}",
                    unexpected_len
                );
                return MmError::err(TxHistoryError::InternalError(error));
            },
        }

        transaction.wait_for_complete().await?;
        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct HistoryId(String);

impl HistoryId {
    fn new(ticker: &str, wallet_address: &str) -> HistoryId { HistoryId(format!("{}_{}", ticker, wallet_address)) }

    fn as_str(&self) -> &str { &self.0 }

    fn to_string(&self) -> String { self.0.clone() }
}

#[derive(Debug, Deserialize, Serialize)]
struct TxHistoryTable {
    history_id: HistoryId,
    txs: Vec<TransactionDetails>,
}

impl TableSignature for TxHistoryTable {
    fn table_name() -> &'static str { "tx_history" }

    fn on_upgrade_needed(upgrader: &DbUpgrader, old_version: u32, new_version: u32) -> OnUpgradeResult<()> {
        match (old_version, new_version) {
            (0, 1) => {
                let table = upgrader.create_table(Self::table_name())?;
                table.create_index("history_id", true)?;
            },
            (1, 1) => (),
            v => panic!("Unexpected (old, new) versions: {:?}", v),
        }
        Ok(())
    }
}

mod tests {
    use super::*;
    use serde_json as json;
    use wasm_bindgen_test::*;

    wasm_bindgen_test_configure!(run_in_browser);

    #[wasm_bindgen_test]
    async fn test_tx_history() {
        const DB_NAME: &'static str = "TEST_TX_HISTORY";
        let db = TxHistoryDb::init(DbIdentifier::for_test(DB_NAME))
            .await
            .expect("!TxHistoryDb::init_with_fs_path");

        let history = db
            .load_history("RICK", "RRnMcSeKiLrNdbp91qNVQwwXx5azD4S4CD")
            .await
            .expect("!TxHistoryDb::load_history");
        assert!(history.is_empty());

        let history_str = r#"[{"tx_hex":"0400008085202f89018ec8f6f02e008ebd57bbf94c0d8297c1825f3af204490c43f5652b002a2c8b17010000006b483045022100e625a8b77beac5ec891e7d44b18fc4d780ef8456847eb2c6fa2f765e3379a9c102200f323612189fa44ee16f5deb39809b71c4811545b36ee4d6cc622d01aab10ef3012102043663e9c5af8275771809b3889d437f559e49e8df79b6ba19ade4cc5d8eb3e0ffffffff03809698000000000017a9142ffaf6694c6b441790546eefd277e430e08e47a6870000000000000000166a14094ab490fffa9939544545a656b345bf21920a90f6b35714000000001976a9145ed376ce9faa63cb2fef5862e1a5cc811c17316588acf9d29260000000000000000000000000000000","tx_hash":"f05d786bd4b647a5720094bf0a2c6f23b5e131c451d750a96102898f7b5458e8","from":["RHvavL8j683JwrN2ygk9Bg495DvPu5QVN3"],"to":["RHvavL8j683JwrN2ygk9Bg495DvPu5QVN3","bH6y6RtvbLToqSUNtLA5rQRjSwyNNzUSNc"],"total_amount":"3.51293022","spent_by_me":"3.51293022","received_by_me":"3.41292022","my_balance_change":"-0.10001","block_height":916940,"timestamp":1620235027,"fee_details":{"type":"Utxo","amount":"0.00001"},"coin":"RICK","internal_id":"f05d786bd4b647a5720094bf0a2c6f23b5e131c451d750a96102898f7b5458e8"}]"#;
        let history: Vec<TransactionDetails> = json::from_str(history_str).unwrap();
        db.save_history("RICK", "RRnMcSeKiLrNdbp91qNVQwwXx5azD4S4CD", history)
            .await
            .expect("!TxHistoryDb::save_history");

        let updated_history_str = r#"[{"tx_hex":"0400008085202f89018ec8f6f02e008ebd57bbf94c0d8297c1825f3af204490c43f5652b002a2c8b17010000006b483045022100e625a8b77beac5ec891e7d44b18fc4d780ef8456847eb2c6fa2f765e3379a9c102200f323612189fa44ee16f5deb39809b71c4811545b36ee4d6cc622d01aab10ef3012102043663e9c5af8275771809b3889d437f559e49e8df79b6ba19ade4cc5d8eb3e0ffffffff03809698000000000017a9142ffaf6694c6b441790546eefd277e430e08e47a6870000000000000000166a14094ab490fffa9939544545a656b345bf21920a90f6b35714000000001976a9145ed376ce9faa63cb2fef5862e1a5cc811c17316588acf9d29260000000000000000000000000000000","tx_hash":"f05d786bd4b647a5720094bf0a2c6f23b5e131c451d750a96102898f7b5458e8","from":["RHvavL8j683JwrN2ygk9Bg495DvPu5QVN3"],"to":["RHvavL8j683JwrN2ygk9Bg495DvPu5QVN3","bH6y6RtvbLToqSUNtLA5rQRjSwyNNzUSNc"],"total_amount":"3.51293022","spent_by_me":"3.51293022","received_by_me":"3.41292022","my_balance_change":"-0.10001","block_height":916940,"timestamp":1620235027,"fee_details":{"type":"Utxo","amount":"0.00001"},"coin":"RICK","internal_id":"f05d786bd4b647a5720094bf0a2c6f23b5e131c451d750a96102898f7b5458e8"},{"tx_hex":"0400008085202f8901a5620f30001e5e31bcbffacee7687fd84490fa1f8625ddd1e098f0bc530d673e020000006b483045022100a9864707855307681b81d94ae17328f6feccb2a9439d27378dfeae2df0220cc102207476f8304af14794a9cd2fe6287e07a1c802c05ec134b789ddb22ab51f7d2238012102043663e9c5af8275771809b3889d437f559e49e8df79b6ba19ade4cc5d8eb3e0ffffffff0246320000000000001976a914ca1e04745e8ca0c60d8c5881531d51bec470743f88ac5e4ef014000000001976a9145ed376ce9faa63cb2fef5862e1a5cc811c17316588acacd29260000000000000000000000000000000","tx_hash":"178b2c2a002b65f5430c4904f23a5f82c197820d4cf9bb57bd8e002ef0f6c88e","from":["RHvavL8j683JwrN2ygk9Bg495DvPu5QVN3"],"to":["RHvavL8j683JwrN2ygk9Bg495DvPu5QVN3","RThtXup6Zo7LZAi8kRWgjAyi1s4u6U9Cpf"],"total_amount":"3.51306892","spent_by_me":"3.51306892","received_by_me":"3.51293022","my_balance_change":"-0.0001387","block_height":916939,"timestamp":1620234997,"fee_details":{"type":"Utxo","amount":"0.00001"},"coin":"RICK","internal_id":"178b2c2a002b65f5430c4904f23a5f82c197820d4cf9bb57bd8e002ef0f6c88e"}]"#;
        let updated_history: Vec<TransactionDetails> = json::from_str(updated_history_str).unwrap();
        db.save_history("RICK", "RRnMcSeKiLrNdbp91qNVQwwXx5azD4S4CD", updated_history.clone())
            .await
            .expect("!TxHistoryDb::save_history");

        let actual_history = db
            .load_history("RICK", "RRnMcSeKiLrNdbp91qNVQwwXx5azD4S4CD")
            .await
            .expect("!TxHistoryDb::load_history");
        assert_eq!(actual_history, updated_history);

        db.clear("RICK", "RRnMcSeKiLrNdbp91qNVQwwXx5azD4S4CD")
            .await
            .expect("!TxHistoryDb::clear");

        let history = db
            .load_history("RICK", "RRnMcSeKiLrNdbp91qNVQwwXx5azD4S4CD")
            .await
            .expect("!TxHistoryDb::load_history");
        assert!(history.is_empty());
    }
}
