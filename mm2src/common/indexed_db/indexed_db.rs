//! The representation of [Indexed DB](https://developer.mozilla.org/en-US/docs/Web/API/IndexedDB_API)
//! based on low-level interface described in `db_driver.rs`.
//!
//! # Implementation
//!
//! Since the wrappers represented in `db_driver.rs` are not `Send`,
//! the implementation below initializes and spawns a `IdbDatabaseImpl` database instance locally
//! and communicate with it through the `mpsc` channel.

use crate::executor::spawn_local;
use crate::log::debug;
use crate::mm_error::prelude::*;
use async_trait::async_trait;
use derive_more::Display;
use futures::channel::{mpsc, oneshot};
use futures::StreamExt;
use rand::{thread_rng, Rng};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{self as json, Value as Json};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Mutex;

mod db_driver;
mod db_lock;

pub use db_driver::{DbTransactionError, DbTransactionResult, DbUpgrader, InitDbError, InitDbResult, ItemId,
                    OnUpgradeResult};
pub use db_lock::{get_or_initialize_db, ConstructibleDb, DbLocked};

use db_driver::{IdbDatabaseBuilder, IdbDatabaseImpl, IdbObjectStoreImpl, IdbTransactionImpl, OnUpgradeNeededCb};

type DbEventTx = mpsc::UnboundedSender<internal::DbEvent>;
type DbTransactionEventTx = mpsc::UnboundedSender<internal::DbTransactionEvent>;
type DbTableEventTx = mpsc::UnboundedSender<internal::DbTableEvent>;

/// The database namespace identifier.
/// This is used to distinguish the databases of one test from others.
#[derive(Clone, Copy, Display, PartialEq)]
pub enum DbNamespaceId {
    #[display(fmt = "MAIN")]
    Main,
    #[display(fmt = "TEST_{}", _0)]
    Test(u64),
}

impl Default for DbNamespaceId {
    fn default() -> Self { DbNamespaceId::Main }
}

impl DbNamespaceId {
    pub fn for_test() -> DbNamespaceId {
        let mut rng = thread_rng();
        DbNamespaceId::Test(rng.gen())
    }
}

#[derive(Clone, Display)]
#[display(fmt = "{}::{}", namespace_id, db_name)]
pub struct DbIdentifier {
    namespace_id: DbNamespaceId,
    db_name: &'static str,
}

impl DbIdentifier {
    pub fn db_name(&self) -> &'static str { self.db_name }

    pub fn main_namespace<Db: DbInstance>() -> DbIdentifier {
        DbIdentifier {
            namespace_id: DbNamespaceId::Main,
            db_name: Db::db_name(),
        }
    }

    pub fn for_test(db_name: &'static str) -> DbIdentifier {
        DbIdentifier {
            namespace_id: DbNamespaceId::for_test(),
            db_name,
        }
    }

    fn with_namespace<Db: DbInstance>(namespace_id: DbNamespaceId) -> DbIdentifier {
        DbIdentifier {
            namespace_id,
            db_name: Db::db_name(),
        }
    }
}

pub trait TableSignature: DeserializeOwned + Serialize + 'static {
    fn table_name() -> &'static str;

    fn on_upgrade_needed(upgrader: &DbUpgrader, old_version: u32, new_version: u32) -> OnUpgradeResult<()>;
}

#[async_trait]
pub trait DbInstance: Sized {
    fn db_name() -> &'static str;

    async fn init(db_id: DbIdentifier) -> InitDbResult<Self>;
}

pub struct IndexedDbBuilder {
    db_name: String,
    db_version: u32,
    tables: HashMap<String, OnUpgradeNeededCb>,
}

impl IndexedDbBuilder {
    pub fn new(db_id: DbIdentifier) -> IndexedDbBuilder {
        IndexedDbBuilder {
            db_name: db_id.to_string(),
            db_version: 1,
            tables: HashMap::new(),
        }
    }

    pub fn with_version(mut self, db_version: u32) -> IndexedDbBuilder {
        self.db_version = db_version;
        self
    }

    pub fn with_table<Table: TableSignature>(mut self) -> IndexedDbBuilder {
        let on_upgrade_needed_cb = Box::new(Table::on_upgrade_needed);
        self.tables.insert(Table::table_name().to_owned(), on_upgrade_needed_cb);
        self
    }

    pub async fn build(self) -> InitDbResult<IndexedDb> {
        let (init_tx, init_rx) = oneshot::channel();
        let (event_tx, event_rx) = mpsc::unbounded();

        self.init_and_spawn(init_tx, event_rx);
        init_rx.await.expect("The init channel must not be closed")?;
        Ok(IndexedDb { event_tx })
    }

    fn init_and_spawn(
        self,
        init_tx: oneshot::Sender<InitDbResult<()>>,
        event_rx: mpsc::UnboundedReceiver<internal::DbEvent>,
    ) {
        let fut = async move {
            let db = match IdbDatabaseBuilder::new(&self.db_name)
                .with_version(self.db_version)
                .with_tables(self.tables.into_iter())
                .build()
                .await
            {
                Ok(db) => db,
                Err(e) => {
                    // ignore if the receiver is closed
                    let _res = init_tx.send(Err(e));
                    return;
                },
            };

            // ignore if the receiver is closed
            let _res = init_tx.send(Ok(()));
            // run the event loop
            IndexedDb::event_loop(event_rx, db).await;
        };
        spawn_local(fut);
    }
}

pub struct IndexedDb {
    event_tx: DbEventTx,
}

async fn send_event_recv_response<Event, Result>(
    event_tx: &mpsc::UnboundedSender<Event>,
    event: Event,
    result_rx: oneshot::Receiver<DbTransactionResult<Result>>,
) -> DbTransactionResult<Result> {
    if let Err(e) = event_tx.unbounded_send(event) {
        let error = format!("Error sending event: {}", e);
        return MmError::err(DbTransactionError::UnexpectedState(error));
    }
    match result_rx.await {
        Ok(result) => result,
        Err(e) => {
            let error = format!("Error receiving result: {}", e);
            MmError::err(DbTransactionError::UnexpectedState(error))
        },
    }
}

impl IndexedDb {
    pub async fn transaction(&self) -> DbTransactionResult<DbTransaction<'_>> {
        let (result_tx, result_rx) = oneshot::channel();
        let event = internal::DbEvent::CreateTransaction { result_tx };
        let transaction_event_tx = send_event_recv_response(&self.event_tx, event, result_rx).await?;
        Ok(DbTransaction {
            event_tx: transaction_event_tx,
            phantom: PhantomData::default(),
        })
    }

    async fn event_loop(mut rx: mpsc::UnboundedReceiver<internal::DbEvent>, db: IdbDatabaseImpl) {
        while let Some(event) = rx.next().await {
            match event {
                internal::DbEvent::CreateTransaction { result_tx } => Self::create_transaction(&db, result_tx),
            }
        }
    }

    fn create_transaction(db: &IdbDatabaseImpl, result_tx: oneshot::Sender<DbTransactionResult<DbTransactionEventTx>>) {
        let transaction = match db.transaction() {
            Ok(transaction) => transaction,
            Err(e) => {
                // ignore if the receiver is closed
                result_tx.send(Err(e)).ok();
                return;
            },
        };
        let (transaction_event_tx, transaction_event_rx) = mpsc::unbounded();
        // spawn the event loop
        let fut = async move { DbTransaction::event_loop(transaction_event_rx, transaction).await };
        spawn_local(fut);
        // ignore if the receiver is closed
        result_tx.send(Ok(transaction_event_tx)).ok();
    }
}

pub struct DbTransaction<'a> {
    event_tx: DbTransactionEventTx,
    phantom: PhantomData<&'a ()>,
}

impl DbTransaction<'_> {
    pub async fn table<Table: TableSignature>(&self) -> DbTransactionResult<DbTable<'_, Table>> {
        let (result_tx, result_rx) = oneshot::channel();
        let event = internal::DbTransactionEvent::OpenTable {
            table_name: Table::table_name().to_owned(),
            result_tx,
        };
        let transaction_event_tx = send_event_recv_response(&self.event_tx, event, result_rx).await?;
        Ok(DbTable {
            event_tx: transaction_event_tx,
            phantom: PhantomData::default(),
        })
    }

    pub async fn aborted(&self) -> DbTransactionResult<bool> {
        let (result_tx, result_rx) = oneshot::channel();
        let event = internal::DbTransactionEvent::IsAborted { result_tx };
        send_event_recv_response(&self.event_tx, event, result_rx).await
    }

    pub async fn wait_for_complete(self) -> DbTransactionResult<()> {
        let (result_tx, result_rx) = oneshot::channel();
        let event = internal::DbTransactionEvent::WaitForComplete { result_tx };
        send_event_recv_response(&self.event_tx, event, result_rx).await
    }

    async fn event_loop(
        mut rx: mpsc::UnboundedReceiver<internal::DbTransactionEvent>,
        transaction: IdbTransactionImpl,
    ) {
        while let Some(event) = rx.next().await {
            match event {
                internal::DbTransactionEvent::OpenTable { table_name, result_tx } => {
                    Self::open_table(&transaction, table_name, result_tx).await
                },
                internal::DbTransactionEvent::IsAborted { result_tx } => {
                    result_tx.send(Ok(transaction.aborted())).ok();
                },
                internal::DbTransactionEvent::WaitForComplete { result_tx } => {
                    let res = transaction.wait_for_complete().await;
                    result_tx.send(res).ok();
                    return;
                },
            }
        }
    }

    async fn open_table(
        transaction: &IdbTransactionImpl,
        table_name: String,
        result_tx: oneshot::Sender<DbTransactionResult<mpsc::UnboundedSender<internal::DbTableEvent>>>,
    ) {
        let table = match transaction.open_table(&table_name) {
            Ok(table) => table,
            Err(e) => {
                // ignore if the receiver is closed
                result_tx.send(Err(e)).ok();
                return;
            },
        };
        let (table_event_tx, table_event_rx) = mpsc::unbounded();
        let fut = async move { table_event_loop(table_event_rx, table).await };
        spawn_local(fut);
        // ignore if the receiver is closed
        result_tx.send(Ok(table_event_tx)).ok();
    }
}

pub struct DbTable<'a, Table: TableSignature> {
    event_tx: DbTableEventTx,
    phantom: PhantomData<&'a Table>,
}

impl<Table: TableSignature> DbTable<'_, Table> {
    pub async fn add_item(&self, item: &Table) -> DbTransactionResult<ItemId> {
        let item = json::to_value(&item).map_to_mm(|e| DbTransactionError::ErrorSerializingItem(e.to_string()))?;

        let (result_tx, result_rx) = oneshot::channel();
        let event = internal::DbTableEvent::AddItem { item, result_tx };
        send_event_recv_response(&self.event_tx, event, result_rx).await
    }

    pub async fn get_items<Value>(&self, index: &str, index_value: Value) -> DbTransactionResult<Vec<(ItemId, Table)>>
    where
        Value: Serialize,
    {
        let (result_tx, result_rx) = oneshot::channel();
        let index_value = json::to_value(index_value).map_to_mm(|e| DbTransactionError::ErrorSerializingIndex {
            index: index.to_owned(),
            description: e.to_string(),
        })?;
        let event = internal::DbTableEvent::GetItems {
            index: index.to_owned(),
            index_value,
            result_tx,
        };
        send_event_recv_response(&self.event_tx, event, result_rx)
            .await
            .and_then(|items| Self::deserialize_items(items))
    }

    pub async fn get_item_ids<Value>(&self, index: &str, index_value: Value) -> DbTransactionResult<Vec<ItemId>>
    where
        Value: Serialize,
    {
        let (result_tx, result_rx) = oneshot::channel();
        let index_value = json::to_value(index_value).map_to_mm(|e| DbTransactionError::ErrorSerializingIndex {
            index: index.to_owned(),
            description: e.to_string(),
        })?;
        let event = internal::DbTableEvent::GetItemIds {
            index: index.to_owned(),
            index_value,
            result_tx,
        };
        send_event_recv_response(&self.event_tx, event, result_rx).await
    }

    pub async fn get_all_items(&self) -> DbTransactionResult<Vec<(ItemId, Table)>> {
        let (result_tx, result_rx) = oneshot::channel();
        let event = internal::DbTableEvent::GetAllItems { result_tx };
        send_event_recv_response(&self.event_tx, event, result_rx)
            .await
            .and_then(|items| Self::deserialize_items(items))
    }

    pub async fn replace_item(&self, item_id: ItemId, item: &Table) -> DbTransactionResult<ItemId> {
        let item = json::to_value(item).map_to_mm(|e| DbTransactionError::ErrorSerializingItem(e.to_string()))?;

        let (result_tx, result_rx) = oneshot::channel();
        let event = internal::DbTableEvent::ReplaceItem {
            item_id,
            item,
            result_tx,
        };
        send_event_recv_response(&self.event_tx, event, result_rx).await
    }

    pub async fn delete_item(&self, item_id: ItemId) -> DbTransactionResult<()> {
        let (result_tx, result_rx) = oneshot::channel();
        let event = internal::DbTableEvent::DeleteItem { item_id, result_tx };
        send_event_recv_response(&self.event_tx, event, result_rx).await
    }

    pub async fn clear(&self) -> DbTransactionResult<()> {
        let (result_tx, result_rx) = oneshot::channel();
        let event = internal::DbTableEvent::Clear { result_tx };
        send_event_recv_response(&self.event_tx, event, result_rx).await
    }

    pub async fn aborted(&self) -> DbTransactionResult<bool> {
        let (result_tx, result_rx) = oneshot::channel();
        let event = internal::DbTableEvent::IsAborted { result_tx };
        send_event_recv_response(&self.event_tx, event, result_rx).await
    }

    fn deserialize_items(items: Vec<(ItemId, Json)>) -> DbTransactionResult<Vec<(ItemId, Table)>> {
        items
            .into_iter()
            .map(|(item_id, item)| {
                let item: Table =
                    json::from_value(item).map_to_mm(|e| DbTransactionError::ErrorDeserializingItem(e.to_string()))?;
                Ok((item_id, item))
            })
            .collect()
    }
}

/// This event loop cannot be part of the `DbTable`, because the `Table` type parameter is not known when this function is called.
async fn table_event_loop(mut rx: mpsc::UnboundedReceiver<internal::DbTableEvent>, table: IdbObjectStoreImpl) {
    while let Some(event) = rx.next().await {
        match event {
            internal::DbTableEvent::AddItem { item, result_tx } => {
                let res = table.add_item(&item).await;
                result_tx.send(res).ok();
            },
            internal::DbTableEvent::GetItems {
                index,
                index_value,
                result_tx,
            } => {
                let res = table.get_items(&index, index_value).await;
                result_tx.send(res).ok();
            },
            internal::DbTableEvent::GetItemIds {
                index,
                index_value,
                result_tx,
            } => {
                let res = table.get_item_ids(&index, index_value).await;
                result_tx.send(res).ok();
            },
            internal::DbTableEvent::GetAllItems { result_tx } => {
                let res = table.get_all_items().await;
                result_tx.send(res).ok();
            },
            internal::DbTableEvent::ReplaceItem {
                item_id,
                item,
                result_tx,
            } => {
                let res = table.replace_item(item_id, item).await;
                result_tx.send(res).ok();
            },
            internal::DbTableEvent::DeleteItem { item_id, result_tx } => {
                let res = table.delete_item(item_id).await;
                result_tx.send(res).ok();
            },
            internal::DbTableEvent::Clear { result_tx } => {
                let res = table.clear().await;
                result_tx.send(res).ok();
            },
            internal::DbTableEvent::IsAborted { result_tx } => {
                result_tx.send(Ok(table.aborted())).ok();
            },
        }
    }
}

/// Internal events.
mod internal {
    use super::*;

    pub(super) enum DbEvent {
        CreateTransaction {
            result_tx: oneshot::Sender<DbTransactionResult<DbTransactionEventTx>>,
        },
    }

    pub(super) enum DbTransactionEvent {
        OpenTable {
            table_name: String,
            result_tx: oneshot::Sender<DbTransactionResult<mpsc::UnboundedSender<DbTableEvent>>>,
        },
        IsAborted {
            result_tx: oneshot::Sender<DbTransactionResult<bool>>,
        },
        WaitForComplete {
            result_tx: oneshot::Sender<DbTransactionResult<()>>,
        },
    }

    pub(super) enum DbTableEvent {
        AddItem {
            item: Json,
            result_tx: oneshot::Sender<DbTransactionResult<ItemId>>,
        },
        GetItems {
            index: String,
            index_value: Json,
            result_tx: oneshot::Sender<DbTransactionResult<Vec<(ItemId, Json)>>>,
        },
        GetItemIds {
            index: String,
            index_value: Json,
            result_tx: oneshot::Sender<DbTransactionResult<Vec<ItemId>>>,
        },
        GetAllItems {
            result_tx: oneshot::Sender<DbTransactionResult<Vec<(ItemId, Json)>>>,
        },
        ReplaceItem {
            item_id: ItemId,
            item: Json,
            result_tx: oneshot::Sender<DbTransactionResult<ItemId>>,
        },
        DeleteItem {
            item_id: ItemId,
            result_tx: oneshot::Sender<DbTransactionResult<()>>,
        },
        Clear {
            result_tx: oneshot::Sender<DbTransactionResult<()>>,
        },
        IsAborted {
            result_tx: oneshot::Sender<DbTransactionResult<bool>>,
        },
    }
}

mod tests {
    use super::*;
    use crate::for_tests::register_wasm_log;
    use crate::log::LogLevel;
    use wasm_bindgen_test::*;

    wasm_bindgen_test_configure!(run_in_browser);

    #[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
    #[serde(deny_unknown_fields)]
    struct TxTable {
        ticker: String,
        tx_hash: String,
        block_height: u64,
    }

    impl TableSignature for TxTable {
        fn table_name() -> &'static str { "tx_table" }

        fn on_upgrade_needed(upgrader: &DbUpgrader, old_version: u32, _new_version: u32) -> OnUpgradeResult<()> {
            if old_version > 0 {
                // the table is initialized already
                return Ok(());
            }
            let table_upgrader = upgrader.create_table("tx_table")?;
            table_upgrader.create_index("ticker", false)?;
            table_upgrader.create_index("tx_hash", true)
        }
    }

    #[wasm_bindgen_test]
    async fn test_add_get_item() {
        const DB_NAME: &str = "TEST_ADD_GET_ITEM";
        const DB_VERSION: u32 = 1;

        let rick_tx_1 = TxTable {
            ticker: "RICK".to_owned(),
            tx_hash: "0a0fda88364b960000f445351fe7678317a1e0c80584de0413377ede00ba696f".to_owned(),
            block_height: 10000,
        };
        let rick_tx_2 = TxTable {
            ticker: "RICK".to_owned(),
            tx_hash: "ba881ecca15b5d4593f14f25debbcdfe25f101fd2e9cf8d0b5d92d19813d4424".to_owned(),
            block_height: 10000,
        };
        let morty_tx_1 = TxTable {
            ticker: "MORTY".to_owned(),
            tx_hash: "1fc789133239260ed16361190a026a88cab2243935f02f1ccd794f1d06a22246".to_owned(),
            block_height: 20000,
        };

        register_wasm_log(LogLevel::Debug);

        let db = IndexedDbBuilder::new(DbIdentifier::for_test(DB_NAME))
            .with_version(DB_VERSION)
            .with_table::<TxTable>()
            .build()
            .await
            .expect("!IndexedDb::init");
        let transaction = db.transaction().await.expect("!IndexedDb::transaction()");
        let table = transaction
            .table::<TxTable>()
            .await
            .expect("!DbTransaction::open_table");

        let rick_tx_1_id = table
            .add_item(&rick_tx_1)
            .await
            .expect("!Couldn't add a 'RICK' transaction");
        let rick_tx_2_id = table
            .add_item(&rick_tx_2)
            .await
            .expect("!Couldn't add a 'RICK' transaction with the different 'tx_hash'");
        let morty_tx_1_id = table
            .add_item(&morty_tx_1)
            .await
            .expect("!Couldn't add a 'MORTY' transaction");
        assert!(rick_tx_1_id != rick_tx_2_id && rick_tx_2_id != morty_tx_1_id);

        let actual_rick_txs = table
            .get_items("ticker", "RICK")
            .await
            .expect("!Couldn't get items by the index 'ticker=RICK'");
        let expected_rick_txs = vec![(rick_tx_1_id, rick_tx_1), (rick_tx_2_id, rick_tx_2.clone())];
        assert_eq!(actual_rick_txs, expected_rick_txs);

        let actual_rick_tx_ids = table
            .get_item_ids("ticker", "RICK")
            .await
            .expect("Couldn't get item ids by the index 'ticker=RICK'");
        let expected_rick_tx_ids = vec![rick_tx_1_id, rick_tx_2_id];
        assert_eq!(actual_rick_tx_ids, expected_rick_tx_ids);

        let actual_rick_2_tx = table
            .get_items(
                "tx_hash",
                "ba881ecca15b5d4593f14f25debbcdfe25f101fd2e9cf8d0b5d92d19813d4424",
            )
            .await
            .expect("!Couldn't get items by the index 'tx_hash'");
        let expected_rick_txs = vec![(rick_tx_2_id, rick_tx_2)];
        assert_eq!(actual_rick_2_tx, expected_rick_txs);
    }

    #[wasm_bindgen_test]
    async fn test_replace_item() {
        const DB_NAME: &str = "TEST_REPLACE_ITEM";
        const DB_VERSION: u32 = 1;

        let rick_tx_1 = TxTable {
            ticker: "RICK".to_owned(),
            tx_hash: "0a0fda88364b960000f445351fe7678317a1e0c80584de0413377ede00ba696f".to_owned(),
            block_height: 10000,
        };
        let rick_tx_2 = TxTable {
            ticker: "RICK".to_owned(),
            tx_hash: "ba881ecca15b5d4593f14f25debbcdfe25f101fd2e9cf8d0b5d92d19813d4424".to_owned(),
            block_height: 10000,
        };
        let rick_tx_1_updated = TxTable {
            ticker: "RICK".to_owned(),
            tx_hash: "0a0fda88364b960000f445351fe7678317a1e0c80584de0413377ede00ba696f".to_owned(),
            block_height: 20000,
        };

        register_wasm_log(LogLevel::Debug);

        let db = IndexedDbBuilder::new(DbIdentifier::for_test(DB_NAME))
            .with_version(DB_VERSION)
            .with_table::<TxTable>()
            .build()
            .await
            .expect("!IndexedDb::init");
        let transaction = db.transaction().await.expect("!IndexedDb::transaction()");
        let table = transaction
            .table::<TxTable>()
            .await
            .expect("!DbTransaction::open_table");

        let rick_tx_1_id = table.add_item(&rick_tx_1).await.expect("Couldn't add an item");
        let rick_tx_2_id = table.add_item(&rick_tx_2).await.expect("Couldn't add an item");

        // Wait for the transaction to complete to save the changes to the database.
        transaction
            .wait_for_complete()
            .await
            .expect("Error waiting for the transaction to complete");

        // Open new transaction.
        let transaction = db.transaction().await.expect("!IndexedDb::transaction()");
        let table = transaction
            .table::<TxTable>()
            .await
            .expect("!DbTransaction::open_table");

        // Try to add the updated RICK tx item with the same [`TxTable::tx_hash`].
        // [`TxTable::tx_hash`] is a unique index, so this operation must fail.
        let err = table
            .add_item(&rick_tx_1_updated)
            .await
            .expect_err("'DbTable::add_item' should have failed");
        match err.into_inner() {
            DbTransactionError::ErrorUploadingItem(err) => debug!("error: {}", err),
            e => panic!("Expected 'DbTransactionError::ErrorUploadingItem', found: {:?}", e),
        }
        assert_eq!(table.aborted().await, Ok(true));
        assert_eq!(transaction.aborted().await, Ok(true));

        // But we should be able to replace the updated item.
        // Since the last operation failed, we have to reopen the transaction.
        let transaction = db.transaction().await.expect("!IndexedDb::transaction()");
        let table = transaction
            .table::<TxTable>()
            .await
            .expect("!DbTransaction::open_table");

        let rick_tx_1_updated_id = table
            .replace_item(rick_tx_1_id, &rick_tx_1_updated)
            .await
            .expect("!Couldn't replace an item");
        assert_eq!(rick_tx_1_updated_id, rick_tx_1_id);

        let actual_rick_txs = table
            .get_items("ticker", "RICK")
            .await
            .expect("Couldn't get items by the index 'ticker=RICK'");
        assert_eq!(actual_rick_txs, vec![
            (rick_tx_1_id, rick_tx_1_updated),
            (rick_tx_2_id, rick_tx_2)
        ]);
    }

    #[wasm_bindgen_test]
    async fn test_delete_item() {
        const DB_NAME: &str = "TEST_DELETE_ITEM";
        const DB_VERSION: u32 = 1;

        let rick_tx_1 = TxTable {
            ticker: "RICK".to_owned(),
            tx_hash: "0a0fda88364b960000f445351fe7678317a1e0c80584de0413377ede00ba696f".to_owned(),
            block_height: 10000,
        };
        let rick_tx_2 = TxTable {
            ticker: "RICK".to_owned(),
            tx_hash: "ba881ecca15b5d4593f14f25debbcdfe25f101fd2e9cf8d0b5d92d19813d4424".to_owned(),
            block_height: 10000,
        };

        register_wasm_log(LogLevel::Debug);

        let db = IndexedDbBuilder::new(DbIdentifier::for_test(DB_NAME))
            .with_version(DB_VERSION)
            .with_table::<TxTable>()
            .build()
            .await
            .expect("!IndexedDb::init");
        let transaction = db.transaction().await.expect("!IndexedDb::transaction()");
        let table = transaction
            .table::<TxTable>()
            .await
            .expect("!DbTransaction::open_table");

        let rick_tx_1_id = table.add_item(&rick_tx_1).await.expect("Couldn't add an item");
        let rick_tx_2_id = table.add_item(&rick_tx_2).await.expect("Couldn't add an item");

        table.delete_item(rick_tx_1_id).await.expect("Couldn't delete an item");

        let actual_rick_txs = table
            .get_items("ticker", "RICK")
            .await
            .expect("Couldn't get items by the index 'ticker=RICK'");
        assert_eq!(actual_rick_txs, vec![(rick_tx_2_id, rick_tx_2)]);
    }

    #[wasm_bindgen_test]
    async fn test_clear() {
        const DB_NAME: &str = "TEST_CLEAR";
        const DB_VERSION: u32 = 1;

        let rick_tx_1 = TxTable {
            ticker: "RICK".to_owned(),
            tx_hash: "0a0fda88364b960000f445351fe7678317a1e0c80584de0413377ede00ba696f".to_owned(),
            block_height: 10000,
        };
        let rick_tx_2 = TxTable {
            ticker: "RICK".to_owned(),
            tx_hash: "ba881ecca15b5d4593f14f25debbcdfe25f101fd2e9cf8d0b5d92d19813d4424".to_owned(),
            block_height: 10000,
        };

        register_wasm_log(LogLevel::Debug);

        let db = IndexedDbBuilder::new(DbIdentifier::for_test(DB_NAME))
            .with_version(DB_VERSION)
            .with_table::<TxTable>()
            .build()
            .await
            .expect("!IndexedDb::init");
        let transaction = db.transaction().await.expect("!IndexedDb::transaction()");
        let table = transaction
            .table::<TxTable>()
            .await
            .expect("!DbTransaction::open_table");

        let _rick_tx_1_id = table.add_item(&rick_tx_1).await.expect("Couldn't add an item");
        let _rick_tx_2_id = table.add_item(&rick_tx_2).await.expect("Couldn't add an item");

        table.clear().await.expect("Couldn't clear the database");

        let actual_rick_txs = table
            .get_items("ticker", "RICK")
            .await
            .expect("Couldn't get items by the index 'ticker=RICK'");
        assert!(actual_rick_txs.is_empty());
    }

    #[wasm_bindgen_test]
    async fn test_upgrade_needed() {
        const DB_NAME: &str = "TEST_UPGRADE_NEEDED";

        lazy_static! {
            static ref LAST_VERSIONS: Mutex<Option<(u32, u32)>> = Mutex::new(None);
        }

        #[derive(Serialize, Deserialize)]
        struct UpgradableTable;

        impl TableSignature for UpgradableTable {
            fn table_name() -> &'static str { "upgradable_table" }

            fn on_upgrade_needed(upgrader: &DbUpgrader, old_version: u32, new_version: u32) -> OnUpgradeResult<()> {
                let mut versions = LAST_VERSIONS.lock().expect("!old_new_versions.lock()");
                *versions = Some((old_version, new_version));

                match (old_version, new_version) {
                    (0, 1) => {
                        let table = upgrader.create_table("upgradable_table")?;
                        table.create_index("first_index", false)?;
                    },
                    (0, 2) => {
                        let table = upgrader.create_table("upgradable_table")?;
                        table.create_index("first_index", false)?;
                        table.create_index("second_index", false)?;
                    },
                    (1, 2) => {
                        let table = upgrader.open_table("upgradable_table")?;
                        table.create_index("second_index", false)?;
                    },
                    v => panic!("Unexpected old, new versions: {:?}", v),
                }
                Ok(())
            }
        }

        async fn init_and_check(
            db_identifier: DbIdentifier,
            version: u32,
            expected_old_new_versions: Option<(u32, u32)>,
        ) -> Result<(), String> {
            let mut versions = LAST_VERSIONS.lock().expect("!LAST_VERSIONS.lock()");
            *versions = None;
            drop(versions);

            let _db = IndexedDbBuilder::new(db_identifier)
                .with_version(version)
                .with_table::<UpgradableTable>()
                .build()
                .await
                .map_err(|e| format!("{}", e))?;

            let actual_versions = LAST_VERSIONS.lock().unwrap();
            if *actual_versions == expected_old_new_versions {
                Ok(())
            } else {
                Err(format!(
                    "Expected {:?}, found {:?}",
                    expected_old_new_versions, actual_versions
                ))
            }
        }

        register_wasm_log(LogLevel::Debug);

        let db_identifier = DbIdentifier::for_test(DB_NAME);

        init_and_check(db_identifier.clone(), 1, Some((0, 1))).await.unwrap();
        init_and_check(db_identifier.clone(), 2, Some((1, 2))).await.unwrap();
        // the same 2 version, `on_upgrade_needed` must not be called
        init_and_check(db_identifier, 2, None).await.unwrap();
    }

    #[wasm_bindgen_test]
    async fn test_open_twice() {
        const DB_NAME: &str = "TEST_OPEN_TWICE";
        const DB_VERSION: u32 = 1;

        register_wasm_log(LogLevel::Debug);
        let db_identifier = DbIdentifier::for_test(DB_NAME);

        let _db = IndexedDbBuilder::new(db_identifier.clone())
            .with_version(DB_VERSION)
            .with_table::<TxTable>()
            .build()
            .await
            .expect("!IndexedDb::init first time");

        match IndexedDbBuilder::new(db_identifier.clone())
            .with_version(DB_VERSION + 1)
            .with_table::<TxTable>()
            .build()
            .await
        {
            Ok(_) => panic!("!IndexedDb::init should have failed"),
            Err(e) => assert_eq!(e.into_inner(), InitDbError::DbIsOpenAlready {
                db_name: db_identifier.to_string()
            }),
        }
    }

    #[wasm_bindgen_test]
    async fn test_open_close_and_open() {
        const DB_NAME: &str = "TEST_OPEN_CLOSE_AND_OPEN";
        const DB_VERSION: u32 = 1;

        register_wasm_log(LogLevel::Debug);
        let db_identifier = DbIdentifier::for_test(DB_NAME);

        let db = IndexedDbBuilder::new(db_identifier.clone())
            .with_version(DB_VERSION)
            .with_table::<TxTable>()
            .build()
            .await
            .expect("!IndexedDb::init first time");
        drop(db);

        let _db = IndexedDbBuilder::new(db_identifier)
            .with_version(DB_VERSION)
            .with_table::<TxTable>()
            .build()
            .await
            .expect("!IndexedDb::init second time");
    }

    #[wasm_bindgen_test]
    async fn test_non_string_index() {
        const DB_NAME: &str = "TEST_NON_STRING_INDEX";
        const DB_VERSION: u32 = 1;

        #[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
        struct Uuid(Vec<u64>);

        #[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
        #[serde(deny_unknown_fields)]
        struct SwapTable {
            swap_uuid: Uuid,
            started_at: u64,
            some_data: String,
        }

        impl TableSignature for SwapTable {
            fn table_name() -> &'static str { "swap_table" }

            fn on_upgrade_needed(upgrader: &DbUpgrader, old_version: u32, _new_version: u32) -> OnUpgradeResult<()> {
                if old_version > 0 {
                    // the table is initialized already
                    return Ok(());
                }
                let table_upgrader = upgrader.create_table("swap_table")?;
                table_upgrader.create_index("swap_uuid", false)?;
                table_upgrader.create_index("started_at", false)
            }
        }

        register_wasm_log(LogLevel::Debug);

        let swap_1 = SwapTable {
            swap_uuid: Uuid(vec![1, 2, 3]),
            started_at: 123,
            some_data: "Some data 1".to_owned(),
        };
        let swap_2 = SwapTable {
            swap_uuid: Uuid(vec![3, 2, 1]),
            started_at: 321,
            some_data: "Some data 2".to_owned(),
        };

        let db = IndexedDbBuilder::new(DbIdentifier::for_test(DB_NAME))
            .with_version(DB_VERSION)
            .with_table::<SwapTable>()
            .build()
            .await
            .expect("!IndexedDb::init");
        let transaction = db.transaction().await.expect("!IndexedDb::transaction()");
        let table = transaction
            .table::<SwapTable>()
            .await
            .expect("!DbTransaction::open_table");

        let swap_1_id = table.add_item(&swap_1).await.expect("Couldn't add an item");
        let swap_2_id = table.add_item(&swap_2).await.expect("Couldn't add an item");

        let actual = table
            .get_items("swap_uuid", vec![3, 2, 1])
            .await
            .expect("Couldn't get items");
        assert_eq!(actual, vec![(swap_2_id, swap_2)]);

        let actual = table.get_items("started_at", 123).await.expect("Couldn't get items");
        assert_eq!(actual, vec![(swap_1_id, swap_1)]);
    }
}
