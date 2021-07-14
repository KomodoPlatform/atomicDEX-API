//! The representation of [Indexed DB](https://developer.mozilla.org/en-US/docs/Web/API/IndexedDB_API).
//! This module consists of low-level Rust wrappers over basic JS structures like `IDBDatabase`, `IDBTransaction`, `IDBObjectStore` etc.
//!
//! # Usage
//!
//! Since the wrappers represented below are not `Send`, it's strongly recommended NOT to use them directly.
//! Please consider using a higher-level interface from `indexed_db.rs`.

use crate::log::{debug, error};
use crate::mm_error::prelude::*;
use crate::stringify_js_error;
use derive_more::Display;
use futures::channel::{mpsc, oneshot};
use futures::StreamExt;
use js_sys::Array;
use serde_json::Value as Json;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::iter::IntoIterator;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use web_sys::{IdbDatabase, IdbIndexParameters, IdbObjectStore, IdbObjectStoreParameters, IdbOpenDbRequest, IdbRequest,
              IdbTransaction, IdbTransactionMode, IdbVersionChangeEvent};

lazy_static! {
    static ref OPEN_DATABASES: Mutex<HashSet<String>> = Mutex::new(HashSet::new());
}

const ITEM_KEY_PATH: &str = "_item_id";

pub type ItemId = u32;
pub type OnUpgradeResult<T> = Result<T, MmError<OnUpgradeError>>;
pub type InitDbResult<T> = Result<T, MmError<InitDbError>>;
pub type DbTransactionResult<T> = Result<T, MmError<DbTransactionError>>;

pub(super) type OnUpgradeNeededCb = Box<dyn FnOnce(&DbUpgrader, u32, u32) -> OnUpgradeResult<()> + Send>;

#[derive(Debug, Display, PartialEq)]
pub enum InitDbError {
    #[display(fmt = "Cannot initialize a Database without tables")]
    EmptyTableList,
    #[display(fmt = "Database '{}' is open already", db_name)]
    DbIsOpenAlready { db_name: String },
    #[display(fmt = "It seems this browser doesn't support 'IndexedDb': {}", _0)]
    NotSupported(String),
    #[display(fmt = "Invalid Database version: {}", _0)]
    InvalidVersion(String),
    #[display(fmt = "Couldn't open Database: {}", _0)]
    OpeningError(String),
    #[display(fmt = "Type mismatch: expected '{}', found '{}'", expected, found)]
    TypeMismatch { expected: String, found: String },
    #[display(fmt = "Error occurred due to an unexpected state: {:?}", _0)]
    UnexpectedState(String),
    #[display(
        fmt = "Error occurred due to the Database upgrading from {} to {} version: {}",
        old_version,
        new_version,
        error
    )]
    UpgradingError {
        old_version: u32,
        new_version: u32,
        error: OnUpgradeError,
    },
}

#[derive(Debug, Display, PartialEq)]
pub enum OnUpgradeError {
    #[display(fmt = "Error occurred due to creating the '{}' table: {}", table, description)]
    ErrorCreatingTable { table: String, description: String },
    #[display(fmt = "Error occurred due to opening the '{}' table: {}", table, description)]
    ErrorOpeningTable { table: String, description: String },
    #[display(fmt = "Error occurred due to creating the '{}' index: {}", index, description)]
    ErrorCreatingIndex { index: String, description: String },
}

#[derive(Debug, Display, PartialEq)]
pub enum DbTransactionError {
    #[display(fmt = "No such table '{}'", table)]
    NoSuchTable { table: String },
    #[display(fmt = "Error creating DbTransaction: {:?}", _0)]
    ErrorCreatingTransaction(String),
    #[display(fmt = "Error opening the '{}' table: {}", table, description)]
    ErrorOpeningTable { table: String, description: String },
    #[display(fmt = "Error serializing the '{}' index: {:?}", index, description)]
    ErrorSerializingIndex { index: String, description: String },
    #[display(fmt = "Error serializing an item: {:?}", _0)]
    ErrorSerializingItem(String),
    #[display(fmt = "Error deserializing an item: {:?}", _0)]
    ErrorDeserializingItem(String),
    #[display(fmt = "Error uploading an item: {:?}", _0)]
    ErrorUploadingItem(String),
    #[display(fmt = "Error getting items: {:?}", _0)]
    ErrorGettingItems(String),
    #[display(fmt = "Error deleting items: {:?}", _0)]
    ErrorDeletingItems(String),
    #[display(fmt = "No such index '{}'", index)]
    NoSuchIndex { index: String },
    #[display(fmt = "Invalid index '{}:{}': {:?}", index, index_value, description)]
    InvalidIndex {
        index: String,
        index_value: Json,
        description: String,
    },
    #[display(fmt = "Error occurred due to an unexpected state: {:?}", _0)]
    UnexpectedState(String),
    #[display(fmt = "Transaction was aborted")]
    TransactionAborted,
}

pub(super) struct IdbDatabaseBuilder {
    db_name: String,
    db_version: u32,
    tables: HashMap<String, OnUpgradeNeededCb>,
}

impl IdbDatabaseBuilder {
    pub fn new(db_name: &str) -> IdbDatabaseBuilder {
        IdbDatabaseBuilder {
            db_name: db_name.to_owned(),
            db_version: 1,
            tables: HashMap::new(),
        }
    }

    pub fn with_version(mut self, db_version: u32) -> IdbDatabaseBuilder {
        self.db_version = db_version;
        self
    }

    pub fn with_tables<Tables>(mut self, tables: Tables) -> IdbDatabaseBuilder
    where
        Tables: IntoIterator<Item = (String, OnUpgradeNeededCb)>,
    {
        self.tables.extend(tables);
        self
    }

    pub async fn build(self) -> InitDbResult<IdbDatabaseImpl> {
        Self::check_if_db_is_not_open(&self.db_name)?;
        let (table_names, on_upgrade_needed_handlers) = Self::tables_into_parts(self.tables)?;
        debug!("Open '{}' database with tables: {:?}", self.db_name, table_names);

        let window = web_sys::window().expect("!window");
        let indexed_db = match window.indexed_db() {
            Ok(Some(db)) => db,
            Ok(None) => return MmError::err(InitDbError::NotSupported("Unknown error".to_owned())),
            Err(e) => return MmError::err(InitDbError::NotSupported(stringify_js_error(&e))),
        };

        let db_request = match indexed_db.open_with_u32(&self.db_name, self.db_version) {
            Ok(r) => r,
            Err(e) => return MmError::err(InitDbError::InvalidVersion(stringify_js_error(&e))),
        };
        let (tx, mut rx) = mpsc::channel(1);

        let onerror_closure = construct_event_closure(DbOpenEvent::Failed, tx.clone());
        let onsuccess_closure = construct_event_closure(DbOpenEvent::Success, tx.clone());
        let onupgradeneeded_closure = construct_event_closure(DbOpenEvent::UpgradeNeeded, tx.clone());

        db_request.set_onerror(Some(onerror_closure.as_ref().unchecked_ref()));
        db_request.set_onsuccess(Some(onsuccess_closure.as_ref().unchecked_ref()));
        db_request.set_onupgradeneeded(Some(onupgradeneeded_closure.as_ref().unchecked_ref()));

        let mut on_upgrade_needed_handlers = Some(on_upgrade_needed_handlers);
        while let Some(event) = rx.next().await {
            match event {
                DbOpenEvent::Failed(e) => return MmError::err(InitDbError::OpeningError(stringify_js_error(&e))),
                DbOpenEvent::UpgradeNeeded(event) => {
                    Self::on_upgrade_needed(event, &db_request, &mut on_upgrade_needed_handlers)?
                },
                DbOpenEvent::Success(_) => {
                    let db = Self::get_db_from_request(&db_request)?;
                    Self::cache_open_db(self.db_name.clone());

                    return Ok(IdbDatabaseImpl {
                        db,
                        db_name: self.db_name,
                        tables: table_names,
                    });
                },
            }
        }
        unreachable!("The event channel must not be closed before either 'DbOpenEvent::Success' or 'DbOpenEvent::Failed' is received");
    }

    fn on_upgrade_needed(
        event: JsValue,
        db_request: &IdbOpenDbRequest,
        handlers: &mut Option<Vec<OnUpgradeNeededCb>>,
    ) -> InitDbResult<()> {
        let handlers = match handlers.take() {
            Some(handlers) => handlers,
            None => {
                return MmError::err(InitDbError::UnexpectedState(
                    "'IndexedDbBuilder::on_upgraded_needed' was called twice".to_owned(),
                ))
            },
        };

        let db = Self::get_db_from_request(&db_request)?;
        let transaction = Self::get_transaction_from_request(&db_request)?;

        let version_event = match event.dyn_into::<IdbVersionChangeEvent>() {
            Ok(version) => version,
            Err(e) => {
                return MmError::err(InitDbError::TypeMismatch {
                    expected: "IdbVersionChangeEvent".to_owned(),
                    found: format!("{:?}", e),
                })
            },
        };
        let old_version = version_event.old_version() as u32;
        let new_version = version_event
            .new_version()
            .ok_or(MmError::new(InitDbError::InvalidVersion(
                "Expected a new_version".to_owned(),
            )))? as u32;

        let upgrader = DbUpgrader { db, transaction };
        for on_upgrade_needed_cb in handlers {
            on_upgrade_needed_cb(&upgrader, old_version, new_version).mm_err(|error| InitDbError::UpgradingError {
                old_version,
                new_version,
                error,
            })?;
        }
        Ok(())
    }

    fn cache_open_db(db_name: String) {
        let mut open_databases = OPEN_DATABASES.lock().expect("!OPEN_DATABASES.lock()");
        open_databases.insert(db_name);
    }

    fn check_if_db_is_not_open(db_name: &str) -> InitDbResult<()> {
        let open_databases = OPEN_DATABASES.lock().expect("!OPEN_DATABASES.lock()");
        if open_databases.contains(db_name) {
            MmError::err(InitDbError::DbIsOpenAlready {
                db_name: db_name.to_owned(),
            })
        } else {
            Ok(())
        }
    }

    fn get_db_from_request(db_request: &IdbOpenDbRequest) -> InitDbResult<IdbDatabase> {
        let db_result = match db_request.result() {
            Ok(res) => res,
            Err(e) => return MmError::err(InitDbError::UnexpectedState(stringify_js_error(&e))),
        };
        db_result.dyn_into::<IdbDatabase>().map_err(|db_result| {
            MmError::new(InitDbError::TypeMismatch {
                expected: "IdbDatabase".to_owned(),
                found: format!("{:?}", db_result),
            })
        })
    }

    fn get_transaction_from_request(db_request: &IdbOpenDbRequest) -> InitDbResult<IdbTransaction> {
        let transaction = match db_request.transaction() {
            Some(res) => res,
            None => {
                return MmError::err(InitDbError::UnexpectedState(
                    "Expected 'IdbOpenDbRequest::transaction'".to_owned(),
                ))
            },
        };
        transaction.dyn_into::<IdbTransaction>().map_err(|transaction| {
            MmError::new(InitDbError::TypeMismatch {
                expected: "IdbTransaction".to_owned(),
                found: format!("{:?}", transaction),
            })
        })
    }

    fn tables_into_parts(
        tables: HashMap<String, OnUpgradeNeededCb>,
    ) -> InitDbResult<(HashSet<String>, Vec<OnUpgradeNeededCb>)> {
        if tables.is_empty() {
            return MmError::err(InitDbError::EmptyTableList);
        }

        let mut table_names = HashSet::with_capacity(tables.len());
        let mut on_upgrade_needed_handlers = Vec::with_capacity(tables.len());
        for (table_name, handler) in tables {
            table_names.insert(table_name);
            on_upgrade_needed_handlers.push(handler);
        }
        Ok((table_names, on_upgrade_needed_handlers))
    }
}

pub(super) struct IdbDatabaseImpl {
    db: IdbDatabase,
    db_name: String,
    tables: HashSet<String>,
}

impl !Send for IdbDatabaseImpl {}

impl fmt::Debug for IdbDatabaseImpl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "IndexedDb {{ db_name: {:?}, tables: {:?} }}",
            self.db_name, self.tables
        )
    }
}

impl IdbDatabaseImpl {
    pub fn transaction(&self) -> DbTransactionResult<IdbTransactionImpl> {
        let store_names = Array::new();
        for table in self.tables.iter() {
            store_names.push(&JsValue::from(table));
        }

        match self
            .db
            .transaction_with_str_sequence_and_mode(&store_names, IdbTransactionMode::Readwrite)
        {
            Ok(transaction) => Ok(IdbTransactionImpl::init(transaction, self.tables.clone())),
            Err(e) => MmError::err(DbTransactionError::ErrorCreatingTransaction(stringify_js_error(&e))),
        }
    }
}

impl Drop for IdbDatabaseImpl {
    fn drop(&mut self) {
        self.db.close();
        let mut open_databases = OPEN_DATABASES.lock().expect("!OPEN_DATABASES.lock()");
        open_databases.remove(&self.db_name);
    }
}

pub(super) struct IdbTransactionImpl {
    transaction: IdbTransaction,
    tables: HashSet<String>,
    aborted: Arc<AtomicBool>,
    complete_rx: oneshot::Receiver<Result<JsValue, JsValue>>,
}

impl !Send for IdbTransactionImpl {}

impl IdbTransactionImpl {
    pub fn aborted(&self) -> bool { self.aborted.load(Ordering::Relaxed) }

    pub fn open_table(&self, table_name: &str) -> DbTransactionResult<IdbObjectStoreImpl> {
        if self.aborted.load(Ordering::Relaxed) {
            return MmError::err(DbTransactionError::TransactionAborted);
        }

        if !self.tables.contains(table_name) {
            let table = table_name.to_owned();
            return MmError::err(DbTransactionError::NoSuchTable { table });
        }

        match self.transaction.object_store(table_name) {
            Ok(object_store) => Ok(IdbObjectStoreImpl {
                object_store,
                aborted: self.aborted.clone(),
            }),
            Err(e) => MmError::err(DbTransactionError::ErrorOpeningTable {
                table: table_name.to_owned(),
                description: stringify_js_error(&e),
            }),
        }
    }

    pub async fn wait_for_complete(self) -> DbTransactionResult<()> {
        if self.aborted.load(Ordering::Relaxed) {
            return MmError::err(DbTransactionError::TransactionAborted);
        }

        let result = self.complete_rx.await.expect("The complete channel must not be closed");
        match result {
            Ok(_event) => Ok(()),
            Err(_error_event) => return MmError::err(DbTransactionError::TransactionAborted),
        }
    }

    fn init(transaction: IdbTransaction, tables: HashSet<String>) -> IdbTransactionImpl {
        let (complete_tx, complete_rx) = oneshot::channel();
        let (event_tx, mut event_rx) = mpsc::channel(2);

        let oncomplete_closure = construct_event_closure(Ok, event_tx.clone());
        let onabort_closure = construct_event_closure(Err, event_tx.clone());

        transaction.set_oncomplete(Some(oncomplete_closure.as_ref().unchecked_ref()));
        // Don't set the `onerror` closure, because the `onabort` is called immediately after the error.
        transaction.set_onabort(Some(onabort_closure.as_ref().unchecked_ref()));

        // move the closures into this async block to keep it alive until either `oncomplete` or `onabort` handler is called
        let fut = async move {
            let complete_or_abort = event_rx.next().await.expect("The event channel must not be closed");
            // ignore if the receiver is closed
            let _res = complete_tx.send(complete_or_abort);

            // do any action to move the closures into this async block to keep it alive until the `state_machine` finishes
            drop(oncomplete_closure);
            drop(onabort_closure);
        };

        wasm_bindgen_futures::spawn_local(fut);
        IdbTransactionImpl {
            transaction,
            tables,
            aborted: Arc::new(AtomicBool::new(false)),
            complete_rx,
        }
    }
}

pub(super) struct IdbObjectStoreImpl {
    object_store: IdbObjectStore,
    aborted: Arc<AtomicBool>,
}

impl !Send for IdbObjectStoreImpl {}

impl IdbObjectStoreImpl {
    pub fn aborted(&self) -> bool { self.aborted.load(Ordering::Relaxed) }

    pub async fn add_item(&self, item: &Json) -> DbTransactionResult<ItemId> {
        if self.aborted.load(Ordering::Relaxed) {
            return MmError::err(DbTransactionError::TransactionAborted);
        }

        // The [`InternalItem::item`] is a flatten field, so if we add the item without the [`InternalItem::_item_id`] id,
        // it will be calculated automatically.
        let js_value = match JsValue::from_serde(item) {
            Ok(value) => value,
            Err(e) => return MmError::err(DbTransactionError::ErrorSerializingItem(e.to_string())),
        };
        let add_request = match self.object_store.add(&js_value) {
            Ok(request) => request,
            Err(e) => return MmError::err(DbTransactionError::ErrorUploadingItem(stringify_js_error(&e))),
        };

        if let Err(_error_event) = Self::wait_for_request_complete(&add_request).await {
            self.aborted.store(true, Ordering::Relaxed);
            let error = Self::error_from_failed_request(&add_request);
            return MmError::err(DbTransactionError::ErrorUploadingItem(error));
        }

        Self::item_id_from_completed_request(&add_request)
    }

    pub async fn get_items(&self, index_str: &str, index_value: Json) -> DbTransactionResult<Vec<(ItemId, Json)>> {
        if self.aborted.load(Ordering::Relaxed) {
            return MmError::err(DbTransactionError::TransactionAborted);
        }

        let index = index_str.to_owned();
        let index_value_js =
            JsValue::from_serde(&index_value).map_to_mm(|e| DbTransactionError::ErrorSerializingIndex {
                index: index.clone(),
                description: e.to_string(),
            })?;

        let db_index = match self.object_store.index(index_str) {
            Ok(index) => index,
            Err(_) => return MmError::err(DbTransactionError::NoSuchIndex { index }),
        };
        let get_request = match db_index.get_all_with_key(&index_value_js) {
            Ok(request) => request,
            Err(e) => {
                return MmError::err(DbTransactionError::InvalidIndex {
                    index,
                    index_value,
                    description: stringify_js_error(&e),
                })
            },
        };

        if let Err(_error_event) = Self::wait_for_request_complete(&get_request).await {
            self.aborted.store(true, Ordering::Relaxed);
            let error = Self::error_from_failed_request(&get_request);
            return MmError::err(DbTransactionError::ErrorGettingItems(error));
        }

        Self::items_from_completed_request(&get_request)
    }

    pub async fn get_item_ids(&self, index_str: &str, index_value: Json) -> DbTransactionResult<Vec<ItemId>> {
        if self.aborted.load(Ordering::Relaxed) {
            return MmError::err(DbTransactionError::TransactionAborted);
        }

        let index = index_str.to_owned();
        let index_value_js =
            JsValue::from_serde(&index_value).map_to_mm(|e| DbTransactionError::ErrorSerializingIndex {
                index: index.clone(),
                description: e.to_string(),
            })?;

        let db_index = match self.object_store.index(index_str) {
            Ok(index) => index,
            Err(_) => return MmError::err(DbTransactionError::NoSuchIndex { index }),
        };
        let get_request = match db_index.get_all_keys_with_key(&index_value_js) {
            Ok(request) => request,
            Err(e) => {
                return MmError::err(DbTransactionError::InvalidIndex {
                    index,
                    index_value,
                    description: stringify_js_error(&e),
                })
            },
        };

        if let Err(_error_event) = Self::wait_for_request_complete(&get_request).await {
            self.aborted.store(true, Ordering::Relaxed);
            let error = Self::error_from_failed_request(&get_request);
            return MmError::err(DbTransactionError::ErrorGettingItems(error));
        }

        Self::item_ids_from_completed_request(&get_request)
    }

    pub async fn get_all_items(&self) -> DbTransactionResult<Vec<(ItemId, Json)>> {
        if self.aborted.load(Ordering::Relaxed) {
            return MmError::err(DbTransactionError::TransactionAborted);
        }

        let get_request = match self.object_store.get_all() {
            Ok(request) => request,
            Err(e) => return MmError::err(DbTransactionError::UnexpectedState(stringify_js_error(&e))),
        };

        if let Err(_error_event) = Self::wait_for_request_complete(&get_request).await {
            self.aborted.store(true, Ordering::Relaxed);
            let error = Self::error_from_failed_request(&get_request);
            return MmError::err(DbTransactionError::ErrorGettingItems(error));
        }

        Self::items_from_completed_request(&get_request)
    }

    pub async fn replace_item(&self, _item_id: ItemId, item: Json) -> DbTransactionResult<ItemId> {
        if self.aborted.load(Ordering::Relaxed) {
            return MmError::err(DbTransactionError::TransactionAborted);
        }

        let item_with_key = InternalItem { _item_id, item };
        let js_value = match JsValue::from_serde(&item_with_key) {
            Ok(value) => value,
            Err(e) => return MmError::err(DbTransactionError::ErrorSerializingItem(e.to_string())),
        };
        let replace_request = match self.object_store.put(&js_value) {
            Ok(request) => request,
            Err(e) => return MmError::err(DbTransactionError::ErrorUploadingItem(stringify_js_error(&e))),
        };

        if let Err(_error_event) = Self::wait_for_request_complete(&replace_request).await {
            self.aborted.store(true, Ordering::Relaxed);
            let error = Self::error_from_failed_request(&replace_request);
            return MmError::err(DbTransactionError::ErrorUploadingItem(error));
        }

        Self::item_id_from_completed_request(&replace_request)
    }

    pub async fn delete_item(&self, item_id: ItemId) -> DbTransactionResult<()> {
        if self.aborted.load(Ordering::Relaxed) {
            return MmError::err(DbTransactionError::TransactionAborted);
        }

        let item_id = JsValue::from(item_id);

        let delete_request = match self.object_store.delete(&item_id) {
            Ok(request) => request,
            Err(e) => return MmError::err(DbTransactionError::ErrorDeletingItems(stringify_js_error(&e))),
        };

        if let Err(_error_event) = Self::wait_for_request_complete(&delete_request).await {
            self.aborted.store(true, Ordering::Relaxed);
            let error = Self::error_from_failed_request(&delete_request);
            return MmError::err(DbTransactionError::ErrorDeletingItems(error));
        }

        Ok(())
    }

    pub async fn clear(&self) -> DbTransactionResult<()> {
        if self.aborted.load(Ordering::Relaxed) {
            return MmError::err(DbTransactionError::TransactionAborted);
        }

        let clear_request = match self.object_store.clear() {
            Ok(request) => request,
            Err(e) => return MmError::err(DbTransactionError::ErrorDeletingItems(stringify_js_error(&e))),
        };

        if let Err(_error_event) = Self::wait_for_request_complete(&clear_request).await {
            self.aborted.store(true, Ordering::Relaxed);
            let error = Self::error_from_failed_request(&clear_request);
            return MmError::err(DbTransactionError::ErrorDeletingItems(error));
        }

        Ok(())
    }

    async fn wait_for_request_complete(request: &IdbRequest) -> Result<JsValue, JsValue> {
        let (tx, mut rx) = mpsc::channel(2);

        let onsuccess_closure = construct_event_closure(Ok, tx.clone());
        let onerror_closure = construct_event_closure(Err, tx.clone());

        request.set_onsuccess(Some(onsuccess_closure.as_ref().unchecked_ref()));
        request.set_onerror(Some(onerror_closure.as_ref().unchecked_ref()));

        rx.next().await.expect("The request event channel must not be closed")
    }

    fn item_id_from_completed_request(request: &IdbRequest) -> DbTransactionResult<ItemId> {
        let result_js_value = match request.result() {
            Ok(res) => res,
            Err(e) => return MmError::err(DbTransactionError::UnexpectedState(stringify_js_error(&e))),
        };

        result_js_value
            .into_serde()
            .map_to_mm(|e| DbTransactionError::ErrorDeserializingItem(e.to_string()))
    }

    fn items_from_completed_request(request: &IdbRequest) -> DbTransactionResult<Vec<(ItemId, Json)>> {
        let result_js_value = match request.result() {
            Ok(res) => res,
            Err(e) => return MmError::err(DbTransactionError::UnexpectedState(stringify_js_error(&e))),
        };

        if result_js_value.is_null() || result_js_value.is_undefined() {
            return Ok(Vec::new());
        }

        let items: Vec<InternalItem> = match result_js_value.into_serde() {
            Ok(items) => items,
            Err(e) => return MmError::err(DbTransactionError::ErrorDeserializingItem(e.to_string())),
        };

        Ok(items.into_iter().map(|item| (item._item_id, item.item)).collect())
    }

    fn item_ids_from_completed_request(request: &IdbRequest) -> DbTransactionResult<Vec<ItemId>> {
        let result_js_value = match request.result() {
            Ok(res) => res,
            Err(e) => return MmError::err(DbTransactionError::UnexpectedState(stringify_js_error(&e))),
        };

        if result_js_value.is_null() || result_js_value.is_undefined() {
            return Ok(Vec::new());
        }

        result_js_value
            .into_serde()
            .map_to_mm(|e| DbTransactionError::ErrorDeserializingItem(e.to_string()))
    }

    fn error_from_failed_request(request: &IdbRequest) -> String {
        match request.error() {
            Ok(Some(exception)) => format!("{:?}", exception),
            _ => "Unknown".to_owned(),
        }
    }
}

pub struct DbUpgrader {
    db: IdbDatabase,
    transaction: IdbTransaction,
}

impl DbUpgrader {
    pub fn create_table(&self, table: &str) -> OnUpgradeResult<TableUpgrader> {
        // We use the [in-line](https://developer.mozilla.org/en-US/docs/Web/API/IndexedDB_API/Basic_Concepts_Behind_IndexedDB#gloss_inline_key) primary keys.
        let key_path = JsValue::from(ITEM_KEY_PATH);

        let mut params = IdbObjectStoreParameters::new();
        params.key_path(Some(&key_path));
        params.auto_increment(true);

        match self.db.create_object_store_with_optional_parameters(table, &params) {
            Ok(object_store) => Ok(TableUpgrader { object_store }),
            Err(e) => MmError::err(OnUpgradeError::ErrorCreatingTable {
                table: table.to_owned(),
                description: stringify_js_error(&e),
            }),
        }
    }

    /// Open the `table` if it was created already.
    pub fn open_table(&self, table: &str) -> OnUpgradeResult<TableUpgrader> {
        match self.transaction.object_store(table) {
            Ok(object_store) => Ok(TableUpgrader { object_store }),
            Err(e) => MmError::err(OnUpgradeError::ErrorOpeningTable {
                table: table.to_owned(),
                description: stringify_js_error(&e),
            }),
        }
    }
}

pub struct TableUpgrader {
    object_store: IdbObjectStore,
}

impl TableUpgrader {
    pub fn create_index(&self, index: &str, unique: bool) -> OnUpgradeResult<()> {
        let mut params = IdbIndexParameters::new();
        params.unique(unique);
        self.object_store
            .create_index_with_str_and_optional_parameters(index, index, &params)
            .map(|_| ())
            .map_to_mm(|e| OnUpgradeError::ErrorCreatingIndex {
                index: index.to_owned(),
                description: stringify_js_error(&e),
            })
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct InternalItem {
    _item_id: ItemId,
    #[serde(flatten)]
    item: Json,
}

#[derive(Debug)]
enum DbOpenEvent {
    Failed(JsValue),
    UpgradeNeeded(JsValue),
    Success(JsValue),
}

/// Please note the `Event` type can be `JsValue`. It doesn't lead to a runtime error, because [`JsValue::dyn_into<JsValue>()`] returns itself.
fn construct_event_closure<F, Event>(mut f: F, mut event_tx: mpsc::Sender<Event>) -> Closure<dyn FnMut(JsValue)>
where
    F: FnMut(JsValue) -> Event + 'static,
    Event: fmt::Debug + 'static,
{
    Closure::new(move |event: JsValue| {
        let open_event = f(event);
        if let Err(e) = event_tx.try_send(open_event) {
            let error = e.to_string();
            let event = e.into_inner();
            error!("Error sending the '{:?}' event: {}", event, error);
        }
    })
}
