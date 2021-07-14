use async_trait::async_trait;
use common::indexed_db::{DbIdentifier, DbInstance, DbUpgrader, IndexedDb, IndexedDbBuilder, OnUpgradeResult,
                         TableSignature};
use common::mm_error::prelude::*;
use derive_more::Display;
use std::ops::Deref;
use uuid::Uuid;

pub use common::indexed_db::{DbTransactionError, DbTransactionResult, InitDbError, InitDbResult, ItemId};
pub use tables::SwapLockTable;

const DB_NAME: &str = "swap";
const DB_VERSION: u32 = 1;

pub struct SwapDb {
    inner: IndexedDb,
}

#[async_trait]
impl DbInstance for SwapDb {
    fn db_name() -> &'static str { DB_NAME }

    async fn init(db_id: DbIdentifier) -> InitDbResult<Self> {
        let inner = IndexedDbBuilder::new(db_id)
            .with_version(DB_VERSION)
            .with_table::<SwapLockTable>()
            .build()
            .await?;
        Ok(SwapDb { inner })
    }
}

impl Deref for SwapDb {
    type Target = IndexedDb;

    fn deref(&self) -> &Self::Target { &self.inner }
}

pub mod tables {
    use super::*;

    #[derive(Debug, Deserialize, Clone, Serialize, PartialEq)]
    pub struct SwapLockTable {
        pub uuid: Uuid,
        pub timestamp: u64,
    }

    impl TableSignature for SwapLockTable {
        fn table_name() -> &'static str { "swap_lock" }

        fn on_upgrade_needed(upgrader: &DbUpgrader, old_version: u32, new_version: u32) -> OnUpgradeResult<()> {
            match (old_version, new_version) {
                (0, 1) => {
                    let table = upgrader.create_table(Self::table_name())?;
                    table.create_index("uuid", true)?;
                },
                (1, 1) => (),
                v => panic!("Unexpected (old, new) versions: {:?}", v),
            }
            Ok(())
        }
    }
}
