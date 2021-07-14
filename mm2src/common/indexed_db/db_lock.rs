use super::{DbIdentifier, DbInstance, DbNamespaceId, InitDbResult};
use futures::lock::{MappedMutexGuard as AsyncMappedMutexGuard, Mutex as AsyncMutex, MutexGuard as AsyncMutexGuard};

/// The mapped mutex guard.
/// This implements `Deref<Db>`.
pub type DbLocked<'a, Db> = AsyncMappedMutexGuard<'a, Option<Db>, Db>;
/// It's better to use something like [`Constructible`], but it doesn't provide a method to get the inner value by the mutable reference.
pub type ConstructibleDb<Db> = AsyncMutex<Option<Db>>;

/// Locks the given mutex and checks if the inner database is initialized already or not,
/// initializes it if it's required, and returns the locked instance.
pub async fn get_or_initialize_db<Db>(
    mutex: &ConstructibleDb<Db>,
    namespace_id: DbNamespaceId,
) -> InitDbResult<DbLocked<'_, Db>>
where
    Db: DbInstance,
{
    let mut locked_db = mutex.lock().await;
    // Db is initialized already
    if locked_db.is_some() {
        return Ok(unwrap_tx_history_db(locked_db));
    }

    let db_id = DbIdentifier::with_namespace::<Db>(namespace_id);

    let db = Db::init(db_id).await?;
    *locked_db = Some(db);
    Ok(unwrap_tx_history_db(locked_db))
}

/// # Panics
///
/// This function will `panic!()` if the inner value of the `guard` is `None`.
fn unwrap_tx_history_db<Db>(guard: AsyncMutexGuard<'_, Option<Db>>) -> DbLocked<'_, Db> {
    AsyncMutexGuard::map(guard, |wrapped_db| {
        wrapped_db
            .as_mut()
            .expect("The locked 'Option<Db>' must contain a value")
    })
}
