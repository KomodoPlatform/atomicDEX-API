use crate::mm_error::prelude::*;
use crate::{now_float, now_ms};
use derive_more::Display;
use std::path::{Path, PathBuf};

pub type FileLockResult<T> = std::result::Result<T, MmError<FileLockError>>;

#[derive(Debug, Display)]
pub enum FileLockError {
    #[display(fmt = "Error reading timestamp from {:?}: {}", path, error)]
    ErrorReadingTimestamp { path: PathBuf, error: String },
    #[display(fmt = "Error writing timestamp to {:?}: {}", path, error)]
    ErrorWritingTimestamp { path: PathBuf, error: String },
    #[display(fmt = "Error creating {:?}: {}", path, error)]
    ErrorCreatingLockFile { path: PathBuf, error: String },
}

pub struct FileLock<T: AsRef<Path>> {
    /// Filesystem path of the lock file.
    lock_path: T,
    /// The time in seconds after which an outdated lock file can be removed.
    #[allow(dead_code)]
    ttl_sec: f64,
}

/// Records timestamp to a file contents.
fn touch(path: &dyn AsRef<Path>, timestamp: u64) -> FileLockResult<()> {
    std::fs::write(path.as_ref(), timestamp.to_string()).map_to_mm(|error| FileLockError::ErrorWritingTimestamp {
        path: path.as_ref().to_path_buf(),
        error: error.to_string(),
    })
}

/// Attempts to read timestamp recorded to a file
fn read_timestamp(path: &dyn AsRef<Path>) -> FileLockResult<Option<u64>> {
    match std::fs::read_to_string(path) {
        Ok(content) => Ok(content.parse().ok()),
        Err(e) => MmError::err(FileLockError::ErrorReadingTimestamp {
            path: path.as_ref().to_path_buf(),
            error: e.to_string(),
        }),
    }
}

impl<T: AsRef<Path>> FileLock<T> {
    pub fn lock(lock_path: T, ttl_sec: f64) -> FileLockResult<Option<FileLock<T>>> {
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(lock_path.as_ref())
        {
            Ok(_) => {
                let file_lock = FileLock { lock_path, ttl_sec };
                file_lock.touch()?;
                Ok(Some(file_lock))
            },
            Err(ref ie) if ie.kind() == std::io::ErrorKind::AlreadyExists => {
                // See if the existing lock is old enough to be discarded.
                match read_timestamp(&lock_path)? {
                    Some(lm) => {
                        if now_float() - lm as f64 > ttl_sec {
                            let file_lock = FileLock { lock_path, ttl_sec };
                            file_lock.touch()?;
                            Ok(Some(file_lock))
                        } else {
                            Ok(None)
                        }
                    },
                    None => {
                        let file_lock = FileLock { lock_path, ttl_sec };
                        file_lock.touch()?;
                        Ok(Some(file_lock))
                    },
                }
            },
            Err(ie) => MmError::err(FileLockError::ErrorCreatingLockFile {
                path: lock_path.as_ref().to_path_buf(),
                error: ie.to_string(),
            }),
        }
    }

    pub fn touch(&self) -> FileLockResult<()> { touch(&self.lock_path, now_ms() / 1000) }
}

impl<T: AsRef<Path>> Drop for FileLock<T> {
    fn drop(&mut self) { let _ = std::fs::remove_file(&self.lock_path); }
}

#[cfg(test)]
mod file_lock_tests {
    use super::*;
    use std::{thread::sleep, time::Duration};

    #[test]
    fn test_file_lock_should_create_file_and_record_timestamp_and_then_delete_on_drop() {
        let now = now_ms() / 1000;
        let path = Path::new("test1.lock");
        let lock = FileLock::lock(&path, 1000.).unwrap().unwrap();
        assert!(path.exists());
        let timestamp = read_timestamp(&path).unwrap().unwrap();
        assert!(timestamp >= now);
        drop(lock);
        assert!(!path.exists());
    }

    #[test]
    fn test_file_lock_should_return_none_if_lock_acquired() {
        let path = Path::new("test2.lock");
        let _lock = FileLock::lock(&path, 1000.).unwrap().unwrap();
        let new_lock = FileLock::lock(&path, 1000.).unwrap();
        assert!(new_lock.is_none());
    }

    #[test]
    fn test_file_lock_should_acquire_if_ttl_expired_and_update_timestamp() {
        let path = Path::new("test3.lock");
        let _lock = FileLock::lock(&path, 1.).unwrap().unwrap();
        sleep(Duration::from_secs(2));
        let old_timestamp = read_timestamp(&path).unwrap();
        let _new_lock = FileLock::lock(&path, 1.).unwrap().unwrap();
        let new_timestamp = read_timestamp(&path).unwrap();
        assert!(new_timestamp > old_timestamp);
    }

    #[test]
    fn test_file_lock_should_acquire_if_file_is_empty() {
        let now = now_ms() / 1000;
        let path = Path::new("test4.lock");
        std::fs::write(&path, &[]).unwrap();
        let _new_lock = FileLock::lock(&path, 1.).unwrap().unwrap();
        let timestamp = read_timestamp(&path).unwrap().unwrap();
        assert!(timestamp >= now);
    }

    #[test]
    fn test_file_lock_should_acquire_if_file_does_not_contain_parsable_timestamp() {
        let now = now_ms() / 1000;
        let path = Path::new("test5.lock");
        std::fs::write(&path, &[12, 13]).unwrap();
        let _new_lock = FileLock::lock(&path, 1.).unwrap().unwrap();
        let timestamp = read_timestamp(&path).unwrap().unwrap();
        assert!(timestamp >= now);
    }
}
