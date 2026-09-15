//! Cooperative process/attempt exclusion. The lock file is never unlinked:
//! deleting it while a holder lives would allow another inode to be locked.
use sparrow_model::{ErrorCode, Result, SparrowError};
use std::fs::{File, OpenOptions};
use std::path::Path;

#[derive(Debug)]
pub struct FileLock {
    _file: File,
}
impl FileLock {
    pub fn acquire(path: &Path) -> Result<Self> {
        if let Ok(meta) = std::fs::symlink_metadata(path) {
            if !meta.is_file() || meta.file_type().is_symlink() {
                return Err(SparrowError::new(
                    ErrorCode::PolicyDenied,
                    "lock path must be a regular file",
                ));
            }
        }
        let mut options = OpenOptions::new();
        options.create_new(true).read(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        // Exclusive creation never follows a dangling symlink. Existing names
        // are opened WITHOUT create, so a check/open race cannot create a file
        // at a substituted target. fstat/lstat below still validates identity.
        let file = match options.open(path) {
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                options.create_new(false).create(false).open(path)
            }
            result => result,
        }
            .map_err(|e| SparrowError::new(ErrorCode::Internal, format!("open lock: {e}")))?;
        let opened = file
            .metadata()
            .map_err(|e| SparrowError::new(ErrorCode::Internal, format!("stat lock: {e}")))?;
        let named = std::fs::symlink_metadata(path)
            .map_err(|e| SparrowError::new(ErrorCode::Internal, format!("stat lock path: {e}")))?;
        let mut same = opened.is_file() && named.is_file() && !named.file_type().is_symlink();
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            same &= opened.dev() == named.dev() && opened.ino() == named.ino();
        }
        if !same {
            return Err(SparrowError::new(
                ErrorCode::PolicyDenied,
                "lock path changed or is not a regular file",
            ));
        }
        file.try_lock().map_err(lock_error)?;
        Ok(Self { _file: file })
    }
}

fn lock_error(error: std::fs::TryLockError) -> SparrowError {
    match error {
        std::fs::TryLockError::WouldBlock => {
            SparrowError::new(ErrorCode::ResourceExhausted, "resource already owned")
                .retryable(true)
        }
        std::fs::TryLockError::Error(error) => SparrowError::new(
            ErrorCode::Internal,
            format!("filesystem lock failed: {error}"),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn r10_unsupported_locks_are_not_reported_as_busy() {
        assert_eq!(
            lock_error(std::fs::TryLockError::WouldBlock).code,
            ErrorCode::ResourceExhausted
        );
        assert_eq!(
            lock_error(std::fs::TryLockError::Error(std::io::Error::from(
                std::io::ErrorKind::Unsupported
            )))
            .code,
            ErrorCode::Internal
        );
    }
    #[test]
    fn production_lock_excludes_another_owner_and_releases_on_drop() {
        let dir = std::env::temp_dir().join(format!(
            "sparrow-lock-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("LOCK");
        let first = FileLock::acquire(&path).unwrap();
        assert!(FileLock::acquire(&path).is_err());
        drop(first);
        drop(FileLock::acquire(&path).unwrap());
        assert!(path.exists());
        std::fs::remove_dir_all(dir).unwrap();
    }
}
