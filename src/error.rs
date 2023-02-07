use std::{error::Error, fmt};

use sea_orm::{ConnectionTrait, DbErr};

/// Lock errors
#[derive(Debug)]
pub enum Lock<C>
where
    C: ConnectionTrait + fmt::Debug,
{
    /// Database errors while locking
    DbErr(String, C, Option<DbErr>),
    /// Lock failed
    Failed(String, C),
}

impl<C> fmt::Display for Lock<C>
where
    C: ConnectionTrait + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Lock::DbErr(key, _, Some(e)) => {
                write!(f, "error while locking for key {key}: {e}")
            }
            Lock::DbErr(key, _, None) => {
                write!(f, "error while locking for key {key}: unknown error")
            }
            Lock::Failed(key, _) => write!(f, "lock failed for key {key}"),
        }
    }
}

impl<C> Error for Lock<C> where C: ConnectionTrait + fmt::Debug {}

/// Unlock errors
#[derive(Debug)]
pub enum Unlock<C>
where
    C: ConnectionTrait + fmt::Debug,
{
    /// Database errors while unlocking
    DbErr(super::Lock<C>, Option<DbErr>),
    /// Unlock failed
    Failed(super::Lock<C>),
}

impl<C> fmt::Display for Unlock<C>
where
    C: ConnectionTrait + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Unlock::DbErr(lock, Some(e)) => {
                write!(f, "error while unlocking for key {}: {}", lock.get_key(), e)
            }
            Unlock::DbErr(lock, None) => {
                write!(
                    f,
                    "error while unlocking for key {}: unknown error",
                    lock.get_key()
                )
            }
            Unlock::Failed(lock) => write!(f, "unlock failed for key {}", lock.get_key()),
        }
    }
}

impl<C> Error for Unlock<C> where C: ConnectionTrait + fmt::Debug {}
