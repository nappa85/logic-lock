use std::{borrow::Cow, future::Future, pin::Pin};

use sea_orm::{
    ConnectionTrait, DatabaseTransaction, DbBackend, DbErr, ExecResult, QueryResult, Statement,
    StreamTrait, TransactionError, TransactionTrait, Value, Values,
};

use tracing::{error, instrument};

mod cown;
pub use cown::Cown;

#[derive(Debug)]
pub struct Lock<'a, C>
where
    C: ConnectionTrait + std::fmt::Debug,
{
    key: Cow<'a, str>,
    conn: Option<Cown<'a, C>>,
}

macro_rules! if_let_unreachable {
    ($val:expr, $bind:pat => $e:expr) => {
        if let Some($bind) = &$val {
            $e
        } else {
            unreachable!()
        }
    };
}

#[async_trait::async_trait]
impl<'a, C> ConnectionTrait for Lock<'a, C>
where
    C: ConnectionTrait + std::fmt::Debug + Send,
{
    fn get_database_backend(&self) -> DbBackend {
        if_let_unreachable!(self.conn, conn => conn.get_database_backend())
    }

    async fn execute(&self, stmt: Statement) -> Result<ExecResult, DbErr> {
        if_let_unreachable!(self.conn, conn => conn.execute(stmt).await)
    }

    async fn query_one(&self, stmt: Statement) -> Result<Option<QueryResult>, DbErr> {
        if_let_unreachable!(self.conn, conn => conn.query_one(stmt).await)
    }

    async fn query_all(&self, stmt: Statement) -> Result<Vec<QueryResult>, DbErr> {
        if_let_unreachable!(self.conn, conn => conn.query_all(stmt).await)
    }
}

impl<'a, C> StreamTrait<'a> for Lock<'a, C>
where
    C: ConnectionTrait + StreamTrait<'a> + std::fmt::Debug,
{
    type Stream = C::Stream;

    fn stream(
        &'a self,
        stmt: Statement,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Stream, DbErr>> + 'a + Send>> {
        if_let_unreachable!(self.conn, conn => conn.stream(stmt))
    }
}

#[async_trait::async_trait]
impl<'a, C> TransactionTrait for Lock<'a, C>
where
    C: ConnectionTrait + TransactionTrait + std::fmt::Debug + Send,
{
    async fn begin(&self) -> Result<DatabaseTransaction, DbErr> {
        if_let_unreachable!(self.conn, conn => conn.begin().await)
    }

    async fn transaction<F, T, E>(&self, callback: F) -> Result<T, TransactionError<E>>
    where
        F: for<'c> FnOnce(
                &'c DatabaseTransaction,
            ) -> Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'c>>
            + Send,
        T: Send,
        E: std::error::Error + Send,
    {
        if_let_unreachable!(self.conn, conn => conn.transaction(callback).await)
    }
}

impl<'a, C> Drop for Lock<'a, C>
where
    C: ConnectionTrait + std::fmt::Debug,
{
    fn drop(&mut self) {
        if self.conn.is_some() {
            // panicing here could create a panic-while-panic situatiuon
            error!("Dropping unreleased lock {}", self.key);
        }
    }
}

impl<'a, C> Lock<'a, C>
where
    C: ConnectionTrait + std::fmt::Debug + 'a,
{
    #[instrument(level = "trace")]
    pub async fn build<S, CC>(
        key: S,
        conn: CC,
        timeout: Option<u8>,
    ) -> Result<Lock<'a, C>, Error<'a, C>>
    where
        S: Into<Cow<'a, str>> + std::fmt::Debug,
        CC: Into<Cown<'a, C>> + std::fmt::Debug,
    {
        let key = key.into();
        let conn = conn.into();
        let mut stmt = Statement::from_string(
            conn.get_database_backend(),
            String::from("SELECT GET_LOCK(?, ?) AS res"),
        );
        stmt.values = Some(Values(vec![
            Value::from(key.as_ref()),
            Value::from(timeout.unwrap_or(1)),
        ]));
        let res = conn
            .query_one(stmt)
            .await
            .map_err(|e| Error::Locking(key.clone(), Some(e)))?
            .ok_or_else(|| Error::Locking(key.clone(), None))?;
        let lock = res
            .try_get::<Option<bool>>("", "res")
            .map_err(|e| Error::Locking(key.clone(), Some(e)))?
            .ok_or_else(|| Error::Locking(key.clone(), None))?;

        if lock {
            Ok(Lock {
                key,
                conn: Some(conn),
            })
        } else {
            Err(Error::LockFailed(key))
        }
    }

    #[must_use]
    pub fn get_key(&self) -> &str {
        self.key.as_ref()
    }

    #[instrument(level = "trace")]
    pub async fn release(mut self) -> Result<Cown<'a, C>, Error<'a, C>> {
        if_let_unreachable!(self.conn, conn => {
            let mut stmt =
                Statement::from_string(conn.get_database_backend(), String::from("SELECT RELEASE_LOCK(?) AS res"));
            stmt.values = Some(Values(vec![Value::from(self.key.as_ref())]));
            let res = conn
                .query_one(stmt)
                .await
                .map_err(|e| Error::Unlocking(self.key.clone(), Some(e)))?
                .ok_or_else(|| Error::Unlocking(self.key.clone(), None))?;
            let released = res
                .try_get::<Option<bool>>("", "res")
                .map_err(|e| Error::Unlocking(self.key.clone(), Some(e)))?
                .ok_or_else(|| Error::Unlocking(self.key.clone(), None))?;

            if released {
                Ok(self.conn.take().unwrap())
            }
            else {
                Err(Error::UnlockFailed(self))
            }
        })
    }
}

#[derive(Debug)]
pub enum Error<'a, C>
where
    C: ConnectionTrait + std::fmt::Debug,
{
    Locking(Cow<'a, str>, Option<DbErr>),
    LockFailed(Cow<'a, str>),
    Unlocking(Cow<'a, str>, Option<DbErr>),
    UnlockFailed(Lock<'a, C>),
}

impl<'a, C> std::fmt::Display for Error<'a, C>
where
    C: ConnectionTrait + std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Locking(key, Some(e)) => write!(f, "error while locking for key {}: {}", key, e),
            Error::Locking(key, None) => {
                write!(f, "error while locking for key {}: unknown error", key)
            }
            Error::LockFailed(key) => write!(f, "lock failed for key {}", key),
            Error::Unlocking(key, Some(e)) => {
                write!(f, "error while unlocking for key {}: {}", key, e)
            }
            Error::Unlocking(key, None) => {
                write!(f, "error while unlocking for key {}: unknown error", key)
            }
            Error::UnlockFailed(lock) => write!(f, "unlock failed for key {}", lock.get_key()),
        }
    }
}

impl<'a, C> std::error::Error for Error<'a, C> where C: ConnectionTrait + std::fmt::Debug {}

#[cfg(test)]
mod tests {
    use sea_orm::{
        ConnectionTrait, Database, DatabaseConnection, DbErr, Statement, StreamTrait,
        TransactionTrait,
    };

    use tokio_stream::StreamExt;

    async fn get_conn() -> DatabaseConnection {
        let url = std::env::var("DATABASE_URL");
        Database::connect(url.as_deref().unwrap_or("mysql://root@127.0.0.1/test"))
            .await
            .unwrap()
    }

    async fn generic_method_who_needs_a_connection<C>(conn: &C) -> Result<bool, DbErr>
    where
        C: ConnectionTrait + std::fmt::Debug,
    {
        let stmt =
            Statement::from_string(conn.get_database_backend(), String::from("SELECT 1 AS res"));
        let res = conn
            .query_one(stmt)
            .await?
            .ok_or_else(|| DbErr::RecordNotFound(String::from("1")))?;
        res.try_get::<Option<bool>>("", "res")?
            .ok_or_else(|| DbErr::Custom(String::from("Unknown error")))
    }

    async fn generic_method_who_creates_a_transaction<C>(conn: &C) -> Result<bool, DbErr>
    where
        C: ConnectionTrait + TransactionTrait + std::fmt::Debug,
    {
        let txn = conn.begin().await?;
        let lock = super::Lock::build("barfoo", txn, None).await.unwrap();
        let res = generic_method_who_needs_a_connection(&lock).await;
        if let super::Cown::Owned(txn) = lock.release().await.unwrap() {
            txn.commit().await?;
        } else {
            unreachable!();
        }
        res
    }

    async fn generic_method_who_makes_a_stream<'a, C>(conn: &'a C) -> Result<bool, DbErr>
    where
        C: ConnectionTrait + StreamTrait<'a> + std::fmt::Debug,
    {
        let stmt =
            Statement::from_string(conn.get_database_backend(), String::from("SELECT 1 AS res"));
        let res = conn.stream(stmt).await?;
        let row = Box::pin(res)
            .next()
            .await
            .ok_or_else(|| DbErr::RecordNotFound(String::from("1")))??;
        row.try_get::<Option<bool>>("", "res")?
            .ok_or_else(|| DbErr::Custom(String::from("Unknown error")))
    }

    async fn generic_method_who_makes_a_stream_inside_a_transaction<C>(
        conn: &C,
    ) -> Result<bool, DbErr>
    where
        C: ConnectionTrait + TransactionTrait + std::fmt::Debug,
    {
        let txn = conn.begin().await?;
        let lock = super::Lock::build("barfoo", txn, None).await.unwrap();
        let res = generic_method_who_makes_a_stream(&lock).await;
        if let super::Cown::Owned(txn) = lock.release().await.unwrap() {
            txn.commit().await?;
        } else {
            unreachable!();
        }
        res
    }

    #[tokio::test]
    async fn simple() {
        tracing_subscriber::fmt::try_init().ok();

        let conn = get_conn().await;

        let lock = super::Lock::build("foobar", &conn, None).await.unwrap();
        let res = generic_method_who_needs_a_connection(&lock).await;
        assert!(lock.release().await.is_ok());
        res.unwrap();
    }

    #[tokio::test]
    async fn transaction() {
        tracing_subscriber::fmt::try_init().ok();

        let conn = get_conn().await;

        generic_method_who_creates_a_transaction(&conn)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn stream() {
        tracing_subscriber::fmt::try_init().ok();

        let conn = get_conn().await;

        let lock = super::Lock::build("foobar", &conn, None).await.unwrap();
        let res = generic_method_who_makes_a_stream(&lock).await;
        assert!(lock.release().await.is_ok());
        res.unwrap();
    }

    #[tokio::test]
    async fn transaction_stream() {
        tracing_subscriber::fmt::try_init().ok();

        let conn = get_conn().await;

        generic_method_who_makes_a_stream_inside_a_transaction(&conn)
            .await
            .unwrap();
    }
}
