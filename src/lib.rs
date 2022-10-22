use std::{future::Future, pin::Pin};

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
    key: String,
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
        S: Into<String> + std::fmt::Debug,
        CC: Into<Cown<'a, C>> + std::fmt::Debug,
    {
        let key = key.into();
        let conn = conn.into();
        let mut stmt = Statement::from_string(
            conn.get_database_backend(),
            String::from("SELECT GET_LOCK(?, ?) AS res"),
        );
        stmt.values = Some(Values(vec![
            Value::from(key.as_str()),
            Value::from(timeout.unwrap_or(1)),
        ]));
        let res = conn
            .query_one(stmt)
            .await
            .map_err(|e| error!("MySQL get_lock {} error: {}", key, e))?
            .ok_or_else(|| error!("MySQL get_lock {} unknown error", key))?;
        let lock = res
            .try_get::<Option<bool>>("", "res")
            .map_err(|e| error!("MySQL get_lock {} retrieve error: {}", key, e))?
            .ok_or_else(|| error!("MySQL get_lock {} retrieve unknown error", key))?;

        if lock {
            Ok(Lock {
                key,
                conn: Some(conn),
            })
        } else {
            error!("MySQL get_lock {} failed", key);
            Err(Error::LockFailed)
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
            stmt.values = Some(Values(vec![Value::from(self.key.as_str())]));
            let res = conn
                .query_one(stmt)
                .await
                .map_err(|e| {
                    error!("MySQL release_lock {} error: {}", self.key, e);
                })?
                .ok_or_else(|| {
                    error!("MySQL release_lock {} unknown error", self.key);
                })?;
            let released = res
                .try_get::<Option<bool>>("", "res")
                .map_err(|e| {
                    error!("MySQL release_lock {} retrieve error: {}", self.key, e);
                })?
                .ok_or_else(|| {
                    error!("MySQL release_lock {} retrieve unknown error", self.key);
                })?;

            if released {
                Ok(self.conn.take().unwrap())
            }
            else {
                error!("MySQL release_lock {} failed", self.key);
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
    None,
    LockFailed,
    UnlockFailed(Lock<'a, C>),
}

impl<'a, C> std::fmt::Display for Error<'a, C>
where
    C: ConnectionTrait + std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::None => Ok(()),
            Error::LockFailed => write!(f, "lock failed"),
            Error::UnlockFailed(_) => write!(f, "unlock failed"),
        }
    }
}

impl<'a, C> std::error::Error for Error<'a, C> where C: ConnectionTrait + std::fmt::Debug {}

impl<'a, C> From<()> for Error<'a, C>
where
    C: ConnectionTrait + std::fmt::Debug,
{
    fn from(_: ()) -> Self {
        Error::None
    }
}

#[cfg(test)]
mod tests {
    use sea_orm::{
        ConnectionTrait, Database, DatabaseConnection, DbErr, Statement, TransactionError,
        TransactionTrait,
    };

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
        // the problem here is we don't have access to DatabaseTransaction::commit
        // so we end up using the closure version
        conn.transaction(|txn| {
            Box::pin(async move {
                let lock = super::Lock::build("barfoo", txn, None).await.unwrap();
                let res = generic_method_who_needs_a_connection(&lock).await;
                lock.release().await.unwrap();
                res
            })
        })
        .await
        .map_err(|e| match e {
            TransactionError::Connection(e) => e,
            TransactionError::Transaction(e) => e,
        })
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
}
