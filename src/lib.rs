#![deny(warnings)]
#![deny(missing_docs)]

//! # logic-lock
//!
//! MySQL logic locks implemented over sea-orm

use std::{future::Future, pin::Pin};

use sea_orm::{
    ConnectionTrait, DatabaseTransaction, DbBackend, DbErr, ExecResult, QueryResult, Statement,
    StreamTrait, TransactionError, TransactionTrait, Value, Values,
};

use tracing::{error, instrument};

/// Lock and Unlock error types
pub mod error;

/// Lock entity
#[derive(Debug)]
pub struct Lock<C>
where
    C: ConnectionTrait + std::fmt::Debug,
{
    key: String,
    conn: Option<C>,
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
impl<C> ConnectionTrait for Lock<C>
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

impl<'a, C> StreamTrait<'a> for Lock<C>
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
impl<C> TransactionTrait for Lock<C>
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

impl<C> Drop for Lock<C>
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

impl<C> Lock<C>
where
    C: ConnectionTrait + std::fmt::Debug,
{
    /// Lock builder
    /// Takes anything can become a String as key, an owned connection (it can be a `sea_orm::DatabaseConnection`,
    /// a `sea_orm::DatabaseTransaction or another `Lock` himself), and an optional timeout in seconds, defaulting to 1 second
    #[instrument(level = "trace")]
    pub async fn build<S>(key: S, conn: C, timeout: Option<u8>) -> Result<Lock<C>, error::Lock<C>>
    where
        S: Into<String> + std::fmt::Debug,
    {
        let key = key.into();
        let mut stmt = Statement::from_string(
            conn.get_database_backend(),
            String::from("SELECT GET_LOCK(?, ?) AS res"),
        );
        stmt.values = Some(Values(vec![
            Value::from(key.as_str()),
            Value::from(timeout.unwrap_or(1)),
        ]));
        let res = match conn.query_one(stmt).await {
            Ok(Some(res)) => res,
            Ok(None) => return Err(error::Lock::DbErr(key, conn, None)),
            Err(e) => return Err(error::Lock::DbErr(key, conn, Some(e))),
        };
        let lock = match res.try_get::<Option<bool>>("", "res") {
            Ok(Some(res)) => res,
            Ok(None) => return Err(error::Lock::DbErr(key, conn, None)),
            Err(e) => return Err(error::Lock::DbErr(key, conn, Some(e))),
        };

        if lock {
            Ok(Lock {
                key,
                conn: Some(conn),
            })
        } else {
            Err(error::Lock::Failed(key, conn))
        }
    }

    /// returns locked key
    #[must_use]
    pub fn get_key(&self) -> &str {
        self.key.as_ref()
    }

    /// releases the lock, returning the owned connection on success
    /// on error it will return the `Lock` himself alongside with the database error, if any
    #[instrument(level = "trace")]
    pub async fn release(mut self) -> Result<C, error::Unlock<C>> {
        if_let_unreachable!(self.conn, conn => {
            let mut stmt =
                Statement::from_string(conn.get_database_backend(), String::from("SELECT RELEASE_LOCK(?) AS res"));
            stmt.values = Some(Values(vec![Value::from(self.key.as_str())]));
            let res = match conn.query_one(stmt).await {
                Ok(Some(res)) => res,
                Ok(None) => return Err(error::Unlock::DbErr(self, None)),
                Err(e) => return Err(error::Unlock::DbErr(self, Some(e))),
            };
            let released = match res.try_get::<Option<bool>>("", "res") {
                Ok(Some(res)) => res,
                Ok(None) => return Err(error::Unlock::DbErr(self, None)),
                Err(e) => return Err(error::Unlock::DbErr(self, Some(e))),
            };

            if released {
                Ok(self.conn.take().unwrap())
            }
            else {
                Err(error::Unlock::Failed(self))
            }
        })
    }

    /// forgets the lock and returns inner connection
    /// WARNING: the lock will continue to live in the database session
    #[must_use]
    pub fn into_inner(mut self) -> C {
        self.conn.take().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use sea_orm::{
        ConnectionTrait, Database, DatabaseConnection, DbErr, Statement, StreamTrait,
        TransactionTrait,
    };

    use tokio_stream::StreamExt;

    fn metric_mysql(info: &sea_orm::metric::Info<'_>) {
        tracing::debug!(
            "mysql query{} took {}s: {}",
            if info.failed { " failed" } else { "" },
            info.elapsed.as_secs_f64(),
            info.statement.sql
        );
    }

    async fn get_conn() -> DatabaseConnection {
        let url = std::env::var("DATABASE_URL");
        let mut conn = Database::connect(url.as_deref().unwrap_or("mysql://root@127.0.0.1/test"))
            .await
            .unwrap();
        conn.set_metric_callback(metric_mysql);
        conn
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
        let txn = lock.release().await.unwrap();
        txn.commit().await?;
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
        let txn = lock.release().await.unwrap();
        txn.commit().await?;
        res
    }

    #[tokio::test]
    async fn simple() {
        tracing_subscriber::fmt::try_init().ok();

        let conn = get_conn().await;

        let lock = super::Lock::build("foobar", conn, None).await.unwrap();
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

        let lock = super::Lock::build("foobar", conn, None).await.unwrap();
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
