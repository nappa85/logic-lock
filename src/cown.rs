use std::{future::Future, pin::Pin};

use sea_orm::{
    ConnectionTrait, DatabaseTransaction, DbBackend, DbErr, ExecResult, QueryResult, Statement,
    StreamTrait, TransactionError, TransactionTrait,
};

/// Cown is a mashup between Cow and Conn
#[derive(Debug)]
pub enum Cown<'a, C>
where
    C: ConnectionTrait + std::fmt::Debug,
{
    Borrowed(&'a C),
    Owned(C),
}

impl<'a, C> From<C> for Cown<'a, C>
where
    C: ConnectionTrait + std::fmt::Debug,
{
    fn from(c: C) -> Self {
        Cown::Owned(c)
    }
}

impl<'a, C> From<&'a C> for Cown<'a, C>
where
    C: ConnectionTrait + std::fmt::Debug,
{
    fn from(c: &'a C) -> Self {
        Cown::Borrowed(c)
    }
}

macro_rules! match_enum {
    ($val:expr, $bind:pat => $e:expr) => {
        match $val {
            Cown::Borrowed($bind) => $e,
            Cown::Owned($bind) => $e,
        }
    };
}

#[async_trait::async_trait]
impl<'a, C> ConnectionTrait for Cown<'a, C>
where
    C: ConnectionTrait + std::fmt::Debug,
{
    fn get_database_backend(&self) -> DbBackend {
        match_enum!(self, c => c.get_database_backend())
    }

    async fn execute(&self, stmt: Statement) -> Result<ExecResult, DbErr> {
        match_enum!(self, c => c.execute(stmt).await)
    }

    async fn query_one(&self, stmt: Statement) -> Result<Option<QueryResult>, DbErr> {
        match_enum!(self, c => c.query_one(stmt).await)
    }

    async fn query_all(&self, stmt: Statement) -> Result<Vec<QueryResult>, DbErr> {
        match_enum!(self, c => c.query_all(stmt).await)
    }
}

impl<'a, C> StreamTrait<'a> for Cown<'a, C>
where
    C: ConnectionTrait + StreamTrait<'a> + std::fmt::Debug,
{
    type Stream = C::Stream;

    fn stream(
        &'a self,
        stmt: Statement,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Stream, DbErr>> + 'a + Send>> {
        match_enum!(self, c => c.stream(stmt))
    }
}

#[async_trait::async_trait]
impl<'a, C> TransactionTrait for Cown<'a, C>
where
    C: ConnectionTrait + TransactionTrait + std::fmt::Debug,
{
    async fn begin(&self) -> Result<DatabaseTransaction, DbErr> {
        match_enum!(self, c => c.begin().await)
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
        match_enum!(self, c => c.transaction(callback).await)
    }
}
