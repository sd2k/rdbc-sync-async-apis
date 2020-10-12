//! Postgres RDBC Driver
//!
//! This crate implements an RDBC Driver for the `tokio-postgres` crate.
//!
//! The RDBC (Rust DataBase Connectivity) API is loosely based on the ODBC and JDBC standards.
//!
//! ```rust,no_run
//! use futures_util::stream::StreamExt;
//! use rdbc::{prelude::*, r#async::*};
//! use rdbc_tokio_postgres::TokioPostgresDriver;
//!
//! # async fn go() {
//! let driver = TokioPostgresDriver::new("postgres://postgres:password@localhost:5433".to_string());
//! let mut conn = driver.connect().await.unwrap();
//! let mut stmt = conn.prepare("SELECT a FROM b WHERE c = ?").await.unwrap();
//! let rs = stmt.query(&[Value::Int32(123)]).await.unwrap();
//! let mut rows = rs.rows();
//! while let Some(row) = rows.next().await {
//!   println!("{:?}", row.unwrap().get::<&str>(0));
//! }
//! # }
//! ```

use std::sync::Arc;

use async_trait::async_trait;
use bytes::BytesMut;
use futures_util::stream::{StreamExt, TryStreamExt};
use sqlparser::{
    dialect::{keywords::Keyword, PostgreSqlDialect},
    tokenizer::{Token, Tokenizer, Word},
};
use tokio_postgres::{
    types::{to_sql_checked, IsNull, ToSql, Type},
    Client, NoTls,
};

#[derive(Debug)]
pub enum Error {
    TokioPostgres(tokio_postgres::Error),
}

impl From<tokio_postgres::Error> for Error {
    fn from(other: tokio_postgres::Error) -> Self {
        Self::TokioPostgres(other)
    }
}

pub struct TokioPostgresDriver {
    // TODO store a connection pool here?
    url: String,
}

#[async_trait]
impl rdbc::r#async::Driver for TokioPostgresDriver {
    type Connection = TokioPostgresConnection;
    type Error = Error;

    fn new(url: String) -> Self {
        Self { url }
    }

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let (client, conn) = tokio_postgres::connect(&self.url, NoTls).await?;
        tokio::spawn(conn);
        Ok(TokioPostgresConnection {
            inner: Arc::new(client),
        })
    }
}

pub struct TokioPostgresConnection {
    inner: Arc<Client>,
}

#[async_trait]
impl rdbc::r#async::Connection for TokioPostgresConnection {
    type Statement = TokioPostgresStatement;
    type Error = Error;

    async fn create(&mut self, sql: &str) -> Result<Self::Statement, Self::Error> {
        let sql = {
            let dialect = PostgreSqlDialect {};
            let mut tokenizer = Tokenizer::new(&dialect, sql);
            let tokens = tokenizer.tokenize().unwrap();
            let mut i = 0_usize;
            let tokens: Vec<Token> = tokens
                .iter()
                .map(|t| match t {
                    Token::Char(c) if *c == '?' => {
                        i += 1;
                        Token::Word(Word {
                            value: format!("${}", i),
                            quote_style: None,
                            keyword: Keyword::NoKeyword,
                        })
                    }
                    _ => t.clone(),
                })
                .collect();
            tokens
                .iter()
                .map(|t| format!("{}", t))
                .collect::<Vec<String>>()
                .join("")
        };
        let statement = self.inner.prepare(&sql).await?;
        Ok(TokioPostgresStatement {
            client: Arc::clone(&self.inner),
            statement,
        })
    }

    async fn prepare(&mut self, sql: &str) -> Result<Self::Statement, Self::Error> {
        self.create(sql).await
    }
}

pub struct TokioPostgresStatement {
    client: Arc<Client>,
    statement: tokio_postgres::Statement,
}

const fn to_rdbc_type(ty: &Type) -> Option<rdbc::DataType> {
    match *ty {
        Type::BOOL => Some(rdbc::DataType::Bool),
        Type::CHAR => Some(rdbc::DataType::Char),
        //TODO all types
        _ => Some(rdbc::DataType::Utf8),
    }
}

#[derive(Debug)]
pub struct PostgresType<'a>(&'a rdbc::Value);

impl ToSql for PostgresType<'_> {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + 'static + Sync + Send>> {
        match self.0 {
            rdbc::Value::Int32(i) => i.to_sql(ty, out),
            rdbc::Value::UInt32(i) => i.to_sql(ty, out),
            rdbc::Value::String(i) => i.to_sql(ty, out),
            // TODO add other types and convert to macro
        }
    }
    to_sql_checked!();
    fn accepts(ty: &Type) -> bool {
        to_rdbc_type(ty).is_some()
    }
}

#[async_trait]
impl rdbc::r#async::Statement for TokioPostgresStatement {
    type ResultSet = TokioPostgresResultSet;
    type Error = Error;

    async fn query(&mut self, params: &[rdbc::Value]) -> Result<Self::ResultSet, Self::Error> {
        let meta = self
            .statement
            .columns()
            .iter()
            .map(|c| rdbc::Column::new(c.name(), to_rdbc_type(c.type_()).unwrap()))
            .collect();
        // let pg_params: Vec<_> = params.iter().map(PostgresType).collect();
        // let params: Vec<&(dyn ToSql + Sync)> =
        //     pg_params.iter().map(|x| x as &(dyn ToSql + Sync)).collect();
        let row_stream = self
            .client
            .query_raw(
                &self.statement,
                params
                    .iter()
                    .map(PostgresType)
                    .collect::<Vec<_>>()
                    .iter()
                    .map(|x| x as &dyn ToSql),
            )
            .await?;
        Ok(TokioPostgresResultSet {
            rows: row_stream,
            meta,
        })
    }

    async fn execute(&mut self, params: &[rdbc::Value]) -> Result<u64, Self::Error> {
        let pg_params: Vec<_> = params.iter().map(PostgresType).collect();
        let params: Vec<&(dyn ToSql + Sync)> =
            pg_params.iter().map(|x| x as &(dyn ToSql + Sync)).collect();
        Ok(self
            .client
            .execute(&self.statement, params.as_slice())
            .await?)
    }
}

pub struct TokioPostgresResultSet {
    meta: Vec<rdbc::Column>,
    rows: tokio_postgres::RowStream,
}

impl rdbc::r#async::ResultSet for TokioPostgresResultSet {
    type MetaData = Vec<rdbc::Column>;
    type Row = TokioPostgresRow;
    type Error = Error;

    fn meta_data(&self) -> Result<&Self::MetaData, Self::Error> {
        Ok(&self.meta)
    }

    fn rows(self) -> rdbc::r#async::RowStream<Self::Row, Self::Error> {
        Box::pin(
            self.rows
                .map_ok(|inner| TokioPostgresRow { inner })
                .map_err(Into::into),
        )
    }

    fn batches(self, capacity: usize) -> rdbc::r#async::BatchStream<Self::Row, Self::Error> {
        Box::pin(
            self.rows
                .map_ok(|inner| TokioPostgresRow { inner })
                .map_err(Into::into)
                .ready_chunks(capacity)
                .map(|x| x.into_iter().collect()),
        )
    }
}

pub struct TokioPostgresRow {
    inner: tokio_postgres::Row,
}

impl rdbc::Backend for TokioPostgresDriver {
    type Row = tokio_postgres::Row;
}

impl rdbc::Row for TokioPostgresRow {
    type Backend = TokioPostgresDriver;
    type Error = Error;

    fn get<'a, T>(&'a self, i: usize) -> Result<T, Self::Error>
    where
        T: rdbc::FromRow<'a, Self::Backend, Self::Error>,
    {
        T::from_row(&self.inner, i)
    }
}

macro_rules! impl_from_row {
    ($($ty: ty),* $(,)?) => {
        $(
            impl<'a> rdbc::FromRow<'a, TokioPostgresDriver, Error> for $ty {
                fn from_row(row: &'a tokio_postgres::Row, i: usize) -> Result<Self, Error> {
                    Ok(row.try_get(i)?)
                }
            }
        )*
    };
}

impl_from_row! {
    bool,
    i8,
    i16,
    i32,
    u32,
    i64,
    f32,
    f64,
    String,
    Vec<u8>,
    std::collections::HashMap<String, Option<String>>,
    std::net::IpAddr,
    std::time::SystemTime,
}

impl<'a> rdbc::FromRow<'a, TokioPostgresDriver, Error> for &'a str {
    fn from_row(row: &'a tokio_postgres::Row, i: usize) -> Result<Self, Error> {
        Ok(row.try_get(i)?)
    }
}

impl<'a> rdbc::FromRow<'a, TokioPostgresDriver, Error> for &'a [u8] {
    fn from_row(row: &'a tokio_postgres::Row, i: usize) -> Result<Self, Error> {
        Ok(row.try_get(i)?)
    }
}

impl<'a, T> rdbc::FromRow<'a, TokioPostgresDriver, Error> for Option<T>
where
    T: rdbc::FromRow<'a, TokioPostgresDriver, Error>,
    for<'b> T: tokio_postgres::types::FromSql<'b>,
{
    fn from_row(row: &'a tokio_postgres::Row, i: usize) -> Result<Self, Error> {
        Ok(row.try_get(i)?)
    }
}

#[cfg(test)]
mod tests {

    use rdbc::{prelude::*, r#async::*};
    use tokio::stream::StreamExt;

    use super::{Error, TokioPostgresDriver};

    #[tokio::test]
    async fn execute_queries() -> Result<(), Error> {
        let driver = TokioPostgresDriver::new("postgres://rdbc:secret@127.0.0.1:5433".to_string());
        println!("Connecting");
        let mut conn = driver.connect().await?;
        println!("Dropping table");
        conn.prepare("DROP TABLE IF EXISTS test")
            .await?
            .execute(&[])
            .await?;
        println!("Creating table");
        conn.prepare("CREATE TABLE test (a INT NOT NULL)")
            .await?
            .execute(&[])
            .await?;
        println!("Inserting into table");
        conn.prepare("INSERT INTO test (a) VALUES (?)")
            .await?
            .execute(&[rdbc::Value::Int32(123)])
            .await?;
        println!("Selecting from table using batches");
        let res: i32 = conn
            .prepare("SELECT a FROM test")
            .await?
            .query(&[])
            .await?
            .batches(10)
            .next()
            .await
            .unwrap()?
            .get(0)
            .unwrap()
            .get(0)?;
        assert_eq!(res, 123);
        println!("Selecting from table using rows");
        assert_eq!(
            conn.prepare("SELECT a FROM test")
                .await?
                .query(&[])
                .await?
                .rows()
                .next()
                .await
                .unwrap()?
                .get::<Option<i32>>(0)?,
            Some(123)
        );
        Ok(())
    }
}
