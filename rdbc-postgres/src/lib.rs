//! Postgres RDBC Driver
//!
//! This crate implements an RDBC Driver for the `postgres` crate.
//!
//! The RDBC (Rust DataBase Connectivity) API is loosely based on the ODBC and JDBC standards.
//!
//! ```rust,no_run
//! use rdbc::{prelude::*, sync::*};
//! use rdbc_postgres::PostgresDriver;
//!
//! let driver = PostgresDriver::new("postgres://postgres:password@localhost:5433".to_string());
//! let mut conn = driver.connect().unwrap();
//! let mut stmt = conn.prepare("SELECT a FROM b WHERE c = ?").unwrap();
//! let rs = stmt.query(&[Value::Int32(123)]).unwrap();
//! let mut rows = rs.rows();
//! while let Some(row) = rows.next() {
//!   println!("{:?}", row.get::<&str>(0));
//! }
//! ```

use r2d2_postgres::{
    postgres::{
        self,
        types::{FromSql, Type},
        NoTls, Row, Statement,
    },
    r2d2::{self, Pool, PooledConnection},
    PostgresConnectionManager,
};
use sqlparser::{
    dialect::{keywords::Keyword, PostgreSqlDialect},
    tokenizer::{Token, Tokenizer, Word},
};

use rdbc::Column;

#[derive(Debug)]
pub enum Error {
    R2d2(r2d2::Error),
    Postgres(postgres::Error),
    Clone,
}

impl From<postgres::Error> for Error {
    fn from(other: postgres::Error) -> Self {
        Self::Postgres(other)
    }
}

impl From<r2d2::Error> for Error {
    fn from(other: r2d2::Error) -> Self {
        Self::R2d2(other)
    }
}

pub struct PostgresDriver {
    url: String,
}

impl rdbc::Backend for PostgresDriver {
    type Row = Row;
}

impl rdbc::sync::Driver for PostgresDriver {
    type ConnectionPool = PConnectionPool;
    type Error = Error;

    fn new(url: String) -> Self {
        Self { url }
    }

    fn connect(&self) -> Result<Self::ConnectionPool, Self::Error> {
        let manager = PostgresConnectionManager::new(self.url.parse()?, NoTls);
        let pool = Pool::new(manager)?;
        Ok(PConnectionPool { pool })
    }
}

type PgPool = Pool<PostgresConnectionManager<NoTls>>;
type PooledConn = PooledConnection<PostgresConnectionManager<NoTls>>;

pub struct PConnectionPool {
    pool: PgPool,
}

impl rdbc::sync::ConnectionPool for PConnectionPool {
    type Statement = PStatement;
    type Error = Error;

    fn create(&mut self, sql: &str) -> Result<Self::Statement, Self::Error> {
        self.prepare(sql)
    }

    fn prepare(&mut self, sql: &str) -> Result<Self::Statement, Self::Error> {
        // translate SQL, mapping ? into $1 style bound param placeholder
        let dialect = PostgreSqlDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, sql);
        let tokens = tokenizer.tokenize().unwrap();
        let mut i = 0;
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

        let mut client = self.pool.get()?;
        let sql = tokens
            .iter()
            .map(|t| format!("{}", t))
            .collect::<Vec<String>>()
            .join("");
        let statement = client.prepare(&sql)?;

        Ok(PStatement {
            conn: client,
            statement,
        })
    }
}

pub struct PStatement {
    conn: PooledConn,
    statement: Statement,
}

impl rdbc::sync::Statement for PStatement {
    type Error = Error;
    type ResultSet = PResultSet;

    fn query(&mut self, params: &[rdbc::Value]) -> Result<Self::ResultSet, Self::Error> {
        let params = to_postgres_value(params);
        let params: Vec<&(dyn postgres::types::ToSql + Sync)> =
            params.iter().map(AsRef::as_ref).collect();
        let rows = self.conn.query(&self.statement, params.as_slice())?;
        let meta = self
            .statement
            .columns()
            .iter()
            .map(|c| rdbc::Column::new(c.name(), to_rdbc_type(c.type_())))
            .collect();

        Ok(PResultSet { meta, rows })
    }

    fn execute(&mut self, params: &[rdbc::Value]) -> Result<u64, Self::Error> {
        let params = to_postgres_value(params);
        let params: Vec<&(dyn postgres::types::ToSql + Sync)> =
            params.iter().map(AsRef::as_ref).collect();
        self.conn
            .execute(&self.statement, params.as_slice())
            .map_err(Into::into)
    }
}

pub struct PResultSet {
    meta: Vec<Column>,
    rows: Vec<Row>,
}

impl rdbc::sync::ResultSet for PResultSet {
    type MetaData = Vec<rdbc::Column>;
    type Row = PostgresRow;
    type Error = Error;

    fn meta_data(&self) -> Result<&Self::MetaData, Self::Error> {
        Ok(&self.meta)
    }

    fn rows(self) -> Box<dyn Iterator<Item = Self::Row>> {
        Box::new(self.rows.into_iter().map(|inner| PostgresRow { inner }))
    }
}

pub struct PostgresRow {
    inner: Row,
}

impl rdbc::Row for PostgresRow {
    type Backend = PostgresDriver;
    type Error = Error;

    fn get<'a, T>(&'a self, i: usize) -> Result<T, Self::Error>
    where
        T: rdbc::FromSqlRow<'a, Self::Backend, Self::Error>,
    {
        T::from_row(&self.inner, i)
    }
}

macro_rules! impl_from_sql_row {
    ($($ty: ty),* $(,)?) => {
        $(
            impl<'a> rdbc::FromSqlRow<'a, PostgresDriver, Error> for $ty {
                fn from_row(row: &'a Row, i: usize) -> Result<Self, Error> {
                    Ok(row.try_get(i)?)
                }
            }
        )*
    };
}

impl_from_sql_row! {
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

impl<'a> rdbc::FromSqlRow<'a, PostgresDriver, Error> for &'a str {
    fn from_row(row: &'a Row, i: usize) -> Result<Self, Error> {
        Ok(row.try_get(i)?)
    }
}

impl<'a> rdbc::FromSqlRow<'a, PostgresDriver, Error> for &'a [u8] {
    fn from_row(row: &'a Row, i: usize) -> Result<Self, Error> {
        Ok(row.try_get(i)?)
    }
}

impl<'a, T> rdbc::FromSqlRow<'a, PostgresDriver, Error> for Option<T>
where
    T: rdbc::FromSqlRow<'a, PostgresDriver, Error>,
    for<'b> T: FromSql<'b>,
{
    fn from_row(row: &'a Row, i: usize) -> Result<Self, Error> {
        Ok(row.try_get(i)?)
    }
}

fn to_rdbc_type(ty: &Type) -> rdbc::DataType {
    match ty.name() {
        "" => rdbc::DataType::Bool,
        //TODO all types
        _ => rdbc::DataType::Utf8,
    }
}

fn to_postgres_value(values: &[rdbc::Value]) -> Vec<Box<dyn postgres::types::ToSql + Sync>> {
    values
        .iter()
        .map(|v| match v {
            rdbc::Value::String(s) => Box::new(s.clone()) as Box<dyn postgres::types::ToSql + Sync>,
            rdbc::Value::Int32(n) => Box::new(*n) as Box<dyn postgres::types::ToSql + Sync>,
            rdbc::Value::UInt32(n) => Box::new(*n) as Box<dyn postgres::types::ToSql + Sync>,
            //TODO all types
        })
        .collect()
}

#[cfg(test)]
mod tests {

    use rdbc::{prelude::*, sync::*};

    use super::*;

    #[test]
    fn test_execute_query() -> Result<(), Error> {
        let driver = PostgresDriver::new("postgres://rdbc:secret@127.0.0.1:5433".to_string());
        println!("Connecting");
        let mut pool = driver.connect()?;
        println!("Dropping table");
        pool.prepare("DROP TABLE IF EXISTS test")?
            .execute(&vec![])?;
        println!("Creating table");
        pool.prepare("CREATE TABLE test (a INT NOT NULL)")?
            .execute(&vec![])?;
        println!("Inserting into table");
        pool.prepare("INSERT INTO test (a) VALUES (?)")?
            .execute(&vec![rdbc::Value::Int32(123)])?;

        println!("Selecting from table");
        let rs = pool.prepare("SELECT a FROM test")?.query(&vec![])?;
        let mut rows = rs.rows();
        let row = rows.next();
        assert!(row.is_some());
        assert_eq!(row.unwrap().get::<Option<i32>>(0)?, Some(123));

        Ok(())
    }
}
