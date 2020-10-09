//! The RDBC (Rust DataBase Connectivity) API is loosely based on the ODBC and JDBC standards
//! and provides a database agnostic programming interface for executing queries and fetching
//! results.
//!
//! Reference implementation RDBC Drivers exist for Postgres, MySQL and SQLite.
//!
//! The following example demonstrates how RDBC can be used to run a trivial query against Postgres.
//!
//! ```rust,ignore
//! use rdbc::{prelude::*, sync::*};
//! use rdbc_postgres::PostgresDriver;
//!
//! let driver = PostgresDriver::new("postgres://postgres:password@localhost:5433".to_string());
//! let mut conn = driver.connect().unwrap();
//! let mut stmt = conn.prepare("SELECT a FROM b WHERE c = ?").unwrap();
//! let rs = stmt.execute_query(&[Value::Int32(123)]).unwrap();
//! let mut rows = rs.rows().unwrap();
//! while let Some(row) = rows.next() {
//!   println!("{:?}", row.get::<&str>(0));
//! }
//! ```

#[cfg(feature = "sync")]
pub mod sync;

#[cfg(feature = "async")]
pub mod r#async;

pub mod prelude {
    pub use super::{Column, DataType, Error as RdbcError, MetaData, Row, Value};
}

/// RDBC Error
#[derive(Debug)]
pub enum Error {
    General(String),
}

#[derive(Debug, Clone)]
pub enum Value {
    Int32(i32),
    UInt32(u32),
    String(String),
    //TODO add other types
}

impl ToString for Value {
    fn to_string(&self) -> String {
        match self {
            Self::Int32(n) => n.to_string(),
            Self::UInt32(n) => n.to_string(),
            Self::String(s) => s.clone(),
        }
    }
}

pub trait Backend {
    type Row;
}

pub trait Row {
    type Backend: Backend;
    type Error;
    fn get<'a, T>(&'a self, i: usize) -> Result<T, Self::Error>
    where
        T: FromSqlRow<'a, Self::Backend, Self::Error>;
}

pub trait FromSqlRow<'a, B, E>: Sized
where
    B: Backend,
{
    fn from_row(row: &'a B::Row, i: usize) -> Result<Self, E>;
}

/// Meta data for result set
pub trait MetaData {
    fn num_columns(&self) -> usize;
    fn column_name(&self, i: usize) -> &str;
    fn column_type(&self, i: usize) -> DataType;
}

/// RDBC Data Types
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum DataType {
    Bool,
    Byte,
    Char,
    Short,
    Integer,
    Float,
    Double,
    Decimal,
    Date,
    Time,
    Datetime,
    Utf8,
    Binary,
}

#[derive(Debug, Clone)]
pub struct Column {
    name: String,
    data_type: DataType,
}

impl Column {
    #[must_use]
    pub fn new(name: &str, data_type: DataType) -> Self {
        Self {
            name: name.to_owned(),
            data_type,
        }
    }
}

impl MetaData for Vec<Column> {
    fn num_columns(&self) -> usize {
        self.len()
    }

    fn column_name(&self, i: usize) -> &str {
        &self[i].name
    }

    fn column_type(&self, i: usize) -> DataType {
        self[i].data_type
    }
}
