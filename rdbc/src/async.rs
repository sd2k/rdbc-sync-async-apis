use std::pin::Pin;

use async_trait::async_trait;
use futures_util::stream::Stream;

use crate::{Backend, MetaData, Row, Value};

/// Represents database driver that can be shared between threads, and can therefore implement
/// a connection pool
#[async_trait]
pub trait Driver: Backend + Sync + Send + Sized {
    /// The type of connection created by this driver.
    type Connection: Connection;

    type Error;

    /// Create a new database driver.
    fn new(url: String) -> Self;

    /// Create a connection to the database. Note that connections are intended to be used
    /// in a single thread since most database connections are not thread-safe
    async fn connect(&self) -> Result<Self::Connection, Self::Error>;
}

/// Represents a connection to a database
#[async_trait]
pub trait Connection {
    /// The type of statement produced by this connection.
    type Statement: Statement;

    type Error;

    /// Create a statement for execution
    async fn create(&mut self, sql: &str) -> Result<Self::Statement, Self::Error>;

    /// Create a prepared statement for execution
    async fn prepare(&mut self, sql: &str) -> Result<Self::Statement, Self::Error>;
}

/// Represents an executable statement
#[async_trait]
pub trait Statement {
    /// The type of ResultSet returned by this statement.
    type ResultSet: ResultSet;

    type Error;

    /// Execute a query that is expected to return a result set, such as a `SELECT` statement
    async fn query(&mut self, params: &[Value]) -> Result<Self::ResultSet, Self::Error>;

    /// Execute a query that is expected to update some rows.
    async fn execute(&mut self, params: &[Value]) -> Result<u64, Self::Error>;
}

pub type RowStream<T, E> = Pin<Box<dyn Stream<Item = Result<T, E>>>>;
pub type BatchStream<T, E> = Pin<Box<dyn Stream<Item = Result<Vec<T>, E>>>>;

/// Result set from executing a query against a statement
pub trait ResultSet: Sized {
    /// The type of metadata associated with this result set.
    type MetaData: MetaData;
    /// The type of row included in this result set.
    type Row: Row;

    type Error;

    /// get meta data about this result set
    fn meta_data(&self) -> Result<&Self::MetaData, Self::Error>;

    /// Get a stream of rows.
    fn rows(self) -> RowStream<Self::Row, Self::Error>;

    /// Get a stream where each item is a batch of rows.
    fn batches(self, capacity: usize) -> BatchStream<Self::Row, Self::Error>;
}
