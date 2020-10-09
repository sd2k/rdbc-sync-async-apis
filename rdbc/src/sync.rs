use crate::{Backend, MetaData, Row, Value};

/// Represents database driver that can be shared between threads, and can therefore implement
/// a connection pool
pub trait Driver: Backend + Sync + Send + Sized {
    /// The type of connection pool created by this driver.
    type ConnectionPool: ConnectionPool;

    type Error;

    /// Create a new database driver.
    fn new(url: String) -> Self;

    /// Create a connection to the database. Note that connections are intended to be used
    /// in a single thread since most database connections are not thread-safe
    fn connect(&self) -> Result<Self::ConnectionPool, Self::Error>;
}

/// Represents a pool of connections to a database.
pub trait ConnectionPool {
    /// The type of statement produced by this connection pool.
    type Statement: Statement;

    type Error;

    /// Create a statement for execution
    fn create(&mut self, sql: &str) -> Result<Self::Statement, Self::Error>;

    /// Create a prepared statement for execution
    fn prepare(&mut self, sql: &str) -> Result<Self::Statement, Self::Error>;
}

/// Represents an executable statement
pub trait Statement {
    /// The type of `ResultSet` returned by this statement.
    type ResultSet: ResultSet;

    type Error;

    /// Execute a query that is expected to return a result set, such as a `SELECT` statement
    fn query(&mut self, params: &[Value]) -> Result<Self::ResultSet, Self::Error>;

    /// Execute a query that is expected to update some rows.
    fn execute(&mut self, params: &[Value]) -> Result<u64, Self::Error>;
}

/// Result set from executing a query against a statement
pub trait ResultSet: Sized {
    /// The type of metadata associated with this result set.
    type MetaData: MetaData;
    /// The type of row included in this result set.
    type Row: Row;

    type Error;

    /// Get metadata about this result set
    fn meta_data(&self) -> Result<&Self::MetaData, Self::Error>;

    /// Get a stream of rows.
    fn rows(self) -> Box<dyn Iterator<Item = Self::Row>>;
}
