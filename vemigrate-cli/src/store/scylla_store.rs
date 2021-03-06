use cdrs::authenticators::StaticPasswordAuthenticator;
use cdrs::cluster::session::{new as new_session, Session};
use cdrs::cluster::{ClusterTcpConfig, NodeTcpConfigBuilder, TcpConnectionPool};
use cdrs::load_balancing::RoundRobin;
use cdrs::types::from_cdrs::FromCDRSByName;
use cdrs::types::prelude::*;
use cdrs::Result as CDRSResult;
use vemigrate::{self, MigrationRow, Store};

use cdrs::query::QueryExecutor;
use std::fmt::{self, Display, Formatter};
use std::{error, io};

pub const SIMPLE_STRATEGY: &str = "SimpleStrategy";
pub const NETWORK_TOPOLOGY_STRATEGY: &str = "NetworkTopologyStrategy";

pub enum ReplicationStrategy {
    Simple,
    NetworkTopology,
}

impl ReplicationStrategy {
    pub fn from_str(val: &str) -> Option<Self> {
        match val {
            SIMPLE_STRATEGY => Some(ReplicationStrategy::Simple),
            NETWORK_TOPOLOGY_STRATEGY => Some(ReplicationStrategy::NetworkTopology),
            _ => None,
        }
    }
}

impl Default for ReplicationStrategy {
    fn default() -> Self {
        ReplicationStrategy::Simple
    }
}

impl Display for ReplicationStrategy {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ReplicationStrategy::Simple => f.write_str(SIMPLE_STRATEGY),
            ReplicationStrategy::NetworkTopology => f.write_str(NETWORK_TOPOLOGY_STRATEGY),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Database(cdrs::Error),
    Io(io::Error),
}

impl error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Io(ref e) => e.fmt(f),
            Error::Database(ref e) => e.fmt(f),
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::Io(err)
    }
}

impl From<cdrs::Error> for Error {
    fn from(err: cdrs::Error) -> Self {
        Error::Database(err)
    }
}

impl Into<vemigrate::Error> for Error {
    fn into(self) -> vemigrate::Error {
        vemigrate::Error::Store(Box::new(self))
    }
}

type ScyllaSession = Session<RoundRobin<TcpConnectionPool<StaticPasswordAuthenticator>>>;

pub struct ScyllaStore<'a> {
    conn: ScyllaSession,
    keyspace: &'a str,
}

impl<'a> ScyllaStore<'a> {
    pub fn with_session(addr: &str, keyspace: &'a str, user: &str, password: &str) -> Result<Self> {
        let auth = StaticPasswordAuthenticator::new(user, password);
        let nodes = vec![NodeTcpConfigBuilder::new(addr, auth).build()];
        let cluster_config = ClusterTcpConfig(nodes);

        let conn = new_session(&cluster_config, RoundRobin::new())?;
        Ok(Self { conn, keyspace })
    }

    pub fn initial_migration_up(
        keyspace: &str,
        replication_strategy: ReplicationStrategy,
        replication_factor: usize,
    ) -> String {
        format!(
            r#"-- This file is automatically @generated by Vemigrate CLI.
create keyspace if not exists {} with replication = {{ 'class' : '{}', 'replication_factor': {} }};
create table if not exists {}.migrations (
    id bigint,
    up boolean,
    primary key(id)
);"#,
            keyspace, replication_strategy, replication_factor, keyspace
        )
    }

    pub fn initial_migration_down(keyspace: &str) -> String {
        format!(
            r#"-- This file is automatically @generated by Vemigrate CLI.
drop table if exists {}.migrations;
drop keyspace if exists {};"#,
            keyspace, keyspace
        )
    }
}

#[derive(Clone, Debug, TryFromRow, PartialEq)]
pub struct Migration {
    pub id: i64,
    pub up: bool,
}

impl MigrationRow for Migration {
    fn id(&self) -> u64 {
        self.id as u64
    }

    fn is_up(&self) -> bool {
        self.up
    }
}

impl<'a> Store for ScyllaStore<'a> {
    type Row = Migration;
    type Error = Error;

    fn get_all(&self) -> Result<Option<Vec<Self::Row>>> {
        debug!("select migrations history");

        let rows = self
            .conn
            .query_with_values_tw(
                "select * from system_schema.keyspaces where keyspace_name = ?;",
                query_values!(self.keyspace),
                false,
                false,
            )?
            .get_body()?
            .into_rows();

        match rows {
            Some(rows) => {
                if rows.is_empty() {
                    debug!("keyspace doe not exist");
                    return Ok(None);
                }
            }
            None => {
                debug!("keyspace doe not exist");
                return Ok(None);
            }
        };

        let res = self
            .conn
            .query_tw(
                format!("select id, up from {}.migrations", self.keyspace),
                false,
                false,
            )?
            .get_body()?
            .into_rows();

        match res {
            Some(rows) => {
                if rows.is_empty() {
                    debug!("no migrations found in history");
                    return Ok(None);
                }

                Ok(Some(
                    rows.into_iter()
                        .map(Self::Row::try_from_row)
                        .collect::<CDRSResult<Vec<Self::Row>>>()
                        .map_err(Error::from)?,
                ))
            }
            None => {
                debug!("no migrations found in history");
                Ok(None)
            }
        }
    }

    fn add(&self, id: u64, up: bool) -> Result<()> {
        debug!("store migration with id = {} and up = {}", id, up);
        self.conn
            .query_with_values_tw(
                format!(
                    "insert into {}.migrations (id,up) values (?, ?);",
                    self.keyspace
                ),
                query_values!(id, up),
                false,
                false,
            )
            .map_err(Error::from)
            .map(|_| ())
    }

    fn exec(&self, q: &str) -> Result<()> {
        debug!("exec query: {}", q);
        self.conn
            .query_tw(q, false, false)
            .map_err(Error::from)
            .map(|_| ())
    }
}
