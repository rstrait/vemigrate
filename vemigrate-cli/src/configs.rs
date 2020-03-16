use structopt::StructOpt;

use crate::store::{NETWORK_TOPOLOGY_STRATEGY, SIMPLE_STRATEGY};

use std::path::PathBuf;

#[derive(Debug, StructOpt)]
pub struct Init {
    /// Replication strategy
    #[structopt(long = "replication-strategy", default_value = "SimpleStrategy", possible_values = &[NETWORK_TOPOLOGY_STRATEGY, SIMPLE_STRATEGY])]
    pub replication_strategy: String,

    /// Replication factor
    #[structopt(long = "replication-factor", default_value = "1")]
    pub replication_factor: usize,
}

#[derive(Debug, StructOpt)]
pub struct New {
    /// Name of a new migration
    #[structopt(short, long)]
    pub name: String,
}

#[derive(Debug, StructOpt)]
pub struct MigrationsCount {
    /// Count of migrations
    #[structopt(short, long, default_value = "1")]
    pub count: usize,
}

#[derive(Debug, StructOpt)]
pub enum Command {
    /// Creates the migrations dir and the initial migration.
    #[structopt(name = "init")]
    Init(Init),

    /// Creates new empty migration with the given name, and the current timestamp as the version.
    #[structopt(name = "new")]
    New(New),

    /// Runs all pending migrations.
    #[structopt(name = "migrate")]
    Migrate,

    /// Rolls back all migrations
    #[structopt(name = "reset")]
    Reset,

    /// Runs `n` pending migrations.
    #[structopt(name = "do")]
    Do(MigrationsCount),

    /// Undoes `n` the latest migrations.
    #[structopt(name = "undo")]
    Undo(MigrationsCount),

    /// Re-runs last migration.
    #[structopt(name = "redo")]
    Redo,
}

#[derive(Debug, StructOpt)]
pub struct Database {
    /// Database node address.
    #[structopt(long = "db-node", env = "VEMIGRATE_NODE_ADDR")]
    pub node: String,

    /// Database keyspace.
    #[structopt(
        long = "db-keyspace",
        env = "VEMIGRATE_KEYSPACE",
        default_value = "vemigrate"
    )]
    pub keyspace: String,

    /// Database user.
    #[structopt(long = "db-user", env = "VEMIGRATE_USER")]
    pub user: String,

    /// Database password.
    #[structopt(long = "db-password", env = "VEMIGRATE_PASSWORD")]
    pub password: String,
}

/// Database migrations tool for Scylla.
#[derive(Debug, StructOpt)]
pub struct Configs {
    #[structopt(subcommand)]
    pub cmd: Command,

    #[structopt(flatten)]
    pub db: Database,

    /// Path to migration folder
    #[structopt(short, long, default_value = "./migrations")]
    pub path: PathBuf,

    // The number of occurrences of the `v/verbose` flag
    /// Verbose mode (-v, -vv, -vvv, etc.)
    #[structopt(short, parse(from_occurrences))]
    pub verbose: u8,
}

impl Configs {
    pub fn parse() -> Self {
        Self::from_args()
    }
}
