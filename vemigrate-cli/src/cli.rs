use crate::database::{NETWORK_TOPOLOGY_STRATEGY, SIMPLE_STRATEGY};
use clap::{App, Arg, SubCommand};

pub const CMD_INIT: &'static str = "init";
pub const CMD_NEW: &'static str = "new";
pub const CMD_MIGRATE: &'static str = "migrate";
pub const CMD_RESET: &'static str = "reset";
pub const CMD_DO: &'static str = "do";
pub const CMD_UNDO: &'static str = "undo";
pub const CMD_REDO: &'static str = "redo";

pub const ARG_REPLICATION_STRATEGY: &'static str = "replication_strategy";
pub const ARG_REPLICATION_FACTOR: &'static str = "replication_factor";

pub fn build() -> App<'static, 'static> {
    let n_arg = Arg::with_name("n").short("n").takes_value(true);

    App::new("vemigrate")
        .version("1.0")
        .author("Alexey B. <https://github.com/Arekkusuva>")
        .about("Database migrations tool for Scylla")
        .subcommand(SubCommand::with_name(CMD_INIT)
            .about("Creates the migrations dir and the initial migration"))
            .arg(Arg::with_name(ARG_REPLICATION_STRATEGY)
                .default_value(SIMPLE_STRATEGY)
                .possible_value(SIMPLE_STRATEGY)
                .possible_value(NETWORK_TOPOLOGY_STRATEGY))
            .arg(Arg::with_name(ARG_REPLICATION_FACTOR)
                .default_value("1"))
        .subcommand(SubCommand::with_name(CMD_NEW)
            .about("Creates new empty migration with the given name, and the current timestamp as the version")
            .arg(Arg::with_name("name")
                .help("The name of the migration")
                .required(true)))
        .subcommand(SubCommand::with_name(CMD_MIGRATE)
            .about("Runs all pending migrations"))
        .subcommand(SubCommand::with_name(CMD_RESET)
            .about("Rolls back all migrations"))
        .subcommand(SubCommand::with_name(CMD_DO)
            .about("Runs `n` pending migrations")
            .arg(n_arg.clone().default_value("1")))
        .subcommand(SubCommand::with_name(CMD_UNDO)
            .about("Undoes `n` the latest migrations")
            .arg(n_arg.clone().default_value("1")))
        .subcommand(SubCommand::with_name(CMD_REDO)
            .about("Re-runs last migration"))
}
