#![allow(clippy::cognitive_complexity)]

#[macro_use]
extern crate cdrs;
#[macro_use]
extern crate cdrs_helpers_derive;
#[macro_use]
extern crate log;

use log::{LevelFilter, Metadata, Record};
use vemigrate::Migrator;

mod configs;
mod store;

use configs::{Command, Configs};
use store::{ReplicationStrategy, ScyllaStore};

use std::fmt::Display;
use std::fs;
use std::path::PathBuf;

const INITIAL_MIGRATION_NAME: &str = "initial";
const NEW_FILE_CONTENT: &str = "-- Add your migration query below";

struct SimpleLogger;

impl log::Log for SimpleLogger {
    fn enabled(&self, _: &Metadata) -> bool {
        true
    }

    fn log(&self, record: &Record) {
        println!("{} - {}", record.level(), record.args());
    }

    fn flush(&self) {}
}

static LOGGER: SimpleLogger = SimpleLogger;

fn main() {
    let cfg = Configs::parse();

    let level = match cfg.verbose {
        0 => LevelFilter::Info,
        1 => LevelFilter::Error,
        2 => LevelFilter::Warn,
        3 => LevelFilter::Info,
        4 => LevelFilter::Debug,
        _ => LevelFilter::Trace,
    };

    log::set_logger(&LOGGER)
        .map(|()| log::set_max_level(level))
        .expect("configure logger");

    match cfg.cmd {
        // Create migrations directory, and initial migration.
        Command::Init(args) => {
            if cfg.path.exists() {
                return fatal_err("migrations dir already exists");
            }

            let replication_strategy =
                ReplicationStrategy::from_str(&args.replication_strategy).unwrap();
            let migration_path = initiate(
                &cfg.path,
                &cfg.db.keyspace,
                replication_strategy,
                args.replication_factor,
            )
            .unwrap_or_else(fatal_err);
            info!("{} was created", migration_path.display())
        }
        // Create new migration with empty `up` and `down` files
        Command::New(args) => {
            if !cfg.path.exists() {
                return fatal_err("please do `cargo-cli init` first");
            }

            let migration_path = vemigrate::create_migration(
                &args.name,
                cfg.path,
                NEW_FILE_CONTENT,
                NEW_FILE_CONTENT,
            )
            .unwrap_or_else(fatal_err);
            info!("{} was created", migration_path.display())
        }
        // Check another subcommands that require db instance
        cmd => {
            if !cfg.path.exists() {
                return fatal_err("please do `cargo-cli init` first");
            }

            // Create Migrator instance with Scylla as a store for migrations
            let db = ScyllaStore::with_session(
                &cfg.db.node,
                &cfg.db.keyspace,
                &cfg.db.user,
                &cfg.db.password,
            )
            .unwrap_or_else(fatal_err);
            let migrator = Migrator::with_store(&cfg.path, db);

            // Do stuff depends on subcommand
            match cmd {
                Command::Migrate => {
                    info!("execute pending migrations");
                    match migrator.migrate_up() {
                        Ok(Some(id)) => info!("migrated up to {}", id),
                        Ok(None) => info!("no pending migrations found"),
                        Err(err) => fatal_err(err),
                    };
                }
                Command::Reset => {
                    info!("rollback all migrations");
                    match migrator.migrate_down() {
                        Ok(Some(id)) => info!("migrated down to {}", id),
                        Ok(None) => info!("no migrations found"),
                        Err(err) => fatal_err(err),
                    };
                }
                Command::Do(n) => {
                    info!("execute {} migrations", n.count);
                    match migrator.migrate_up_n(n.count) {
                        Ok(Some(id)) => info!("migrated up to {}", id),
                        Ok(None) => info!("no pending migrations found"),
                        Err(err) => fatal_err(err),
                    };
                }
                Command::Undo(n) => {
                    info!("rollback {} migrations", n.count);
                    match migrator.migrate_down_n(n.count) {
                        Ok(Some(id)) => info!("migrated down to {}", id),
                        Ok(None) => info!("no migrations found"),
                        Err(err) => fatal_err(err),
                    };
                }
                Command::Redo => {
                    info!("redo the last migration");
                    match migrator.migrate_down_n(1) {
                        Ok(Some(_)) => {
                            info!("the last migration was rolled back");
                            match migrator.migrate_up_n(1) {
                                Ok(Some(_)) => info!("the last migration was executed"),
                                Ok(None) => fatal_err("no pending migrations found"),
                                Err(err) => fatal_err(err),
                            };
                        }
                        Ok(None) => info!("no pending migrations found"),
                        Err(err) => fatal_err(err),
                    };
                }
                _ => unreachable!(),
            }
        }
    }
}

fn initiate(
    path: &PathBuf,
    keyspace: &str,
    replication_strategy: ReplicationStrategy,
    replication_factor: usize,
) -> std::io::Result<PathBuf> {
    if !path.exists() {
        create_migrations_dir(path)?;
    }

    vemigrate::create_migration(
        INITIAL_MIGRATION_NAME,
        path,
        ScyllaStore::initial_migration_up(keyspace, replication_strategy, replication_factor),
        ScyllaStore::initial_migration_down(keyspace),
    )
}

fn create_migrations_dir(path: &PathBuf) -> std::io::Result<()> {
    println!("creating migrations directory at: {}", path.display());
    fs::create_dir(&path)?;
    Ok(())
}

fn fatal_err<E: Display, T>(err: E) -> T {
    error!("{}", err);
    std::process::exit(1);
}
