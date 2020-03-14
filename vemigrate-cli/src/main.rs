#![allow(clippy::cognitive_complexity)]

#[macro_use]
extern crate cdrs;
#[macro_use]
extern crate clap;

use log::{error, info, trace};
use vemigrate::Migrator;

mod cli;
mod config;
mod database;
mod error;

use config::DBConf;
use database::Database;

use std::fmt::Display;
use std::fs;
use std::path::PathBuf;

const MIGRATIONS_DIR_PATH: &str = "./migrations";
const INITIAL_MIGRATION_NAME: &str = "initial";
const GIT_KEEP_FILE: &str = ".gitkeep";

fn main() {
    let path = PathBuf::from(MIGRATIONS_DIR_PATH);
    let matches = cli::build().get_matches();

    match matches.subcommand() {
        // Create migrations directory, and initial migration
        (cli::CMD_INIT, Some(args)) => {
            trace!("init");
            if is_initiated(&path) {
                return fatal_err("already initiated");
            }

            let replication_strategy = database::ReplicationStrategy::from_str(
                args.value_of(cli::ARG_REPLICATION_STRATEGY).unwrap(),
            )
            .unwrap();
            let replication_factor: usize =
                value_t!(args, cli::ARG_REPLICATION_FACTOR, usize).unwrap_or_else(fatal_err);
            initiate(path, replication_strategy, replication_factor).unwrap_or_else(fatal_err)
        }
        // Create new migration with empty `up` and `down` files
        (cli::CMD_NEW, Some(args)) => {
            trace!("new migration");
            vemigrate::create_empty_migration(args.value_of("name").unwrap(), path)
                .unwrap_or_else(fatal_err)
        }
        // Check another subcommands that require db instance
        (cmd, args) => {
            if !is_initiated(&path) {
                return fatal_err("need to run `init` first");
            }

            // Create Migrator instance with Scylla as a store for migrations
            let db_conf = DBConf::parse().unwrap_or_else(fatal_err);
            let db = Database::with_session(&db_conf.addr, &db_conf.user, &db_conf.pwd)
                .unwrap_or_else(fatal_err);
            let migrator = Migrator::with_store(path.to_str().unwrap(), db);

            // Do stuff based on subcommand
            match cmd {
                cli::CMD_MIGRATE => {
                    trace!("execute all pending migrations");
                    match migrator.migrate_up() {
                        Ok(Some(n)) => info!("{} migrations executed", n),
                        Ok(None) => info!("no pending migrations found"),
                        Err(err) => fatal_err(err),
                    };
                }
                cli::CMD_RESET => {
                    trace!("rollback all migrations");
                    match migrator.migrate_down() {
                        Ok(Some(n)) => info!("{} migrations rolled back", n),
                        Ok(None) => info!("no migrations found"),
                        Err(err) => fatal_err(err),
                    };
                }
                cli::CMD_DO => {
                    let n: usize = args
                        .unwrap()
                        .value_of("n")
                        .unwrap()
                        .parse()
                        .unwrap_or_else(fatal_err);
                    trace!("execute {} migrations", n);
                    match migrator.migrate_up_n(n) {
                        Ok(Some(n)) => info!("{} migrations executed", n),
                        Ok(None) => info!("no pending migrations found"),
                        Err(err) => fatal_err(err),
                    };
                }
                cli::CMD_UNDO => {
                    let n: usize = args
                        .unwrap()
                        .value_of("n")
                        .unwrap()
                        .parse()
                        .unwrap_or_else(fatal_err);
                    trace!("rollback {} migrations", n);
                    match migrator.migrate_down_n(n) {
                        Ok(Some(n)) => info!("{} migrations rolled back", n),
                        Ok(None) => info!("no migrations found"),
                        Err(err) => fatal_err(err),
                    };
                }
                cli::CMD_REDO => {
                    trace!("redo the last migration");
                    match migrator.migrate_down_n(1) {
                        Ok(Some(_)) => {
                            info!("1 migration rolled back");
                            match migrator.migrate_up_n(1) {
                                Ok(Some(_)) => info!("1 migration executed"),
                                Ok(None) => fatal_err("no pending migrations found"),
                                Err(err) => fatal_err(err),
                            };
                        }
                        Ok(None) => info!("no migrations found"),
                        Err(err) => fatal_err(err),
                    };
                }
                _ => unreachable!(),
            }
        }
    }
}

fn is_initiated(path: &PathBuf) -> bool {
    if !path.exists() || !path.join(GIT_KEEP_FILE).exists() {
        return false;
    }

    let mut dir = match fs::read_dir(path) {
        Ok(v) => v,
        Err(_) => return false,
    };

    dir.any(|entry| match entry {
        Ok(e) => {
            e.file_name().to_str().unwrap().splitn(2, '_').nth(1) == Some(INITIAL_MIGRATION_NAME)
        }
        Err(_) => false,
    })
}

fn initiate(
    path: PathBuf,
    replication_strategy: database::ReplicationStrategy,
    replication_factor: usize,
) -> std::io::Result<()> {
    if !path.exists() {
        create_migrations_dir(&path)?;
    }

    vemigrate::create_migration(
        INITIAL_MIGRATION_NAME,
        path,
        database::Database::initial_migration_up(replication_strategy, replication_factor),
        database::Database::initial_migration_down(),
    )?;
    Ok(())
}

fn create_migrations_dir(path: &PathBuf) -> std::io::Result<()> {
    println!("creating migrations directory at: {}", path.display());
    fs::create_dir(&path)?;
    fs::File::create(path.join(GIT_KEEP_FILE))?;
    Ok(())
}

fn fatal_err<E: Display, T>(err: E) -> T {
    error!("{}", err);
    std::process::exit(1);
}
