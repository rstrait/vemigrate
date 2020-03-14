#![allow(clippy::type_complexity)]

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fs::{File, ReadDir};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::{error, fmt, fs, io};

pub const MIGRATION_FILE_UP: &str = "up.cql";
pub const MIGRATION_FILE_DOWN: &str = "down.cql";

const COMMENT_LENGTH: usize = 2;
const COMMENT_LINE_TYPE_1: &str = "--";
const COMMENT_LINE_TYPE_2: &str = "//";
const QUERIES_SEPARATOR: char = ';';

const NEW_FILE_CONTENT: &str = "-- Add your migration query below";

#[derive(Debug)]
pub enum Error {
    ParseMigrationFile(String),
    Store(Box<dyn error::Error>),
    Io(io::Error),
}

impl error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::ParseMigrationFile(ref err) => f.write_str(err),
            Error::Store(ref e) => e.fmt(f),
            Error::Io(ref e) => e.fmt(f),
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::Io(err)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

pub trait MigrationRow {
    fn is_up(&self) -> bool;
    fn id(&self) -> i64;
}

pub trait Store {
    type Row: MigrationRow;
    type Error: std::error::Error + 'static;

    fn get_all(&self) -> std::result::Result<Option<Vec<Self::Row>>, Self::Error>;
    fn store_one(&self, id: i64, up: bool) -> std::result::Result<(), Self::Error>;
    fn exec(&self, q: &str) -> std::result::Result<(), Self::Error>;
}

pub fn create_empty_migration<P>(name: &str, migrations_dir: P) -> std::io::Result<()>
where
    P: AsRef<Path>,
{
    create_migration(name, migrations_dir, NEW_FILE_CONTENT, NEW_FILE_CONTENT)
}

pub fn create_migration<P, Q>(
    name: &str,
    migrations_dir: P,
    q_up: Q,
    q_down: Q,
) -> std::io::Result<()>
where
    P: AsRef<Path>,
    Q: AsRef<[u8]>,
{
    let migration_path = migrations_dir.as_ref().join(name);
    fs::create_dir_all(&migration_path)?;
    create_migration_file(migration_path.join(MIGRATION_FILE_UP), Some(q_up.as_ref()))?;
    create_migration_file(
        migration_path.join(MIGRATION_FILE_DOWN),
        Some(q_down.as_ref()),
    )?;
    Ok(())
}

fn create_migration_file(path: PathBuf, q: Option<&[u8]>) -> std::io::Result<()> {
    let mut f = fs::File::create(path)?;
    if let Some(bytes) = q {
        f.write_all(bytes)?;
    }
    f.sync_all()?;
    Ok(())
}

pub struct Migrator<S>
where
    S: Store,
{
    path: PathBuf,
    store: S,
}

impl<S> Migrator<S>
where
    S: Store,
{
    pub fn with_store(path: &str, store: S) -> Migrator<S> {
        Migrator {
            path: path.into(),
            store,
        }
    }

    #[inline]
    fn migrate_n(&self, up: bool, n: Option<usize>) -> Result<Option<i64>> {
        // Try to read migrations dir first
        let dir = fs::read_dir(&self.path)?;

        let migration_history = self.get_migration_history()?;
        match self.filter_migrations(dir, migration_history, up)? {
            Some(migrations_to_execute) => self.execute_migrations(migrations_to_execute, up, n),
            None => Ok(None),
        }
    }

    /// Migrates up,
    /// returns None if database is already up to date.
    pub fn migrate_up(&self) -> Result<Option<i64>> {
        self.migrate_n(true, None)
    }

    /// Migrates down,
    /// returns None if database is already up to date.
    pub fn migrate_down(&self) -> Result<Option<i64>> {
        self.migrate_n(false, None)
    }

    /// Migrates up `n` times or less,
    /// returns None if database is already up to date.
    pub fn migrate_up_n(&self, n: usize) -> Result<Option<i64>> {
        self.migrate_n(true, Some(n))
    }

    /// Migrates down `n` times or less,
    /// returns None if database is already up to date.
    pub fn migrate_down_n(&self, n: usize) -> Result<Option<i64>> {
        self.migrate_n(false, Some(n))
    }

    fn get_migration_history(&self) -> Result<HashMap<i64, isize>> {
        let res: HashMap<i64, isize> = match self
            .store
            .get_all()
            .map_err(|err| Error::Store(Box::new(err)))?
        {
            Some(migrations) => migrations.into_iter().fold(HashMap::new(), |mut acc, m| {
                let increment = if m.is_up() { 1 } else { -1 };
                match acc.entry(m.id()) {
                    Entry::Occupied(o) => {
                        *o.into_mut() += increment;
                    }
                    Entry::Vacant(v) => {
                        v.insert(increment);
                    }
                }
                acc
            }),
            None => HashMap::new(),
        };
        Ok(res)
    }

    fn is_cql_comment_line(line: &str) -> bool {
        let comment_slice = &line[..COMMENT_LENGTH];
        comment_slice == COMMENT_LINE_TYPE_1 || comment_slice == COMMENT_LINE_TYPE_2
    }

    fn parse_cql_file(path: PathBuf) -> Result<Option<Vec<String>>> {
        let file = File::open(path)?;

        let mut queries = Vec::new();
        let mut reader = BufReader::new(file);
        let mut bytes_count: usize;
        let mut buf = String::new();
        let mut is_new_query = false;
        loop {
            bytes_count = reader.read_line(&mut buf)?;
            if bytes_count == 0 {
                break;
            }

            let trimmed = buf.trim();
            if !trimmed.is_empty() && !Self::is_cql_comment_line(trimmed) {
                if is_new_query {
                    queries.push(String::new());
                }
                if trimmed.chars().last().unwrap() == QUERIES_SEPARATOR {
                    is_new_query = true
                } else {
                    is_new_query = false
                }

                if queries.is_empty() {
                    queries.push(trimmed.to_string());
                } else {
                    queries.last_mut().unwrap().push_str(trimmed);
                }
            }

            buf.clear();
        }

        if queries.is_empty() {
            return Ok(None);
        }
        Ok(Some(queries))
    }

    fn filter_migrations(
        &self,
        dir: ReadDir,
        history: HashMap<i64, isize>,
        up: bool,
    ) -> Result<Option<Vec<(i64, Vec<String>)>>> {
        let mut res: Vec<(i64, Vec<String>)> = dir
            .map(|r| r.unwrap())
            .filter(|elem| elem.metadata().unwrap().is_dir())
            .filter_map(
                |elem| match elem.file_name().to_str().unwrap().splitn(2, '_').next() {
                    Some(timestamp_prefix) => match timestamp_prefix.parse::<i64>() {
                        Ok(timestamp) => {
                            let counter = *history.get(&timestamp).unwrap_or(&0);
                            if up && counter == 0 || (!up && counter == 1) {
                                let mut up_path = elem.path();
                                if up {
                                    up_path.push(MIGRATION_FILE_UP);
                                } else {
                                    up_path.push(MIGRATION_FILE_DOWN);
                                }
                                Some((timestamp, up_path))
                            } else {
                                None
                            }
                        }
                        Err(_) => None,
                    },
                    None => None,
                },
            )
            .map(|m| {
                let queries = match Self::parse_cql_file(m.1.clone())? {
                    Some(v) => v,
                    None => {
                        return Err(Error::ParseMigrationFile(format!(
                            "no CQL found in {}",
                            m.1.display()
                        )))
                    }
                };

                Ok((m.0, queries))
            })
            .collect::<Result<Vec<(i64, Vec<String>)>>>()?;
        if res.is_empty() {
            return Ok(None);
        }
        if up {
            res.sort_by(|(a_timestamp, _), (b_timestamp, _)| a_timestamp.cmp(&b_timestamp));
        } else {
            res.sort_by(|(a_timestamp, _), (b_timestamp, _)| b_timestamp.cmp(&a_timestamp));
        }
        Ok(Some(res))
    }

    fn migrate_one(&self, timestamp: i64, queries: Vec<String>, up: bool) -> Result<()> {
        for query in queries {
            self.store
                .exec(&query)
                .map_err(|err| Error::Store(Box::new(err)))?;
        }

        self.store
            .store_one(timestamp, up)
            .map_err(|err| Error::Store(Box::new(err)))
    }

    pub fn execute_migrations(
        &self,
        migration_to_execute: Vec<(i64, Vec<String>)>,
        up: bool,
        n: Option<usize>,
    ) -> Result<Option<i64>> {
        let (last_id, take_n) = match n {
            Some(v) => {
                if migration_to_execute.len() > v {
                    (migration_to_execute.get(v).unwrap().0, v)
                } else {
                    (
                        migration_to_execute.last().unwrap().0,
                        migration_to_execute.len(),
                    )
                }
            }
            None => (
                migration_to_execute.last().unwrap().0,
                migration_to_execute.len(),
            ),
        };

        for (timestamp, queries) in migration_to_execute.into_iter().take(take_n) {
            self.migrate_one(timestamp, queries, up)?;
        }

        Ok(Some(last_id))
    }
}
