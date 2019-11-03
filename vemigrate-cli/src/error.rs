use std::{error, fmt};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    ParseConfigs(ParseConfigsError),
}

impl error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::ParseConfigs(err) => err.fmt(f),
        }
    }
}

#[derive(Debug)]
pub enum ParseConfigsError {
    Address,
    User,
    Password,
}

impl error::Error for ParseConfigsError {}

impl fmt::Display for ParseConfigsError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ParseConfigsError::Address => {
                f.write_str("couldn't read DATABASE_ADDRESS env variable")
            }
            ParseConfigsError::User => f.write_str("couldn't read DATABASE_USER env variable"),
            ParseConfigsError::Password => {
                f.write_str("couldn't read DATABASE_PASSWORD env variable")
            }
        }
    }
}
