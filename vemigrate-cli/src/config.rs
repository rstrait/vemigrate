use crate::error::{Error, ParseConfigsError, Result};

pub struct DBConf {
    pub addr: String,
    pub user: String,
    pub pwd: String,
}

impl DBConf {
    pub fn parse() -> Result<DBConf> {
        Ok(DBConf {
            addr: std::env::var("DATABASE_ADDRESS")
                .map_err(|_| Error::ParseConfigs(ParseConfigsError::Address))?,
            user: std::env::var("DATABASE_USER")
                .map_err(|_| Error::ParseConfigs(ParseConfigsError::User))?,
            pwd: std::env::var("DATABASE_PASSWORD")
                .map_err(|_| Error::ParseConfigs(ParseConfigsError::Password))?,
        })
    }
}
