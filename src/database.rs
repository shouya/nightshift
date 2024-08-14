use crate::errors::Result;
use anyhow::Context;
use rusqlite::params;

pub struct DatabaseOps {
    db: rusqlite::Connection,
}

impl DatabaseOps {
    pub fn open(path: &str, password: Option<&str>) -> anyhow::Result<Self> {
        let db = rusqlite::Connection::open(path)?;
        if let Some(password) = password {
            db.pragma_update(None, "key", password)?;
            match db.prepare("SELECT count(*) FROM sqlite_master")?.query(params![]) {
                Ok(_) => {}
                Err(rusqlite::Error::SqliteFailure(e, _)) if e.code == rusqlite::ffi::ErrorCode::NotADatabase => {
                    anyhow::bail!("Invalid password");
                }
                Err(e) => anyhow::bail!("SQLite error: {e}"),
            }
        }
        db.execute_batch(include_str!("queries/schema.sql")).context("schema")?;
        Ok(DatabaseOps { db })
    }

    pub fn with_read_tx<T, F>(&mut self, scope: F) -> Result<T>
    where
        F: FnOnce(&mut rusqlite::Transaction) -> Result<T>,
    {
        let mut tx = self.db.transaction()?;
        scope(&mut tx)
    }

    pub fn with_write_tx<T, F>(&mut self, scope: F) -> Result<T>
    where
        F: FnOnce(&mut rusqlite::Transaction) -> Result<T>,
    {
        let mut tx = self
            .db
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let val = scope(&mut tx)?;
        tx.commit()?;
        Ok(val)
    }
}
