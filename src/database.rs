use crate::errors::Result;
use anyhow::Context;
use rusqlite::params;

pub struct DatabaseOps {
    db: Option<rusqlite::Connection>,
}

impl DatabaseOps {
    pub fn open(path: &str, password: Option<&str>) -> anyhow::Result<Self> {
        let db = rusqlite::Connection::open(path).context("open")?;
        let password = password.unwrap_or("");
        db.pragma_update(None, "key", password).context("pragma")?;
        match db
            .prepare("SELECT count(*) FROM sqlite_master")
            .context("prep")?
            .query(params![])
        {
            Ok(_) => {}
            Err(rusqlite::Error::SqliteFailure(e, _)) if e.code == rusqlite::ffi::ErrorCode::NotADatabase => {
                anyhow::bail!("Invalid password");
            }
            Err(e) => anyhow::bail!("SQLite error: {e}"),
        }
        db.execute_batch(include_str!("queries/sql/schema.sql"))
            .context("schema")?;
        Ok(DatabaseOps { db: Some(db) })
    }

    pub fn with_read_tx<T, F>(&mut self, scope: F) -> Result<T>
    where
        F: FnOnce(&mut rusqlite::Transaction) -> Result<T>,
    {
        let mut tx = self.db.as_mut().unwrap().transaction()?;
        scope(&mut tx)
    }

    pub fn with_write_tx<T, F>(&mut self, scope: F) -> Result<T>
    where
        F: FnOnce(&mut rusqlite::Transaction) -> Result<T>,
    {
        let mut tx = self
            .db
            .as_mut()
            .unwrap()
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let val = scope(&mut tx)?;
        tx.commit()?;
        Ok(val)
    }
}

impl Drop for DatabaseOps {
    fn drop(&mut self) {
        println!("I drop");
        self.db.take().unwrap().close().unwrap()
    }
}
