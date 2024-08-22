use crate::errors::Result;
use anyhow::Context;
use rusqlite::params;

pub struct DatabaseOps {
    pub(crate) db: rusqlite::Connection,
}

impl DatabaseOps {
    pub fn open(path: &str, mut password: Option<String>) -> anyhow::Result<Self> {
        let db = rusqlite::Connection::open(path).context("open")?;
        let password = password.take().unwrap_or("".into());
        set_cipher_key(&db, password)?;

        db.execute_batch(include_str!("queries/sql/schema.sql"))
            .context("schema")?;
        Ok(DatabaseOps { db })
    }

    #[cfg(test)]
    pub fn open_in_memory() -> anyhow::Result<Self> {
        let db = rusqlite::Connection::open_in_memory().context("open")?;
        db.execute_batch(include_str!("queries/sql/schema.sql"))
            .context("schema")?;
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

fn set_cipher_key(db: &rusqlite::Connection, password: String) -> anyhow::Result<()> {
    db.pragma_update(None, "key", password).context("pragma")?;
    match db
        .prepare("SELECT count(*) FROM sqlite_master")
        .and_then(|mut stmt| stmt.query(params![]).map(|_| ()))
    {
        Ok(_) => Ok(()),
        Err(rusqlite::Error::SqliteFailure(e, _)) if e.code == rusqlite::ffi::ErrorCode::NotADatabase => {
            anyhow::bail!("Invalid password");
        }
        Err(e) => anyhow::bail!("SQLite error: {e}"),
    }
}
