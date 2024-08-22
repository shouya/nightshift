use std::{ffi::OsStr, os::unix::ffi::OsStrExt};

use crate::{
    errors::{Error, Result},
    types::FileType,
};
use rusqlite::params;

pub fn lookup(tx: &mut rusqlite::Transaction, parent_ino: u64, name: &OsStr) -> Result<u64> {
    let mut stmt = tx.prepare_cached("SELECT ino FROM dir_entry WHERE parent_ino = ? AND name = ?")?;
    let ino = stmt.query_row(params![parent_ino, name.as_encoded_bytes()], |row| row.get(0))?;
    Ok(ino)
}

pub fn create(tx: &mut rusqlite::Transaction, parent_ino: u64, name: &OsStr, ino: u64) -> Result<()> {
    let mut stmt = tx.prepare_cached(include_str!("sql/create_dir_entry.sql"))?;
    stmt.insert(params![parent_ino, name.as_encoded_bytes(), ino])?;
    Ok(())
}

pub fn remove(tx: &mut rusqlite::Transaction, parent_ino: u64, name: &OsStr) -> Result<()> {
    let mut stmt = tx.prepare_cached("DELETE FROM dir_entry WHERE parent_ino = ? AND name = ?")?;
    let affected = stmt.execute(params![parent_ino, name.as_bytes()])?;
    if affected == 0 {
        return Err(Error::NotFound);
    }
    Ok(())
}

pub fn rename(
    tx: &mut rusqlite::Transaction,
    parent: u64,
    name: &OsStr,
    new_parent: u64,
    new_name: &OsStr,
) -> Result<()> {
    let mut stmt =
        tx.prepare_cached("UPDATE dir_entry SET parent_ino = ?, name = ? WHERE parent_ino = ? AND name = ?")?;
    stmt.execute(params![new_parent, new_name.as_bytes(), parent, name.as_bytes()])?;
    Ok(())
}

pub fn is_dir_empty(tx: &mut rusqlite::Transaction, ino: u64) -> Result<bool> {
    let mut stmt = tx.prepare_cached("SELECT NOT EXISTS(SELECT 1 FROM dir_entry WHERE parent_ino = ?)")?;
    let empty = stmt.query_row(params![ino], |row| row.get(0))?;
    Ok(empty)
}

pub fn list_dir(
    tx: &mut rusqlite::Transaction,
    parent_ino: u64,
    offset: i64,
    mut iter: impl FnMut(ListDirEntry) -> bool,
) -> Result<()> {
    let mut stmt = tx.prepare_cached(include_str!("sql/list-dir.sql"))?;
    let mut rows = stmt.query(params![parent_ino, offset])?;
    while let Some(row) = rows.next()? {
        let name: Vec<u8> = row.get(2)?;
        let entry = ListDirEntry {
            offset: row.get(0)?,
            ino: row.get(1)?,
            name: OsStr::from_bytes(&name),
            kind: FileType::import(row.get(3)?),
        };
        if !iter(entry) {
            break;
        }
    }
    Ok(())
}

pub struct ListDirEntry<'n> {
    pub offset: i64,
    pub ino: u64,
    pub name: &'n OsStr,
    pub kind: fuser::FileType,
}
