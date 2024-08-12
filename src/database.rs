use std::cmp;
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;

use anyhow::Context;
use fuser::FileAttr;
use rusqlite::params;

use crate::errors::{Error, Result};
use crate::time::TimeSpec;
use crate::types::FileType;

pub const BLOCK_SIZE: u32 = 4096 * 4;

pub struct ListDirEntry<'n> {
    pub offset: i64,
    pub ino: u64,
    pub name: &'n OsStr,
    pub kind: fuser::FileType,
}

pub struct Block {
    pub ino: u64,
    pub offset: u64,
    pub end_offset: u64,
    pub data: Vec<u8>, // UNCOMPRESSED ALWAYS
}

impl Block {
    fn new(ino: u64, offset: u64, size: u32) -> Block {
        Block {
            ino,
            offset,
            end_offset: offset + u64::from(size),
            data: Vec::new(),
        }
    }

    fn available(&self) -> u32 {
        let block_size = self.end_offset - self.offset;
        u32::try_from(block_size - self.data.len() as u64).expect("block size overflow")
    }

    fn consume(&mut self, data: &[u8]) -> u64 {
        let avail = self.available();
        let data_len = u32::try_from(data.len()).expect("data size overflow");
        let max_write = cmp::min(avail, data_len) as usize;
        self.data.extend_from_slice(&data[..max_write]);
        u64::try_from(max_write).expect("written overflow")
    }

    fn write_at(&mut self, inode_offset: u64, data: &[u8]) -> (u64, i64) {
        let start_len = self.data.len();
        let rel_offset = inode_offset - self.offset;
        self.data.truncate(rel_offset as usize);
        let written = self.consume(data);
        let diff = self.data.len() as i64 - start_len as i64;
        (written, diff)
    }

    pub fn copy_into(&self, dest: &mut Vec<u8>) -> usize {
        let remaining = dest.capacity() - dest.len();
        let max_write = cmp::min(remaining, self.data.len());
        dest.extend_from_slice(&self.data[..max_write]);
        max_write
    }
}

impl std::fmt::Debug for Block {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Block")
            .field("ino", &self.ino)
            .field("offset", &self.offset)
            .field("end_offset", &self.end_offset)
            .field("data.len()", &self.data.len())
            .finish()
    }
}

pub struct DatabaseOps {
    db: rusqlite::Connection,
}

impl DatabaseOps {
    pub fn open(path: &str) -> anyhow::Result<Self> {
        let db = rusqlite::Connection::open(path)?;
        db.execute_batch(include_str!("queries/schema.sql")).context("schema")?;
        Ok(DatabaseOps { db })
    }

    pub fn lookup_inode(&mut self, ino: u64) -> Result<fuser::FileAttr> {
        let attr = self
            .db
            .query_row("SELECT * FROM inode WHERE ino = ?", params![ino], |row| {
                let mut rc = RowCounter::default();
                Ok(FileAttr {
                    ino: row.get(rc.next())?,
                    size: row.get(rc.next())?,
                    blocks: row.get(rc.next())?,
                    atime: TimeSpec::new(row.get(rc.next())?, row.get(rc.next())?).into(),
                    mtime: TimeSpec::new(row.get(rc.next())?, row.get(rc.next())?).into(),
                    ctime: TimeSpec::new(row.get(rc.next())?, row.get(rc.next())?).into(),
                    crtime: TimeSpec::new(row.get(rc.next())?, row.get(rc.next())?).into(),
                    kind: import_kind(row.get(rc.next())?),
                    perm: row.get(rc.next())?,
                    nlink: row.get(rc.next())?,
                    uid: row.get(rc.next())?,
                    gid: row.get(rc.next())?,
                    rdev: row.get(rc.next())?,
                    blksize: row.get(rc.next())?,
                    flags: row.get(rc.next())?,
                })
            })?;
        Ok(attr)
    }

    pub fn lookup_dir_entry(&mut self, parent_ino: u64, name: &OsStr) -> Result<u64> {
        let ino = self.db.query_row(
            "SELECT ino FROM dir_entry WHERE parent_ino = ? AND name = ?",
            params![parent_ino, name.as_encoded_bytes()],
            |row| row.get(0),
        )?;
        Ok(ino)
    }

    pub fn set_attr(&mut self, ino: u64, name: &str, value: impl rusqlite::ToSql) -> Result<()> {
        let affected = self.db.execute(
            &format!("UPDATE inode SET `{name}` = ? WHERE ino = ?"),
            params![value, ino],
        )?;
        if affected == 0 {
            return Err(Error::NotFound);
        }
        Ok(())
    }

    pub fn create_inode(&mut self, attr: &mut fuser::FileAttr) -> Result<()> {
        let atime = TimeSpec::from(attr.atime);
        let mtime = TimeSpec::from(attr.mtime);
        let ctime = TimeSpec::from(attr.ctime);
        let crtime = TimeSpec::from(attr.crtime);

        let mut stmt = self.db.prepare_cached(include_str!("queries/insert-inode.sql"))?;
        let ino = stmt.insert(params![
            attr.size,
            attr.blocks,
            atime.secs,
            atime.nanos,
            mtime.secs,
            mtime.nanos,
            ctime.secs,
            ctime.nanos,
            crtime.secs,
            crtime.nanos,
            export_kind(attr.kind),
            attr.perm,
            attr.nlink,
            attr.uid,
            attr.gid,
            attr.rdev,
            attr.blksize,
            attr.flags,
        ])?;

        attr.ino = ino as u64;
        Ok(())
    }

    pub fn remove_inode(&self, ino: u64) -> Result<()> {
        let mut stmt = self.db.prepare_cached("DELETE FROM inode WHERE ino = ?")?;
        let affected = stmt.execute(params![ino])?;
        if affected == 0 {
            return Err(Error::NotFound);
        }
        Ok(())
    }

    pub fn create_dir_entry(&mut self, parent_ino: u64, name: &OsStr, ino: u64) -> Result<()> {
        let mut stmt = self.db.prepare_cached(include_str!("queries/insert-dir-entry.sql"))?;
        stmt.insert(params![parent_ino, name.as_encoded_bytes(), ino])?;
        Ok(())
    }

    pub fn remove_dir_entry(&mut self, parent_ino: u64, name: &OsStr) -> Result<()> {
        let mut stmt = self
            .db
            .prepare_cached("DELETE FROM dir_entry WHERE parent_ino = ? AND name = ?")?;
        let affected = stmt.execute(params![parent_ino, name.as_bytes()])?;
        if affected == 0 {
            return Err(Error::NotFound);
        }
        Ok(())
    }

    pub fn is_dir_empty(&mut self, ino: u64) -> Result<bool> {
        let mut stmt = self
            .db
            .prepare_cached("SELECT NOT EXISTS(SELECT 1 FROM dir_entry WHERE parent_ino = ?)")?;
        let empty = stmt.query_row(params![ino], |row| row.get(0))?;
        Ok(empty)
    }

    pub fn list_dir(&mut self, parent_ino: u64, offset: i64, mut iter: impl FnMut(ListDirEntry) -> bool) -> Result<()> {
        let mut stmt = self.db.prepare_cached(include_str!("queries/list-dir.sql"))?;
        let mut rows = stmt.query(params![parent_ino, offset])?;
        while let Some(row) = rows.next()? {
            let name: Vec<u8> = row.get(2)?;
            let entry = ListDirEntry {
                offset: row.get(0)?,
                ino: row.get(1)?,
                name: OsStr::from_bytes(&name),
                kind: import_kind(row.get(3)?),
            };
            if !iter(entry) {
                break;
            }
        }
        Ok(())
    }

    pub fn get_block_at(&mut self, ino: u64, offset: u64) -> Result<Block> {
        let mut stmt = self.db.prepare_cached(
            "SELECT offset, end_offset, data FROM block WHERE ino = ? AND ? >= offset AND ? < end_offset",
        )?;
        let block = stmt.query_row(params![ino, offset, offset], |row| {
            Ok(Block {
                ino,
                offset: row.get(0)?,
                end_offset: row.get(1)?,
                data: row.get(2)?,
            })
        })?;
        Ok(block)
    }

    pub fn iter_blocks_from(&mut self, ino: u64, offset: u64, mut iter: impl FnMut(Block) -> bool) -> Result<()> {
        let mut stmt = self
            .db
            .prepare_cached("SELECT offset, end_offset, data FROM block WHERE ino = ? AND offset >= ?")?;
        let mut rows = stmt.query(params![ino, offset])?;
        while let Some(row) = rows.next()? {
            let block = Block {
                ino,
                offset: row.get(0)?,
                end_offset: row.get(1)?,
                data: row.get(2)?,
            };
            let more = iter(block);
            if !more {
                break;
            }
        }
        Ok(())
    }

    pub fn update_block(&mut self, ino: u64, offset: u64, data: &[u8]) -> Result<(u64, i64)> {
        let mut block = self.get_block_at(ino, offset)?;
        let (written, diff) = block.write_at(offset, data);

        let mut stmt = self
            .db
            .prepare_cached("UPDATE block SET data = ? WHERE ino = ? AND offset = ?")?;
        stmt.execute(params![block.data, block.ino, block.offset])?;

        Ok((written, diff))
    }

    pub fn create_block(&mut self, ino: u64, offset: u64, data: &[u8]) -> Result<u64> {
        let mut block = Block::new(ino, offset, BLOCK_SIZE);
        let written = block.consume(data);
        {
            let mut stmt = self
                .db
                .prepare_cached("INSERT INTO block (ino, offset, end_offset, data) VALUES (?, ?, ?, ?)")?;
            stmt.execute(params![block.ino, block.offset, block.end_offset, &block.data])?;
        }
        Ok(written)
    }

    pub fn remove_blocks(&mut self, ino: u64) -> Result<()> {
        let mut stmt = self.db.prepare_cached("DELETE FROM block WHERE ino = ?")?;
        stmt.execute(params![ino])?;
        Ok(())
    }

    pub fn rename_entry(&mut self, parent: u64, name: &OsStr, new_parent: u64, new_name: &OsStr) -> Result<()> {
        let mut stmt = self
            .db
            .prepare_cached("UPDATE dir_entry SET parent_ino = ?, name = ? WHERE parent_ino = ? AND name = ?")?;
        stmt.execute(params![new_parent, new_name.as_bytes(), parent, name.as_bytes()])?;
        Ok(())
    }
}

fn import_kind(kind: u8) -> fuser::FileType {
    FileType::try_from(kind).expect("Invalid inode kind").into()
}

fn export_kind(kind: fuser::FileType) -> u8 {
    FileType::from(kind).into()
}

#[derive(Default)]
struct RowCounter {
    c: usize,
}

impl RowCounter {
    fn next(&mut self) -> usize {
        let x = self.c;
        self.c += 1;
        x
    }
}
