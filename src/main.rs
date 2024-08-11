#![allow(clippy::too_many_arguments)]

mod errors;
mod types;

use std::{
    ffi::OsStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::{self, Duration, SystemTime},
};

use anyhow::Context;
use fuser::{FileAttr, TimeOrNow};
use rusqlite::{params, Connection};

use errors::{Error, Result};
use signal_hook::{
    consts::{SIGINT, SIGTERM},
    iterator::Signals,
};
use simple_logger::SimpleLogger;
use types::FileType;

static DURATION: &Duration = &Duration::from_secs(1);

fn import_systemtime(secs: u64, nanos: u32) -> SystemTime {
    time::UNIX_EPOCH + Duration::new(secs, nanos)
}

fn export_systemtime(t: SystemTime) -> (u64, u32) {
    let d = t.duration_since(time::UNIX_EPOCH).expect("invalid time");
    (d.as_secs(), d.subsec_nanos())
}

fn export_time_or_now(t: TimeOrNow) -> (u64, u32) {
    export_systemtime(match t {
        TimeOrNow::Now => SystemTime::now(),
        TimeOrNow::SpecificTime(t) => t,
    })
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

struct ListDirEntry<'n> {
    ino: u64,
    name: &'n OsStr,
    kind: fuser::FileType,
    offset: i64,
}

struct Block {
    ino: u64,
    offset: u64,
    end_offset: u64,
    size: u32,
    data: Vec<u8>,
}

impl std::fmt::Debug for Block {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Block")
            .field("ino", &self.ino)
            .field("offset", &self.offset)
            .field("end_offset", &self.end_offset)
            .field("size", &self.size)
            .finish()
    }
}

struct NightshiftDB {
    db: rusqlite::Connection,
}

impl NightshiftDB {
    fn lookup_inode(&mut self, ino: u64) -> Result<fuser::FileAttr> {
        let attr = self
            .db
            .query_row("SELECT * FROM inode WHERE ino = ?", params![ino], |row| {
                let mut rc = RowCounter::default();
                Ok(FileAttr {
                    ino: row.get(rc.next())?,
                    size: row.get(rc.next())?,
                    blocks: row.get(rc.next())?,
                    atime: import_systemtime(row.get(rc.next())?, row.get(rc.next())?),
                    mtime: import_systemtime(row.get(rc.next())?, row.get(rc.next())?),
                    ctime: import_systemtime(row.get(rc.next())?, row.get(rc.next())?),
                    crtime: import_systemtime(row.get(rc.next())?, row.get(rc.next())?),
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

    fn lookup_dir_entry(&mut self, parent_ino: u64, name: &OsStr) -> Result<u64> {
        let ino = self.db.query_row(
            "SELECT ino FROM dir_entry WHERE parent_ino = ? AND name = ?",
            params![parent_ino, name.as_encoded_bytes()],
            |row| row.get(0),
        )?;
        Ok(ino)
    }

    fn set_attr(&mut self, name: &str, value: impl rusqlite::ToSql) -> Result<()> {
        let affected = self
            .db
            .execute(&format! {"UPDATE inode SET `{name}` = ?"}, params![value])?;
        if affected == 0 {
            return Err(Error::NotFound);
        }
        Ok(())
    }

    fn create_inode(&mut self, attr: &mut fuser::FileAttr) -> Result<()> {
        let (atime_secs, atime_nanos) = export_systemtime(attr.atime);
        let (mtime_secs, mtime_nanos) = export_systemtime(attr.mtime);
        let (ctime_secs, ctime_nanos) = export_systemtime(attr.ctime);
        let (crtime_secs, crtime_nanos) = export_systemtime(attr.crtime);

        let mut stmt = self.db.prepare_cached(include_str!("queries/insert-inode.sql"))?;
        let ino = stmt.insert(params![
            attr.size,
            attr.blocks,
            atime_secs,
            atime_nanos,
            mtime_secs,
            mtime_nanos,
            ctime_secs,
            ctime_nanos,
            crtime_secs,
            crtime_nanos,
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

    fn create_dir_entry(&mut self, parent_ino: u64, name: &OsStr, ino: u64) -> Result<()> {
        let mut stmt = self.db.prepare_cached(include_str!("queries/insert-dir-entry.sql"))?;
        stmt.insert(params![parent_ino, name.as_encoded_bytes(), ino])?;
        Ok(())
    }

    fn list_dir(&mut self, parent_ino: u64, offset: i64, mut iter: impl FnMut(ListDirEntry) -> bool) -> Result<()> {
        let mut stmt = self.db.prepare_cached(include_str!("queries/list-dir.sql"))?;
        let mut rows = stmt.query(params![parent_ino, offset])?;
        while let Some(row) = rows.next()? {
            let name: Vec<u8> = row.get(1)?;
            let entry = ListDirEntry {
                ino: row.get(0)?,
                name: unsafe { OsStr::from_encoded_bytes_unchecked(&name) },
                kind: import_kind(row.get(2)?),
                offset: row.get(0)?,
            };
            if !iter(entry) {
                break;
            }
        }
        Ok(())
    }

    fn get_block_at(&mut self, ino: u64, offset: u64) -> Result<Block> {
        let mut stmt = self.db.prepare_cached(
            "SELECT offset, end_offset, size, data FROM block WHERE ino = ? AND ? >= offset AND ? < end_offset",
        )?;
        let block = stmt.query_row(params![ino, offset, offset], |row| {
            Ok(Block {
                ino,
                offset: row.get(0)?,
                end_offset: row.get(1)?,
                size: row.get(2)?,
                data: row.get(3)?,
            })
        })?;
        Ok(block)
    }

    fn truncate_blocks(&mut self, ino: u64, offset: u64) -> Result<usize> {
        let mut stmt = self
            .db
            .prepare_cached("DELETE FROM block WHERE ino = ? AND offset > ?")?;
        let deleted = stmt.execute(params![ino, offset])?;
        Ok(deleted)
    }

    fn update_block(&mut self, ino: u64, offset: u64, data: &[u8]) -> Result<()> {
        // 0 - 4096
        // 4096 - 8192
        // 5000
        let mut block = self.get_block_at(ino, offset)?;

        let internal_offset = offset - block.offset;
        block.data.truncate(internal_offset as usize);
        block.data.extend_from_slice(data);
        block.size = block.data.len() as u32;
        block.end_offset = block.offset + block.size as u64;
        self.set_attr("size", block.end_offset)?;

        let mut stmt = self
            .db
            .prepare_cached("UPDATE block SET end_offset = ?, size = ?, data = ? WHERE ino = ? AND offset = ?")?;
        stmt.execute(params![
            block.end_offset,
            block.size,
            block.data,
            block.ino,
            block.offset
        ])?;

        Ok(())
    }

    fn create_block(&mut self, ino: u64, offset: u64, data: &[u8]) -> Result<()> {
        let end_offset = offset + data.len() as u64;
        {
            let mut stmt = self
                .db
                .prepare_cached("INSERT INTO block (ino, offset, end_offset, size, data) VALUES (?, ?, ?, ?, ?)")?;
            stmt.execute(params![ino, offset, end_offset, data.len(), data])?;
        }
        self.set_attr("size", end_offset)?;
        Ok(())
    }
}

struct NightshiftFuse {
    db: NightshiftDB,
}

impl NightshiftFuse {
    fn ensure_root_exists(&mut self) -> Result<()> {
        match self.db.lookup_inode(1) {
            // If ino is 1, this is the root directory.
            Err(Error::NotFound) => {
                log::debug!("ino=1 requested, but does not exist yet. Creating...");
                let now = SystemTime::now();

                let mut attr = FileAttr {
                    ino: 0,
                    size: 4096,
                    blocks: 1,
                    atime: now,
                    mtime: now,
                    ctime: now,
                    crtime: now,
                    kind: fuser::FileType::Directory,
                    perm: 0o755u16, // TODO probably bad http://web.deu.edu.tr/doc/oreily/networking/puis/ch05_03.htm
                    nlink: 0,
                    uid: 1000, // TODO get real user
                    gid: 1000, // TODO get real group
                    rdev: 0,
                    blksize: 4096,
                    flags: 0,
                };
                self.db.create_inode(&mut attr)?;
                Ok(())
            }
            Err(e) => Err(e),
            Ok(_) => Ok(()),
        }?;
        Ok(())
    }

    fn lookup_impl(&mut self, _req: &fuser::Request<'_>, parent: u64, name: &std::ffi::OsStr) -> Result<FileAttr> {
        let ino = self.db.lookup_dir_entry(parent, name)?;
        let attr = self.db.lookup_inode(ino)?;
        Ok(attr)
    }

    fn setattr_impl(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<fuser::TimeOrNow>,
        mtime: Option<fuser::TimeOrNow>,
        ctime: Option<SystemTime>,
        _fh: Option<u64>,
        crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        flags: Option<u32>,
    ) -> Result<FileAttr> {
        // TODO optimize
        if let Some(mode) = mode {
            self.db.set_attr("mode", mode)?;
        }
        if let Some(uid) = uid {
            self.db.set_attr("uid", uid)?;
        }
        if let Some(gid) = gid {
            self.db.set_attr("gid", gid)?;
        }
        if let Some(size) = size {
            self.db.set_attr("size", size)?;
        }
        if let Some(atime) = atime {
            let (atime_secs, atime_nanos) = export_time_or_now(atime);
            self.db.set_attr("atime_secs", atime_secs)?;
            self.db.set_attr("atime_nanos", atime_nanos)?;
        }
        if let Some(mtime) = mtime {
            let (mtime_secs, mtime_nanos) = export_time_or_now(mtime);
            self.db.set_attr("mtime_secs", mtime_secs)?;
            self.db.set_attr("mtime_nanos", mtime_nanos)?;
        }
        if let Some(ctime) = ctime {
            let (ctime_secs, ctime_nanos) = export_systemtime(ctime);
            self.db.set_attr("ctime_secs", ctime_secs)?;
            self.db.set_attr("ctime_nanos", ctime_nanos)?;
        }
        if let Some(crtime) = crtime {
            let (crtime_secs, crtime_nanos) = export_systemtime(crtime);
            self.db.set_attr("crtime_secs", crtime_secs)?;
            self.db.set_attr("crtime_nanos", crtime_nanos)?;
        }
        if let Some(flags) = flags {
            self.db.set_attr("flags", flags)?;
        }

        self.db.lookup_inode(ino)
    }

    fn mknod_impl(
        &mut self,
        req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        rdev: u32,
    ) -> Result<FileAttr> {
        log::info!("mknod mode is o={:#o} umask={:#o}", mode, umask);
        let kind = FileType::from_mode(mode).ok_or(Error::InvalidArgument)?;
        let now = SystemTime::now();

        let mut attr = FileAttr {
            ino: 0,
            size: 0,
            blocks: 0,
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind: kind.into(),
            perm: (mode & !umask) as u16, // TODO probably bad http://web.deu.edu.tr/doc/oreily/networking/puis/ch05_03.htm
            nlink: 1,
            uid: req.uid(),
            gid: req.gid(),
            rdev,
            blksize: 4096,
            flags: 0,
        };
        // TODO transaction
        self.db.create_inode(&mut attr)?;
        self.db.create_dir_entry(parent, name, attr.ino)?;
        Ok(attr)
    }

    fn mkdir_impl(
        &mut self,
        req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
    ) -> Result<FileAttr> {
        log::info!("mkdir mode is {:#o} umask is {:#}", mode, umask);
        let now = SystemTime::now();
        let mut attr = FileAttr {
            ino: 0,
            size: 4096,
            blocks: 1,
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind: fuser::FileType::Directory,
            perm: (mode & !umask) as u16, // TODO probably bad
            nlink: 0,
            uid: req.uid(),
            gid: req.gid(),
            rdev: 0, // Not given for directory?
            blksize: 4096,
            flags: 0,
        };
        self.db.create_inode(&mut attr)?;
        self.db.create_dir_entry(parent, name, attr.ino)?;
        Ok(attr)
    }

    fn readdir_impl<F>(&mut self, _req: &fuser::Request<'_>, ino: u64, _fh: u64, offset: i64, iter: F) -> Result<()>
    where
        F: FnMut(ListDirEntry) -> bool,
    {
        self.db.list_dir(ino, offset, iter)?;
        Ok(())
    }

    fn read_impl(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
    ) -> Result<Vec<u8>> {
        let mut block = self.db.get_block_at(ino, offset as u64)?;
        block.data.truncate(size as usize);
        Ok(block.data)
    }

    fn write_impl(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
    ) -> Result<u32> {
        let offset = offset as u64;
        self.db.truncate_blocks(ino, offset)?;
        match self.db.update_block(ino, offset, data) {
            Ok(_) => {
                log::debug!("block modified");
                Ok(data.len() as u32)
            }
            Err(Error::NotFound) => {
                log::debug!("block not modified, creating new block");
                self.db.create_block(ino, offset, data)?;
                Ok(data.len() as u32)
            }
            Err(e) => Err(e),
        }
    }
}

impl fuser::Filesystem for NightshiftFuse {
    fn init(
        &mut self,
        _req: &fuser::Request<'_>,
        config: &mut fuser::KernelConfig,
    ) -> std::result::Result<(), libc::c_int> {
        config.set_max_write(4096);
        match self.ensure_root_exists() {
            Ok(()) => Ok(()),
            Err(e) => {
                log::error!("init error: {}", e);
                Err(e.errno())
            }
        }
    }

    fn lookup(&mut self, req: &fuser::Request<'_>, parent: u64, name: &std::ffi::OsStr, reply: fuser::ReplyEntry) {
        log::debug!("lookup(parent={}, name={:?})", parent, name.to_string_lossy());
        let res = self.lookup_impl(req, parent, name);
        log::debug!("lookup: {:?}", res);

        match res {
            Ok(attr) => reply.entry(DURATION, &attr, 0),
            Err(e) => reply.error(e.errno()),
        }
    }

    fn getattr(&mut self, _req: &fuser::Request<'_>, ino: u64, reply: fuser::ReplyAttr) {
        log::debug!("getattr(ino={})", ino);
        let res = self.db.lookup_inode(ino);
        log::debug!("getattr: {:?}", res);

        match res {
            Ok(attr) => reply.attr(DURATION, &attr),
            Err(e) => reply.error(e.errno()),
        }
    }

    fn setattr(
        &mut self,
        req: &fuser::Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<fuser::TimeOrNow>,
        mtime: Option<fuser::TimeOrNow>,
        ctime: Option<SystemTime>,
        fh: Option<u64>,
        crtime: Option<SystemTime>,
        chgtime: Option<SystemTime>,
        bkuptime: Option<SystemTime>,
        flags: Option<u32>,
        reply: fuser::ReplyAttr,
    ) {
        log::debug!(
            "setattr(ino={}, mode={:#?}, uid={:?}, gid={:?}, size={:?})",
            ino,
            mode,
            uid,
            gid,
            size,
        );
        let res = self.setattr_impl(
            req, ino, mode, uid, gid, size, atime, mtime, ctime, fh, crtime, chgtime, bkuptime, flags,
        );
        log::debug!("setattr: {:?}", res);

        match res {
            Ok(attr) => reply.attr(DURATION, &attr),
            Err(e) => reply.error(e.errno()),
        }
    }

    fn mknod(
        &mut self,
        req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        rdev: u32,
        reply: fuser::ReplyEntry,
    ) {
        log::debug!(
            "mknod(parent={}, name={:?}, mode={}, umask={:#o}, rdev={})",
            parent,
            name.to_string_lossy(),
            mode,
            umask,
            rdev
        );
        let res = self.mknod_impl(req, parent, name, mode, umask, rdev);
        log::debug!("mknod: {:?}", res);

        match res {
            Ok(attr) => reply.entry(DURATION, &attr, 0),
            Err(e) => reply.error(e.errno()),
        }
    }

    fn mkdir(
        &mut self,
        req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: fuser::ReplyEntry,
    ) {
        log::debug!(
            "mkdir(parent={}, name={:?}, mode={}, umask={:#o})",
            parent,
            name.to_string_lossy(),
            mode,
            umask,
        );
        let res = self.mkdir_impl(req, parent, name, mode, umask);
        log::debug!("mkdir: {:?}", res);

        match res {
            Ok(attr) => reply.entry(DURATION, &attr, 0),
            Err(e) => reply.error(e.errno()),
        }
    }

    fn readdir(&mut self, req: &fuser::Request<'_>, ino: u64, fh: u64, offset: i64, mut reply: fuser::ReplyDirectory) {
        log::debug!("readdir(ino={}, fh={}, offset={})", ino, fh, offset);
        let res = self.readdir_impl(req, ino, fh, offset, |entry| {
            reply.add(entry.ino, entry.offset, entry.kind, entry.name)
        });
        log::debug!("readdir: {:?}", res);

        match res {
            Ok(_) => reply.ok(),
            Err(e) => reply.error(e.errno()),
        }
    }

    fn open(&mut self, _req: &fuser::Request<'_>, _ino: u64, _flags: i32, reply: fuser::ReplyOpen) {
        reply.opened(1337, 0)
    }

    fn read(
        &mut self,
        req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        flags: i32,
        lock_owner: Option<u64>,
        reply: fuser::ReplyData,
    ) {
        log::debug!("read(ino={}, offset={}, size={}", ino, offset, size);
        let res = self.read_impl(req, ino, fh, offset, size, flags, lock_owner);
        log::debug!("read: {:?}", res);

        match res {
            Ok(data) => reply.data(&data),
            Err(Error::NotFound) => reply.data(&[]),
            Err(e) => reply.error(e.errno()),
        }
    }

    fn write(
        &mut self,
        req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        write_flags: u32,
        flags: i32,
        lock_owner: Option<u64>,
        reply: fuser::ReplyWrite,
    ) {
        log::debug!("write(ino={}, offset={}, data_len={}", ino, offset, data.len());
        let res = self.write_impl(req, ino, fh, offset, data, write_flags, flags, lock_owner);
        log::debug!("write: {:?}", res);

        match res {
            Ok(written) => reply.written(written),
            Err(e) => reply.error(e.errno()),
        }
    }
}

fn main() -> anyhow::Result<()> {
    SimpleLogger::new()
        .with_level(log::LevelFilter::Debug)
        .init()
        .context("unable to install logging")?;

    let fs = NightshiftFuse {
        db: NightshiftDB {
            db: Connection::open("foo.db").context("unable to open database")?,
        },
    };
    fs.db
        .db
        .execute_batch(include_str!("queries/schema.sql"))
        .context("schema")?;

    let mount = fuser::spawn_mount2(fs, "mnt-target", &[]).context("unable to create mount")?;

    let term = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::consts::SIGTERM, Arc::clone(&term))?;
    signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(&term))?;
    while !term.load(Ordering::Relaxed) {
        thread::sleep(Duration::from_millis(100));
    }
    drop(mount);
    Ok(())
}
