mod errors;
mod types;

use std::{
    ffi::OsStr,
    io,
    time::{self, Duration, SystemTime},
};

use anyhow::Context;
use fuser::FileAttr;
use rusqlite::{params, Connection};

use errors::{Error, Result};
use signal_hook::{
    consts::{SIGINT, SIGTERM},
    iterator::Signals,
};
use simple_logger::SimpleLogger;
use types::FileType;

static DURATION: &'static Duration = &Duration::from_secs(1);

fn import_systemtime(secs: u64, nanos: u32) -> SystemTime {
    time::UNIX_EPOCH + Duration::new(secs, nanos)
}

fn export_systemtime(t: SystemTime) -> (u64, u32) {
    let d = t.duration_since(time::UNIX_EPOCH).expect("invalid time");
    (d.as_secs(), d.subsec_nanos())
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
}

struct NightshiftFuse {
    db: NightshiftDB,
}

impl NightshiftFuse {
    fn lookup_impl(&mut self, req: &fuser::Request<'_>, parent: u64, name: &std::ffi::OsStr) -> Result<FileAttr> {
        let ino = self.db.lookup_dir_entry(parent, name)?;
        let attr = self.db.lookup_inode(ino)?;
        Ok(attr)
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
        let kind = FileType::from_mode(mode).ok_or_else(|| Error::InvalidArgument)?;
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
            perm: 0766u16 & umask as u16, // TODO probably bad http://web.deu.edu.tr/doc/oreily/networking/puis/ch05_03.htm
            nlink: 0,
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
        _mode: u32,
        umask: u32,
    ) -> Result<FileAttr> {
        let now = SystemTime::now();
        let mut attr = FileAttr {
            ino: 0,
            size: 0,
            blocks: 0,
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind: fuser::FileType::Directory,
            perm: 0755u16 & umask as u16, // TODO probably bad
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

    fn readdir_impl(&mut self, req: &fuser::Request<'_>, ino: u64, fh: u64, offset: i64) -> Result<()> {
        let attr = match self.db.lookup_inode(ino) {
            Ok(attr) => attr,
            // If ino is 1, this is the root directory.
            Err(e) if e == Error::NotFound && ino == 1 => self.mkdir_impl(req, ino, &OsStr::new("."), 0755, 0022)?,
            Err(e) => return Err(e),
        };

        Ok(())
    }
}

impl fuser::Filesystem for NightshiftFuse {
    fn lookup(&mut self, req: &fuser::Request<'_>, parent: u64, name: &std::ffi::OsStr, reply: fuser::ReplyEntry) {
        log::debug!("lookup(parent={}, name={:?})", parent, name.to_string_lossy());

        match self.lookup_impl(req, parent, name) {
            Ok(attr) => reply.entry(&DURATION, &attr, 0),
            Err(e) => reply.error(e.errno()),
        }
    }

    fn getattr(&mut self, _req: &fuser::Request<'_>, ino: u64, reply: fuser::ReplyAttr) {
        log::debug!("getattr(ino={})", ino);

        match self.db.lookup_inode(ino) {
            Ok(attr) => reply.attr(&DURATION, &attr),
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

        match self.mknod_impl(req, parent, name, mode, umask, rdev) {
            Ok(attr) => reply.entry(&DURATION, &attr, 0),
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

        match self.mkdir_impl(req, parent, name, mode, umask) {
            Ok(attr) => reply.entry(&DURATION, &attr, 0),
            Err(e) => reply.error(e.errno()),
        }
    }

    fn opendir(&mut self, _req: &fuser::Request<'_>, ino: u64, flags: i32, reply: fuser::ReplyOpen) {
        log::debug!("opendir(ino={}, flags={})", ino, flags);

        reply.opened(1337, 0)
    }

    fn readdir(&mut self, req: &fuser::Request<'_>, ino: u64, fh: u64, offset: i64, reply: fuser::ReplyDirectory) {
        log::debug!("readdir(ino={}, fh={}, offset={})", ino, fh, offset);
    }
}

fn main() -> anyhow::Result<()> {
    SimpleLogger::new()
        .with_level(log::LevelFilter::Debug)
        .init()
        .context("unable to install logging")?;

    let fs = NightshiftFuse {
        db: NightshiftDB {
            db: Connection::open_in_memory().context("unable to open database")?,
        },
    };
    let mount = fuser::spawn_mount2(fs, "mnt-target", &[]).context("unable to create mount")?;

    let mut signals = Signals::new(&[SIGTERM, SIGINT])?;
    loop {
        for signal in signals.pending() {
            match signal as libc::c_int {
                SIGINT | SIGTERM => {
                    drop(mount);
                    return Ok(());
                }
                _ => {}
            }
        }
    }
}
