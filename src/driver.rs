#![allow(clippy::too_many_arguments)]
use std::{
    cmp,
    ffi::OsStr,
    time::{Duration, SystemTime},
};

use fuser::FileAttr;
use log::debug;
use slab::Slab;

use crate::types::FileType;
use crate::{database::DatabaseOps, time::TimeSpec};
use crate::{
    errors::{Error, Result},
    models::Block,
};
use crate::{models::ListDirEntry, queries};

const DURATION: Duration = Duration::from_secs(0);
const POSIX_BLOCK_SIZE: u32 = 512;
const BUFFER_SIZE: usize = 2 * 1024 * 1024;

#[derive(Clone, Copy, Debug)]
struct OpenFlags {
    bits: i32,
    read: bool,
    write: bool,
    create: bool,
    append: bool,
    truncate: bool,
    sync: bool,
}

impl From<i32> for OpenFlags {
    fn from(flags: i32) -> Self {
        let read = flags & libc::O_WRONLY == libc::O_RDONLY || flags & libc::O_RDWR == libc::O_RDWR;
        let write = flags & libc::O_WRONLY != 0 || flags & libc::O_RDWR == libc::O_RDWR;
        let create = flags & libc::O_CREAT == libc::O_CREAT;
        let append = flags & libc::O_APPEND == libc::O_APPEND;
        let truncate = flags & libc::O_TRUNC == libc::O_TRUNC;
        let sync = flags & libc::O_SYNC == libc::O_SYNC;
        OpenFlags {
            bits: flags,
            read,
            write,
            create,
            append,
            truncate,
            sync,
        }
    }
}

struct FileHandle {
    ino: u64,
    size: u64,
    flags: OpenFlags,
    /// Stores the write position where buf must be written.
    write_offset: u64,
    /// Write data buffer used to optimize writes.
    buf: Vec<u8>,
}

impl FileHandle {
    fn buffer_remaining(&self) -> usize {
        self.buf.capacity() - self.buf.len()
    }

    pub fn buffer_full(&self) -> bool {
        self.buffer_remaining() == 0
    }

    pub fn write_offset(&self) -> u64 {
        self.write_offset + self.buf.len() as u64
    }

    pub fn consume_input(&mut self, buf: &[u8]) -> usize {
        let write = cmp::min(buf.len(), self.buffer_remaining());
        self.buf.extend_from_slice(&buf[..write]);
        write
    }

    pub fn flush(&mut self, tx: &mut rusqlite::Transaction) -> Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        log::debug!("flush, data is {}, offset = {}", self.buf.len(), self.write_offset);

        let mut attr = queries::inode::lookup(tx, self.ino)?;
        let mut new_offset = self.write_offset;
        let mut data = &self.buf[..];
        let mut modified_blocks = Vec::new();

        dbg!(attr.size);

        // Update blocks if the start offset overrides blocks.
        queries::block::iter_blocks_from(tx, self.ino, new_offset, |mut block| {
            let (written, diff) = block.write_at(new_offset, data);
            data = &data[written as usize..];
            new_offset += written;
            attr.size = (attr.size as i64 + diff) as u64;
            dbg!(attr.size, written, diff);
            if written > 0 {
                modified_blocks.push(block);
            }
            log::debug!("update block");
            Ok(!data.is_empty())
        })?;

        for block in modified_blocks {
            queries::block::update(tx, &block)?;
        }

        // Write the rest of the data in a new block.
        while !data.is_empty() {
            log::debug!("created new block");
            let written = queries::block::create(tx, self.ino, new_offset, data)?;
            data = &data[written as usize..];
            new_offset += written;
            attr.size += written;
        }

        attr.blocks = attr.size.div_ceil(POSIX_BLOCK_SIZE as u64);
        queries::inode::set_attr(tx, self.ino, "size", attr.size)?;
        queries::inode::set_attr(tx, self.ino, "blocks", attr.blocks)?;

        self.buf.clear();
        self.write_offset = new_offset;
        self.size = attr.size;

        Ok(())
    }
}

pub struct FuseDriver {
    pub db: DatabaseOps,
    handles: Slab<FileHandle>,
}

impl FuseDriver {
    pub fn new(db: DatabaseOps) -> Self {
        Self {
            db,
            handles: Slab::new(),
        }
    }
    fn ensure_root_exists(&mut self) -> Result<()> {
        self.db.with_write_tx(|tx| {
            match queries::inode::lookup(tx, 1) {
                // If ino is 1, this is the root directory.
                Err(Error::NotFound) => {
                    log::debug!("ino=1 requested, but does not exist yet, will create.");
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
                        perm: 0o755u16, // TODO probably bad http://web.deu.edu.tr/doc/oreily/networking/puis/ch05_03.htm
                        nlink: 2,
                        uid: 1000, // TODO get real user
                        gid: 1000, // TODO get real group
                        rdev: 0,
                        blksize: 0,
                        flags: 0,
                    };
                    queries::inode::create(tx, &mut attr)?;
                    Ok(())
                }
                Err(e) => Err(e),
                Ok(_) => Ok(()),
            }
        })
    }

    fn lookup_impl(&mut self, _req: &fuser::Request<'_>, parent: u64, name: &std::ffi::OsStr) -> Result<FileAttr> {
        self.db.with_read_tx(|tx| {
            let ino = queries::dir_entry::lookup(tx, parent, name)?;
            let attr = queries::inode::lookup(tx, ino)?;
            Ok(attr)
        })
    }

    fn setattr_impl(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeSpec>,
        mtime: Option<TimeSpec>,
        ctime: Option<TimeSpec>,
        _fh: Option<u64>,
        crtime: Option<TimeSpec>,
        _chgtime: Option<TimeSpec>,
        _bkuptime: Option<TimeSpec>,
        flags: Option<u32>,
    ) -> Result<FileAttr> {
        self.db.with_write_tx(|tx| {
            if let Some(mode) = mode {
                queries::inode::set_attr(tx, ino, "perm", mode)?;
            }
            if let Some(uid) = uid {
                queries::inode::set_attr(tx, ino, "uid", uid)?;
            }
            if let Some(gid) = gid {
                queries::inode::set_attr(tx, ino, "gid", gid)?;
            }
            if let Some(size) = size {
                let bno = Block::offset_to_bno(size);
                queries::block::remove_blocks_from(tx, ino, bno + 1);
                match queries::block::get_block(tx, ino, bno) {
                    Ok(mut block) => {
                        block.truncate(size);
                        queries::block::update(tx, &block);
                    }
                    Err(Error::NotFound) => {}
                    Err(e) => return Err(e),
                }
                queries::inode::set_attr(tx, ino, "size", size)?;
            }
            if let Some(atime) = atime {
                queries::inode::set_attr(tx, ino, "atime_secs", atime.secs)?;
                queries::inode::set_attr(tx, ino, "atime_nanos", atime.nanos)?;
            }
            if let Some(mtime) = mtime {
                queries::inode::set_attr(tx, ino, "mtime_secs", mtime.secs)?;
                queries::inode::set_attr(tx, ino, "mtime_nanos", mtime.nanos)?;
            }
            if let Some(ctime) = ctime {
                queries::inode::set_attr(tx, ino, "ctime_secs", ctime.secs)?;
                queries::inode::set_attr(tx, ino, "ctime_nanos", ctime.nanos)?;
            }
            if let Some(crtime) = crtime {
                queries::inode::set_attr(tx, ino, "crtime_secs", crtime.secs)?;
                queries::inode::set_attr(tx, ino, "crtime_nanos", crtime.nanos)?;
            }
            if let Some(flags) = flags {
                queries::inode::set_attr(tx, ino, "flags", flags)?;
            }

            queries::inode::lookup(tx, ino)
        })
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
            blksize: POSIX_BLOCK_SIZE,
            flags: 0,
        };

        self.db.with_write_tx(|tx| {
            queries::inode::create(tx, &mut attr)?;
            queries::dir_entry::create(tx, parent, name, attr.ino)?;
            Ok(attr)
        })
    }

    fn link_impl(&mut self, _req: &fuser::Request<'_>, ino: u64, newparent: u64, newname: &OsStr) -> Result<FileAttr> {
        self.db.with_write_tx(|tx| {
            let mut attr = queries::inode::lookup(tx, ino)?;
            attr.nlink += 1;
            queries::dir_entry::create(tx, newparent, newname, ino)?;
            queries::inode::set_attr(tx, ino, "nlink", attr.nlink)?;
            Ok(attr)
        })
    }

    fn unlink_impl(&mut self, _req: &fuser::Request<'_>, parent: u64, name: &OsStr) -> Result<()> {
        self.db.with_write_tx(|tx| {
            let ino = queries::dir_entry::lookup(tx, parent, name)?;
            let mut attr = queries::inode::lookup(tx, ino)?;
            attr.nlink -= 1;
            if attr.nlink > 0 {
                queries::inode::set_attr(tx, ino, "nlink", attr.nlink)?;
            } else {
                queries::block::remove_blocks_from(tx, ino, 0)?;
                queries::inode::remove(tx, ino)?;
            }
            queries::dir_entry::remove(tx, parent, name)?;
            Ok(())
        })
    }

    fn mkdir_impl(
        &mut self,
        req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
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
            perm: (mode & !umask) as u16, // TODO probably bad
            nlink: 2,
            uid: req.uid(),
            gid: req.gid(),
            rdev: 0, // Not given for directory?
            blksize: 0,
            flags: 0,
        };
        self.db.with_write_tx(|tx| {
            queries::inode::create(tx, &mut attr)?;
            queries::dir_entry::create(tx, parent, name, attr.ino)?;
            Ok(attr)
        })
    }

    fn rmdir_impl(&mut self, _req: &fuser::Request<'_>, parent: u64, name: &OsStr) -> Result<()> {
        self.db.with_write_tx(|tx| {
            let ino = queries::dir_entry::lookup(tx, parent, name)?;
            let empty = queries::dir_entry::is_dir_empty(tx, ino)?;
            if !empty {
                return Err(Error::NotEmpty);
            }
            queries::inode::remove(tx, ino)?;
            queries::dir_entry::remove(tx, parent, name)?;

            Ok(())
        })
    }

    fn readdir_impl<F>(&mut self, _req: &fuser::Request<'_>, ino: u64, _fh: u64, offset: i64, iter: F) -> Result<()>
    where
        F: FnMut(ListDirEntry) -> bool,
    {
        self.db.with_read_tx(|tx| {
            queries::dir_entry::list_dir(tx, ino, offset, iter)?;
            Ok(())
        })
    }

    fn open_impl(&mut self, _req: &fuser::Request<'_>, ino: u64, flags: OpenFlags) -> Result<(u64, u32)> {
        let attr = self.db.with_read_tx(|tx| queries::inode::lookup(tx, ino))?;
        let fh = self.handles.insert(FileHandle {
            ino,
            write_offset: 0,
            size: attr.size,
            flags,
            buf: Vec::with_capacity(BUFFER_SIZE),
        });
        let fh = u64::try_from(fh).map_err(|_| Error::Overflow)?;
        Ok((fh, flags.bits as u32))
    }

    fn release_impl(
        &mut self,
        _req: &fuser::Request<'_>,
        _ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
    ) -> Result<()> {
        let fh = usize::try_from(fh).map_err(|_| Error::Overflow)?;
        let mut handle = self.handles.try_remove(fh).ok_or(Error::NotFound)?;
        self.db.with_write_tx(|tx| handle.flush(tx))?;
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
        self.db.with_read_tx(|tx| {
            let attr = queries::inode::lookup(tx, ino)?;
            let offset = offset as u64;
            let remaining = attr.size - offset;
            let cap = cmp::min(size as u64, remaining) as usize;
            let mut buf = Vec::with_capacity(cap);

            queries::block::iter_blocks_from(tx, ino, offset, |block| {
                block.copy_into(&mut buf);
                Ok(buf.len() < buf.capacity())
            })?;

            Ok(buf)
        })
    }

    fn write_impl(
        &mut self,
        _req: &fuser::Request<'_>,
        _ino: u64,
        fh: u64,
        offset: i64,
        mut data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
    ) -> Result<u32> {
        let fh = usize::try_from(fh).map_err(|_| Error::Overflow)?;
        let handle = self.handles.get_mut(fh).ok_or(Error::NotFound)?;
        let start_size = data.len();
        let offset = offset as u64;

        // Detect if seek happened. If it did flush whatever is in the buffer
        // where it belongs and then update the offset where to write to.
        if handle.write_offset() != offset {
            debug!("seek occured, flushing, new offset = {}", offset);
            self.db.with_write_tx(|tx| handle.flush(tx))?;
            handle.write_offset = offset;
        }

        while !data.is_empty() {
            if handle.buffer_full() {
                self.db.with_write_tx(|tx| handle.flush(tx))?;
            }
            let consumed = handle.consume_input(data);
            log::debug!("consumed {}", consumed);
            data = &data[consumed..];
        }
        Ok(start_size as u32)
    }

    fn flush_impl(&mut self, _req: &fuser::Request<'_>, _ino: u64, fh: u64, _lock_owner: u64) -> Result<()> {
        let fh = usize::try_from(fh).map_err(|_| Error::Overflow)?;
        let handle = self.handles.get_mut(fh).ok_or(Error::NotFound)?;
        self.db.with_write_tx(|tx| handle.flush(tx))
    }

    fn rename_impl(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        _flags: u32,
    ) -> Result<()> {
        self.db
            .with_write_tx(|tx| queries::dir_entry::rename(tx, parent, name, newparent, newname))
    }
}

impl fuser::Filesystem for FuseDriver {
    fn init(
        &mut self,
        _req: &fuser::Request<'_>,
        _config: &mut fuser::KernelConfig,
    ) -> std::result::Result<(), libc::c_int> {
        // config.set_max_write(crate::database::BLOCK_SIZE).unwrap();
        match self.ensure_root_exists() {
            Ok(()) => Ok(()),
            Err(e) => {
                log::error!("init error: {}", e);
                Err(e.errno())
            }
        }
    }

    fn lookup(&mut self, req: &fuser::Request<'_>, parent: u64, name: &std::ffi::OsStr, reply: fuser::ReplyEntry) {
        log::trace!("lookup(parent={}, name={:?})", parent, name.to_string_lossy());
        let res = self.lookup_impl(req, parent, name);
        log::trace!("lookup: {:?}", res);

        match res {
            Ok(attr) => reply.entry(&DURATION, &attr, 0),
            Err(e) => reply.error(e.errno()),
        }
    }

    fn getattr(&mut self, _req: &fuser::Request<'_>, ino: u64, reply: fuser::ReplyAttr) {
        log::trace!("getattr(ino={})", ino);
        let res = self.db.with_read_tx(|tx| queries::inode::lookup(tx, ino));
        log::trace!("getattr: {:?}", res);

        match res {
            Ok(attr) => reply.attr(&DURATION, &attr),
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
        log::trace!(
            "setattr(ino={}, mode={:#?}, uid={:?}, gid={:?}, size={:?})",
            ino,
            mode,
            uid,
            gid,
            size,
        );
        let res = self.setattr_impl(
            req,
            ino,
            mode,
            uid,
            gid,
            size,
            atime.map(Into::into),
            mtime.map(Into::into),
            ctime.map(Into::into),
            fh,
            crtime.map(Into::into),
            chgtime.map(Into::into),
            bkuptime.map(Into::into),
            flags,
        );
        log::trace!("setattr: {:?}", res);

        match res {
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
        log::trace!(
            "mknod(parent={}, name={:?}, mode={}, umask={:#o}, rdev={})",
            parent,
            name.to_string_lossy(),
            mode,
            umask,
            rdev
        );
        let res = self.mknod_impl(req, parent, name, mode, umask, rdev);
        log::trace!("mknod: {:?}", res);

        match res {
            Ok(attr) => reply.entry(&DURATION, &attr, 0),
            Err(e) => reply.error(e.errno()),
        }
    }

    fn link(&mut self, req: &fuser::Request<'_>, ino: u64, newparent: u64, newname: &OsStr, reply: fuser::ReplyEntry) {
        log::trace!("link(ino={}, newparent={}, newname={:?})", ino, newparent, newname);
        let res = self.link_impl(req, ino, newparent, newname);
        log::trace!("link: {:?}", res);

        match res {
            Ok(attr) => reply.entry(&DURATION, &attr, 0),
            Err(e) => reply.error(e.errno()),
        }
    }

    fn unlink(&mut self, req: &fuser::Request<'_>, parent: u64, name: &OsStr, reply: fuser::ReplyEmpty) {
        log::trace!("unlink(parent={}, name={:?})", parent, name);
        let res = self.unlink_impl(req, parent, name);
        log::trace!("unlink: {:?}", res);

        match res {
            Ok(_) => reply.ok(),
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
        log::trace!(
            "mkdir(parent={}, name={:?}, mode={}, umask={:#o})",
            parent,
            name.to_string_lossy(),
            mode,
            umask,
        );
        let res = self.mkdir_impl(req, parent, name, mode, umask);
        log::trace!("mkdir: {:?}", res);

        match res {
            Ok(attr) => reply.entry(&DURATION, &attr, 0),
            Err(e) => reply.error(e.errno()),
        }
    }

    fn rmdir(&mut self, req: &fuser::Request<'_>, parent: u64, name: &OsStr, reply: fuser::ReplyEmpty) {
        log::trace!("rmdir(parent={}, name={:?})", parent, name);
        let res = self.rmdir_impl(req, parent, name);
        log::trace!("rmdir: {:?}", res);

        match res {
            Ok(_) => reply.ok(),
            Err(e) => reply.error(e.errno()),
        }
    }

    fn readdir(&mut self, req: &fuser::Request<'_>, ino: u64, fh: u64, offset: i64, mut reply: fuser::ReplyDirectory) {
        log::trace!("readdir(ino={}, fh={}, offset={})", ino, fh, offset);
        let res = self.readdir_impl(req, ino, fh, offset, |entry| {
            reply.add(entry.ino, entry.offset, entry.kind, entry.name)
        });
        log::trace!("readdir: {:?}", res);

        match res {
            Ok(_) => reply.ok(),
            Err(e) => reply.error(e.errno()),
        }
    }

    fn open(&mut self, req: &fuser::Request<'_>, ino: u64, flags: i32, reply: fuser::ReplyOpen) {
        let flags = OpenFlags::from(flags);
        log::trace!("open(ino={}, flags={:?})", ino, flags);
        let res = self.open_impl(req, ino, flags);
        log::trace!("open: {:?}", res);

        match res {
            Ok((fh, flags)) => reply.opened(fh, flags),
            Err(e) => reply.error(e.errno()),
        }
    }

    fn release(
        &mut self,
        req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        flags: i32,
        lock_owner: Option<u64>,
        flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        log::trace!("release(ino={}, fh={}, flush={})", ino, fh, flush);
        let res = self.release_impl(req, ino, fh, flags, lock_owner, flush);
        log::trace!("release: {:?}", res);

        match res {
            Ok(_) => reply.ok(),
            Err(e) => reply.error(e.errno()),
        }
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
        log::trace!("read(ino={}, offset={}, size={})", ino, offset, size);
        let res = self.read_impl(req, ino, fh, offset, size, flags, lock_owner);
        log::trace!("read: {:?}", res.as_ref().map(|d| d.len()));

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
        log::trace!("write(ino={}, offset={}, data_len={})", ino, offset, data.len());
        let res = self.write_impl(req, ino, fh, offset, data, write_flags, flags, lock_owner);
        log::trace!("write: {:?}", res);

        match res {
            Ok(written) => reply.written(written),
            Err(e) => reply.error(e.errno()),
        }
    }

    fn flush(&mut self, req: &fuser::Request<'_>, ino: u64, fh: u64, lock_owner: u64, reply: fuser::ReplyEmpty) {
        log::trace!("flush(ino={}, fh={})", ino, fh);
        let res = self.flush_impl(req, ino, fh, lock_owner);
        log::trace!("flush: {:?}", res);

        match res {
            Ok(_) => reply.ok(),
            Err(e) => reply.error(e.errno()),
        }
    }

    fn rename(
        &mut self,
        req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        flags: u32,
        reply: fuser::ReplyEmpty,
    ) {
        log::trace!(
            "rename(parent={}, name={:?}, newparent={}, newname={:?}",
            parent,
            name,
            newparent,
            newname
        );
        let res = self.rename_impl(req, parent, name, newparent, newname, flags);
        log::trace!("rename: {:?}", res);

        match res {
            Ok(_) => reply.ok(),
            Err(e) => reply.error(e.errno()),
        }
    }
}

#[test]
fn test_open_flags() {
    let flags = OpenFlags::from(libc::O_RDONLY);
    assert_eq!(
        (
            flags.read,
            flags.write,
            flags.create,
            flags.append,
            flags.truncate,
            flags.sync
        ),
        (true, false, false, false, false, false)
    );

    let flags = OpenFlags::from(libc::O_WRONLY);
    assert_eq!(
        (
            flags.read,
            flags.write,
            flags.create,
            flags.append,
            flags.truncate,
            flags.sync
        ),
        (false, true, false, false, false, false)
    );

    let flags = OpenFlags::from(libc::O_RDWR);
    assert_eq!(
        (
            flags.read,
            flags.write,
            flags.create,
            flags.append,
            flags.truncate,
            flags.sync
        ),
        (true, true, false, false, false, false)
    );

    let flags = OpenFlags::from(libc::O_WRONLY | libc::O_CREAT | libc::O_APPEND);
    assert_eq!(
        (
            flags.read,
            flags.write,
            flags.create,
            flags.append,
            flags.truncate,
            flags.sync
        ),
        (false, true, true, true, false, false)
    );

    let flags = OpenFlags::from(libc::O_RDWR | libc::O_TRUNC | libc::O_SYNC);
    assert_eq!(
        (
            flags.read,
            flags.write,
            flags.create,
            flags.append,
            flags.truncate,
            flags.sync
        ),
        (true, true, false, false, true, true)
    );
}
