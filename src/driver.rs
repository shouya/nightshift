#![allow(clippy::too_many_arguments)]
use std::{
    cmp,
    ffi::OsStr,
    time::{Duration, SystemTime},
};

use fuser::FileAttr;

use crate::database::ListDirEntry;
use crate::errors::{Error, Result};
use crate::types::FileType;
use crate::{database::DatabaseOps, time::TimeSpec};

const DURATION: Duration = Duration::from_secs(0);
const POSIX_BLOCK_SIZE: u32 = 512;

pub struct FuseDriver {
    pub db: DatabaseOps,
}

impl FuseDriver {
    fn ensure_root_exists(&mut self) -> Result<()> {
        match self.db.lookup_inode(1) {
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
        atime: Option<TimeSpec>,
        mtime: Option<TimeSpec>,
        ctime: Option<TimeSpec>,
        _fh: Option<u64>,
        crtime: Option<TimeSpec>,
        _chgtime: Option<TimeSpec>,
        _bkuptime: Option<TimeSpec>,
        flags: Option<u32>,
    ) -> Result<FileAttr> {
        // TODO optimize
        if let Some(mode) = mode {
            self.db.set_attr(ino, "mode", mode)?;
        }
        if let Some(uid) = uid {
            self.db.set_attr(ino, "uid", uid)?;
        }
        if let Some(gid) = gid {
            self.db.set_attr(ino, "gid", gid)?;
        }
        if let Some(size) = size {
            self.db.set_attr(ino, "size", size)?;
        }
        if let Some(atime) = atime {
            self.db.set_attr(ino, "atime_secs", atime.secs)?;
            self.db.set_attr(ino, "atime_nanos", atime.nanos)?;
        }
        if let Some(mtime) = mtime {
            self.db.set_attr(ino, "mtime_secs", mtime.secs)?;
            self.db.set_attr(ino, "mtime_nanos", mtime.nanos)?;
        }
        if let Some(ctime) = ctime {
            self.db.set_attr(ino, "ctime_secs", ctime.secs)?;
            self.db.set_attr(ino, "ctime_nanos", ctime.nanos)?;
        }
        if let Some(crtime) = crtime {
            self.db.set_attr(ino, "crtime_secs", crtime.secs)?;
            self.db.set_attr(ino, "crtime_nanos", crtime.nanos)?;
        }
        if let Some(flags) = flags {
            self.db.set_attr(ino, "flags", flags)?;
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
            blksize: POSIX_BLOCK_SIZE,
            flags: 0,
        };
        // TODO transaction
        self.db.create_inode(&mut attr)?;
        self.db.create_dir_entry(parent, name, attr.ino)?;
        Ok(attr)
    }

    fn link_impl(&mut self, _req: &fuser::Request<'_>, ino: u64, newparent: u64, newname: &OsStr) -> Result<FileAttr> {
        let mut attr = self.db.lookup_inode(ino)?;
        attr.nlink += 1;
        self.db.create_dir_entry(newparent, newname, ino)?;
        self.db.set_attr(ino, "nlink", attr.nlink)?;
        Ok(attr)
    }

    fn unlink_impl(&mut self, _req: &fuser::Request<'_>, parent: u64, name: &OsStr) -> Result<()> {
        let ino = self.db.lookup_dir_entry(parent, name)?;
        let mut attr = self.db.lookup_inode(ino)?;
        attr.nlink -= 1;
        if attr.nlink > 0 {
            self.db.set_attr(ino, "nlink", attr.nlink)?;
        } else {
            self.db.remove_blocks(ino)?;
            self.db.remove_inode(ino)?;
        }
        self.db.remove_dir_entry(parent, name)?;
        Ok(())
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
        self.db.create_inode(&mut attr)?;
        self.db.create_dir_entry(parent, name, attr.ino)?;
        Ok(attr)
    }

    fn rmdir_impl(&mut self, _req: &fuser::Request<'_>, parent: u64, name: &OsStr) -> Result<()> {
        let ino = self.db.lookup_dir_entry(parent, name)?;
        let empty = self.db.is_dir_empty(ino)?;
        if !empty {
            return Err(Error::NotEmpty);
        }
        self.db.remove_inode(ino)?;
        self.db.remove_dir_entry(parent, name)?;
        Ok(())
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
        let attr = self.db.lookup_inode(ino)?;
        let offset = offset as u64;
        let remaining = attr.size - offset;
        let cap = cmp::min(size as u64, remaining) as usize;
        let mut buf = Vec::with_capacity(cap);

        self.db.iter_blocks_from(ino, offset, |block| {
            block.copy_into(&mut buf);
            buf.len() < buf.capacity()
        })?;
        dbg!(buf.len(), buf.capacity());

        Ok(buf)
    }

    fn write_impl(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
    ) -> Result<u32> {
        let size = data.len();
        let mut offset = offset as u64;

        let mut attr = self.db.lookup_inode(ino)?;

        // Overwrite existing blocks until we get a NotFound error indicating
        // that there's no more blocks to overwrite. This usually happens when
        // seek is used or the block is incomplete.
        while !data.is_empty() {
            match self.db.update_block(ino, offset, data) {
                Ok((written, bytes_diff)) => {
                    data = &data[written as usize..];
                    offset += written;
                    attr.size = (attr.size as i64 + bytes_diff) as u64;
                }
                Err(Error::NotFound) => break,
                Err(e) => return Err(e),
            }
        }

        // Write the rest of the data in a new block.
        while !data.is_empty() {
            let written = self.db.create_block(ino, offset, data)?;
            data = &data[written as usize..];
            offset += written;
            attr.size += written;
        }

        attr.blocks = attr.size.div_ceil(POSIX_BLOCK_SIZE as u64);

        self.db.set_attr(ino, "size", attr.size)?;
        self.db.set_attr(ino, "blocks", attr.blocks)?;

        Ok(size as u32)
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
        self.db.rename_entry(parent, name, newparent, newname)
    }
}

impl fuser::Filesystem for FuseDriver {
    fn init(
        &mut self,
        _req: &fuser::Request<'_>,
        config: &mut fuser::KernelConfig,
    ) -> std::result::Result<(), libc::c_int> {
        config.set_max_write(crate::database::BLOCK_SIZE).unwrap();
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
        let res = self.db.lookup_inode(ino);
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
        log::trace!("link(ino={}, newparent={}, newname={:?}", ino, newparent, newname);
        let res = self.link_impl(req, ino, newparent, newname);
        log::trace!("link: {:?}", res);

        match res {
            Ok(attr) => reply.entry(&DURATION, &attr, 0),
            Err(e) => reply.error(e.errno()),
        }
    }

    fn unlink(&mut self, req: &fuser::Request<'_>, parent: u64, name: &OsStr, reply: fuser::ReplyEmpty) {
        log::trace!("unlink(parent={}, name={:?}", parent, name);
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
        log::trace!("rmdir(parent={}, name={:?}", parent, name);
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
        log::trace!("read(ino={}, offset={}, size={}", ino, offset, size);
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
        log::trace!("write(ino={}, offset={}, data_len={}", ino, offset, data.len());
        let res = self.write_impl(req, ino, fh, offset, data, write_flags, flags, lock_owner);
        log::trace!("write: {:?}", res);

        match res {
            Ok(written) => reply.written(written),
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
