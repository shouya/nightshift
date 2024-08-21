#![allow(dead_code)]

#[derive(Clone, Copy, Debug)]
pub struct OpenFlags {
    pub bits: i32,
    pub read: bool,
    pub write: bool,
    pub create: bool,
    pub append: bool,
    pub truncate: bool,
    pub sync: bool,
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

#[cfg(test)]
mod tests {
    use crate::driver::OpenFlags;

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
}
