#[derive(Debug, Copy, Clone)]
#[repr(u8)]
pub enum FileType {
    NamedPipe = 1,
    CharDevice = 2,
    BlockDevice = 3,
    Directory = 4,
    RegularFile = 5,
    Symlink = 6,
    Socket = 7,
}

impl FileType {
    pub fn from_mode(value: libc::mode_t) -> Option<FileType> {
        // https://man7.org/linux/man-pages/man2/mknod.2.html
        match value {
            libc::S_IFREG => Some(FileType::RegularFile),
            libc::S_IFCHR => Some(FileType::CharDevice),
            libc::S_IFBLK => Some(FileType::BlockDevice),
            libc::S_IFIFO => Some(FileType::NamedPipe),
            libc::S_IFSOCK => Some(FileType::Socket),
            _ => None,
        }
    }
}

impl Into<u8> for FileType {
    fn into(self) -> u8 {
        self as u8
    }
}

impl TryFrom<u8> for FileType {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Ok(match value {
            x if x == FileType::NamedPipe as u8 => FileType::NamedPipe,
            x if x == FileType::CharDevice as u8 => FileType::CharDevice,
            x if x == FileType::BlockDevice as u8 => FileType::BlockDevice,
            x if x == FileType::Directory as u8 => FileType::Directory,
            x if x == FileType::RegularFile as u8 => FileType::RegularFile,
            x if x == FileType::Symlink as u8 => FileType::Symlink,
            x if x == FileType::Socket as u8 => FileType::Socket,
            _ => return Err(()),
        })
    }
}

impl From<fuser::FileType> for FileType {
    fn from(value: fuser::FileType) -> Self {
        match value {
            fuser::FileType::NamedPipe => FileType::NamedPipe,
            fuser::FileType::CharDevice => FileType::CharDevice,
            fuser::FileType::BlockDevice => FileType::BlockDevice,
            fuser::FileType::Directory => FileType::Directory,
            fuser::FileType::RegularFile => FileType::RegularFile,
            fuser::FileType::Symlink => FileType::Symlink,
            fuser::FileType::Socket => FileType::Socket,
        }
    }
}

impl Into<fuser::FileType> for FileType {
    fn into(self) -> fuser::FileType {
        match self {
            FileType::NamedPipe => fuser::FileType::NamedPipe,
            FileType::CharDevice => fuser::FileType::CharDevice,
            FileType::BlockDevice => fuser::FileType::BlockDevice,
            FileType::Directory => fuser::FileType::Directory,
            FileType::RegularFile => fuser::FileType::RegularFile,
            FileType::Symlink => fuser::FileType::Symlink,
            FileType::Socket => fuser::FileType::Socket,
        }
    }
}
