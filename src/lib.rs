#![feature(macro_metavar_expr)]

use std::borrow::Cow;
use std::ffi::OsStr;
use std::path::Path;

use nix::sys::stat::Mode;
use nix::unistd::Gid;
use nix::unistd::Uid;
use uuid::Uuid;

mod wire;

#[derive(Debug, thiserror::Error)]
pub enum Error<'a> {
    // TODO(vmagro): expose more granular errors at some point?
    // #[error("parse error: {0:?}")]
    // Parse(nom::error::ErrorKind),
    #[error("unexpected trailing data: {0:?}")]
    TrailingData(&'a [u8]),
}

pub type Result<'a, R> = std::result::Result<R, Error<'a>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sendstream<'a> {
    commands: Vec<Command<'a>>,
}

impl<'a> Sendstream<'a> {
    pub fn commands(&self) -> &[Command<'a>] {
        &self.commands
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command<'a> {
    Chmod(Chmod<'a>),
    Chown(Chown<'a>),
    Clone(Clone<'a>),
    End,
    Link(Link<'a>),
    Mkdir(Mkdir<'a>),
    Mkfifo(Mkfifo<'a>),
    Mkfile(Mkfile<'a>),
    Mknod(Mknod<'a>),
    Mksock(Mksock<'a>),
    RemoveXattr(RemoveXattr<'a>),
    Rename(Rename<'a>),
    Rmdir(Rmdir<'a>),
    SetXattr(SetXattr<'a>),
    Snapshot(Snapshot<'a>),
    Subvol(Subvol<'a>),
    Symlink(Symlink<'a>),
    Truncate(Truncate<'a>),
    Unlink(Unlink<'a>),
    UpdateExtent(UpdateExtent<'a>),
    Utimes(Utimes<'a>),
    Write(Write<'a>),
}

impl<'a> Command<'a> {
    /// Exposed for tests to ensure that the demo sendstream is exhaustive and
    /// exercises all commands
    #[cfg(test)]
    pub(crate) fn command_type(&self) -> wire::cmd::CommandType {
        match self {
            Self::Chmod(_) => wire::cmd::CommandType::Chmod,
            Self::Chown(_) => wire::cmd::CommandType::Chown,
            Self::Clone(_) => wire::cmd::CommandType::Clone,
            Self::End => wire::cmd::CommandType::End,
            Self::Link(_) => wire::cmd::CommandType::Link,
            Self::Mkdir(_) => wire::cmd::CommandType::Mkdir,
            Self::Mkfifo(_) => wire::cmd::CommandType::Mkfifo,
            Self::Mkfile(_) => wire::cmd::CommandType::Mkfile,
            Self::Mknod(_) => wire::cmd::CommandType::Mknod,
            Self::Mksock(_) => wire::cmd::CommandType::Mksock,
            Self::RemoveXattr(_) => wire::cmd::CommandType::RemoveXattr,
            Self::Rename(_) => wire::cmd::CommandType::Rename,
            Self::Rmdir(_) => wire::cmd::CommandType::Rmdir,
            Self::SetXattr(_) => wire::cmd::CommandType::SetXattr,
            Self::Snapshot(_) => wire::cmd::CommandType::Snapshot,
            Self::Subvol(_) => wire::cmd::CommandType::Subvol,
            Self::Symlink(_) => wire::cmd::CommandType::Symlink,
            Self::Truncate(_) => wire::cmd::CommandType::Truncate,
            Self::Unlink(_) => wire::cmd::CommandType::Unlink,
            Self::UpdateExtent(_) => wire::cmd::CommandType::UpdateExtent,
            Self::Utimes(_) => wire::cmd::CommandType::Utimes,
            Self::Write(_) => wire::cmd::CommandType::Write,
        }
    }
}

macro_rules! from_cmd {
    ($t:ident) => {
        impl<'a> From<$t<'a>> for Command<'a> {
            fn from(c: $t<'a>) -> Self {
                Self::$t(c)
            }
        }
    };
}

/// Because the stream is emitted in inode order, not FS order, the destination
/// directory may not exist at the time that a creation command is emitted, so
/// it will end up with an opaque name that will end up getting renamed to the
/// final name later in the stream.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct TemporaryPath<'a>(pub Cow<'a, Path>);

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Ctransid(pub u64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subvol<'a> {
    pub(crate) path: Cow<'a, Path>,
    pub(crate) uuid: Uuid,
    pub(crate) ctransid: Ctransid,
}
from_cmd!(Subvol);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chmod<'a> {
    pub(crate) path: Cow<'a, Path>,
    pub(crate) mode: Mode,
}
from_cmd!(Chmod);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chown<'a> {
    pub(crate) path: Cow<'a, Path>,
    pub(crate) uid: Uid,
    pub(crate) gid: Gid,
}
from_cmd!(Chown);

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct CloneLen(usize);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Clone<'a> {
    pub(crate) src_offset: FileOffset,
    pub(crate) len: CloneLen,
    pub(crate) src_path: Cow<'a, Path>,
    pub(crate) uuid: Uuid,
    pub(crate) ctransid: Ctransid,
    pub(crate) dst_path: Cow<'a, Path>,
    pub(crate) dst_offset: FileOffset,
}
from_cmd!(Clone);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkTarget<'a>(Cow<'a, Path>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Link<'a> {
    pub(crate) link_name: Cow<'a, Path>,
    pub(crate) target: LinkTarget<'a>,
}
from_cmd!(Link);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mkdir<'a> {
    pub(crate) path: TemporaryPath<'a>,
    pub(crate) ino: Ino,
}
from_cmd!(Mkdir);

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Rdev(pub u64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mkfifo<'a> {
    pub(crate) path: TemporaryPath<'a>,
    pub(crate) ino: Ino,
    pub(crate) rdev: Rdev,
    pub(crate) mode: Mode,
}
from_cmd!(Mkfifo);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mkfile<'a> {
    pub(crate) path: TemporaryPath<'a>,
    pub(crate) ino: Ino,
}
from_cmd!(Mkfile);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mknod<'a> {
    pub(crate) path: TemporaryPath<'a>,
    pub(crate) ino: Ino,
    pub(crate) rdev: Rdev,
    pub(crate) mode: Mode,
}
from_cmd!(Mknod);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mksock<'a> {
    pub(crate) path: TemporaryPath<'a>,
    pub(crate) ino: Ino,
    pub(crate) rdev: Rdev,
    pub(crate) mode: Mode,
}
from_cmd!(Mksock);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoveXattr<'a> {
    pub(crate) path: Cow<'a, Path>,
    pub(crate) name: XattrName<'a>,
}
from_cmd!(RemoveXattr);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rename<'a> {
    pub(crate) from: Cow<'a, Path>,
    pub(crate) to: Cow<'a, Path>,
}
from_cmd!(Rename);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rmdir<'a> {
    pub(crate) path: Cow<'a, Path>,
}
from_cmd!(Rmdir);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Symlink<'a> {
    pub(crate) link_name: Cow<'a, Path>,
    pub(crate) ino: Ino,
    pub(crate) target: LinkTarget<'a>,
}
from_cmd!(Symlink);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct XattrName<'a>(Cow<'a, OsStr>);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct XattrData<'a>(Cow<'a, [u8]>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetXattr<'a> {
    pub(crate) path: Cow<'a, Path>,
    pub(crate) name: XattrName<'a>,
    pub(crate) data: XattrData<'a>,
}
from_cmd!(SetXattr);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot<'a> {
    pub(crate) path: Cow<'a, Path>,
    pub(crate) uuid: Uuid,
    pub(crate) ctransid: Ctransid,
    pub(crate) clone_uuid: Uuid,
    pub(crate) clone_ctransid: Ctransid,
}
from_cmd!(Snapshot);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Truncate<'a> {
    pub(crate) path: Cow<'a, Path>,
    pub(crate) size: usize,
}
from_cmd!(Truncate);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unlink<'a> {
    pub(crate) path: Cow<'a, Path>,
}
from_cmd!(Unlink);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateExtent<'a> {
    pub(crate) path: Cow<'a, Path>,
    pub(crate) offset: FileOffset,
    pub(crate) len: usize,
}
from_cmd!(UpdateExtent);

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Time(std::time::SystemTime);

macro_rules! time_alias {
    ($a:ident) => {
        #[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
        pub struct $a(std::time::SystemTime);

        impl AsRef<std::time::SystemTime> for $a {
            fn as_ref(&self) -> &std::time::SystemTime {
                &self
            }
        }

        impl std::ops::Deref for $a {
            type Target = std::time::SystemTime;

            fn deref(&self) -> &std::time::SystemTime {
                &self.0
            }
        }
    };
}

time_alias!(Atime);
time_alias!(Ctime);
time_alias!(Mtime);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Utimes<'a> {
    pub(crate) path: Cow<'a, Path>,
    pub(crate) atime: Atime,
    pub(crate) mtime: Mtime,
    pub(crate) ctime: Ctime,
}
from_cmd!(Utimes);

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Ino(u64);

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct FileOffset(usize);

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Data<'a>(Cow<'a, [u8]>);

impl<'a> std::fmt::Debug for Data<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match std::str::from_utf8(&self.0) {
            Ok(s) => Cow::Borrowed(s),
            Err(_) => Cow::Owned(hex::encode(&self.0)),
        };
        if s.len() <= 128 {
            write!(f, "{:?}", s)
        } else {
            write!(f, "{:?} <truncated> {:?}", &s[..64], &s[s.len() - 64..])
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Write<'a> {
    pub(crate) path: Cow<'a, Path>,
    pub(crate) offset: FileOffset,
    pub(crate) data: Data<'a>,
}
from_cmd!(Write);
