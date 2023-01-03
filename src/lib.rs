#![feature(macro_metavar_expr)]

use std::borrow::Cow;
use std::ffi::OsStr;
use std::os::unix::prelude::PermissionsExt;
use std::path::Path;

use derive_more::AsRef;
use derive_more::Deref;
use derive_more::From;
use nix::sys::stat::SFlag;
use nix::unistd::Gid;
use nix::unistd::Uid;
use uuid::Uuid;

mod wire;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    // TODO(vmagro): expose more granular errors at some point?
    // #[error("parse error: {0:?}")]
    // Parse(nom::error::ErrorKind),
    #[error("sendstream had unexpected trailing data: {0:?}")]
    TrailingData(Vec<u8>),
}

pub type Result<R> = std::result::Result<R, Error>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sendstream<'a> {
    commands: Vec<Command<'a>>,
}

impl<'a> Sendstream<'a> {
    pub fn commands(&self) -> &[Command<'a>] {
        &self.commands
    }

    pub fn into_commands(self) -> Vec<Command<'a>> {
        self.commands
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

macro_rules! one_getter {
    ($f:ident, $ft:ty, copy) => {
        pub fn $f(&self) -> $ft {
            self.$f
        }
    };
    ($f:ident, $ft:ty, borrow) => {
        pub fn $f(&self) -> &$ft {
            &self.$f
        }
    };
}

macro_rules! getters {
    ($t:ident, [$(($f:ident, $ft:ident, $ref:tt)),+]) => {
        impl<'a> $t<'a> {
            $(
                one_getter!($f, $ft, $ref);
            )+
        }
    };
}

/// Because the stream is emitted in inode order, not FS order, the destination
/// directory may not exist at the time that a creation command is emitted, so
/// it will end up with an opaque name that will end up getting renamed to the
/// final name later in the stream.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, AsRef, Deref)]
#[as_ref(forward)]
pub struct TemporaryPath<'a>(pub(crate) &'a Path);

impl<'a> TemporaryPath<'a> {
    pub fn path(&self) -> &Path {
        &self.0
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Ctransid(pub u64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subvol<'a> {
    pub(crate) path: &'a Path,
    pub(crate) uuid: Uuid,
    pub(crate) ctransid: Ctransid,
}
from_cmd!(Subvol);
getters! {Subvol, [(path, Path, borrow), (uuid, Uuid, copy), (ctransid, Ctransid, copy)]}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, AsRef, Deref)]
pub struct Mode(u32);

impl Mode {
    pub fn mode(self) -> nix::sys::stat::Mode {
        nix::sys::stat::Mode::from_bits_truncate(self.0)
    }

    pub fn permissions(self) -> std::fs::Permissions {
        std::fs::Permissions::from_mode(self.0)
    }

    pub fn file_type(self) -> SFlag {
        SFlag::from_bits_truncate(self.0)
    }
}

impl std::fmt::Debug for Mode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Mode")
            .field("permissions", &self.permissions())
            .field("type", &self.file_type())
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chmod<'a> {
    pub(crate) path: &'a Path,
    pub(crate) mode: Mode,
}
from_cmd!(Chmod);
getters! {Chmod, [(path, Path, borrow), (mode, Mode, copy)]}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chown<'a> {
    pub(crate) path: &'a Path,
    pub(crate) uid: Uid,
    pub(crate) gid: Gid,
}
from_cmd!(Chown);
getters! {Chown, [(path, Path, borrow), (uid, Uid, copy), (gid, Gid, copy)]}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, AsRef, Deref)]
pub struct CloneLen(usize);

impl CloneLen {
    pub fn as_usize(self) -> usize {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Clone<'a> {
    pub(crate) src_offset: FileOffset,
    pub(crate) len: CloneLen,
    pub(crate) src_path: &'a Path,
    pub(crate) uuid: Uuid,
    pub(crate) ctransid: Ctransid,
    pub(crate) dst_path: &'a Path,
    pub(crate) dst_offset: FileOffset,
}
from_cmd!(Clone);
getters! {Clone, [
    (src_offset, FileOffset, copy),
    (len, CloneLen, copy),
    (src_path, Path, borrow),
    (uuid, Uuid, copy),
    (ctransid, Ctransid, copy),
    (dst_path, Path, borrow),
    (dst_offset, FileOffset, copy)
]}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, AsRef, Deref)]
#[as_ref(forward)]
pub struct LinkTarget<'a>(&'a Path);

impl<'a> LinkTarget<'a> {
    pub fn path(&self) -> &Path {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Link<'a> {
    pub(crate) link_name: &'a Path,
    pub(crate) target: LinkTarget<'a>,
}
from_cmd!(Link);
getters! {Link, [(link_name, Path, borrow), (target, LinkTarget, borrow)]}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mkdir<'a> {
    pub(crate) path: TemporaryPath<'a>,
    pub(crate) ino: Ino,
}
from_cmd!(Mkdir);
getters! {Mkdir, [(path, TemporaryPath, borrow), (ino, Ino, copy)]}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Rdev(u64);

impl Rdev {
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mkspecial<'a> {
    pub(crate) path: TemporaryPath<'a>,
    pub(crate) ino: Ino,
    pub(crate) rdev: Rdev,
    pub(crate) mode: Mode,
}
getters! {Mkspecial, [
    (path, TemporaryPath, borrow),
    (ino, Ino, copy),
    (rdev, Rdev, copy),
    (mode, Mode, copy)
]}

macro_rules! special {
    ($t:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, AsRef, Deref)]
        #[repr(transparent)]
        pub struct $t<'a>(Mkspecial<'a>);
        from_cmd!($t);
    };
}
special!(Mkfifo);
special!(Mknod);
special!(Mksock);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mkfile<'a> {
    pub(crate) path: TemporaryPath<'a>,
    pub(crate) ino: Ino,
}
from_cmd!(Mkfile);
getters! {Mkfile, [(path, TemporaryPath, borrow), (ino, Ino, copy)]}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoveXattr<'a> {
    pub(crate) path: &'a Path,
    pub(crate) name: XattrName<'a>,
}
from_cmd!(RemoveXattr);
getters! {RemoveXattr, [(path, Path, borrow), (name, XattrName, borrow)]}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rename<'a> {
    pub(crate) from: &'a Path,
    pub(crate) to: &'a Path,
}
from_cmd!(Rename);
getters! {Rename, [(from, Path, borrow), (to, Path, borrow)]}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rmdir<'a> {
    pub(crate) path: &'a Path,
}
from_cmd!(Rmdir);
getters! {Rmdir, [(path, Path, borrow)]}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Symlink<'a> {
    pub(crate) link_name: &'a Path,
    pub(crate) ino: Ino,
    pub(crate) target: LinkTarget<'a>,
}
from_cmd!(Symlink);
getters! {Symlink, [(link_name, Path, borrow), (ino, Ino, copy), (target, LinkTarget, borrow)]}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, AsRef, Deref, From)]
#[as_ref(forward)]
#[from(forward)]
pub struct XattrName<'a>(&'a OsStr);

#[derive(Debug, Clone, PartialEq, Eq, AsRef, Deref, From)]
#[as_ref(forward)]
#[from(forward)]
pub struct XattrData<'a>(&'a [u8]);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetXattr<'a> {
    pub(crate) path: &'a Path,
    pub(crate) name: XattrName<'a>,
    pub(crate) data: XattrData<'a>,
}
from_cmd!(SetXattr);
getters! {SetXattr, [(path, Path, borrow), (name, XattrName, borrow), (data, XattrData, borrow)]}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot<'a> {
    pub(crate) path: &'a Path,
    pub(crate) uuid: Uuid,
    pub(crate) ctransid: Ctransid,
    pub(crate) clone_uuid: Uuid,
    pub(crate) clone_ctransid: Ctransid,
}
from_cmd!(Snapshot);
getters! {Snapshot, [
    (path, Path, borrow),
    (uuid, Uuid, copy),
    (ctransid, Ctransid, copy),
    (clone_uuid, Uuid, copy),
    (clone_ctransid, Ctransid, copy)
]}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Truncate<'a> {
    pub(crate) path: &'a Path,
    pub(crate) size: usize,
}
from_cmd!(Truncate);
getters! {Truncate, [(path, Path, borrow), (size, usize, copy)]}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unlink<'a> {
    pub(crate) path: &'a Path,
}
from_cmd!(Unlink);
getters! {Unlink, [(path, Path, borrow)]}

#[allow(clippy::len_without_is_empty)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateExtent<'a> {
    pub(crate) path: &'a Path,
    pub(crate) offset: FileOffset,
    pub(crate) len: usize,
}
from_cmd!(UpdateExtent);
getters! {UpdateExtent, [(path, Path, borrow), (offset, FileOffset, copy), (len, usize, copy)]}

macro_rules! time_alias {
    ($a:ident) => {
        #[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, AsRef, Deref)]
        #[as_ref(forward)]
        #[repr(transparent)]
        pub struct $a(std::time::SystemTime);
    };
}

time_alias!(Atime);
time_alias!(Ctime);
time_alias!(Mtime);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Utimes<'a> {
    pub(crate) path: &'a Path,
    pub(crate) atime: Atime,
    pub(crate) mtime: Mtime,
    pub(crate) ctime: Ctime,
}
from_cmd!(Utimes);
getters! {Utimes, [(path, Path, borrow), (atime, Atime, copy), (mtime, Mtime,copy), (ctime, Ctime, copy)]}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, AsRef, Deref)]
pub struct Ino(u64);

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, AsRef, Deref)]
pub struct FileOffset(usize);

impl FileOffset {
    pub fn as_u64(self) -> u64 {
        self.0 as u64
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, AsRef, Deref)]
#[as_ref(forward)]
pub struct Data<'a>(&'a [u8]);

impl<'a> Data<'a> {
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl<'a> std::fmt::Debug for Data<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match std::str::from_utf8(&self.0) {
            Ok(s) => Cow::Borrowed(s),
            Err(_) => Cow::Owned(hex::encode(&self.0)),
        };
        if s.len() <= 128 {
            write!(f, "{s:?}")
        } else {
            write!(f, "{:?} <truncated> {:?}", &s[..64], &s[s.len() - 64..])
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Write<'a> {
    pub(crate) path: &'a Path,
    pub(crate) offset: FileOffset,
    pub(crate) data: Data<'a>,
}
from_cmd!(Write);
getters! {Write, [(path, Path, borrow), (offset, FileOffset, copy), (data, Data, borrow)]}
