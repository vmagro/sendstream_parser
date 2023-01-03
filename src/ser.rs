pub(crate) mod uid {
    use nix::unistd::Uid;
    use serde::Deserialize;
    use serde::Deserializer;
    use serde::Serialize;
    use serde::Serializer;

    pub fn deserialize<'de, D>(d: D) -> Result<Uid, D::Error>
    where
        D: Deserializer<'de>,
    {
        u32::deserialize(d).map(Uid::from_raw)
    }

    pub fn serialize<S>(u: &Uid, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        u.as_raw().serialize(s)
    }
}

pub(crate) mod gid {
    use nix::unistd::Gid;
    use serde::Deserialize;
    use serde::Deserializer;
    use serde::Serialize;
    use serde::Serializer;

    pub fn deserialize<'de, D>(d: D) -> Result<Gid, D::Error>
    where
        D: Deserializer<'de>,
    {
        u32::deserialize(d).map(Gid::from_raw)
    }

    pub fn serialize<S>(g: &Gid, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        g.as_raw().serialize(s)
    }
}
