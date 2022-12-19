use nom::IResult;

use crate::Sendstream;

static MAGIC_HEADER: &[u8] = b"btrfs-stream\0";

pub(crate) mod cmd;
mod tlv;
pub use cmd::Command;

#[derive(Debug, thiserror::Error)]
enum Error {}

impl<'a> Sendstream<'a> {
    fn parse(input: &'a [u8]) -> IResult<&'a [u8], Self> {
        let (input, _) = nom::bytes::complete::tag(MAGIC_HEADER)(input)?;
        let (input, version) = nom::number::complete::le_u32(input)?;
        assert_eq!(1, version);
        let (input, commands) = nom::multi::many1(crate::Command::parse)(input)?;
        // for cmd in &cmds {
        //     println!("{:?}", cmd);
        // }
        // todo!("left = {:?}", input)
        Ok((input, Self { commands }))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn parse_demo() {
        let (left, sendstreams) =
            nom::multi::many1(Sendstream::parse)(include_bytes!("../../demo.sendstream"))
                .expect("failed to parse gold.sendstream");
        for (i, stream) in sendstreams.iter().enumerate() {
            for command in &stream.commands {
                println!("s{i} {command:?}");
            }
        }
        assert!(left.is_empty(), "there should not be any trailing data");
        panic!("lol");
    }

    #[test]
    fn sendstream_covers_all_commands() {
        let all_cmds: BTreeSet<_> = cmd::CommandType::iter()
            .filter(|c| *c != cmd::CommandType::Unspecified)
            .collect();
        let (left, sendstreams) =
            nom::multi::many1(Sendstream::parse)(include_bytes!("../../demo.sendstream"))
                .expect("failed to parse gold.sendstream");
        assert!(left.is_empty(), "there should not be any trailing data");
        let seen_cmds = sendstreams
            .iter()
            .flat_map(|s| s.commands.iter().map(|c| c.command_type()))
            .collect();
        if all_cmds != seen_cmds {
            let missing: BTreeSet<_> = all_cmds.difference(&seen_cmds).collect();
            panic!("sendstream did not include some commands: {:?}", missing,);
        }
    }
}
