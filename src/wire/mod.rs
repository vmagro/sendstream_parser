use nom::IResult;

use crate::Sendstream;

static MAGIC_HEADER: &[u8] = b"btrfs-stream\0";

pub(crate) mod cmd;
mod tlv;
use crate::Error;
use crate::Result;

impl<'a> Sendstream<'a> {
    fn parse(input: &'a [u8]) -> IResult<&'a [u8], Self> {
        let (input, _) = nom::bytes::complete::tag(MAGIC_HEADER)(input)?;
        let (input, version) = nom::number::complete::le_u32(input)?;
        assert_eq!(1, version);
        let (input, commands) = nom::multi::many1(crate::Command::parse)(input)?;
        Ok((input, Self { commands }))
    }

    pub fn parse_all(input: &'a [u8]) -> Result<Vec<Self>> {
        let (left, sendstreams) =
            nom::combinator::complete(nom::multi::many1(Sendstream::parse))(input).expect("todo");
        if !left.is_empty() {
            Err(Error::TrailingData(left.to_vec()))
        } else {
            Ok(sendstreams)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn parse_demo() {
        let sendstreams = Sendstream::parse_all(include_bytes!("../../demo.sendstream"))
            .expect("failed to parse gold.sendstream");
        for (i, stream) in sendstreams.iter().enumerate() {
            for command in &stream.commands {
                println!("s{i} {command:?}");
            }
        }
        panic!("making the output readable above");
    }

    #[test]
    fn sendstream_covers_all_commands() {
        let all_cmds: BTreeSet<_> = cmd::CommandType::iter()
            .filter(|c| *c != cmd::CommandType::Unspecified)
            // update_extent is used for no-file-data sendstreams (`btrfs send
            // --no-data`), so it's not super useful to cover here
            .filter(|c| *c != cmd::CommandType::UpdateExtent)
            .collect();
        let sendstreams = Sendstream::parse_all(include_bytes!("../../demo.sendstream"))
            .expect("failed to parse gold.sendstream");
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
