// NOTE: many thanks to @Lehona from the Nom Gitter (https://gitter.im/Geal/nom) for putting
// up with my newbie questions and helping me get through some of the peculiarities of nom
// and parser combinators. I would not have been able to write any of the code in this file
// without their assistance.

use nom::types::CompleteByteSlice;

// MSG <subject> <sid> [reply-to] <#bytes>\r\n[payload]\r\n
#[derive(Debug)]
pub struct MessageHeader {
    pub subject: String,
    pub sid: u64,
    pub reply_to: Option<String>,
    pub message_len: u64,
}

// PUB <subject> [reply-to] <#bytes>\r\n[payload]\r\n
#[derive(Debug)]
pub struct PubHeader {
    pub subject: String,
    pub reply_to: Option<String>,
    pub message_len: u64,
}

// SUB <subject> [queue group] <sid>\r\n
#[derive(Debug)]
pub struct SubHeader {
    pub subject: String,
    pub queue_group: Option<String>,
    pub sid: u64,
}

// UNSUB <sid> [max_msgs]
#[derive(Debug)]
pub struct UnsubHeader {
    pub sid: u64,
    pub max_messages: Option<u64>,
}

// -ERR <error message>
#[derive(Debug)]
pub struct ErrorHeader {
    pub message: String,
}

fn is_digit(chr: u8) -> bool {
    chr == b'1'
        || chr == b'0'
        || chr == b'2'
        || chr == b'3'
        || chr == b'4'
        || chr == b'5'
        || chr == b'6'
        || chr == b'7'
        || chr == b'8'
        || chr == b'9'
}

fn is_not_space(chr: u8) -> bool {
    chr != b' ' && chr != b'\r' && chr != b'\n'
}

fn is_not_tick(chr: u8) -> bool {
    chr != b'\''
}

pub fn split_header_and_payload(source: &[u8]) -> Option<(&[u8], &[u8])> {
    let len = source.len();
    source
        .windows(2)
        .position(|w| w == b"\r\n")
        .map(|idx| (&source[..idx], &source[idx + 2..len - 2]))
}

named!(parse_u64<::nom::types::CompleteByteSlice, u64>,
    flat_map!(take_while1!(is_digit), parse_to!(u64))
);

named!(parse_complete<CompleteByteSlice, &[u8]>, map!(
    take_while1!(is_not_space),
    |r|&r[..]
));

named!(parse_alpha<CompleteByteSlice, &[u8]>, map!(
    take_while1!(is_not_tick),
    |r|&r[..]
));

named!(spec_whitespace, eat_separator!(&b" \t"[..]));

named!(msg_header<::nom::types::CompleteByteSlice, MessageHeader>,
    do_parse!(
        tag_s!("MSG")                           >>
        is_a!(" \t")                            >>
        subject: parse_complete                 >>
        is_a!(" \t")                            >>
        sid:  parse_u64                         >>
        is_a!(" \t")                            >>
        reply_to: opt!(terminated!(parse_complete, is_a!(" \t"))) >>
        message_len: parse_u64                  >>

        ( MessageHeader {
            sid,
            subject: std::str::from_utf8(subject).unwrap().into(),
            reply_to: reply_to.map(|rt| std::str::from_utf8(rt).unwrap().into()),
            message_len
        } )
    )
);
pub fn parse_msg_header(header: &[u8]) -> Option<MessageHeader> {
    msg_header(CompleteByteSlice(header)).ok().map(|h| h.1)
}

named!(pub_header<CompleteByteSlice, PubHeader>,
    do_parse!(
        tag_s!("PUB")                               >>
        is_a!(" \t")                                >>
        subject: parse_complete                  >>
        is_a!(" \t")                                >>
        reply_to: opt!(terminated!(parse_complete, is_a!(" \t"))) >>
        message_len: parse_u64                      >>

        ( PubHeader {
            subject: std::str::from_utf8(subject).unwrap().into(),
            reply_to: reply_to.map(|rt| std::str::from_utf8(rt).unwrap().into()),
            message_len
        } )
    )
);
pub fn parse_pub_header(header: &[u8]) -> Option<PubHeader> {
    pub_header(CompleteByteSlice(header)).ok().map(|h| h.1)
}

named!(sub_header<CompleteByteSlice, SubHeader>,
    do_parse!(
        tag_s!("SUB")                                   >>
        is_a!(" \t")                                    >>
        subject: parse_complete                      >>
        is_a!(" \t")                                    >>
        queue_group: opt!(terminated!(parse_complete, is_a!(b" \t"))) >>
        sid: parse_u64                                  >>

        ( SubHeader {
            subject: std::str::from_utf8(subject).unwrap().into(),
            queue_group: queue_group.map(|qg| std::str::from_utf8(qg).unwrap().into()),
            sid
        } )
    )
);

pub fn parse_sub_header(header: &[u8]) -> Option<SubHeader> {
    sub_header(CompleteByteSlice(header)).ok().map(|h| h.1)
}

named!(unsub_header<CompleteByteSlice, UnsubHeader>,
    do_parse!(
        tag_s!("UNSUB")                 >>
        is_a!(" \t")                    >>
        sid: parse_u64                  >>
        opt!(is_a!(" \t"))              >>
        max_messages: opt!(parse_u64)   >>

        ( UnsubHeader { sid, max_messages })
    )
);
pub fn parse_unsub_header(header: &[u8]) -> Option<UnsubHeader> {
    unsub_header(CompleteByteSlice(header)).ok().map(|h| h.1)
}

named!(err_header<CompleteByteSlice, ErrorHeader>,
    do_parse!(
        tag_s!("-ERR '") >>
        message: parse_alpha >>
        char!('\'') >>

        ( ErrorHeader { message: std::str::from_utf8(message).unwrap().into() } )
    )
);
pub fn parse_err_header(header: &[u8]) -> Option<ErrorHeader> {
    err_header(CompleteByteSlice(header)).ok().map(|h| h.1)
}

#[cfg(test)]
mod test {
    use super::{
        err_header, msg_header, pub_header, split_header_and_payload, sub_header, unsub_header,
    };
    use nom::types::CompleteByteSlice;

    #[test]
    fn msg_reply_to() {
        let raw = b"MSG workdispatch 1 reply.topic 11\r\nHello World\r\n";
        let split = split_header_and_payload(raw);
        assert!(split.is_some());
        if let Some(split) = split {
            let hdr = split.0;
            let payload = split.1;

            assert_eq!(std::str::from_utf8(payload).unwrap(), "Hello World");
            let res = msg_header(CompleteByteSlice(hdr));
            println!("{:?}", res);
            assert!(res.is_ok());
        }
    }

    #[test]
    fn msg_irreg_whitespace() {
        let raw = b"MSG\tworkdispatch 1 reply.topic 11\r\nHello World\r\n";
        let split = split_header_and_payload(raw);
        assert!(split.is_some());
        if let Some(split) = split {
            let hdr = split.0;
            let payload = split.1;

            assert_eq!(std::str::from_utf8(payload).unwrap(), "Hello World");
            let res = msg_header(CompleteByteSlice(hdr));
            assert!(res.is_ok());
        }
    }

    #[test]
    fn unsub_no_max() {
        let msg = b"UNSUB 1";
        let res = unsub_header(CompleteByteSlice(msg));
        assert!(res.is_ok());
        if let Ok(header) = res {
            assert_eq!(header.1.sid, 1);
            assert_eq!(header.1.max_messages, None);
        }
    }

    #[test]
    fn unsub_max() {
        let msg = b"UNSUB 1 5";
        let res = unsub_header(CompleteByteSlice(msg));
        assert!(res.is_ok());
        if let Ok(header) = res {
            assert_eq!(header.1.sid, 1);
            assert_eq!(header.1.max_messages, Some(5));
        }
    }

    #[test]
    fn pub_no_reply() {
        let msg = b"PUB FOO 11";
        let res = pub_header(CompleteByteSlice(msg));
        assert!(res.is_ok());
        if let Ok(header) = res {
            assert_eq!(header.1.subject, "FOO");
            assert!(header.1.reply_to.is_none());
        }
    }

    #[test]
    fn pub_reply() {
        let msg = b"PUB FRONT.DOOR INBOX.22 11";
        let res = pub_header(CompleteByteSlice(msg));
        assert!(res.is_ok());
        if let Ok(header) = res {
            assert_eq!(header.1.subject, "FRONT.DOOR");
            assert_eq!(header.1.reply_to, Some("INBOX.22".to_string()));
        }
    }

    #[test]
    fn sub_no_qg() {
        let msg = b"SUB FOO 1";
        let res = sub_header(CompleteByteSlice(msg));
        assert!(res.is_ok());
        if let Ok(header) = res {
            assert_eq!(header.1.subject, "FOO");
            assert_eq!(header.1.sid, 1);
            assert_eq!(header.1.queue_group, None);
        }
    }

    #[test]
    fn sub_qg() {
        let msg = b"SUB BAR G1 44";
        let res = sub_header(CompleteByteSlice(msg));
        assert!(res.is_ok());
        if let Ok(header) = res {
            assert_eq!(header.1.subject, "BAR");
            assert_eq!(header.1.sid, 44);
            assert_eq!(header.1.queue_group, Some("G1".to_string()));
        }
    }

    #[test]
    fn msg_no_reply() {
        let msg = b"MSG workdispatch 1 11";
        let res = msg_header(CompleteByteSlice(msg));
        println!("{:?}", res);
        assert!(res.is_ok());
    }

    #[test]
    fn error_header() {
        let msg = b"-ERR 'Attempted To Connect To Route Port'";
        let res = err_header(CompleteByteSlice(msg));
        println!("{:?}", res);
        assert!(res.is_ok());
        if let Ok(header) = res {
            assert_eq!(header.1.message, "Attempted To Connect To Route Port");
        }
    }
}
