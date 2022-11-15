use anyhow::Result;
use std::collections::{BTreeMap, BTreeSet};
use std::io::prelude::*;
use std::{io::BufReader, path::PathBuf};

#[derive(Debug)]
struct Args {
    gfa: PathBuf,
    alignments: PathBuf,
}

struct PathIndex {
    path_names: Vec<String>,
}

fn bam_main() -> Result<()> {
    use noodles::bam;

    let Ok(args) = parse_args() else {
        println!("USAGE: `gfa-injection --gfa <gfa-path> --bam <bam-path>`");
        return Ok(());
    };

    let mut bam =
        std::fs::File::open(&args.alignments).map(bam::Reader::new)?;

    let header = {
        use noodles::sam;
        // the noodles parse() impl demands that the @HD lines go first,
        // but that's clearly not a guarantee i can enforce
        let raw = bam.read_header()?;
        let mut header = sam::Header::builder();

        for line in raw.lines() {
            use noodles::sam::header::Record as HRecord;
            if let Ok(record) = line.parse::<HRecord>() {
                header = match record {
                    HRecord::Header(hd) => header.set_header(hd),
                    HRecord::ReferenceSequence(sq) => {
                        header.add_reference_sequence(sq)
                    }
                    HRecord::ReadGroup(rg) => header.add_read_group(rg),
                    HRecord::Program(pg) => header.add_program(pg),
                    HRecord::Comment(co) => header.add_comment(co),
                };
            }
        }

        header.build()
    };
    bam.read_reference_sequences()?;

    // for result in bam.records() {
    // let record = result?;
    // println!("{:?}", record);
    // }

    println!();

    let mut max_tag_count = 0;
    let mut tag_counts: BTreeMap<[u8; 2], usize> = BTreeMap::new();

    let m150 = {
        use cigar::{op::Kind, Op};
        use noodles::sam::record::{cigar, Cigar};
        Cigar::try_from(vec![Op::new(Kind::Match, 150)])?
    };

    for rec in bam.records() {
        let record = rec?;

        max_tag_count = max_tag_count.max(record.data().len());
        for tag in record.data().keys() {
            *tag_counts.entry(*tag.as_ref()).or_default() += 1;
        }

        use cigar::{op::Kind, Op};
        use noodles::sam::record::{cigar, Cigar};
        let cigar: &Cigar = record.cigar();
        if cigar.as_ref() != m150.as_ref() {
            // if cigar.as_ref() != &[Op::new(Kind::Match, 150)] {
            let string = format!("{cigar}");
            // if string.len() != 4 {
                println!("{string}");
            // }

            //
            // } else {
            // println!("{cigar}");
        }
        // print!("{cigar:?}");
    }
    

    /*
    // prints 5 for a bam version of the same file that returned 1 for the below
    println!("max tag count: {max_tag_count}");

    for (name, sq) in header.reference_sequences() {
        println!("{name} - {}", sq.length());
    }

    println!();

    println!("tag   -   count");
    for (tag, count) in tag_counts {
        let tag = std::str::from_utf8(tag.as_slice())?;
        println!("{tag} - {count}");
    }
    */

    Ok(())
}

fn sam_main() -> Result<()> {
    use noodles::sam;

    let args = parse_args()?;

    dbg!();
    let mut sam = std::fs::File::open(&args.alignments)
        .map(BufReader::new)
        .map(sam::Reader::new)?;

    dbg!();

    let header = {
        // the noodles parse() impl demands that the @HD lines go first,
        // but that's clearly not a guarantee i can enforce
        let raw = sam.read_header()?;
        let mut header = sam::Header::builder();

        for line in raw.lines() {
            use noodles::sam::header::Record as HRecord;
            if let Ok(record) = line.parse::<HRecord>() {
                header = match record {
                    HRecord::Header(hd) => header.set_header(hd),
                    HRecord::ReferenceSequence(sq) => {
                        header.add_reference_sequence(sq)
                    }
                    HRecord::ReadGroup(rg) => header.add_read_group(rg),
                    HRecord::Program(pg) => header.add_program(pg),
                    HRecord::Comment(co) => header.add_comment(co),
                };
            }
        }

        header.build()
    };
    dbg!(&header);

    let mut tag_counts: BTreeMap<[u8; 2], usize> = BTreeMap::new();
    // let mut tags: BTreeMap<_, _> = BTreeMap::new();
    // let mut tags: BTreeSet<_> = BTreeSet::new();

    // NB: it seems like noodles only reads the first tag
    let mut max_tag_count = 0;

    for rec in sam.records(&header) {
        let record = rec?;

        max_tag_count = max_tag_count.max(record.data().len());
        for tag in record.data().keys() {
            *tag_counts.entry(*tag.as_ref()).or_default() += 1;
        }
    }

    println!();

    println!("tag   -   count");
    for (tag, count) in tag_counts {
        let tag = std::str::from_utf8(tag.as_slice())?;
        println!("{tag} - {count}");
    }

    // this prints 1. lol. lmao.
    println!("max tag count: {}", max_tag_count);

    Ok(())
}




fn main() -> Result<()> {
    let args = parse_args()?;

    let gfa = std::fs::File::open(&args.gfa)?;
    let mut gfa_reader = BufReader::new(gfa);

    let mut line_buf = Vec::new();

    let mut name_map = BTreeMap::default();
    let mut seg_lens = Vec::new();

    let mut seg_id_range = (std::usize::MAX, 0usize);
    // dbg!();

    loop {
        line_buf.clear();

        let len = gfa_reader.read_until(0xA, &mut line_buf)?;
        if len == 0 {
            break;
        }

        let line = &line_buf[..len];
        let line_str = std::str::from_utf8(&line)?;
        // println!("{line_str}");

        if !matches!(line.first(), Some(b'S')) {
            continue;
        }

        let mut fields = line.split(|&c| c == b'\t');

        let Some((name, seq)) = fields.next().and_then(|_type| {
            let name = fields.next()?;
            let seq = fields.next()?;
            Some((name, seq ))
        }) else {
            continue;
        };

        let name = std::str::from_utf8(name)?;
        let seg_id = name.parse::<usize>()?;

        seg_id_range.0 = seg_id_range.0.min(seg_id);
        seg_id_range.1 = seg_id_range.1.max(seg_id);

        name_map.insert(seg_id, seq.len());
        seg_lens.push(seq.len());
    }

    assert!(
        seg_id_range.1 - seg_id_range.0 == seg_lens.len() - 1,
        "GFA segments must be tightly packed: min ID {}, max ID {}, node count {}, was {}",
        seg_id_range.0, seg_id_range.1, seg_lens.len(),
        seg_id_range.1 - seg_id_range.0,
    );

    let gfa = std::fs::File::open(&args.gfa)?;
    let mut gfa_reader = BufReader::new(gfa);

    let mut path_names = Vec::new();

    let mut path_steps: Vec<Vec<u32>> = Vec::new();
    let mut path_pos: Vec<Vec<usize>> = Vec::new();

    loop {
        line_buf.clear();

        let len = gfa_reader.read_until(b'\n', &mut line_buf)?;
        if len == 0 {
            break;
        }

        let line = &line_buf[..len];
        if !matches!(line.first(), Some(b'P')) {
            continue;
        }

        let mut fields = line.split(|&c| c == b'\t');

        let Some((name, steps)) = fields.next().and_then(|_type| {
            let name = fields.next()?;
            let steps = fields.next()?;
            Some((name, steps))
        }) else {
            continue;
        };

        let name = std::str::from_utf8(name)?;
        path_names.push(name.to_string());

        let mut pos = 0;
        let mut step_str_pos = 0;

        let mut parsed_steps = Vec::new();
        let mut offsets = Vec::new();

        loop {
            // i bet the steps range bit *might* crash
            let Some(p) = memchr::memchr(b',', &steps[step_str_pos..])
            else {
                break;
            };

            let start = step_str_pos;
            let end = step_str_pos + p;

            let string = std::str::from_utf8(&steps[start..end]).unwrap();
            let seg = &steps[start..end];
            let is_rev = seg.last().copied() == Some(b'-');

            let seg = &seg[..seg.len() - 1];
            let seg_ix = btoi::btou::<usize>(seg)? - seg_id_range.0;
            let len = seg_lens[seg_ix];

            parsed_steps.push(seg_ix as u32);
            offsets.push(pos);

            pos += len;
            step_str_pos = end + 1;
        }

        path_steps.push(parsed_steps);
        path_pos.push(offsets);
    }

    Ok(())
}


fn parse_args() -> std::result::Result<Args, pico_args::Error> {
    let mut pargs = pico_args::Arguments::from_env();

    let args = Args {
        gfa: pargs.value_from_os_str("--gfa", parse_path)?,
        alignments: pargs.value_from_os_str("--bam", parse_path)?,
    };

    Ok(args)
}

fn parse_path(s: &std::ffi::OsStr) -> Result<std::path::PathBuf, &'static str> {
    Ok(s.into())
}
