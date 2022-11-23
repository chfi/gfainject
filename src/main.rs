use anyhow::Result;
use roaring::RoaringBitmap;
use std::collections::{BTreeMap, BTreeSet};
use std::io::prelude::*;
use std::{io::BufReader, path::PathBuf};

#[derive(Debug)]
struct Args {
    gfa: PathBuf,
    alignments: Option<PathBuf>,

    path_range: Option<(String, usize, usize)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct PathStep {
    node: u32,
    reverse: bool,
}

struct PathIndex {
    segment_id_range: (usize, usize),
    segment_lens: Vec<usize>,

    path_names: BTreeMap<String, usize>,
    // path_names: Vec<String>,
    path_steps: Vec<Vec<PathStep>>,

    // path_step_offsets: Vec<Vec<usize>>,
    path_step_offsets: Vec<roaring::RoaringBitmap>,
}

struct PathStepRangeIter<'a> {
    path_id: usize,
    pos_range: std::ops::Range<u32>,
    // start_pos: usize,
    // end_pos: usize,
    steps: Box<dyn Iterator<Item = (usize, &'a PathStep)> + 'a>,
    // first_step_start_pos: u32,
    // last_step_end_pos: u32,
}

impl<'a> Iterator for PathStepRangeIter<'a> {
    type Item = (usize, &'a PathStep);

    fn next(&mut self) -> Option<Self::Item> {
        self.steps.next()
    }
}

impl PathIndex {
    fn path_step_range_iter<'a>(
        &'a self,
        path_name: &str,
        pos_range: std::ops::Range<u32>,
    ) -> Option<PathStepRangeIter<'a>> {
        let path_id = *self.path_names.get(path_name)?;
        let offsets = self.path_step_offsets.get(path_id)?;

        let start = pos_range.start;
        let end = pos_range.end;
        let start_ix = offsets.rank(start);
        let step_count = offsets.range_cardinality(start..=end);
        let span = pos_range.end - pos_range.start;
        println!("start: {start}\tend: {end}\tspan: {span}\tstep count: {step_count}");

        // let first_step_start_pos =
        //     offsets.select(start_ix as u32).unwrap_or(offsets.min().unwrap() as u32);
        // let last_step_end_pos = offsets
        //     .select((start_ix + step_count + 1) as u32)
        //     .unwrap_or(offsets.max().unwrap() as u32 - 1);

        // print!("wow! {}", last_step_end_pos - first_step_start_pos);
        // println!("\tbut {}", end - start);

        let steps = {
            let path_steps = self.path_steps.get(path_id)?;
            let iter = path_steps
                .iter()
                .skip(start_ix as usize)
                .take(1 + step_count as usize)
                .enumerate()
                .map(move |(ix, step)| (start_ix as usize + ix, step))
                .fuse();

            Box::new(iter) as Box<dyn Iterator<Item = _>>
        };

        Some(PathStepRangeIter {
            path_id,
            pos_range,
            steps,
            // first_step_start_pos,
            // last_step_end_pos,
        })
    }

    fn from_gfa(gfa_path: impl AsRef<std::path::Path>) -> Result<Self> {
        let gfa = std::fs::File::open(&gfa_path)?;
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

        let gfa = std::fs::File::open(&gfa_path)?;
        let mut gfa_reader = BufReader::new(gfa);

        let mut path_names = BTreeMap::default();

        let mut path_steps: Vec<Vec<PathStep>> = Vec::new();
        let mut path_step_offsets: Vec<RoaringBitmap> = Vec::new();
        // let mut path_pos: Vec<Vec<usize>> = Vec::new();

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
            path_names.insert(name.to_string(), path_steps.len());

            let mut pos = 0;
            let mut step_str_pos = 0;

            let mut parsed_steps = Vec::new();

            let mut offsets = RoaringBitmap::new();

            loop {
                let Some(p) = memchr::memchr(b',', &steps[step_str_pos..])
                else {
                    break;
                };

                let start = step_str_pos;
                let end = step_str_pos + p;

                // let string = std::str::from_utf8(&steps[start..end]).unwrap();
                let seg = &steps[start..end];
                let is_rev = seg.last().copied() == Some(b'-');

                let seg = &seg[..seg.len() - 1];
                let seg_ix = btoi::btou::<usize>(seg)? - seg_id_range.0;
                let len = seg_lens[seg_ix];

                let step = PathStep {
                    node: seg_ix as u32,
                    reverse: is_rev,
                };
                parsed_steps.push(step);
                // parsed_steps.push(seg_ix as u32);
                offsets.push(pos as u32);

                pos += len;
                step_str_pos = end + 1;
            }

            path_steps.push(parsed_steps);
            path_step_offsets.push(offsets);
        }

        Ok(Self {
            path_names,
            path_steps,
            path_step_offsets,

            segment_id_range: seg_id_range,
            segment_lens: seg_lens,
        })
    }
}

fn main() -> Result<()> {
    let Ok(args) = parse_args() else {
        println!("USAGE: `gfa-injection --gfa <gfa-path> --bam <bam-path>`");
        return Ok(());
    };

    let path_index = PathIndex::from_gfa(&args.gfa)?;

    if let Some(bam_path) = args.alignments {
        return main_cmd(path_index, bam_path);
    } else if let Some((path, start, end)) = args.path_range {
        return path_range_cmd(path_index, path, start, end);
    }

    Ok(())
}

fn path_range_cmd(
    path_index: PathIndex,
    path_name: String,
    start: usize,
    end: usize,
) -> Result<()> {
    let path = path_index
        .path_names
        .get(&path_name)
        .expect("Path not found");


    let offsets = path_index.path_step_offsets.get(*path).unwrap();

    let start_rank = offsets.rank(start as u32);
    let end_rank = offsets.rank(end as u32);

    let cardinality = offsets.range_cardinality((start as u32)..(end as u32));

    println!("start_rank: {start_rank}");
    println!("end_rank: {end_rank}");
    println!("cardinality: {cardinality}");

    println!("------");
    let skip = (start_rank as usize).checked_sub(1).unwrap_or_default();
    let take = end_rank as usize - skip;
    for step in offsets.iter().skip(skip).take(take) {
        println!("{step}");
    }


    Ok(())
}

fn main_cmd(path_index: PathIndex, bam_path: PathBuf) -> Result<()> {
    use noodles::bam;

    let Ok(args) = parse_args() else {
        println!("USAGE: `gfa-injection --gfa <gfa-path> --bam <bam-path>`");
        return Ok(());
    };

    let mut bam = std::fs::File::open(&bam_path).map(bam::Reader::new)?;

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

    let ref_seqs = bam.read_reference_sequences()?;

    let m150 = {
        use cigar::{op::Kind, Op};
        use noodles::sam::record::{cigar, Cigar};
        Cigar::try_from(vec![Op::new(Kind::Match, 150)])?
    };

    for rec in bam.records() {
        let record = rec?;

        let Some(read_name) = record.read_name() else {
            continue;
        };

        // let name = read_name.to_string();
        // dbg!(&name);
        // let name = std::str::from_utf8(read_name.to_)?;

        let Some(ref_name) = record.reference_sequence(&header).and_then(|s| s.ok().map(|s| s.name())) else {
            continue;
        };

        let Some(path_id) = path_index.path_names.get(ref_name.as_str()).copied() else {
            continue;
        };

        // 1-based
        let start = record.alignment_start().unwrap();
        let end = record.alignment_end().unwrap();
        let al_len = record.alignment_span();
        // let al_len = end.get() - start.get();

        let pos_range = (start.get() as u32)..(1 + end.get() as u32);
        if let Some(steps) =
            path_index.path_step_range_iter(ref_name.as_str(), pos_range)
        {
            let mut path_len = 0;
            let mut path_str = String::new();

            // let path_start = steps.first_step_start_pos;
            // let path_end = steps.last_step_end_pos;
            // println!("path_start: {path_start}\tpath_end: {path_end}")  ;

            let steps = steps.collect::<Vec<_>>();
            let step_count = steps.len();
            dbg!(step_count);

            for &(step_ix, step) in &steps {
                use std::fmt::Write;
                if step.reverse {
                    write!(&mut path_str, ">")?;
                } else {
                    write!(&mut path_str, "<")?;
                }
                path_len += path_index.segment_lens[step.node as usize];
                write!(
                    &mut path_str,
                    "{}",
                    step.node + path_index.segment_id_range.0 as u32
                )?;
            }

            // let path_len_field = al_len;

            // query name
            print!("{}\t", read_name);
            // query len
            print!("{}\t", al_len);
            // query start (0-based, closed)
            print!("0\t");
            // print!("{}\t", start.get());
            // query end (0-based, open)
            print!("{}\t", al_len);
            // print!("{}\t", end.get() + 1);
            // strand
            if record.flags().is_reverse_complemented() {
                print!("-\t");
            } else {
                print!("+\t");
            }
            // path
            print!("{path_str}\t");
            // path length
            print!("{path_len}\t");
            // start on path
            let start = 0usize;
            print!("{start}\t");
            // end on path
            print!("{}\t", start + al_len);
            // number of matches
            {
                use noodles::sam::record::cigar::{op::Kind, Op};

                fn match_len(op: &Op) -> usize {
                    match op.kind() {
                        Kind::Match
                        | Kind::SequenceMatch
                        | Kind::SequenceMismatch => op.len(),
                        _ => 0,
                    }
                }
                let matches =
                    record.cigar().iter().map(match_len).sum::<usize>();
                print!("{matches}\t");
            }
            // alignment block length
            // mapping quality
            println!();
        } else {
        }

        // let offset_map = &path_index.path_step_offsets[path_id];
        // let steps = &path_index.path_steps[path_id];

        // find start of alignment
        // let start_ix = offset_map.rank(start.get() as u32 - 1);
        // let end_ix = offset_map.rank(end.get() as u32 + 1);

        // println!("{start}:{end}");
        // println!("{start_ix} - {end_ix}: {} steps", end_ix - start_ix);
        // println!("first step: {:?}", steps[start_ix as usize]);
    }

    Ok(())
}

fn parse_args() -> std::result::Result<Args, pico_args::Error> {
    let mut pargs = pico_args::Arguments::from_env();

    let path_range = pargs.opt_value_from_str("--path").and_then(
        |path: Option<String>| {
            let start: Option<usize> = pargs.opt_value_from_str("--start")?;
            let end: Option<usize> = pargs.opt_value_from_str("--end")?;
            let path_range = path.and_then(|path| {
                let path: String = path.into();
                let start = start?;
                let end = end?;
                Some((path, start, end))
            });
            Ok(path_range)
        },
    )?;

    let args = Args {
        gfa: pargs.value_from_os_str("--gfa", parse_path)?,
        alignments: pargs.opt_value_from_os_str("--bam", parse_path)?,
        path_range,
    };

    Ok(args)
}

fn parse_path(s: &std::ffi::OsStr) -> Result<std::path::PathBuf, &'static str> {
    Ok(s.into())
}
