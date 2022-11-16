use anyhow::Result;
use roaring::RoaringBitmap;
use std::collections::{BTreeMap, BTreeSet};
use std::io::prelude::*;
use std::{io::BufReader, path::PathBuf};

#[derive(Debug)]
struct Args {
    gfa: PathBuf,
    alignments: PathBuf,
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

    first_step_start_pos: u32,
    last_step_end_pos: u32,
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
        let step_count = offsets.range_cardinality(start..end);

        let first_step_start_pos =
            offsets.select(start_ix as u32).unwrap_or(start);
        let last_step_end_pos = offsets
            .select((start_ix + step_count + 1) as u32)
            .unwrap_or(end);

        // println!("wow! {}", last_step_end_pos - first_step_start_pos);

        let steps = {
            let path_steps = self.path_steps.get(path_id)?;
            let iter = path_steps
                .iter()
                .skip(start_ix as usize)
                .take(step_count as usize)
                .enumerate()
                .map(move |(ix, step)| (start_ix as usize + ix, step))
                .fuse();

            Box::new(iter) as Box<dyn Iterator<Item = _>>
        };

        Some(PathStepRangeIter {
            path_id,
            pos_range,
            steps,
            first_step_start_pos,
            last_step_end_pos,
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

                let string = std::str::from_utf8(&steps[start..end]).unwrap();
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
    use noodles::bam;

    let Ok(args) = parse_args() else {
        println!("USAGE: `gfa-injection --gfa <gfa-path> --bam <bam-path>`");
        return Ok(());
    };

    let path_index = PathIndex::from_gfa(&args.gfa)?;

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

        let pos_range = (start.get() as u32)..(end.get() as u32);
        if let Some(steps) =
            path_index.path_step_range_iter(ref_name.as_str(), pos_range)
        {
            let mut path_len = 0;
            let mut path_str = String::new();

            let path_start = steps.first_step_start_pos;
            let path_end = steps.last_step_end_pos;

            let steps = steps.collect::<Vec<_>>();
            let step_count = steps.len();

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
            // end on path
            // number of matches
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

    let args = Args {
        gfa: pargs.value_from_os_str("--gfa", parse_path)?,
        alignments: pargs.value_from_os_str("--bam", parse_path)?,
    };

    Ok(args)
}

fn parse_path(s: &std::ffi::OsStr) -> Result<std::path::PathBuf, &'static str> {
    Ok(s.into())
}
