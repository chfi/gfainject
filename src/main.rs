use anyhow::Result;
use std::collections::BTreeMap;
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

fn main() -> Result<()> {
    let args = parse_args()?;

    let gfa = std::fs::File::open(&args.gfa)?;
    let mut gfa_reader = BufReader::new(gfa);

    let mut line_buf = Vec::new();

    let mut name_map = BTreeMap::default();
    let mut seg_lens = Vec::new();

    let mut seg_id_range = (0usize, std::usize::MAX);

    loop {
        let len = gfa_reader.read_until(b'\n', &mut line_buf)?;
        let line = &line_buf[..len];
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
        seg_id_range.1 - seg_id_range.0 == seg_lens.len(),
        "GFA segments must be tightly packed"
    );

    let mut path_names = Vec::new();
    let mut path_steps: Vec<u32> = Vec::new();
    let mut path_pos: Vec<usize> = Vec::new();

    loop {
        let len = gfa_reader.read_until(b'\n', &mut line_buf)?;
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

        loop {
            // i bet the steps range bit *might* crash
            let Some(p) = memchr::memchr(b',', &steps[step_str_pos..])
            else {
                break;
            };

            let seg = &steps[step_str_pos..p - 1];
            let orient = &steps[p - 1];

            let seg_ix = btoi::btou::<usize>(seg)? - seg_id_range.0;
            let len = seg_lens[seg_ix];

            pos += len;

            step_str_pos = p;
        }
    }

    Ok(())
}

fn parse_args() -> std::result::Result<Args, pico_args::Error> {
    let mut pargs = pico_args::Arguments::from_env();

    let args = Args {
        gfa: pargs.value_from_os_str("--gfa", parse_path)?,
        alignments: pargs.value_from_os_str("--sam", parse_path)?,
    };

    Ok(args)
}

fn parse_path(s: &std::ffi::OsStr) -> Result<std::path::PathBuf, &'static str> {
    Ok(s.into())
}
