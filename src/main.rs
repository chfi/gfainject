use std::path::PathBuf;

use anyhow::Result;

#[derive(Debug)]
struct Args {
    gfa: PathBuf,
    alignments: PathBuf,
}

fn main() -> Result<()> {
    let args = parse_args()?;

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
