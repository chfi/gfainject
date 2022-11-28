
A little tool to map alignments (from a BAM file) to reference paths in a GFA format graph.
Output is a GAF file with one record per alignment, mapping each alignment to a sequence
of steps in the GFA.
The alignment reference names have to match the path names in the GFA.

Usage:

```sh
cargo build --release
./target/release/gfa-injection --gfa some.gfa --bam some.bam
```