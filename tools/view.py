"""
view
samtools view [options] in.sam|in.bam|in.cram [region...]

With no options or regions specified, prints all alignments in the specified input alignment file (in SAM, BAM, or CRAM format) to standard output in SAM format (with no header).
You may specify one or more space-separated region specifications after the input filename to restrict output to only those alignments which overlap the specified region(s). Use of region specifications requires a coordinate-sorted and indexed input file (in BAM or CRAM format).
The -b, -C, -1, -u, -h, -H, and -c options change the output format from the default of headerless SAM, and the -o and -U options set the output file name(s).
The -t and -T options provide additional reference data. One of these two options is required when SAM input does not contain @SQ headers, and the -T option is required whenever writing CRAM output.
The -L, -M, -r, -R, -s, -q, -l, -m, -f, -F, and -G options filter the alignments that will be included in the output to only those alignments that match certain criteria.
The -x and -B options modify the data which is contained in each alignment.
Finally, the -@ option can be used to allocate additional threads to be used for compression, and the -? option requests a long help message.

REGIONS:
Regions can be specified as: RNAME[:STARTPOS[-ENDPOS]] and all position coordinates are 1-based.
Important note: when multiple regions are given, some alignments may be output multiple times if they overlap more than one of the specified regions.

Examples of region specifications:

chr1
Output all alignments mapped to the reference sequence named `chr1' (i.e. @SQ SN:chr1).

chr2:1000000
The region on chr2 beginning at base position 1,000,000 and ending at the end of the chromosome.

chr3:1000-2000
The 1001bp region on chr3 beginning at base position 1,000 and ending at base position 2,000 (including both end positions).

'*'
Output the unmapped reads at the end of the file. (This does not include any unmapped reads placed on a reference sequence alongside their mapped mates.)

.
Output all alignments. (Mostly unnecessary as not specifying a region at all has the same effect.)

OPTIONS:

-b Output in the BAM format.
-C Output in the CRAM format (requires -T).
-1 Enable fast BAM compression (implies -b).
-u Output uncompressed BAM. This option saves time spent on compression/decompression and is thus preferred when the output is piped to another samtools command.
-h Include the header in the output.
-H Output the header only.
-c Instead of printing the alignments, only count them and print the total number. All filter options, such as -f, -F, and -q, are taken into account.
-? Output long help and exit immediately.
-o FILE Output to FILE [stdout].
-U FILE Write alignments that are not selected by the various filter options to FILE. When this option is used, all alignments (or all alignments intersecting the regions specified) are written to either the output file or this file, but never both.
-t FILE A tab-delimited FILE. Each line must contain the reference name in the first column and the length of the reference in the second column, with one line for each distinct reference. Any additional fields beyond the second column are ignored. This file also defines the order of the reference sequences in sorting. If you run: `samtools faidx <ref.fa>', the resulting index file <ref.fa>.fai can be used as this FILE.
-T FILE A FASTA format reference FILE, optionally compressed by bgzip and ideally indexed by samtools faidx. If an index is not present, one will be generated for you.
-L FILE Only output alignments overlapping the input BED FILE [null].
-M Use the multi-region iterator on the union of the BED file and command-line region arguments. This avoids re-reading the same regions of files so can sometimes be much faster. Note this also removes duplicate sequences. Without this a sequence that overlaps multiple regions specified on the command line will be reported multiple times.
-r STR Only output alignments in read group STR [null].
-R FILE Output alignments in read groups listed in FILE [null].
-q INT Skip alignments with MAPQ smaller than INT [0].
-l STR Only output alignments in library STR [null].
-m INT Only output alignments with number of CIGAR bases consuming query sequence ≥ INT [0]
-f INT Only output alignments with all bits set in INT present in the FLAG field. INT can be specified in hex by beginning with `0x' (i.e. /^0x[0-9A-F]+/) or in octal by beginning with `0' (i.e. /^0[0-7]+/) [0].
-F INT Do not output alignments with any bits set in INT present in the FLAG field. INT can be specified in hex by beginning with `0x' (i.e. /^0x[0-9A-F]+/) or in octal by beginning with `0' (i.e. /^0[0-7]+/) [0].
-G INT Do not output alignments with all bits set in INT present in the FLAG field. This is the opposite of -f such that -f12 -G12 is the same as no filtering at all. INT can be specified in hex by beginning with `0x' (i.e. /^0x[0-9A-F]+/) or in octal by beginning with `0' (i.e. /^0[0-7]+/) [0].
-x STR Read tag to exclude from output (repeatable) [null]
-B Collapse the backward CIGAR operation.
-s FLOAT Output only a proportion of the input alignments. This subsampling acts in the same way on all of the alignment records in the same template or read pair, so it never keeps a read but not its mate.
    The integer and fractional parts of the -s INT.FRAC option are used separately: the part after the decimal point sets the fraction of templates/pairs to be kept, while the integer part is used as a seed that influences which subset of reads is kept.
    When subsampling data that has previously been subsampled, be sure to use a different seed value from those used previously; otherwise more reads will be retained than expected.
-@ INT Number of BAM compression threads to use in addition to main thread [0].
-S Ignored for compatibility with previous samtools versions. Previously this option was required if input was in SAM format, but now the correct format is automatically detected by examining the first few characters of input.
"""

import getopt, sys
from bampy.util import open_buffer
import bampy.mt as bampy

if __name__ == '__main__':
    opts, args = getopt.gnu_getopt(sys.argv, 'bC1uhHc?o:U:t:T:LM:r:R:q:l:m:f:F:G:x:Bs:@:S')

    assert len(args), "No input file specified"

    try:
        input = open_buffer(args[0])
    except FileNotFoundError:
        input = open(args[0])

    reader = bampy
