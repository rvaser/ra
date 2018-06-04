#!/usr/bin/env python

from __future__ import print_function
import os, sys, time, shutil, argparse, subprocess

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

#*******************************************************************************

class Ra:

    __minimap = '@minimap_path@'
    __rala = '@rala_path@'
    __racon = '@racon_path@'

    def __init__(self, tgs_sequences, tgs_type, ngs_sequences, include_unused,
        threads):

        self.tgs_sequences = tgs_sequences
        self.tgs_type = tgs_type
        self.ngs_sequences = ngs_sequences
        self.include_unused = include_unused
        self.threads = threads
        self.work_directory = os.getcwd() + '/ra_work_directory_' + str(time.time())

    def __enter__(self):
        try:
            os.makedirs(self.work_directory)
        except OSError:
            if (not os.path.isdir(self.work_directory)):
                eprint('[Ra::__enter__] error: unable to create work directory!')
                sys.exit(1)

    def __exit__(self, exception_type, exception_value, traceback):
        try:
            shutil.rmtree(self.work_directory)
        except OSError:
            eprint('[Ra::__exit__] warning: unable to clean work directory!')

    def run(self):
        # overlap
        eprint('[Ra::run] overlap stage')

        minimap_params = [Ra.__minimap, '-t', str(self.threads)]
        if (self.tgs_type == 'ont'):
            minimap_params.extend(['-x', 'ava-ont'])
        else:
            minimap_params.extend(['-x', 'ava-pb'])
        minimap_params.extend([self.tgs_sequences, self.tgs_sequences])

        overlaps = os.path.join(self.work_directory, 'overlaps.paf')
        try:
            overlaps_file = open(overlaps, 'w')
        except OSError:
            eprint('[Ra::run] error: unable to create overlap file!')
            sys.exit(1)

        try:
            p = subprocess.Popen(minimap_params, stdout=overlaps_file)
        except OSError:
            eprint('[Ra::run] error: unable to run minimap2!')
            sys.exit(1)
        p.communicate()
        if (p.returncode != 0):
            sys.exit(1)

        overlaps_file.close()

        # layout
        eprint('[Ra::run] layout stage')

        rala_params = [Ra.__rala, '-t', str(self.threads)]
        if (self.include_unused):
            rala_params.extend(['-u'])
        rala_params.extend([self.tgs_sequences, overlaps])

        layout = os.path.join(self.work_directory, 'iter0.fasta')
        try:
            layout_file = open(layout, 'w')
        except OSError:
            eprint('[Ra::run] error: unable to create layout file!')
            sys.exit(1)

        try:
            p = subprocess.Popen(rala_params, stdout=layout_file)
        except OSError:
            eprint('[Ra::run] error: unable to run rala!')
            sys.exit(1)
        p.communicate()
        if (p.returncode != 0):
            sys.exit(1)

        layout_file.close()

        # consensus
        eprint('[Ra::run] consensus stage')

        # first iteration
        minimap_params = [Ra.__minimap, '-t', str(self.threads)]
        if (self.tgs_type == 'ont'):
            minimap_params.extend(['-x', 'map-ont'])
        else:
            minimap_params.extend(['-x', 'map-pb'])
        minimap_params.extend([layout, self.tgs_sequences])

        mappings = os.path.join(self.work_directory, 'mappings_iter0.paf')
        try:
            mappings_file = open(mappings, 'w')
        except OSError:
            eprint('[Ra::run] error: unable to create mappings file!')
            sys.exit(1)

        try:
            p = subprocess.Popen(minimap_params, stdout=mappings_file)
        except OSError:
            eprint('[Ra::run] error: unable to run minimap2!')
            sys.exit(1)
        p.communicate()
        if (p.returncode != 0):
            sys.exit(1)

        mappings_file.close()

        racon_params = [Ra.__racon, '-t', str(self.threads)]
        if (self.include_unused):
            racon_params.extend(['-u'])

        racon_params.extend([self.tgs_sequences, mappings, layout])

        consensus = os.path.join(self.work_directory, 'iter1.fasta')
        try:
            consensus_file = open(consensus, 'w')
        except OSError:
            eprint('[Ra::run] error: unable to create consensus file!')
            sys.exit(1)

        try:
            p = subprocess.Popen(racon_params, stdout=consensus_file)
        except OSError:
            eprint('[Ra::run] error: unable to run racon!')
            sys.exit(1)
        p.communicate()
        if (p.returncode != 0):
            sys.exit(1)

        consensus_file.close()

        # second iteration
        minimap_params[-2] = consensus

        mappings = os.path.join(self.work_directory, 'mappings_iter1.paf')
        try:
            mappings_file = open(mappings, 'w')
        except OSError:
            eprint('[Ra::run] error: unable to create mappings file!')
            sys.exit(1)

        try:
            p = subprocess.Popen(minimap_params, stdout=mappings_file)
        except OSError:
            eprint('[Ra::run] error: unable to run minimap2!')
            sys.exit(1)
        p.communicate()
        if (p.returncode != 0):
            sys.exit(1)

        mappings_file.close()

        racon_params[-2] = mappings
        racon_params[-1] = consensus

        if (self.ngs_sequences is not None):
            consensus = os.path.join(self.work_directory, 'iter2.fasta')
            try:
                consensus_file = open(consensus, 'w')
            except OSError:
                eprint('[Ra::run] error: unable to create consensus file!')
                sys.exit(1)

        try:
            if (self.ngs_sequences is not None):
                p = subprocess.Popen(racon_params, stdout=consensus_file)
            else:
                p = subprocess.Popen(racon_params)
        except OSError:
            eprint('[Ra::run] error: unable to run racon!')
            sys.exit(1)
        p.communicate()
        if (p.returncode != 0):
            sys.exit(1)

        if (self.ngs_sequences is not None):
            consensus_file.close()

        if (self.ngs_sequences is None):
            return

        # polish (third iteration)
        eprint('[Ra::run] polish stage')

        minimap_params[-3] = 'sr'
        minimap_params[-2] = consensus
        minimap_params[-1] = self.ngs_sequences

        mappings = os.path.join(self.work_directory, 'mappings_iter2.sam')
        try:
            mappings_file = open(mappings, 'w')
        except OSError:
            eprint('[Ra::run] error: unable to create mappings file!')
            sys.exit(1)

        try:
            p = subprocess.Popen(minimap_params, stdout=mappings_file)
        except OSError:
            eprint('[Ra::run] error: unable to run minimap2!')
            sys.exit(1)
        p.communicate()
        if (p.returncode != 0):
            sys.exit(1)

        mappings_file.close()

        racon_params[-3] = self.ngs_sequences
        racon_params[-2] = mappings
        racon_params[-1] = consensus

        try:
            p = subprocess.Popen(racon_params)
        except OSError:
            eprint('[Ra::run] error: unable to run racon!')
            sys.exit(1)
        p.communicate()
        if (p.returncode != 0):
            sys.exit(1)

#*******************************************************************************

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description=''' ''',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('sequences', help='''input file in FASTA/FASTQ format
        (can be compressed with gzip) containing third generation sequences for
        assembly''')
    parser.add_argument('ngs_sequences', nargs='?', help='''input file in FASTA/FASTQ
        format (can be compressed with gzip) containing next generation sequences
        for polishing''')
    parser.add_argument('-u', '--include-unused', action='store_true',
        help='''output unassembled and unpolished sequences''')
    parser.add_argument('-t', '--threads', default=1, help='''number of threads''')
    parser.add_argument('--version', action='version', version='v0.2.1')

    required_arguments = parser.add_argument_group('required arguments')
    required_arguments.add_argument('-x', dest='type', choices=['ont', 'pb'],
        help='''sequencing technology of input sequences''', required=True)

    args = parser.parse_args()

    ra = Ra(args.sequences, args.type, args.ngs_sequences, args.include_unused,
        args.threads)

    with ra:
        ra.run()
