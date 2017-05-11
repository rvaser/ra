#! /usr/bin/env python

import os, re, sys, time, getopt, subprocess, multiprocessing
from collections import deque

#*******************************************************************************
#*******************************************************************************

def main():

    options = "h"
    long_options = ["help", "install", "data=", "out=", "num_threads="]

    install = False
    data_path = None
    out_path = None
    num_threads = multiprocessing.cpu_count()

    try:
        opts, args = getopt.getopt(sys.argv[1:], options, long_options)
    except getopt.GetoptError as err:
        print(str(err))
        help()
        sys.exit()

    for option, argument in opts:
        if option == "--install":
            install = True
        elif option == "--data":
            data_path = argument
        elif option == "--out":
            out_path = argument
        elif option == "--num_threads":
            num_threads = int(argument)
        elif option in ("-h", "--help"):
            help()
            sys.exit()

    if install == True:
        ra_install()
        sys.exit()
    else:
        ra_check_install()

    if data_path is None:
        error("missing option: --data <file>")
    elif not os.path.isfile(data_path):
        error("non-existent file: --data {}".format(data_path))

    if out_path is None:
        out_path = os.path.splitext(os.path.basename(data_path))[0] + "_consensus.fasta"

    if num_threads < 1:
        error("invalid number of threads: --num_threads {}".format(num_threads))

    ra(data_path, out_path, num_threads);

#*******************************************************************************
#*******************************************************************************

install_dir = os.path.dirname(os.path.realpath(__file__))

def ra_install():
    print("Installing Ra {")

    log = open(os.path.join(install_dir, "install.log"), "w")

    wait(subprocess.Popen(["git", "submodule", "init"], stdout=log, stderr=log))
    wait(subprocess.Popen(["git", "submodule", "update"], stdout=log, stderr=log))

    print("    compiling Minimap ... ", end="", flush=True)

    os.chdir(os.path.join(install_dir, "vendor/minimap"))
    wait(subprocess.Popen("make", stdout=log, stderr=log))

    if not os.path.isfile(os.path.join(os.path.realpath("."), "minimap")):
        error("unable to compile Minimap, check install.log file.")
    print("complete!")

    print("    compiling Rala ... ", end="", flush=True)

    os.chdir(os.path.join(install_dir, "vendor/rala"))
    wait(subprocess.Popen(["git", "submodule", "init"], stdout=log, stderr=log))
    wait(subprocess.Popen(["git", "submodule", "update"], stdout=log, stderr=log))
    wait(subprocess.Popen("make", stdout=log, stderr=log))

    if not os.path.isfile(os.path.join(os.path.realpath("."), "rala")):
        error("unable to compile Rala, check install.log file.")
    print("complete!")

    print("    compiling Racon ... ", end="", flush=True)
    os.chdir(os.path.join(install_dir, "vendor/racon"))
    wait(subprocess.Popen(["make", "modules"], stdout=log, stderr=log))
    wait(subprocess.Popen("make", stdout=log, stderr=log))

    if not os.path.isfile(os.path.join(os.path.realpath("."), "bin/racon")):
        error("unable to compile Racon, check install.log file.")
    print("complete!")
    print("}")

def ra_check_install():
    if not os.path.isfile(os.path.join(install_dir, "vendor/minimap/minimap")):
        error("run install before runnin Ra!")
    if not os.path.isfile(os.path.join(install_dir, "vendor/rala/rala")):
        error("run install before runnin Ra!")
    if not os.path.isfile(os.path.join(install_dir, "vendor/racon/bin/racon")):
        error("run install before runnin Ra!")

#*******************************************************************************
#*******************************************************************************

def ra(data_path, out_path, num_threads):
    print("Running Ra on %d threads {" % num_threads)

    log = open(os.path.join(install_dir, "run.log"), "w")

    print("    <- input: {}".format(data_path))
    print("    preprocessing {")
    start = time.time()
    working_directory = os.path.join(install_dir, "working_directory")
    wait(subprocess.Popen(["mkdir", "-p", working_directory], stdout=log, stderr=log))

    base = os.path.join(working_directory, os.path.splitext(os.path.basename(data_path))[0])
    formatted_data_path = base + "_formatted.fastq"
    with open(formatted_data_path, "w") as f:
        wait(subprocess.Popen(["python", os.path.join(install_dir, "vendor/rala/misc/fastq_formatter.py"),
            data_path], stdout=f, stderr=log))

    end = time.time()
    print("        elapsed time = %d s" % (end - start))
    print("    }")

    print("    overlap stage {")
    start = time.time()
    overlaps_path = base + "_overlaps.paf"
    with open(overlaps_path, "w") as f:
        wait(subprocess.Popen([os.path.join(install_dir, "vendor/minimap/minimap"),
            "-t%d" % num_threads, "-Sw5", "-m0", "-L100", formatted_data_path,
            formatted_data_path], stdout=f, stderr=log))

    end = time.time()
    print("        elapsed time = %d s" % (end - start))
    print("    }")

    print("    layout stage {")
    start = time.time()
    layout_path = base + "_layout.fasta"
    with open(layout_path, "w") as f:
        wait(subprocess.Popen([os.path.join(install_dir, "vendor/rala/rala"),
            "0", formatted_data_path, overlaps_path, "1"], stdout=f, stderr=log))

    end = time.time()
    print("        elapsed time = %d s" % (end - start))
    print("    }")

    print("    consensus stage {")
    start = time.time()
    mappings_iter0_path = base + "_mappings_iter0.paf"
    with open(mappings_iter0_path, "w") as f:
        wait(subprocess.Popen([os.path.join(install_dir, "vendor/minimap/minimap"),
            "-t%d" % num_threads, layout_path, formatted_data_path], stdout=f, stderr=log))

    consensus_iter1_path = base + "_consensus_iter0.fasta"
    wait(subprocess.Popen([os.path.join(install_dir, "vendor/racon/bin/racon"),
        "-t", str(num_threads), formatted_data_path, mappings_iter0_path,
        layout_path, consensus_iter1_path], stderr=log))

    mappings_iter1_path = base + "_mappings_iter1.paf"
    with open(mappings_iter1_path, "w") as f:
        wait(subprocess.Popen([os.path.join(install_dir, "vendor/minimap/minimap"),
            "-t%d" % num_threads, consensus_iter1_path, formatted_data_path], stdout=f, stderr=log))

    wait(subprocess.Popen([os.path.join(install_dir, "vendor/racon/bin/racon"),
        "-t", str(num_threads), formatted_data_path, mappings_iter1_path,
        consensus_iter1_path, out_path], stderr=log))

    end = time.time()
    print("        elapsed time = %d s" % (end - start))
    print("    }")

    print("    -> output: {}".format(out_path))
    print("}")

    wait(subprocess.Popen(["rm", "-rf", working_directory], stdout=log, stderr=log))

#*******************************************************************************
#*******************************************************************************

def wait(process):
    if process.poll() is None:
        process.wait()

def error(message):
    print("[ERROR] {}".format(message))
    sys.exit()

def help():
    print(
    "usage: python ra.py [arguments ...]\n"
    "arguments:\n"
    "    --install\n"
    "        downloads and compiles all prerequisites\n"
    "    --data <file>\n"
    "        (required)\n"
    "        input FASTQ file containing raw reads from 3rd generation sequencing\n"
    "    --num_threads <int>\n"
    "        default: number of CPU cores\n"
    "        number of threads with which the modules are run\n"
    "    -h, --help\n"
    "    prints out the help")

#*******************************************************************************
#*******************************************************************************

main()
