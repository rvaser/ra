#! /usr/bin/env python

import os, re, sys, getopt, subprocess, multiprocessing
from collections import deque

#*******************************************************************************
#*******************************************************************************

def main():

    options = "h"
    long_options = ["help", "install", "data=", "num_threads="]

    install = False
    data_path = None
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
        elif option == "--num_threads":
            num_threads = int(argument)
        elif option in ("-h", "--help"):
            help()
            sys.exit()

    if install == True:
        install_ra()
        sys.exit()

    if data_path is None:
        error("missing option: --data <file>")
    elif not os.path.isfile(data_path):
        error("non-existent file: --data {}".format(data_path))

    if num_threads < 1:
        error("invalid number of threads: --num_threads {}".format(num_threads))

    ra(data_path, num_threads);

#*******************************************************************************
#*******************************************************************************

def install_ra():
    print("Installing Ra")

    install_dir = os.path.realpath(".")
    log = open(install_dir + "ra.log", "w")

    wait(subprocess.Popen(["git", "submodule", "init"], stdout=log, stderr=log))
    wait(subprocess.Popen(["git", "submodule", "update"], stdout=log, stderr=log))

    print("  compiling Minimap ... ", end="", flush=True)

    os.chdir(os.path.join(install_dir, "vendor/minimap"))
    wait(subprocess.Popen("make", stdout=log, stderr=log))

    if not os.path.isfile(os.path.join(os.path.realpath("."), "minimap")):
        error("unable to compile Minimap, check ra.log file.")
    print("complete!")

    print("  compiling Rala ... ", end="", flush=True)

    os.chdir(os.path.join(install_dir, "vendor/rala"))
    wait(subprocess.Popen(["git", "submodule", "init"], stdout=log, stderr=log))
    wait(subprocess.Popen(["git", "submodule", "update"], stdout=log, stderr=log))
    wait(subprocess.Popen("make", stdout=log, stderr=log))

    if not os.path.isfile(os.path.join(os.path.realpath("."), "rala")):
        error("unable to compile Rala, check ra.log file.")
    print("complete!")

    print("  compiling Racon ... ", end="", flush=True)
    os.chdir(os.path.join(install_dir, "vendor/racon"))
    wait(subprocess.Popen(["make", "modules"], stdout=log, stderr=log))
    wait(subprocess.Popen("make", stdout=log, stderr=log))

    if not os.path.isfile(os.path.join(os.path.realpath("."), "bin/racon")):
        error("unable to compile Racon, check ra.log file.")
    print("complete!")

#*******************************************************************************
#*******************************************************************************

def ra(data_path, num_threads):
    print("Running Ra with {}".format(data_path))

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
