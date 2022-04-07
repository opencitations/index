#!/usr/bin/env python

import json
import time
import os
import sys
import importlib

from argparse import ArgumentParser

def main():
    # Loads the config file
    with open(os.path.expanduser("~/.opencitations/index/config.json"), "r") as file:
        config_file = json.load(file)

    runtimes = config_file["runtimes"]

    # This is required in order to choose the proper runtime before the arguments parsing
    if len(sys.argv) <= 1 or not sys.argv[1] in runtimes.keys():
        print(
            "In order to start cnc, a valid runtime must be specified as the first parameter.\n"
            "The -h parameter can then be entered to receive information on the start-up parameters.\n\n"
            "The available options are listed below:"
        )
        print(*runtimes.keys(), sep=", ")
        sys.exit(-1)

    arg_parser = ArgumentParser(
        "cnc.py - Create New Citations",
        description="This tool allows one to take a series of entity-to-entity"
        "citation data, and to store it according to CSV used by"
        "the OpenCitations Indexes so as to be added to an Index. It uses"
        "several online services to check several things to create the"
        "final CSV/TTL/Scholix files.",
    )
    arg_parser.add_argument(
        "runtime", help="The name of the runtime to use", choices=[sys.argv[1]]
    )
    arg_parser.add_argument(
        "-v",
        "--verbose",
        required=False,
        type=bool,
        default=False,
        help="Set it as true if you want to output "
        "all the messages, it can be usefull "
        "for debugging and understanding what "
        "is going on",
    )

    runtime = runtimes[sys.argv[1]].split(":")

    # Import and initialize the runtime chosen
    runtime_module = importlib.import_module(runtime[0])
    runtime_class = getattr(runtime_module, runtime[1])
    runtime = runtime_class()

    # Add the custom args required by the runtime chosen
    runtime.set_args(arg_parser, config_file)

    args = arg_parser.parse_args()

    # Initialize runtime
    runtime.init(args, config_file)

    started = time.time()
    runtime.logger.info("Process started")

    # Start the process
    runtime.run(args, config_file)

    runtime.logger.info("Process ends in " + str(time.time() - started) + "ms")
