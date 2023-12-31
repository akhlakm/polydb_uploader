import os
import argparse
import dotenv

import pylogg as log
from pyenv_enc import enc

import db
import prepare as prep
import test_idempotence as idem
import namelist

def parse_arguments():
    parser = argparse.ArgumentParser(prog='polylet', description="PolyDB uploader")

    parser.add_argument("command")

    parser.add_argument("--loglevel",
                        default=log.Level.INFO,
                        type=int,
                        help="1-8, higher is more verbose (default 6).")

    parser.add_argument("--debug",
                        action="store_true",
                        default=False,
                        help="Enable debugging.")

    args = parser.parse_args()

    if args.debug: 
        args.loglevel = log.DEBUG
        args.max = 100

    return args


def main():
    args = parse_arguments()
    env = dotenv.load_dotenv()

    # Setup logging
    log.setFile(open("polydb_upload.log", "a+"))
    log.setConsoleTimes(show=True)
    log.setFileTimes(show=True)
    log.setLevel(args.loglevel)

    if not env:
        log.error("Error - Could not load ENV.")

    if args.command == "prepare":
        args.session = db.connect()
        prep.prepare(args)

    if args.command == "check":
        idem.check(args)

    elif args.command == "upload":
        print("Non implemented!")

    elif args.command == "namelist":
        args.session = db.connect()
        namelist.run(args)

    else:
        log.error("Unknown command: {}", args.command)
        log.note("Please specify one: {}", ['prepare', 'upload'])

    db.disconnect()
    log.close()

if __name__ == "__main__":
    main()
