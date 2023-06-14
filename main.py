import argparse
import dotenv

import polylet.logg as log
from pyenv_enc import enc

import db
import prepare as prep

def parse_arguments():
    parser = argparse.ArgumentParser(prog='polylet', description="PolyDB uploader")

    parser.add_argument("command")

    parser.add_argument("--loglevel",
                        default=log.INFO,
                        type=int,
                        help="1-8, higher is more verbose (default 6).")

    parser.add_argument("--debug",
                        action="store_true",
                        default=False,
                        help="Enable debugging.")

    args = parser.parse_args()

    args.db = False
    for k in vars(args):
        if k.startswith('db_') and args.__dict__[k] == True:
            args.db = True
            break

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
        log.error("Error - Could not load ENV - DB access disabled.")
        args.db = False
    # else:
    #     if enc.encrypted(os.environ["DB_HOST"]):
    #         log.fatal("Error - Could not load ENV - Encrypted.")
    #         exit(1)

    if not args.db:
        log.note("No DB processing requested.")

    if args.command == "prepare":
        args.session = db.connect()
        prep.prepare(args)

    elif args.command == "upload":
        print("Non implemented!")

    else:
        log.error("Unknown command: {}", args.command)
        log.note("Please specify one: {}", ['prepare', 'upload'])

    db.disconnect()
    log.close()

if __name__ == "__main__":
    main()
