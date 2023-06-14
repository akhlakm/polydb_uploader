import os, sys
import pandas as pd

sys.path.append("polydb")

from polydb.orm import homopolymer
import db

import polylet.logg
log = polylet.logg.New("prep")

def prepare(args):
    datadir = "Kevin_MD_data"

    # Tg
    csv = os.path.join(datadir, "Tg.csv")
    df = pd.read_csv(csv)
    log.done("Read {}, Shape: {}", csv, df.shape)

    # Loop over the rows
    for i in range(df.shape[0]):
        row = df.iloc[i, :]
        log.trace("Row {}, SMILES = {}", i, row.smiles)

        polymer = homopolymer.Homopolymer()
        polymer.smiles = row.smiles

        # Check if polymer exists in DB
        ops = db.Operation(homopolymer.Homopolymer)
        res = ops.get_one(args.session, {'canonical_smiles': polymer.smiles})
        if res is not None:
            log.info("Polymer found in DB.")
            print(res.hp_id)
        else:
            log.warning("Not in database!")

        if i > 10:
            break
