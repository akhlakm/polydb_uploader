import os, sys
import pandas as pd

sys.path.append("polydb")

from polydb.orm import homopolymer, property
import pgfingerprinting.fp as pgfp

import db

import polylet.logg
log = polylet.logg.New("prep")

def get_pg_fingerprint(smiles):
    return pgfp.fingerprint_from_smiles(smiles)

def get_smiles(can_smiles):
    return can_smiles

def save_list(items, fname):
    with open(fname, 'w+') as fp:
        for line in items:
            line = line.strip()
            if len(line) ==  0:
                continue
            fp.write(f"{line}\n")
    log.done("Saved: {}", fname)

def prepareTg(conn, csv, polylist : db.Frame):
    df = pd.read_csv(csv)
    log.done("Read {}, Shape: {}", csv, df.shape)

    newpolyprop = db.Frame()
    oldpolyprop = db.Frame()

    # Get the property id from database
    ops = db.Operation(property.Property())
    propId = ops.get_one(conn, {'short_name': 'Tg'}).prop_id
    log.note("Tg property ID: {}", propId)

    # Loop over the rows
    for i in range(df.shape[0]):
        row = df.iloc[i, :]
        log.trace("Row {}, SMILES = {}", i, row.smiles)

        # Check if polymer exists in DB
        ops = db.Operation(homopolymer.Homopolymer())
        res = ops.get_one(conn, {'canonical_smiles': row.smiles})

        if res is not None:
            log.info("Polymer found in DB.")
            oldpolyprop.add(hp_id = res.hp_id,
                        prop_id = propId,
                        value = row['Value'],
                        calculation_method = "md",
                        conditions = str({}),
                        note = "Source: pmd database by Kevin"
            )

        else:
            if not polylist.contains('canonical_smiles', row.smiles):
                log.info("New homopolymer: {}", row.smiles)
                polylist.add(
                    pid = None,
                    rid = None,
                    smiles = get_smiles(row.smiles),
                    canonical_smiles = row.smiles,
                    pg_fingerprint = str(get_pg_fingerprint(row.smiles)),
                    category = "known"
                )

            newpolyprop.add(prop_id = propId,
                        value = row['Value'],
                        calculation_method = "md",
                        conditions = str({}),
                        note = "Source: pmd database by Kevin"
            )

        if i > 10:
            break

    log.done("Processed Tg dataset: {}", csv)
    return polylist, newpolyprop, oldpolyprop


def prepare(args):
    datadir = "Kevin_MD_data"

    newpolymers = db.Frame() # list of new polymers

    csv = os.path.join(datadir, "Tg.csv")
    newpolymers, newpolyTg, oldpolyTg = prepareTg(args.session, csv, newpolymers)

    # Save
    oldpolyTg.df.to_json("tg_existing_polymers.jsonl", orient='records', lines=True)
    newpolyTg.df.to_json("tg_new_polymers.jsonl", orient='records', lines=True)

    # Save the new polymers lists
    newpolymers.df.to_json("new_polymer_list.jsonl", orient='records', lines=True)
