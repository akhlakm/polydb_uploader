import os, sys
import json
import pandas as pd

import db
import pylogg
import pgfingerprinting.fp as pgfp
from psmiles import PolymerSmiles

sys.path.append("polydb")
from polydb.orm import homopolymer, property

log = pylogg.New("prep")


def canonical(smiles) -> str:
    """ Convert a smiles into it's cannonical form. """
    ps = PolymerSmiles(smiles)
    return str(ps.canonicalize)


def pg_fingerprint(canon):
    """ Calculate Polymer Genome fingerprint from the cannonical smiles."""
    return pgfp.fingerprint_from_smiles(canon)


def add_new_polymer(polylist, smiles, polymer_category = "known"):
    """ Add a new polymer smiles to the list if it already not added. """
    canon = canonical(smiles)

    # Confirm that the polymer is not already added
    if not polylist.contains('canonical_smiles', canon):
        log.info("New homopolymer: {}", smiles)
        polylist.add(
            pid = None,
            rid = None,
            smiles = smiles,
            canonical_smiles = canon,
            pg_fingerprint = json.dumps(pg_fingerprint(canon)),
            # Manually set this using the release version specified in
            # https://github.com/Ramprasad-Group/pgfingerprinting/releases
            pg_fingerprint_version = "2.0.0",
            category = polymer_category
        )


def save_new_polymers_list(df : pd.DataFrame, outfile):
    assert type(df) == pd.DataFrame, "Polymer list must be a dataframe."

    # Sanity check any duplicates.
    new_polymer_count = df.shape[0]
    unique_smiles_count = len(df.smiles.unique())
    unique_canonical_smiles_count = len(df.canonical_smiles.unique())
    assert new_polymer_count == unique_smiles_count, "Unique smiles and total polymer len mismatch."
    assert new_polymer_count == unique_canonical_smiles_count, "Unique canonical smiles and total polymer len mismatch."

    # Save the new polymers lists
    df.to_json(outfile, orient='records', lines=True)



def prepare_property_csv(conn, csv, polylist : db.Frame, shortname : str, *,
            column_map : dict, conditions_map : dict,
            note = "", debug=False):
    """
    Prepare a dataset for insertion into the database.
    Args:
        conn:       Database session object.
        csv:        Input csv filepath to load with pandas.
        polylist:   New polymer list, that will be populated.
        shortname:  The short_name of the property in the DB, or None.
        column_map: A mapping of the columns names between database and CSV file.
                    Use the format { db column : csv column, ... }
        conditions_map: additional conditions to add to the property row.
                    A map of { key : csv column, ... } where key will be used
                    in the conditions json to store in the database.
        debug :     Enable debug mode, maximum 10 rows will be processed.

    Returns:
        A tuple of (
            list of new polymers,
            list of properties for existing polymers,
            list of properties for new polymers
        )
    """

    assert "smiles" in column_map, "Column map must specify the 'smiles' field in the CSV."

    df = pd.read_csv(csv)
    log.done("Read {}, Shape: {}", csv, df.shape)

    # Make special dict objects to iteratively build the rows of a dataframe.
    newpolyprop = db.Frame()
    oldpolyprop = db.Frame()

    # Get the property id from database by it's shortname.
    # If the property does not exist, we will leave it blank.
    if shortname is not None:
        try:
            ops = db.Operation(property.Property())
            propId = ops.get_one(conn, {'short_name': shortname}).prop_id
        except:
            propId = None
    else:
        propId = None
    log.note("{} property ID: {}", shortname, propId)

    # Loop over the rows of the input CSV file.
    for i in range(df.shape[0]):
        row = df.iloc[i, :]

        # Column map is a map between the CSV column names and the DB column names.
        val = row[column_map['value']]
        sml = row[column_map['smiles']]
        csml = canonical(sml)
        log.trace("Row {}, SMILES = {}", i+1, sml)

        # Check if the polymer exists in DB using the cannonical smiles.
        polymer = {"canonical_smiles": csml}
        ops = db.Operation(homopolymer.Homopolymer())
        res = ops.get_one(conn, polymer)

        if res is not None:
            log.info("Polymer found in DB.")
            oldpolyprop.add(
                hp_id = res.hp_id,
                prop_id = propId,
                value = val,
                calculation_method = "md",
                conditions = json.dumps({k : row[v] for k, v in conditions_map.items()}),
                note = note,
            )

        else:
            # Add the item to new polymer list
            add_new_polymer(polylist, sml)

            # Add the property value to the new polymer property list.
            # Since these polymers will need to be added to the db, we keep
            # both smiles and csmiles for referencing.
            newpolyprop.add(
                smiles = sml,
                canonical_smiles = csml,
                prop_id = propId,
                value = val,
                calculation_method = "md",
                conditions = json.dumps({k : row[v] for k, v in conditions_map.items()}),
                note = note,
            )

        if debug and (i+1) >= 10: break

    log.done("Processed {} dataset: {}", shortname, csv)
    return polylist, newpolyprop, oldpolyprop


def prepare(args):
    datadir = "Kevin_MD_data"

    # Make property list
    prop = db.Frame()
    prop.add(name="Gas Diffusivity", short_name="D_gas", unit="cm^2/s", plot_symbol="$\D_\text{g}$")
    prop.add(name="Solvent Diffusivity", short_name="D_sol", unit="cm^2/s", plot_symbol="$\D_\text{s}$")
    prop.add(name="Gas Solubility", short_name="sol_g", unit="cc(STP)/cc*cmHg", plot_symbol="$\delta_\text{g}$")
    prop.df.to_json(datadir + "/new_properties.jsonl", orient='records', lines=True)

    n_poly = db.Frame() # list of new polymers

    # Tg
    csv = os.path.join(datadir, "Tg.csv")
    n_poly, n_prop, o_prop = prepare_property_csv(args.session, csv, n_poly,
                                             "Tg",
                                             column_map = {'smiles': 'smiles', 'value': 'Value'},
                                             conditions_map = {},
                                             note = "Source: pmd database by Kevin",
                                             debug = args.debug)

    # Save
    o_prop.df.to_json(datadir + "/tg_existing_polymers.jsonl", orient='records', lines=True)
    n_prop.df.to_json(datadir + "/tg_new_polymers.jsonl", orient='records', lines=True)

    # Dgas
    csv = os.path.join(datadir, "Dgas.csv")
    n_poly, n_prop, o_prop = prepare_property_csv(args.session, csv, n_poly,
                                             "D_gas",
                                             column_map = {'smiles': 'smiles', 'value': 'value'},
                                             conditions_map = {'gas': 'gas'},
                                             note = "Source: pmd database by Kevin",
                                             debug = args.debug)

    # Save
    o_prop.df.to_json(datadir + "/gas_diffusivity_existing_polymers.jsonl", orient='records', lines=True)
    n_prop.df.to_json(datadir + "/gas_diffusivity_new_polymers.jsonl", orient='records', lines=True)

    # Dsol
    csv = os.path.join(datadir, "Dsol.csv")
    n_poly, n_prop, o_prop = prepare_property_csv(args.session, csv, n_poly,
                                             "D_sol",
                                             column_map = {'smiles': 'smiles', 'value': 'value'},
                                             conditions_map = {
                                                 'solvent_smiles': 'solvent_smiles',
                                                 'ratio': 'ratio',
                                                 'temp': 'temp'
                                             },
                                             note = "Source: pmd database by Kevin.\n"+
                                                    "Ratio is defined as the number of monomers over the number of solvent molecules.",
                                             debug = args.debug)

    # Save
    o_prop.df.to_json(datadir + "/solvent_diffusivity_existing_polymers.jsonl", orient='records', lines=True)
    n_prop.df.to_json(datadir + "/solvent_diffusivity_new_polymers.jsonl", orient='records', lines=True)

    # Sgas
    csv = os.path.join(datadir, "Sgas.csv")
    n_poly, n_prop, o_prop = prepare_property_csv(args.session, csv, n_poly,
                                             "sol_g",
                                             column_map = {'smiles': 'smiles', 'value': 'value'},
                                             conditions_map = {
                                                 'gas': 'gas'
                                             },
                                             note = "Source: pmd database by Kevin",
                                             debug = args.debug)

    # Save
    o_prop.df.to_json(datadir + "/gas_solubility_existing_polymers.jsonl", orient='records', lines=True)
    n_prop.df.to_json(datadir + "/gas_solubility_new_polymers.jsonl", orient='records', lines=True)

    save_new_polymers_list(n_poly.df, datadir + "/new_polymer_list.jsonl")
