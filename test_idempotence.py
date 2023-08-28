import os, sys
import json
import pandas as pd

import db
import pylogg
import pgfingerprinting.fp as pgfp
from psmiles import PolymerSmiles

from canonicalize_psmiles.canonicalize import canonicalize

sys.path.append("polydb")
from polydb.orm import homopolymer, property

log = pylogg.New("test_idem")

idem = db.Frame()
not_idem = db.Frame()

def canonical(smiles) -> str:
    """ Convert a smiles into it's cannonical form. """
    # ps = PolymerSmiles(smiles)
    # return str(ps.canonicalize)

    # use the cannonicalize_psmiles package
    return canonicalize(smiles)


def pg_fingerprint(canon):
    """ Calculate Polymer Genome fingerprint from the cannonical smiles."""
    return pgfp.fingerprint_from_smiles(canon)


def prepare_property_csv(csv, polylist : db.Frame, shortname : str, *,
            column_map : dict, conditions_map : dict,
            note = "", debug=False):
    """
    Prepare a dataset for insertion into the database.
    Args:
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

    # Loop over the rows of the input CSV file.
    for i in range(df.shape[0]):
        row = df.iloc[i, :]

        # Column map is a map between the CSV column names and the DB column names.
        sml = row[column_map['smiles']]
        csml = canonical(sml)
        log.trace("Row {}, SMILES = {}, Canonical = {}", i+1, sml, csml)

        # Check idempotence
        ccsml = canonical(csml)
        if not idem.contains('smiles', sml):
            idem.add(smiles=sml, canonical=csml, canonical2=ccsml, idempotent=csml==ccsml)
            print(csml == ccsml)

            if csml != ccsml:
                not_idem.add(smiles=sml, canonical=csml, canonical2=ccsml, idempotent=csml==ccsml)

        continue

    log.done("Processed {} dataset: {}", shortname, csv)
    return polylist, [], []


def check(args):
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
    n_poly, n_prop, o_prop = prepare_property_csv(csv, n_poly,
                                             "Tg",
                                             column_map = {'smiles': 'smiles', 'value': 'Value'},
                                             conditions_map = {},
                                             note = "Source: pmd database by Kevin",
                                             debug = args.debug)


    # Dgas
    csv = os.path.join(datadir, "Dgas.csv")
    n_poly, n_prop, o_prop = prepare_property_csv(csv, n_poly,
                                             "D_gas",
                                             column_map = {'smiles': 'smiles', 'value': 'value'},
                                             conditions_map = {'gas': 'gas'},
                                             note = "Source: pmd database by Kevin",
                                             debug = args.debug)


    # Dsol
    csv = os.path.join(datadir, "Dsol.csv")
    n_poly, n_prop, o_prop = prepare_property_csv(csv, n_poly,
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


    # Sgas
    csv = os.path.join(datadir, "Sgas.csv")
    n_poly, n_prop, o_prop = prepare_property_csv(csv, n_poly,
                                             "sol_g",
                                             column_map = {'smiles': 'smiles', 'value': 'value'},
                                             conditions_map = {
                                                 'gas': 'gas'
                                             },
                                             note = "Source: pmd database by Kevin",
                                             debug = args.debug)


    idem.df.to_csv("idempotent.csv")
    not_idem.df.to_csv("list_not_idempotent.csv")
    print("done!")
