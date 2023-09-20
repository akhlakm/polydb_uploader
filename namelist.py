"""
    Get the list of polymer names from polydb.
"""
import db
from polydb.orm import homopolymer, polymer

def run(args):
    items = db.Frame()
    ops = db.Operation(polymer.PolymerName(name='pe'))
    names : list[polymer.PolymerName] = ops.get_all(args.session)

    print("Found names:", len(names))

    for name in names:
        items.add(polymer=name.name)
        items.add(polymer=name.search_name)
    
    items.df.to_json("namelist.json", orient="records", lines=True)
    print("Done!")
