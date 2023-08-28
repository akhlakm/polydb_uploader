import pandas as pd
from psmiles import PolymerSmiles

df = pd.read_csv("list_not_idempotent.csv")

for i, row in df.iterrows():
    sml = row['smiles']
    ps1 = PolymerSmiles(sml)
    csml = str(ps1.canonicalize)

    ps2 = PolymerSmiles(csml)
    ccsml = str(ps2.canonicalize)

    print(sml, "\nIdempotent:", csml == ccsml)

