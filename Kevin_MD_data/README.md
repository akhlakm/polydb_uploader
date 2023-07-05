# MD Simulation Data for polymers

## CSV Dataset

Calculated Properties:
- Glass Transition temperature (Tg)
- Solubility of gas in polymer (S_gas)
- Diffusivity of gas in polymer (D_gas)
- Diffusivity of solvent in polymer (D_sol)

`Tg.csv` - Glass Transition Temperatures
- smiles: Polymer cannonical smiles
- Value: Glass transition temperature, Tg in K

`Dgas.csv` - Gas Diffusivity
- ID: row index
- smiles: Polymer cannonical smiles
- value: Diffusivity of gas in cm^2/s
- gas: Molecular formula of gas

`Sgas.csv` - Gas Solubility
- ID: row index
- smiles: Polymer cannonical smiles
- value: Solubility of the gas in cc(STP)/cc*cmHg
- gas: Molecular formula of gas

`Dsol.csv` - Solvent Diffusivity
- ID: row index
- smiles: Polymer cannonical smiles
- solvent_smiles: Solvent smiles
- ratio: Ratio of polymer to solvent (mass ratio or volume?)
- value: Diffusivity of solvent in cm^2/s
- temp: Simulation temperature

## Questions
- Mass or volume ratio?
    - The ratio is number of "polymer repeating units" over number of solvent molecules (not mass ratio nor volume ratio).
- All polymers known, or any of them hypothetical?
    - All the polymers are known polymers.
