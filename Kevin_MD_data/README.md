# MD Simulation Data for polymers

Calculated Properties:
- Glass Transition temperature (Tg)
- Solubility of polymers in gas (S_gas)
- Diffusivity of polymers in gas (D_gas)
- Diffusivity of polymers in Solvent (D_sol)

`Tg.csv` - Glass Transition Temperatures
- smiles: Polymer cannonical smiles
- Value: Glass transition temperature, Tg in K

`Dgas.csv` - Gas Diffusivity
- ID: row index
- smiles: Polymer cannonical smiles
- value: Diffusivity in cm^2/s
- gas: Molecular formula of gas

`Sgas.csv` - Gas Solubility
- ID: row index
- smiles: Polymer cannonical smiles
- value: Solubility in cc(STP)/cc*cmHg
- gas: Molecular formula of gas

`Dsol.csv` - Solvent Diffusivity
- ID: row index
- smiles: Polymer cannonical smiles
- solvent_smiles: Solvent smiles
- ratio: Ratio of polymer to solvent (mass ratio or volume?)
- value: Diffusivity of polymer in cm^2/s
- temp: Simulation temperature

## Questions
- Mass or volume ratio?
- All polymers known, or any of them hypothetical?
