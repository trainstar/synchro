# Test Inventory

Generated inventory files are published at:

- `dist/verification/<candidate-id>/inventory.json`
- `dist/verification/<candidate-id>/inventory.md`

The [conformance test architecture](README.md#test-architecture) defines the seven layers and the one-proof-home rule.

## Layer Mapping

The generated inventory projects coverage rows from the validated CI summary.

| Layer | Current inventory representation |
| --- | --- |
| Contract | The requirement, scenario, proof obligation, and assertion fields identify authored contract inputs. |
| Unit | The coverage projection has no dedicated unit proof home. |
| Real integration | Rows use the `real-integration` proof home. |
| Scenario | Rows use the `scenario` proof home. |
| Cell smoke | CI summary obligations use the `smoke` kind. Coverage rows do not project these obligations. |
| Adversarial | Rows use the `adversarial` proof home. |
| Randomized soak | The coverage projection has no dedicated randomized soak proof home. |
