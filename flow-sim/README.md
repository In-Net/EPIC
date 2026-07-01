# OMNeT++ Flow Simulation Projects

This repository holds multiple OMNeT++ flow simulation setups along with supporting configuration and helper scripts.

## Directory overview
- `Code/`: each subdirectory contains a simulation case (single-tenant vs. multi-tenant, flow-sim vs. new-scaleup). Current projects:
  - `single-tenant-newscaleup`
  - `singletenant-flow-sim`
  - `multi-tenant-newscaleup`
  - `multitenant-flow-sim`
- `config/`: frequently used `.ini` configuration files (GPT3, LLAMA, qwen, Crux, etc.) and related model definitions.
- `scripts/`: helper scripts such as `extract_jct.py` for parsing simulation outputs.

## Suggestions
1. Run the OMNeT++ simulator (e.g., `opp_run` or the IDE) from the simulation folder you care about and point it at the appropriate `.ini` file.
2. If you rename directories, update any scripts or documentation that reference those paths.
3. Feel free to expand `scripts/` with additional tooling for result processing, log extraction, etc.

Add a subdirectory and document its purpose in this README whenever you introduce a new scenario.
