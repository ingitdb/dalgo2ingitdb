# Original native Shipping logs regression fixture

The eight inGitDB input files are unchanged copies from `demo-db/shipping-logs`
commit `51cd9c646d6f950f705e941e27a932fab1dac8df`, directory `ingitdb/`.
They retain the original three logical-root mapping and native JSONL records.
License: CC0-1.0; see LICENSE. Tests copy these files to a temporary directory
before opening the real driver, because read locks create auxiliary files.
