# sota2pota

A Python utility for converting **Summits on the Air (SOTA)** activator and summit-to-summit (S2S) log exports into **Parks on the Air (POTA)** ADIF log files.

The script reads SOTA CSV log files, maps summit references to associated POTA park references using a local dataset, and generates ADIF output files suitable for POTA submission. It also detects S2S contacts and includes corresponding P2P information when a mapped remote park exists.

## Features

- Converts SOTA activator CSV logs into POTA ADIF files
- Uses a summit-to-park mapping dataset from `data/sota_pota.csv`
- Adds P2P information when S2S contacts map to POTA parks
- Filters output by earliest QSO date
- Writes separate ADIF files by operator and activated park

## Prerequisites

Before using this project, make sure you have:

- Python 3.10 or newer
- `pip`
- The Python package `pandas`
- Access to SOTA log exports from Sotadata
- The repository data file at `data/sota_pota.csv`
- An `out/` directory available for generated ADIF files

Install the Python dependency with:

```bash
pip install pandas
```

## Project Files

- `sota_to_pota.py` — main conversion script
- `data/sota_pota.csv` — summit-to-POTA mapping data
- `out/` — output directory for generated `.adi` files

## Input Files

The script expects:

1. An **activator log CSV** exported from the SOTA database
2. An **S2S log CSV** exported from the SOTA database

Default filenames in the script are:

- `W7DLZ_activator_20250827.csv`
- `W7DLZ_s2s_20250825.csv`

You can override them with command-line arguments.

## Usage

Run the converter from the repository root:

```bash
python sota_to_pota.py --activator <activator_log.csv> --s2s <s2s_log.csv> --date <YYYYMMDD>
```

### Arguments

- `--activator` — path to the SOTA activator CSV file
- `--s2s` — path to the SOTA summit-to-summit CSV file
- `--date` — earliest QSO date to include in output, in `YYYYMMDD` format

### Example

```bash
python sota_to_pota.py \
  --activator W7DLZ_activator_20250827.csv \
  --s2s W7DLZ_s2s_20250825.csv \
  --date 20240814
```

## Output

The script generates one or more ADIF files in the `out/` directory.

Each output filename follows this pattern:

```text
<operator>@<park>-<first_date>.adi
```

Example:

```text
W7DLZ@K-1234-20240814.adi
```

## Notes

- The script expects the input CSV formats exported by Sotadata.
- Mode values such as `DATA` and `DV` are normalized to POTA-friendly values.
- If a summit does not map to a POTA park, that QSO is not included in the generated POTA output.

## License

This repository is licensed under the MIT License.
