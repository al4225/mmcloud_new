# S3-Synapse Transfer Tool

A Python script for transferring files from Amazon S3 to Synapse using file handlers.

## Overview

This tool creates Synapse file entities that reference existing S3 objects without copying the actual data. It uses the Synapse file handler mechanism to register S3 files with Synapse, maintaining the original storage in S3.

## Installation

```bash
pip install synapseclient s3fs
```

## Usage

```
python s3_handler_to_synapse.py --synid SYN123456 --bucket my-bucket --path data/files/ \
                     --token-file token.txt [options]
```

## Examples

1. Transfer a single file:
```bash
python s3_handler_to_synapse.py --synid syn12345678 --bucket my-data-bucket \
                     --path path/to/file.csv --token-file synapse_token.txt
```

2. Transfer a directory recursively:
```bash
python s3_handler_to_synapse.py --synid syn12345678 --bucket my-data-bucket \
                     --path data/files/ --recursive --token-file synapse_token.txt
```
### Transfer only CSV and TXT files
```
python3 s3_synapse_transfer.py --synid syn123456 --bucket my-bucket --path data/ --token-file synapse_token.txt --patterns csv txt --recursive
```
### Transfer only XLSX files
```
python3 s3_synapse_transfer.py --synid syn123456 --bucket my-bucket --path data/ --token-file synapse_token.txt --patterns xlsx
```
### Original behavior (transfer all files)
```
python3 s3_synapse_transfer.py --synid syn123456 --bucket my-bucket --path data/ --recursive --token-file synapse_token.txt
```

## Features

- Uses file handlers to link S3 files to Synapse without copying data
- Supports single files or recursive directory processing
- Automatically calculates MD5 checksums for file verification
- Preserves file metadata during transfer

## Options

| Option | Description |
|--------|-------------|
| `--synid` | Synapse folder ID (destination) |
| `--bucket` | S3 bucket name |
| `--path` | Path in S3 bucket (file or directory) |
| `--token-file` | File containing Synapse authentication token |
| `--recursive` | Process directories recursively |
| `--patterns`| Specify file types to transfer(optional) |
| `--skip-md5` | Skip MD5 hash calculation (faster) |
| `--verbose` | Enable verbose logging |

## Notes

- Authentication uses a token file rather than username/password
- S3 credentials are read from environment or AWS config
- The Synapse file entities will reference the S3 location
- File handlers maintain the link between Synapse and S3 storage
- I’ve added a `command_generator.sh` script as an example for uploading files in bulk using an s3_to_synapse metadata table(in `command_generator.sh`).


# Synapse Move Tool

A Python script for moving files in a folder to a subdirectory within this folder.

## Usage

```
python synapse_move_files.py --synid SYN123456 --bucket my-bucket --path data/files/ \
                     --token-file token.txt [options]
```

## Example
1. Not designded for moving a single file, you can do it manually on the interface.

2. Move multiple files
### Original behavior (transfer all files to the subfolder)
```
python synapse_move_files.py --synid syn123456 --token-file token.txt
```

### Move only specific file types
```
python synapse_move_files.py --synid syn123456 --token-file token.txt --patterns csv txt
```
### Specify a different subfolder name
```
python synapse_move_files.py --synid syn123456 --token-file token.txt --subfolder "eQTL_data"
```

### Enable verbose logging
```
python synapse_move_files.py --synid syn123456 --token-file token.txt --verbose
```


## Note
- The synapse id in commands is for the folder that contains files to move and the subfolder instead of for the subfolder.
- Don't need to manually create the subfolder. The script will create it if not exist.