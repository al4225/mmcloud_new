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

Transfer a single file:
```bash
python s3_handler_to_synapse.py --synid syn12345678 --bucket my-data-bucket \
                     --path path/to/file.csv --token-file synapse_token.txt
```

Transfer a directory recursively:
```bash
python s3_handler_to_synapse.py --synid syn12345678 --bucket my-data-bucket \
                     --path data/files/ --recursive --token-file synapse_token.txt
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
| `--skip-md5` | Skip MD5 hash calculation (faster) |
| `--verbose` | Enable verbose logging |

## Notes

- Authentication uses a token file rather than username/password
- S3 credentials are read from environment or AWS config
- The Synapse file entities will reference the S3 location
- File handlers maintain the link between Synapse and S3 storage
