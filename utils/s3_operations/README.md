# S3 Versioned Operations Tool

This script provides a safe and reliable way to manage versioned objects in Amazon S3. It supports three operations:

- **copy**: Copies all versions of objects from a source prefix to a destination prefix.
- **move**: Copies all versions and then deletes them from the source location.
- **delete**: Permanently deletes all versions of objects under a given prefix.

This is useful when working with versioned S3 buckets where standard `aws s3 cp` or `mv` commands only operate on the latest version, leaving older versions behind and potentially incurring unexpected storage costs.

## Features

- Handles all versions of objects in versioned S3 buckets.
- Supports copy, move, and delete operations.
- Operates recursively under a given prefix.
- Ensures that no hidden versions are left behind.

## Requirements

- Python 3.6 or higher
- `boto3` library (`pip install boto3`)
- AWS credentials configured via AWS CLI or environment variables

## Usage

```bash
python version-aware-cleanup.py \
  --operation <copy|move|delete> \
  --source-bucket <bucket-name> \
  --source-prefix <path/in/bucket/> \
  [--dest-bucket <bucket-name>] \
  [--dest-prefix <path/in/bucket/>]
```

## Examples

- Copy all versions from one prefix to another

```bash
python version-aware-cleanup.py \
  --operation copy \
  --source-bucket my-bucket \
  --source-prefix data/input/ \
  --dest-bucket my-bucket \
  --dest-prefix data/archive/
```

- Move all versions (Copy + Delete)

```bash
python version-aware-cleanup.py \
  --operation move \
  --source-bucket my-bucket \
  --source-prefix scratch/ \
  --dest-bucket my-bucket \
  --dest-prefix scratch-archived/
```

- Delete all versions under a given prefix

```bash
python version-aware-cleanup.py \
  --operation delete \
  --source-bucket my-bucket \
  --source-prefix temp-outputs/
```

## Notes
- The move operation performs a full versioned copy followed by deletion of all versions in the source.
- The delete operation permanently deletes all versions under the given prefix. This cannot be undone.
-	This script is designed for versioned buckets. For non-versioned buckets, simpler aws s3 cp/mv/rm commands may suffice.
-	For large datasets or prefixes containing millions of versions, consider running with additional logging and batching strategies.