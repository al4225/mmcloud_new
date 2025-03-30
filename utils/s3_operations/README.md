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
- Supports merging content from source into destination (preserving subfolder structure).
- Automatically strips trailing slashes from all prefixes.

## Requirements

- Python 3.6 or higher
- `boto3` library (`pip install boto3`, or for those using StatFunGen Lab default setup, `pixi global install --environment python boto3`)
- AWS credentials configured via AWS CLI or environment variables

## Usage

```bash
python version-aware-cleanup.py \
  --operation <copy|move|delete> \
  --source-bucket <bucket-name> \
  --source-prefix <path/in/bucket/> \
  [--dest-bucket <bucket-name>] \
  [--dest-prefix <path/in/bucket/>] \
  [--merge]
```

### Parameters

- `--operation`: The operation to perform (copy, move, or delete).
- `--source-bucket`: Source bucket name.
- `--source-prefix`: Source prefix (folder path).
- `--dest-bucket`: Destination bucket name (required for copy/move).
- `--dest-prefix`: Destination prefix (required for copy/move).
- `--merge`: Optional flag to merge contents of source into destination while preserving subfolder structure (only for copy/move).

### Path Handling

- All trailing slashes (`/`) are automatically stripped from prefixes.
- By default, the source folder name is preserved in the destination path (creating a nested structure).
- With the `--merge` option, contents under the source folder are copied directly to the destination while preserving their subfolder structure.

## Examples

### Default Copy (Preserving Full Folder Structure)

Copy all versions from `ftp_fgc_xqtl/20250218_ADSP_LD_matrix_APOEblocks_merge` to `ftp_fgc_xqtl/resource/20240409_ADSP_LD_matrix`, preserving the complete folder structure:

```bash
python version-aware-cleanup.py \
  --operation copy \
  --source-bucket statfungen \
  --source-prefix ftp_fgc_xqtl/20250218_ADSP_LD_matrix_APOEblocks_merge \
  --dest-prefix ftp_fgc_xqtl/resource/20240409_ADSP_LD_matrix
```

This will create:
```
ftp_fgc_xqtl/resource/20240409_ADSP_LD_matrix/20250218_ADSP_LD_matrix_APOEblocks_merge/
ftp_fgc_xqtl/resource/20240409_ADSP_LD_matrix/20250218_ADSP_LD_matrix_APOEblocks_merge/chr19_42346101_46842901.cor.xz
ftp_fgc_xqtl/resource/20240409_ADSP_LD_matrix/20250218_ADSP_LD_matrix_APOEblocks_merge/chr19_42346101_46842901.cor.xz.bim
ftp_fgc_xqtl/resource/20240409_ADSP_LD_matrix/20250218_ADSP_LD_matrix_APOEblocks_merge/ld_meta_file_apoe.tsv
```

### Merge Copy (Preserving Subfolder Structure)

Copy all versions from `ftp_fgc_xqtl/20250218_ADSP_LD_matrix_APOEblocks_merge` directly into `ftp_fgc_xqtl/resource/20240409_ADSP_LD_matrix` while preserving subfolder structure:

```bash
python version-aware-cleanup.py \
  --operation copy \
  --source-bucket statfungen \
  --source-prefix ftp_fgc_xqtl/20250218_ADSP_LD_matrix_APOEblocks_merge \
  --dest-prefix ftp_fgc_xqtl/resource/20240409_ADSP_LD_matrix \
  --merge
```

This will create:
```
ftp_fgc_xqtl/resource/20240409_ADSP_LD_matrix/chr19_42346101_46842901.cor.xz
ftp_fgc_xqtl/resource/20240409_ADSP_LD_matrix/chr19_42346101_46842901.cor.xz.bim
ftp_fgc_xqtl/resource/20240409_ADSP_LD_matrix/ld_meta_file_apoe.tsv
```

If the source had subfolders like `ftp_fgc_xqtl/20250218_ADSP_LD_matrix_APOEblocks_merge/subset1/file.txt`, it would be copied to `ftp_fgc_xqtl/resource/20240409_ADSP_LD_matrix/subset1/file.txt`.

### Move with Merge Example

Move all versions and merge into destination:

```bash
python version-aware-cleanup.py \
  --operation move \
  --source-bucket statfungen \
  --source-prefix ftp_fgc_xqtl/old_data \
  --dest-prefix ftp_fgc_xqtl/merged_data \
  --merge
```

### Delete All Versions Example

Delete all versions under a given prefix:

```bash
python version-aware-cleanup.py \
  --operation delete \
  --source-bucket statfungen \
  --source-prefix ftp_fgc_xqtl/temp_data
```

## Notes
- The move operation performs a full versioned copy followed by deletion of all versions in the source.
- The delete operation permanently deletes all versions under the given prefix. This cannot be undone.
- This script is designed for versioned buckets. For non-versioned buckets, simpler aws s3 cp/mv/rm commands may suffice.
- For large datasets or prefixes containing millions of versions, consider running with additional logging and batching strategies.