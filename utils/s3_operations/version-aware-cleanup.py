# Copyright 2025 Achyutha Harish, MemVerge Inc.
# With edits from Gao Wang
import boto3
import argparse
import os
from urllib.parse import quote

s3 = boto3.client('s3')

def strip_trailing_slashes(path):
    """Strip trailing slashes from a path."""
    return path.rstrip('/')

def check_prefix_exists(bucket, prefix):
    """Check if a prefix exists in the bucket."""
    prefix = strip_trailing_slashes(prefix) + '/'  # Ensure trailing slash for prefix check
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    return 'Contents' in resp or 'CommonPrefixes' in resp

def create_prefix_marker(bucket, prefix):
    """Create an empty marker object to represent a 'folder'."""
    prefix = strip_trailing_slashes(prefix) + '/'  # Ensure trailing slash for folder marker
    print(f"Creating folder marker: {prefix}")
    s3.put_object(Bucket=bucket, Key=prefix, Body='')

def list_all_versions(bucket, prefix):
    """List all versions of objects under the given prefix."""
    paginator = s3.get_paginator('list_object_versions')
    versions = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for v in page.get('Versions', []):
            versions.append({
                'Key': v['Key'],
                'VersionId': v['VersionId']
            })
    return versions

def copy_versioned_objects(source_bucket, source_prefix, dest_bucket, dest_prefix, merge=False):
    """
    Copy all versioned objects from source to destination.
    
    Args:
        source_bucket (str): Source bucket name
        source_prefix (str): Source prefix (folder path)
        dest_bucket (str): Destination bucket name
        dest_prefix (str): Destination prefix (folder path)
        merge (bool): If True, merge the contents of source into destination
                      If False, preserve the original folder name in destination
    """
    source_prefix = strip_trailing_slashes(source_prefix)
    dest_prefix = strip_trailing_slashes(dest_prefix)
    
    # Check if destination exists, create it if it doesn't
    if not check_prefix_exists(dest_bucket, dest_prefix):
        print(f"Destination prefix {dest_prefix} doesn't exist, creating it...")
        create_prefix_marker(dest_bucket, dest_prefix)
    
    versions = list_all_versions(source_bucket, source_prefix)
    
    # Get source folder base name and parent path
    source_base = os.path.basename(source_prefix)
    
    for v in versions:
        src_key = v['Key']
        
        if merge:
            # In merge mode, we copy the contents under the source prefix directly to the destination
            # while preserving the subfolder structure
            if src_key.startswith(source_prefix + '/'):
                # This handles files within subfolders
                rel_path = src_key[len(source_prefix) + 1:]  # +1 for the trailing slash
                dst_key = os.path.join(dest_prefix, rel_path)
            elif src_key == source_prefix:
                # Handle the case where the key is exactly the prefix (folder marker)
                dst_key = dest_prefix
            else:
                # Fallback case
                rel_path = src_key[len(source_prefix):].lstrip('/')
                dst_key = os.path.join(dest_prefix, rel_path)
        else:
            # In normal mode, we preserve the source folder name in the destination
            rel_path = src_key[len(source_prefix):].lstrip('/')
            dst_key = os.path.join(dest_prefix, source_base, rel_path)
        
        # Normalize path separators for S3 (use forward slashes)
        dst_key = dst_key.replace('\\', '/')
        
        copy_source = {
            'Bucket': source_bucket,
            'Key': src_key,
            'VersionId': v['VersionId']
        }
        print(f"Copying {src_key} (v:{v['VersionId']}) -> {dst_key}")
        s3.copy_object(Bucket=dest_bucket, Key=dst_key, CopySource=copy_source)

def delete_versioned_objects(bucket, prefix):
    """Delete all versioned objects under the given prefix."""
    prefix = strip_trailing_slashes(prefix)
    versions = list_all_versions(bucket, prefix)
    for v in versions:
        print(f"Deleting {v['Key']} (v:{v['VersionId']})")
        s3.delete_object(Bucket=bucket, Key=v['Key'], VersionId=v['VersionId'])

def move_versioned_objects(source_bucket, source_prefix, dest_bucket, dest_prefix, merge=False):
    """Move all versioned objects from source to destination."""
    copy_versioned_objects(source_bucket, source_prefix, dest_bucket, dest_prefix, merge)
    delete_versioned_objects(source_bucket, source_prefix)

def main():
    parser = argparse.ArgumentParser(description="S3 Versioned Operations Tool")
    parser.add_argument('--operation', choices=['copy', 'move', 'delete'], required=True, help="Operation to perform")
    parser.add_argument('--source-bucket', required=True, help="Source bucket name")
    parser.add_argument('--source-prefix', required=True, help="Source prefix (folder path)")
    parser.add_argument('--dest-bucket', help="Destination bucket name (required for copy/move)")
    parser.add_argument('--dest-prefix', help="Destination prefix (required for copy/move)")
    parser.add_argument('--merge', action='store_true', help="Merge contents of source into destination preserving subfolder structure (only for copy/move)")

    args = parser.parse_args()
    if not args.dest_bucket:
        args.dest_bucket = args.source_bucket
    
    # Strip trailing slashes from paths
    args.source_prefix = strip_trailing_slashes(args.source_prefix)
    if args.dest_prefix:
        args.dest_prefix = strip_trailing_slashes(args.dest_prefix)

    if args.operation in ['copy', 'move'] and (not args.dest_bucket or not args.dest_prefix):
        parser.error("--dest-bucket and --dest-prefix are required for copy and move operations")

    if args.operation == 'delete' and args.merge:
        parser.error("--merge option is only applicable to copy and move operations")
    
    if args.operation == 'copy':
        copy_versioned_objects(args.source_bucket, args.source_prefix, args.dest_bucket, args.dest_prefix, args.merge)
    elif args.operation == 'move':
        move_versioned_objects(args.source_bucket, args.source_prefix, args.dest_bucket, args.dest_prefix, args.merge)
    elif args.operation == 'delete':
        delete_versioned_objects(args.source_bucket, args.source_prefix)

if __name__ == '__main__':
    main()