# Copyright 2025 Achyutha Harish, MemVerge Inc.
# With edits from Gao Wang
import boto3
import argparse
import os
from urllib.parse import quote

s3 = boto3.client('s3')

def list_all_versions(bucket, prefix):
    paginator = s3.get_paginator('list_object_versions')
    versions = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for v in page.get('Versions', []):
            versions.append({
                'Key': v['Key'],
                'VersionId': v['VersionId']
            })
    return versions

def copy_versioned_objects(source_bucket, source_prefix, dest_bucket, dest_prefix):
    versions = list_all_versions(source_bucket, source_prefix)
    for v in versions:
        src_key = v['Key']
        dst_key = src_key.replace(source_prefix, dest_prefix, 1)
        copy_source = {
            'Bucket': source_bucket,
            'Key': src_key,
            'VersionId': v['VersionId']
        }
        print(f"Copying {src_key} (v:{v['VersionId']}) -> {dst_key}")
        s3.copy_object(Bucket=dest_bucket, Key=dst_key, CopySource=copy_source)

def delete_versioned_objects(bucket, prefix):
    versions = list_all_versions(bucket, prefix)
    for v in versions:
        print(f"Deleting {v['Key']} (v:{v['VersionId']})")
        s3.delete_object(Bucket=bucket, Key=v['Key'], VersionId=v['VersionId'])

def move_versioned_objects(source_bucket, source_prefix, dest_bucket, dest_prefix):
    copy_versioned_objects(source_bucket, source_prefix, dest_bucket, dest_prefix)
    delete_versioned_objects(source_bucket, source_prefix)

def main():
    parser = argparse.ArgumentParser(description="S3 Versioned Operations Tool")
    parser.add_argument('--operation', choices=['copy', 'move', 'delete'], required=True, help="Operation to perform")
    parser.add_argument('--source-bucket', required=True, help="Source bucket name")
    parser.add_argument('--source-prefix', required=True, help="Source prefix (folder path)")
    parser.add_argument('--dest-bucket', help="Destination bucket name (required for copy/move)")
    parser.add_argument('--dest-prefix', help="Destination prefix (required for copy/move)")

    args = parser.parse_args()

    if args.operation in ['copy', 'move'] and (not args.dest_bucket or not args.dest_prefix):
        parser.error("--dest-bucket and --dest-prefix are required for copy and move operations")

    if args.operation == 'copy':
        copy_versioned_objects(args.source_bucket, args.source_prefix, args.dest_bucket, args.dest_prefix)
    elif args.operation == 'move':
        move_versioned_objects(args.source_bucket, args.source_prefix, args.dest_bucket, args.dest_prefix)
    elif args.operation == 'delete':
        delete_versioned_objects(args.source_bucket, args.source_prefix)

if __name__ == '__main__':
    main()
