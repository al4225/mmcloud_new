#!/usr/bin/env python3
# Copyright 2025 Achyutha Harish, MemVerge Inc.
# With edits from Gao Wang
"""
S3 Versioned Operations Tool - Manages all versions of objects in S3 buckets.
Supports copy, move, and delete operations with proper version handling.
"""

import boto3
import argparse
import os
import sys
from botocore.exceptions import ClientError

# Initialize S3 client
s3 = boto3.client('s3')

class S3VersionedOps:
    """Class to handle versioned S3 operations."""
    
    @staticmethod
    def strip_slashes(path):
        """Remove leading and trailing slashes from a path."""
        return path.lstrip('/').rstrip('/')
    
    @staticmethod
    def check_bucket(bucket_name):
        """Verify a bucket exists and is accessible."""
        try:
            s3.head_bucket(Bucket=bucket_name)
            return True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                print(f"Error: Bucket '{bucket_name}' does not exist")
            elif error_code == '403':
                print(f"Error: No permission to access bucket '{bucket_name}'")
            else:
                print(f"Error accessing bucket '{bucket_name}': {e}")
            return False
    
    @staticmethod
    def check_prefix(bucket, prefix):
        """Check if a prefix exists in the bucket."""
        prefix = S3VersionedOps.strip_slashes(prefix) + '/'
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        return 'Contents' in resp or 'CommonPrefixes' in resp
    
    @staticmethod
    def create_folder(bucket, prefix):
        """Create an empty marker object for a folder."""
        prefix = S3VersionedOps.strip_slashes(prefix) + '/'
        print(f"Creating folder marker: {prefix}")
        s3.put_object(Bucket=bucket, Key=prefix, Body='')
    
    @staticmethod
    def list_versions(bucket, prefix):
        """List all versions of objects under a prefix."""
        prefix = S3VersionedOps.strip_slashes(prefix)
        paginator = s3.get_paginator('list_object_versions')
        versions = []
        
        try:
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for v in page.get('Versions', []):
                    versions.append({
                        'Key': v['Key'],
                        'VersionId': v['VersionId']
                    })
        except ClientError as e:
            print(f"Error accessing bucket '{bucket}': {e}")
            sys.exit(1)
            
        if not versions:
            print(f"Error: No objects found with prefix '{prefix}' in bucket '{bucket}'")
            print("Please check that the source prefix exists and you have permission to access it.")
            sys.exit(1)
            
        return versions
    
    def copy(self, source_bucket, source_prefix, dest_bucket, dest_prefix, merge=False):
        """Copy all versioned objects from source to destination."""
        source_prefix = self.strip_slashes(source_prefix)
        dest_prefix = self.strip_slashes(dest_prefix)
        
        # Check if source exists
        if not self.check_prefix(source_bucket, source_prefix):
            print(f"Error: Source prefix '{source_prefix}' does not exist in bucket '{source_bucket}'")
            sys.exit(1)
        
        # Create destination if needed
        if not self.check_prefix(dest_bucket, dest_prefix):
            try:
                self.create_folder(dest_bucket, dest_prefix)
            except ClientError as e:
                print(f"Error creating destination '{dest_prefix}': {e}")
                sys.exit(1)
        
        versions = self.list_versions(source_bucket, source_prefix)
        source_base = os.path.basename(source_prefix)
        
        # Determine the destination path
        full_dest_prefix = dest_prefix
        if not merge and not dest_prefix.endswith('/' + source_base):
            full_dest_prefix = os.path.join(dest_prefix, source_base)
        
        success_count = 0
        error_count = 0
        
        for v in versions:
            src_key = v['Key']
            
            # Calculate destination key
            if src_key.startswith(source_prefix + '/'):
                # File inside subfolder
                rel_path = src_key[len(source_prefix) + 1:]
                dst_key = os.path.join(full_dest_prefix, rel_path)
            elif src_key == source_prefix:
                # Folder marker
                if not merge:
                    dst_key = full_dest_prefix
                else:
                    continue  # Skip folder marker in merge mode
            else:
                # Edge case
                rel_path = src_key[len(source_prefix):].lstrip('/')
                dst_key = os.path.join(full_dest_prefix, rel_path)
                
            # Use forward slashes for S3
            dst_key = dst_key.replace('\\', '/')
            
            copy_source = {
                'Bucket': source_bucket,
                'Key': src_key,
                'VersionId': v['VersionId']
            }
            
            try:
                print(f"Copying {src_key} (v:{v['VersionId']}) -> {dst_key}")
                s3.copy_object(Bucket=dest_bucket, Key=dst_key, CopySource=copy_source)
                success_count += 1
            except ClientError as e:
                print(f"Error copying {src_key}: {e}")
                error_count += 1
        
        print(f"Copy complete: {success_count} copied, {error_count} errors")
        return error_count == 0
    
    def delete(self, bucket, prefix):
        """Delete all versioned objects under a prefix."""
        prefix = self.strip_slashes(prefix)
        
        # Check if prefix exists
        if not self.check_prefix(bucket, prefix):
            print(f"Error: Prefix '{prefix}' does not exist in bucket '{bucket}'")
            sys.exit(1)
        
        versions = self.list_versions(bucket, prefix)
        success_count = 0
        error_count = 0
        
        for v in versions:
            try:
                print(f"Deleting {v['Key']} (v:{v['VersionId']})")
                s3.delete_object(Bucket=bucket, Key=v['Key'], VersionId=v['VersionId'])
                success_count += 1
            except ClientError as e:
                print(f"Error deleting {v['Key']}: {e}")
                error_count += 1
        
        print(f"Delete complete: {success_count} deleted, {error_count} errors")
        return error_count == 0
    
    def move(self, source_bucket, source_prefix, dest_bucket, dest_prefix, merge=False):
        """Move objects (copy and then delete if successful)."""
        # Only delete if copy was successful
        if self.copy(source_bucket, source_prefix, dest_bucket, dest_prefix, merge):
            print("Copy phase successful. Starting delete phase...")
            self.delete(source_bucket, source_prefix)
        else:
            print("Copy phase had errors. Skipping delete to avoid data loss.")
            sys.exit(1)

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="S3 Versioned Operations Tool")
    parser.add_argument('--operation', choices=['copy', 'move', 'delete'], 
                        required=True, help="Operation to perform")
    parser.add_argument('--source-bucket', required=True, help="Source bucket name")
    parser.add_argument('--source-prefix', required=True, help="Source prefix (folder path)")
    parser.add_argument('--dest-bucket', help="Destination bucket name (for copy/move)")
    parser.add_argument('--dest-prefix', help="Destination prefix (for copy/move)")
    parser.add_argument('--merge', action='store_true', 
                        help="Merge contents of source into destination (for copy/move)")
    return parser.parse_args()

def main():
    """Main entry point for the script."""
    args = parse_args()
    
    # Set defaults and clean paths
    s3ops = S3VersionedOps()
    args.source_prefix = s3ops.strip_slashes(args.source_prefix)
    
    if not args.dest_bucket:
        args.dest_bucket = args.source_bucket
    
    if args.dest_prefix:
        args.dest_prefix = s3ops.strip_slashes(args.dest_prefix)
    
    # Validate arguments
    if args.operation in ['copy', 'move'] and (not args.dest_bucket or not args.dest_prefix):
        print("Error: --dest-bucket and --dest-prefix are required for copy/move operations")
        sys.exit(1)
    
    if args.operation == 'delete' and args.merge:
        print("Error: --merge option is only applicable to copy/move operations")
        sys.exit(1)
    
    # Check buckets exist
    if not s3ops.check_bucket(args.source_bucket):
        sys.exit(1)
    
    if args.operation in ['copy', 'move'] and args.dest_bucket != args.source_bucket:
        if not s3ops.check_bucket(args.dest_bucket):
            sys.exit(1)
    
    # For rename operations, use merge-like behavior when destination doesn't exist
    use_merge = args.merge
    if (args.operation == 'move' and 
            not s3ops.check_prefix(args.dest_bucket, args.dest_prefix)):
        use_merge = True
    
    # Perform the requested operation
    try:
        if args.operation == 'copy':
            s3ops.copy(args.source_bucket, args.source_prefix, 
                      args.dest_bucket, args.dest_prefix, args.merge)
        elif args.operation == 'move':
            s3ops.move(args.source_bucket, args.source_prefix, 
                      args.dest_bucket, args.dest_prefix, use_merge)
        elif args.operation == 'delete':
            s3ops.delete(args.source_bucket, args.source_prefix)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()