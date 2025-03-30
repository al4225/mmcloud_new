#!/usr/bin/env python3
# Copyright 2025 Achyutha Harish, MemVerge Inc.
# With edits from Gao Wang
"""
S3 Versioned Operations Tool - Manages all versions of objects in S3 buckets.
Supports copy, move, and delete operations with proper version handling.
Handles large files using multipart upload and quits on first error.
"""

import boto3
import argparse
import os
import sys
from botocore.exceptions import ClientError

# Initialize S3 client
s3 = boto3.client('s3')
# Max size for direct copy (5GB)
MAX_DIRECT_COPY_SIZE = 5 * 1024 * 1024 * 1024

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
        try:
            s3.put_object(Bucket=bucket, Key=prefix, Body='')
        except ClientError as e:
            print(f"Error creating folder marker '{prefix}': {e}")
            sys.exit(1)
    
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
                        'VersionId': v['VersionId'],
                        'Size': v.get('Size', 0)
                    })
        except ClientError as e:
            print(f"Error accessing bucket '{bucket}': {e}")
            sys.exit(1)
            
        if not versions:
            print(f"Error: No objects found with prefix '{prefix}' in bucket '{bucket}'")
            print("Please check that the source prefix exists and you have permission to access it.")
            sys.exit(1)
            
        return versions

    def multipart_copy(self, source_bucket, source_key, source_version_id, 
                       dest_bucket, dest_key):
        """
        Copy a large object using multipart upload.
        This handles objects larger than 5GB which can't be copied directly.
        """
        print(f"Large file detected, using multipart copy for {source_key}...")
        
        # Get object metadata to check size
        try:
            response = s3.head_object(
                Bucket=source_bucket, 
                Key=source_key,
                VersionId=source_version_id
            )
            size = response['ContentLength']
        except ClientError as e:
            print(f"Error getting object metadata: {e}")
            sys.exit(1)
        
        # Initiate multipart upload
        try:
            mpu = s3.create_multipart_upload(Bucket=dest_bucket, Key=dest_key)
            upload_id = mpu['UploadId']
        except ClientError as e:
            print(f"Error initiating multipart upload: {e}")
            sys.exit(1)
        
        # Calculate part size (10MB minimum)
        part_size = max(10 * 1024 * 1024, (size // 10000) + 1)
        
        # Copy parts
        parts = []
        part_number = 1
        
        for offset in range(0, size, part_size):
            last_byte = min(offset + part_size - 1, size - 1)
            range_string = f"bytes={offset}-{last_byte}"
            
            print(f"  Copying part {part_number} ({range_string})...")
            
            try:
                part = s3.upload_part_copy(
                    Bucket=dest_bucket,
                    Key=dest_key,
                    UploadId=upload_id,
                    CopySource={
                        'Bucket': source_bucket,
                        'Key': source_key,
                        'VersionId': source_version_id
                    },
                    CopySourceRange=range_string,
                    PartNumber=part_number
                )
                
                parts.append({
                    'PartNumber': part_number,
                    'ETag': part['CopyPartResult']['ETag']
                })
                
                part_number += 1
            except ClientError as e:
                print(f"Error copying part {part_number}: {e}")
                # Abort the multipart upload
                s3.abort_multipart_upload(
                    Bucket=dest_bucket,
                    Key=dest_key,
                    UploadId=upload_id
                )
                sys.exit(1)
        
        # Complete the multipart upload
        try:
            s3.complete_multipart_upload(
                Bucket=dest_bucket,
                Key=dest_key,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )
            print(f"Multipart copy completed successfully: {source_key} -> {dest_key}")
        except ClientError as e:
            print(f"Error completing multipart upload: {e}")
            # Abort the multipart upload
            s3.abort_multipart_upload(
                Bucket=dest_bucket,
                Key=dest_key,
                UploadId=upload_id
            )
            sys.exit(1)
    
    def copy_object(self, source_bucket, source_key, source_version_id, source_size,
                   dest_bucket, dest_key):
        """Copy a single object with error handling and size check."""
        # For large files, use multipart copy
        if source_size > MAX_DIRECT_COPY_SIZE:
            self.multipart_copy(source_bucket, source_key, source_version_id,
                              dest_bucket, dest_key)
        else:
            # Standard copy for smaller objects
            try:
                print(f"Copying {source_key} (v:{source_version_id}) -> {dest_key}")
                s3.copy_object(
                    Bucket=dest_bucket, 
                    Key=dest_key, 
                    CopySource={
                        'Bucket': source_bucket,
                        'Key': source_key,
                        'VersionId': source_version_id
                    }
                )
            except ClientError as e:
                print(f"Error copying {source_key}: {e}")
                sys.exit(1)
    
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
            self.create_folder(dest_bucket, dest_prefix)
        
        versions = self.list_versions(source_bucket, source_prefix)
        source_base = os.path.basename(source_prefix)
        
        # Determine the destination path
        full_dest_prefix = dest_prefix
        if not merge and not dest_prefix.endswith('/' + source_base):
            full_dest_prefix = os.path.join(dest_prefix, source_base)
        
        for v in versions:
            src_key = v['Key']
            src_version_id = v['VersionId']
            src_size = v.get('Size', 0)
            
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
            
            # Copy the object (exit on error)
            self.copy_object(source_bucket, src_key, src_version_id, src_size,
                           dest_bucket, dst_key)
        
        print(f"Copy operation completed successfully")
        return True
    
    def delete_object(self, bucket, key, version_id):
        """Delete a single object with error handling."""
        try:
            print(f"Deleting {key} (v:{version_id})")
            s3.delete_object(Bucket=bucket, Key=key, VersionId=version_id)
        except ClientError as e:
            print(f"Error deleting {key}: {e}")
            sys.exit(1)
    
    def delete(self, bucket, prefix):
        """Delete all versioned objects under a prefix."""
        prefix = self.strip_slashes(prefix)
        
        # Check if prefix exists
        if not self.check_prefix(bucket, prefix):
            print(f"Error: Prefix '{prefix}' does not exist in bucket '{bucket}'")
            sys.exit(1)
        
        versions = self.list_versions(bucket, prefix)
        
        for v in versions:
            self.delete_object(bucket, v['Key'], v['VersionId'])
        
        print(f"Delete operation completed successfully")
        return True
    
    def move(self, source_bucket, source_prefix, dest_bucket, dest_prefix, merge=False):
        """Move objects (copy and then delete if successful)."""
        # First copy everything
        self.copy(source_bucket, source_prefix, dest_bucket, dest_prefix, merge)
        # Then delete the source
        print("Copy phase successful. Starting delete phase...")
        self.delete(source_bucket, source_prefix)

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