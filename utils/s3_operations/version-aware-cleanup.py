#!/usr/bin/env python3
"""
S3 Direct File Operations - For copying, moving, or deleting direct files in S3 folders
Copyright 2025 Achyutha Harish, MemVerge Inc.
With edits from Gao Wang via claude.ai
"""

import boto3
import argparse
import os
import sys
import fnmatch
import re
from botocore.exceptions import ClientError

# Initialize S3 client with extended timeout
s3 = boto3.client('s3', config=boto3.session.Config(connect_timeout=60, read_timeout=60))
# Max size for direct copy (4GB)
MAX_DIRECT_COPY_SIZE = 4 * 1024 * 1024 * 1024

class S3DirectOps:
    """S3 operations for direct folder contents only."""
    
    @staticmethod
    def strip_slashes(path):
        """Remove leading and trailing slashes from a path."""
        return path.lstrip('/').rstrip('/')
    
    @staticmethod
    def ensure_prefix_format(prefix):
        """Ensure the prefix is properly formatted (with trailing slash if not empty)."""
        if not prefix:
            return ''
        return S3DirectOps.strip_slashes(prefix) + '/'
    
    @staticmethod
    def check_bucket(bucket_name):
        """Verify a bucket exists and is accessible."""
        try:
            s3.head_bucket(Bucket=bucket_name)
            return True
        except ClientError as e:
            print(f"Error: Cannot access bucket '{bucket_name}': {e}")
            return False
    
    @staticmethod
    def create_folder(bucket, prefix):
        """Create an empty marker object for a folder."""
        prefix = S3DirectOps.ensure_prefix_format(prefix)
        print(f"Creating folder marker: {prefix}")
        try:
            s3.put_object(Bucket=bucket, Key=prefix, Body='')
            return True
        except ClientError as e:
            print(f"Error creating folder marker '{prefix}': {e}")
            return False

    @staticmethod
    def match_pattern(filename, pattern, pattern_type):
        """Match a filename against a pattern."""
        if pattern_type == 'glob':
            return fnmatch.fnmatch(filename, pattern)
        elif pattern_type == 'regex':
            return bool(re.search(pattern, filename))
        elif pattern_type == 'exact':
            return filename == pattern
        else:
            return fnmatch.fnmatch(filename, pattern)
    
    def list_direct_files(self, bucket, prefix, pattern=None, pattern_type=None):
        """
        List only direct files (not folders) within a prefix.
        """
        normalized_prefix = self.ensure_prefix_format(prefix)
        print(f"Listing direct files in '{normalized_prefix}'...")
        
        try:
            # List all objects under the prefix with delimiter
            response = s3.list_objects_v2(
                Bucket=bucket,
                Prefix=normalized_prefix,
                Delimiter='/'  # Key parameter for direct-only listing
            )
            
            matched_files = []
            
            # Process files (direct children only)
            for obj in response.get('Contents', []):
                key = obj.get('Key')
                
                # Skip the folder marker itself
                if key == normalized_prefix:
                    continue
                
                # Extract just the filename for matching
                file_name = os.path.basename(key)
                
                # Apply pattern matching if needed
                if not pattern or self.match_pattern(file_name, pattern, pattern_type):
                    matched_files.append({
                        'Key': key,
                        'Size': obj.get('Size', 0),
                        'Name': file_name
                    })
            
            # Count folders too, for informational purposes
            folder_count = len(response.get('CommonPrefixes', []))
            
            print(f"Found {len(matched_files)} matching files and {folder_count} folders")
            
            if pattern:
                print(f"Files matching pattern '{pattern}':")
                for file in matched_files:
                    print(f"  - {file['Name']}")
            
            if pattern and not matched_files:
                print(f"No files match the pattern '{pattern}'")
                sys.exit(1)
                
            return matched_files
            
        except ClientError as e:
            print(f"Error listing objects: {e}")
            sys.exit(1)
    
    def get_object_versions(self, bucket, key):
        """Get all versions of a specific object."""
        try:
            response = s3.list_object_versions(
                Bucket=bucket,
                Prefix=key
            )
            
            versions = []
            for version in response.get('Versions', []):
                if version.get('Key') == key:
                    versions.append({
                        'Key': key,
                        'VersionId': version.get('VersionId'),
                        'Size': version.get('Size', 0)
                    })
            
            return versions
        except ClientError as e:
            print(f"Error getting versions for {key}: {e}")
            sys.exit(1)
    
    def copy_file(self, source_bucket, source_key, dest_bucket, dest_key, version_id=None):
        """Copy a file (with optional specific version)."""
        try:
            # Get versions if not specified
            versions = []
            if version_id:
                # Use the specified version
                versions = [{
                    'Key': source_key,
                    'VersionId': version_id,
                    'Size': 0  # Will get size from head_object if needed
                }]
            else:
                # Get all versions of the file
                versions = self.get_object_versions(source_bucket, source_key)
            
            # Copy each version
            for version in versions:
                source_version_id = version.get('VersionId')
                source_size = version.get('Size', 0)
                
                # Get size if not provided
                if source_size == 0:
                    try:
                        response = s3.head_object(
                            Bucket=source_bucket,
                            Key=source_key,
                            VersionId=source_version_id
                        )
                        source_size = response.get('ContentLength', 0)
                    except ClientError:
                        # If can't get size, assume it's small
                        source_size = 0
                
                # Choose copy method based on size
                if source_size > MAX_DIRECT_COPY_SIZE:
                    self.copy_large_file(source_bucket, source_key, source_version_id, 
                                        dest_bucket, dest_key)
                else:
                    print(f"Copying {source_key} (v:{source_version_id}) → {dest_key}")
                    copy_source = {
                        'Bucket': source_bucket,
                        'Key': source_key
                    }
                    if source_version_id:
                        copy_source['VersionId'] = source_version_id
                        
                    s3.copy_object(
                        Bucket=dest_bucket,
                        Key=dest_key,
                        CopySource=copy_source
                    )
            
            return True
        except ClientError as e:
            print(f"Error copying {source_key}: {e}")
            return False
    
    def copy_large_file(self, source_bucket, source_key, source_version_id, 
                        dest_bucket, dest_key):
        """Copy a large file using multipart upload."""
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
            return False
        
        # Initiate multipart upload
        try:
            mpu = s3.create_multipart_upload(Bucket=dest_bucket, Key=dest_key)
            upload_id = mpu['UploadId']
        except ClientError as e:
            print(f"Error initiating multipart upload: {e}")
            return False
        
        # Calculate part size (10MB minimum)
        part_size = max(10 * 1024 * 1024, (size // 10000) + 1)
        
        # Copy parts
        parts = []
        part_number = 1
        success = True
        
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
                success = False
                break
        
        # Complete or abort the multipart upload
        if success and parts:
            try:
                s3.complete_multipart_upload(
                    Bucket=dest_bucket,
                    Key=dest_key,
                    UploadId=upload_id,
                    MultipartUpload={'Parts': parts}
                )
                print(f"Multipart copy completed successfully: {source_key} → {dest_key}")
                return True
            except ClientError as e:
                print(f"Error completing multipart upload: {e}")
                success = False
        
        # Abort if any part failed
        if not success:
            try:
                s3.abort_multipart_upload(
                    Bucket=dest_bucket,
                    Key=dest_key,
                    UploadId=upload_id
                )
                print(f"Multipart upload aborted for {dest_key}")
            except ClientError as e:
                print(f"Error aborting multipart upload: {e}")
            return False
    
    def delete_file(self, bucket, key, delete_all_versions=True):
        """Delete a file and optionally all its versions."""
        try:
            if delete_all_versions:
                versions = self.get_object_versions(bucket, key)
                for version in versions:
                    version_id = version.get('VersionId')
                    print(f"Deleting {key} (v:{version_id})")
                    s3.delete_object(
                        Bucket=bucket,
                        Key=key,
                        VersionId=version_id
                    )
            else:
                print(f"Deleting {key} (latest version)")
                s3.delete_object(
                    Bucket=bucket,
                    Key=key
                )
            return True
        except ClientError as e:
            print(f"Error deleting {key}: {e}")
            return False

    def copy_files_with_pattern(self, source_bucket, source_prefix, dest_bucket, dest_prefix,
                              pattern=None, pattern_type=None):
        """Copy files matching a pattern from source to destination."""
        # Normalize prefixes
        source_prefix = self.strip_slashes(source_prefix)
        dest_prefix = self.strip_slashes(dest_prefix)
        
        # Ensure destination folder exists
        if not self.create_folder(dest_bucket, dest_prefix):
            print(f"Could not create destination folder: {dest_prefix}")
            return False
        
        # List files matching the pattern (direct files only)
        matched_files = self.list_direct_files(source_bucket, source_prefix, pattern, pattern_type)
        
        if not matched_files:
            print("No matching files to copy.")
            return False
        
        # Copy each matched file
        success_count = 0
        failed_count = 0
        
        for file in matched_files:
            source_key = file['Key']
            file_name = file['Name']
            
            # Calculate destination key (just append filename to destination prefix)
            dest_key = f"{dest_prefix}/{file_name}"
            
            # Copy the file with all its versions
            if self.copy_file(source_bucket, source_key, dest_bucket, dest_key):
                success_count += 1
            else:
                failed_count += 1
        
        print(f"Copy operation completed: {success_count} files copied, {failed_count} failed")
        return failed_count == 0
    
    def delete_files_with_pattern(self, bucket, prefix, pattern=None, pattern_type=None):
        """Delete files matching a pattern."""
        # Normalize prefix
        prefix = self.strip_slashes(prefix)
        
        # List files matching the pattern (direct files only)
        matched_files = self.list_direct_files(bucket, prefix, pattern, pattern_type)
        
        if not matched_files:
            print("No matching files to delete.")
            return False
        
        # Ask for confirmation
        file_count = len(matched_files)
        if pattern:
            confirmation = input(f"Are you sure you want to delete {file_count} files matching '{pattern}'? (y/n): ")
            if confirmation.lower() != 'y':
                print("Operation cancelled.")
                return False
        
        # Delete each matched file
        success_count = 0
        failed_count = 0
        
        for file in matched_files:
            source_key = file['Key']
            
            # Delete the file with all its versions
            if self.delete_file(bucket, source_key):
                success_count += 1
            else:
                failed_count += 1
        
        print(f"Delete operation completed: {success_count} files deleted, {failed_count} failed")
        return failed_count == 0
    
    def move_files_with_pattern(self, source_bucket, source_prefix, dest_bucket, dest_prefix,
                              pattern=None, pattern_type=None):
        """Move files matching a pattern from source to destination."""
        # First copy the files
        copy_result = self.copy_files_with_pattern(
            source_bucket, source_prefix, dest_bucket, dest_prefix, pattern, pattern_type
        )
        
        # Only delete if copy was successful
        if copy_result:
            print("Copy completed successfully. Starting delete phase...")
            return self.delete_files_with_pattern(source_bucket, source_prefix, pattern, pattern_type)
        else:
            print("Copy operation had errors. Skipping delete phase to prevent data loss.")
            return False

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="S3 Direct File Operations with Pattern Matching")
    parser.add_argument('--operation', choices=['copy', 'move', 'delete', 'list'], 
                        required=True, help="Operation to perform")
    parser.add_argument('--source-bucket', required=True, help="Source bucket name")
    parser.add_argument('--source-prefix', required=True, help="Source prefix (folder path)")
    parser.add_argument('--dest-bucket', help="Destination bucket name (for copy/move)")
    parser.add_argument('--dest-prefix', help="Destination prefix (for copy/move)")
    
    # Pattern matching options
    pattern_group = parser.add_argument_group('Pattern matching')
    pattern_group.add_argument('--pattern', help="Pattern to filter files by name")
    pattern_group.add_argument('--pattern-type', choices=['glob', 'regex', 'exact'],
                            default='glob', help="Type of pattern matching (default: glob)")
    
    return parser.parse_args()

def main():
    """Main entry point for the script."""
    args = parse_args()
    s3ops = S3DirectOps()
    
    # Set defaults
    if not args.dest_bucket:
        args.dest_bucket = args.source_bucket
    
    # Validate arguments
    if args.operation in ['copy', 'move'] and (not args.dest_bucket or not args.dest_prefix):
        print("Error: --dest-bucket and --dest-prefix are required for copy/move operations")
        sys.exit(1)
    
    # Check buckets exist
    if not s3ops.check_bucket(args.source_bucket):
        sys.exit(1)
    
    if args.operation in ['copy', 'move'] and args.dest_bucket != args.source_bucket:
        if not s3ops.check_bucket(args.dest_bucket):
            sys.exit(1)
    
    # Perform the requested operation
    try:
        if args.operation == 'list':
            # Just list the files
            s3ops.list_direct_files(args.source_bucket, args.source_prefix, 
                                  args.pattern, args.pattern_type)
        elif args.operation == 'copy':
            s3ops.copy_files_with_pattern(
                args.source_bucket, args.source_prefix,
                args.dest_bucket, args.dest_prefix,
                args.pattern, args.pattern_type
            )
        elif args.operation == 'move':
            s3ops.move_files_with_pattern(
                args.source_bucket, args.source_prefix,
                args.dest_bucket, args.dest_prefix,
                args.pattern, args.pattern_type
            )
        elif args.operation == 'delete':
            s3ops.delete_files_with_pattern(
                args.source_bucket, args.source_prefix,
                args.pattern, args.pattern_type
            )
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()