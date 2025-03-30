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
    def normalize_prefix(prefix):
        """Normalize a prefix path (remove leading, ensure trailing slash)."""
        return prefix.lstrip('/').rstrip('/') + '/'
    
    @staticmethod
    def get_basename(path):
        """Get the base name (last component) of a path."""
        return os.path.basename(path.rstrip('/'))
    
    @staticmethod
    def match_pattern(filename, pattern, pattern_type):
        """Match a filename against a pattern."""
        if not pattern:
            return True
        
        if pattern_type == 'glob':
            return fnmatch.fnmatch(filename, pattern)
        elif pattern_type == 'regex':
            return bool(re.search(pattern, filename))
        elif pattern_type == 'exact':
            return filename == pattern
        return fnmatch.fnmatch(filename, pattern)
    
    def check_and_create_folder(self, bucket, prefix):
        """Check if a bucket/prefix exists, create if needed."""
        # Verify bucket exists
        try:
            s3.head_bucket(Bucket=bucket)
        except ClientError as e:
            print(f"Error: Cannot access bucket '{bucket}': {e}")
            return False
            
        # Create folder if needed
        try:
            # Check if prefix already exists
            norm_prefix = self.normalize_prefix(prefix)
            resp = s3.list_objects_v2(Bucket=bucket, Prefix=norm_prefix, MaxKeys=1)
            
            # Create folder marker if it doesn't exist
            if 'Contents' not in resp and 'CommonPrefixes' not in resp:
                print(f"Creating folder marker: {norm_prefix}")
                s3.put_object(Bucket=bucket, Key=norm_prefix, Body='')
            
            return True
        except ClientError as e:
            print(f"Error with folder '{prefix}': {e}")
            return False
    
    def list_direct_files(self, bucket, prefix, pattern=None, pattern_type=None):
        """List only direct files (not folders) within a prefix."""
        norm_prefix = self.normalize_prefix(prefix)
        print(f"Listing direct files in '{norm_prefix}'...")
        
        try:
            # List objects with delimiter to get only direct children
            response = s3.list_objects_v2(
                Bucket=bucket,
                Prefix=norm_prefix,
                Delimiter='/'
            )
            
            matched_files = []
            
            # Process direct files
            for obj in response.get('Contents', []):
                key = obj.get('Key')
                
                # Skip folder marker itself
                if key == norm_prefix:
                    continue
                
                # Apply pattern matching to filename
                file_name = os.path.basename(key)
                if self.match_pattern(file_name, pattern, pattern_type):
                    matched_files.append({
                        'Key': key,
                        'Size': obj.get('Size', 0),
                        'Name': file_name
                    })
            
            # Log the results
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
    
    def copy_file(self, source_bucket, source_key, dest_bucket, dest_key, current_version_only=False):
        """Copy a file (with all versions or just current version)."""
        try:
            if current_version_only:
                # Copy only the current version
                print(f"Copying {source_key} (current version only) → {dest_key}")
                s3.copy_object(
                    Bucket=dest_bucket, 
                    Key=dest_key, 
                    CopySource={'Bucket': source_bucket, 'Key': source_key}
                )
                return True
            else:
                # Copy all versions
                return self.copy_with_versions(source_bucket, source_key, dest_bucket, dest_key)
        except ClientError as e:
            print(f"Error copying {source_key}: {e}")
            return False
    
    def copy_with_versions(self, source_bucket, source_key, dest_bucket, dest_key):
        """Copy a file with all its versions."""
        try:
            # Get all versions
            response = s3.list_object_versions(Bucket=source_bucket, Prefix=source_key)
            versions = [v for v in response.get('Versions', []) if v.get('Key') == source_key]
            
            if not versions:
                print(f"Warning: No versions found for {source_key}")
                return False
            
            # Copy each version
            for version in versions:
                version_id = version.get('VersionId')
                size = version.get('Size', 0)
                
                copy_source = {
                    'Bucket': source_bucket,
                    'Key': source_key,
                    'VersionId': version_id
                }
                
                if size > MAX_DIRECT_COPY_SIZE:
                    # Use multipart copy for large files
                    print(f"Large file detected: {source_key} (v:{version_id})")
                    self._multipart_copy(source_bucket, source_key, version_id, dest_bucket, dest_key)
                else:
                    # Standard copy for regular files
                    print(f"Copying {source_key} (v:{version_id}) → {dest_key}")
                    s3.copy_object(Bucket=dest_bucket, Key=dest_key, CopySource=copy_source)
            
            return True
            
        except ClientError as e:
            print(f"Error copying {source_key}: {e}")
            return False
    
    def _multipart_copy(self, source_bucket, source_key, version_id, dest_bucket, dest_key):
        """Helper method for multipart copying of large files."""
        try:
            # Get size information
            response = s3.head_object(
                Bucket=source_bucket, 
                Key=source_key,
                VersionId=version_id
            )
            size = response['ContentLength']
            
            # Start multipart upload
            mpu = s3.create_multipart_upload(Bucket=dest_bucket, Key=dest_key)
            upload_id = mpu['UploadId']
            
            # Calculate optimal part size (10MB minimum)
            part_size = max(10 * 1024 * 1024, (size // 10000) + 1)
            
            try:
                # Copy parts
                parts = []
                for i, offset in enumerate(range(0, size, part_size), 1):
                    last_byte = min(offset + part_size - 1, size - 1)
                    range_string = f"bytes={offset}-{last_byte}"
                    
                    print(f"  Copying part {i} ({range_string})...")
                    part = s3.upload_part_copy(
                        Bucket=dest_bucket,
                        Key=dest_key,
                        UploadId=upload_id,
                        CopySource={
                            'Bucket': source_bucket,
                            'Key': source_key,
                            'VersionId': version_id
                        },
                        CopySourceRange=range_string,
                        PartNumber=i
                    )
                    
                    parts.append({
                        'PartNumber': i,
                        'ETag': part['CopyPartResult']['ETag']
                    })
                
                # Complete the upload
                s3.complete_multipart_upload(
                    Bucket=dest_bucket,
                    Key=dest_key,
                    UploadId=upload_id,
                    MultipartUpload={'Parts': parts}
                )
                print(f"Multipart copy completed: {source_key} → {dest_key}")
                return True
                
            except Exception as e:
                # Abort on failure
                print(f"Error during multipart upload: {e}")
                s3.abort_multipart_upload(
                    Bucket=dest_bucket,
                    Key=dest_key,
                    UploadId=upload_id
                )
                return False
                
        except ClientError as e:
            print(f"Error preparing multipart copy: {e}")
            return False
    
    def delete_file(self, bucket, key, current_version_only=False):
        """Delete a file (all versions or just current version)."""
        try:
            if current_version_only:
                # Delete only the current version
                print(f"Deleting {key} (current version only)")
                s3.delete_object(Bucket=bucket, Key=key)
                return True
            else:
                # Delete all versions
                return self.delete_with_versions(bucket, key)
        except ClientError as e:
            print(f"Error deleting {key}: {e}")
            return False
    
    def delete_with_versions(self, bucket, key):
        """Delete a file with all its versions."""
        try:
            # Get all versions
            response = s3.list_object_versions(Bucket=bucket, Prefix=key)
            versions = [v for v in response.get('Versions', []) if v.get('Key') == key]
            
            if not versions:
                print(f"Warning: No versions found for {key}")
                return False
            
            # Delete each version
            for version in versions:
                version_id = version.get('VersionId')
                print(f"Deleting {key} (v:{version_id})")
                s3.delete_object(Bucket=bucket, Key=key, VersionId=version_id)
            
            return True
            
        except ClientError as e:
            print(f"Error deleting {key}: {e}")
            return False
    
    def process_files(self, operation, source_bucket, source_prefix, dest_bucket=None, 
                     dest_prefix=None, pattern=None, pattern_type=None, current_version_only=False,
                     merge=False):
        """Process files with given operation (copy/move/delete/list)."""
        # For list operation, just return the matched files
        if operation == 'list':
            return self.list_direct_files(source_bucket, source_prefix, pattern, pattern_type)
        
        # For copy/move, verify destination
        if operation in ['copy', 'move']:
            if not dest_bucket or not dest_prefix:
                print("Error: Destination bucket and prefix required for copy/move operations")
                return False
                
            if not self.check_and_create_folder(dest_bucket, dest_prefix):
                print(f"Error: Could not access or create destination {dest_bucket}/{dest_prefix}")
                return False
        
        # Get matching files
        matched_files = self.list_direct_files(source_bucket, source_prefix, pattern, pattern_type)
        if not matched_files:
            return False
        
        # Ask confirmation for delete
        if operation in ['delete', 'move'] and pattern:
            version_str = "current versions only" if current_version_only else "all versions"
            confirmation = input(f"Are you sure you want to {operation} {len(matched_files)} files ({version_str})? (y/n): ")
            if confirmation.lower() != 'y':
                print("Operation cancelled.")
                return False
        
        # Process operations
        success_count = 0
        failed_count = 0
        
        for file in matched_files:
            source_key = file['Key']
            file_name = file['Name']
            
            # When pattern is specified, ALWAYS use direct paths (merge behavior)
            use_merge = merge or (pattern is not None)
            
            # Perform the operation
            if operation == 'delete':
                success = self.delete_file(source_bucket, source_key, current_version_only)
            elif operation in ['copy', 'move']:
                # Calculate destination path
                if use_merge:
                    # When merging, files go directly to the destination folder
                    dest_key = f"{self.normalize_prefix(dest_prefix)}{file_name}"
                else:
                    # When preserving structure, files go to a subfolder named after the source folder
                    source_base = self.get_basename(source_prefix)
                    dest_subfolder = f"{self.normalize_prefix(dest_prefix)}{source_base}"
                    dest_key = f"{dest_subfolder}/{file_name}"
                    
                    # Ensure the subfolder exists
                    self.check_and_create_folder(dest_bucket, dest_subfolder)
                
                # Copy the file
                success = self.copy_file(source_bucket, source_key, dest_bucket, dest_key, current_version_only)
                
                # For move, also delete if copy was successful
                if operation == 'move' and success:
                    success = self.delete_file(source_bucket, source_key, current_version_only)
            else:
                success = False
            
            if success:
                success_count += 1
            else:
                failed_count += 1
        
        version_str = "current versions only" if current_version_only else "all versions"
        if pattern:
            merge_str = "directly to destination"
        else:
            merge_str = "merged into destination" if merge else "preserving folder structure"
            
        print(f"{operation.capitalize()} operation completed ({version_str}, {merge_str}): {success_count} files processed, {failed_count} failed")
        return failed_count == 0

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
    
    # Version handling options
    version_group = parser.add_argument_group('Version handling')
    version_group.add_argument('--current-version-only', action='store_true',
                            help="Only operate on current versions (ignore history)")
    
    # Path structure options
    path_group = parser.add_argument_group('Path handling')
    path_group.add_argument('--merge', action='store_true',
                         help="Merge files directly into destination (don't preserve source folder)")
    
    return parser.parse_args()

def main():
    """Main entry point for the script."""
    args = parse_args()
    s3ops = S3DirectOps()
    
    # Set defaults
    if not args.dest_bucket:
        args.dest_bucket = args.source_bucket
    
    try:
        s3ops.process_files(
            args.operation,
            args.source_bucket,
            args.source_prefix,
            args.dest_bucket,
            args.dest_prefix,
            args.pattern,
            args.pattern_type,
            args.current_version_only,
            args.merge
        )
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()