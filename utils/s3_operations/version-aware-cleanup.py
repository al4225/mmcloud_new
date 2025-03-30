#!/usr/bin/env python3
"""
S3 Direct File Operations - For copying, moving, or deleting direct files in S3 folders
Originally created by Achyutha Harish, MemVerge Inc.
With heavy edits from Gao Wang via claude.ai
"""
import boto3
import argparse
import os
import sys
import fnmatch
import re
from botocore.exceptions import ClientError
import datetime
import time
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize S3 client with extended timeout
s3 = boto3.client('s3', config=boto3.session.Config(connect_timeout=60, read_timeout=60))
s3_resource = boto3.resource('s3')

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
    
    def check_prefix_exists(self, bucket, prefix):
        """Check if a prefix exists in the bucket (without creating it)."""
        norm_prefix = self.normalize_prefix(prefix)
        
        try:
            # Check if prefix exists by listing objects
            resp = s3.list_objects_v2(Bucket=bucket, Prefix=norm_prefix, MaxKeys=1)
            return 'Contents' in resp or 'CommonPrefixes' in resp
        except ClientError as e:
            logger.error(f"Error checking prefix '{prefix}': {e}")
            return False
    
    def check_and_create_folder(self, bucket, prefix):
        """Check if a bucket/prefix exists, create if needed."""
        # Verify bucket exists
        try:
            s3.head_bucket(Bucket=bucket)
        except ClientError as e:
            logger.error(f"Error: Cannot access bucket '{bucket}': {e}")
            return False
            
        # Create folder if needed
        try:
            # Check if prefix already exists
            norm_prefix = self.normalize_prefix(prefix)
            resp = s3.list_objects_v2(Bucket=bucket, Prefix=norm_prefix, MaxKeys=1)
            
            # Create folder marker if it doesn't exist
            if 'Contents' not in resp and 'CommonPrefixes' not in resp:
                logger.info(f"Creating folder marker: {norm_prefix}")
                s3.put_object(Bucket=bucket, Key=norm_prefix, Body='')
            
            return True
        except ClientError as e:
            logger.error(f"Error with folder '{prefix}': {e}")
            return False
    
    def list_direct_files(self, bucket, prefix, pattern=None, pattern_type=None):
        """List only direct files (not folders) within a prefix."""
        norm_prefix = self.normalize_prefix(prefix)
        logger.info(f"Listing direct files in '{norm_prefix}'...")
        
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
                        'Name': file_name,
                        'LastModified': obj.get('LastModified')
                    })
            
            # Log the results
            folder_count = len(response.get('CommonPrefixes', []))
            logger.info(f"Found {len(matched_files)} matching files and {folder_count} folders")
            
            if pattern:
                logger.info(f"Files matching pattern '{pattern}':")
                for file in matched_files:
                    logger.info(f"  - {file['Name']}")
            
            if pattern and not matched_files:
                logger.error(f"No files match the pattern '{pattern}'")
                sys.exit(1)
                
            return matched_files
            
        except ClientError as e:
            logger.error(f"Error listing objects: {e}")
            sys.exit(1)
    
    def get_file_timestamp(self, bucket, key):
        """Get the timestamp of a file."""
        try:
            response = s3.head_object(Bucket=bucket, Key=key)
            
            # Check if there's an original timestamp in metadata
            if 'Metadata' in response and 'original-last-modified' in response['Metadata']:
                try:
                    # Try to parse the timestamp from metadata
                    return datetime.datetime.fromisoformat(response['Metadata']['original-last-modified'])
                except (ValueError, TypeError):
                    # If parsing fails, fall back to LastModified
                    pass
            
            return response.get('LastModified')
        except ClientError as e:
            logger.error(f"Error getting timestamp for {key}: {e}")
            return None
    
    def get_object_metadata(self, bucket, key):
        """Get full metadata of an object."""
        try:
            return s3.head_object(Bucket=bucket, Key=key)
        except ClientError as e:
            logger.error(f"Error getting metadata for {key}: {e}")
            return None
    
    def copy_with_metadata_preservation(self, source_bucket, source_key, dest_bucket, dest_key, source_metadata=None):
        """
        Copy a file with complete metadata preservation, including timestamps in metadata.
        
        This method first gets all metadata from the source if not provided,
        then performs a copy operation with metadata preservation.
        """
        try:
            # Get object metadata if not provided
            if not source_metadata:
                source_metadata = self.get_object_metadata(source_bucket, source_key)
                if not source_metadata:
                    return False
            
            # Extract important metadata
            source_timestamp = source_metadata.get('LastModified')
            if not source_timestamp:
                logger.warning(f"Warning: Could not get timestamp for {source_key}")
                return False
            
            # Copy existing metadata and add/update original timestamp
            metadata = source_metadata.get('Metadata', {})
            metadata['original-last-modified'] = source_timestamp.isoformat()
            
            # Get content type and other important headers
            content_type = source_metadata.get('ContentType', 'binary/octet-stream')
            content_disposition = source_metadata.get('ContentDisposition', '')
            content_encoding = source_metadata.get('ContentEncoding', '')
            cache_control = source_metadata.get('CacheControl', '')
            
            # Handle tagging if present
            try:
                tags_response = s3.get_object_tagging(Bucket=source_bucket, Key=source_key)
                tags = tags_response.get('TagSet', [])
            except Exception as e:
                logger.warning(f"Could not get tags for {source_key}: {e}")
                tags = []
            
            # Determine if we need multipart copy based on size
            size = source_metadata.get('ContentLength', 0)
            logger.info(f"Copying {source_key} → {dest_key} (size: {size} bytes)")
            
            if size > MAX_DIRECT_COPY_SIZE:
                logger.info(f"Large file detected ({size} bytes): using multipart copy")
                return self._multipart_copy_with_metadata(
                    source_bucket, source_key, None, dest_bucket, dest_key,
                    metadata, content_type, content_disposition, content_encoding, cache_control, tags
                )
            else:
                # Standard copy with metadata preservation
                copy_args = {
                    'Bucket': dest_bucket,
                    'Key': dest_key,
                    'CopySource': {'Bucket': source_bucket, 'Key': source_key},
                    'MetadataDirective': 'REPLACE',
                    'Metadata': metadata,
                    'ContentType': content_type
                }
                
                # Add optional parameters if they exist
                if content_disposition:
                    copy_args['ContentDisposition'] = content_disposition
                if content_encoding:
                    copy_args['ContentEncoding'] = content_encoding
                if cache_control:
                    copy_args['CacheControl'] = cache_control
                
                s3.copy_object(**copy_args)
                
                # Apply tags if any exist
                if tags:
                    s3.put_object_tagging(
                        Bucket=dest_bucket,
                        Key=dest_key,
                        Tagging={'TagSet': tags}
                    )
                
                logger.info(f"Successfully copied object with metadata: {source_key} → {dest_key}")
                logger.info(f"  Original timestamp: {source_timestamp}")
                
                # Display new timestamp for verification
                new_timestamp = self.get_file_timestamp(dest_bucket, dest_key)
                logger.info(f"  New timestamp: {new_timestamp}")
                logger.info(f"  Original timestamp preserved in metadata as 'original-last-modified'")
                
                return True
        except ClientError as e:
            logger.error(f"Error copying {source_key}: {e}")
            return False
    
    def _multipart_copy_with_metadata(self, source_bucket, source_key, version_id, dest_bucket, dest_key,
                                     metadata, content_type, content_disposition, content_encoding, cache_control, tags):
        """Helper method for multipart copying of large files with metadata preservation."""
        try:
            # Get size information
            if version_id:
                response = s3.head_object(Bucket=source_bucket, Key=source_key, VersionId=version_id)
            else:
                response = s3.head_object(Bucket=source_bucket, Key=source_key)
            size = response['ContentLength']
            
            # Start multipart upload with metadata
            mpu_args = {
                'Bucket': dest_bucket,
                'Key': dest_key,
                'Metadata': metadata,
                'ContentType': content_type
            }
            
            # Add optional parameters if they exist
            if content_disposition:
                mpu_args['ContentDisposition'] = content_disposition
            if content_encoding:
                mpu_args['ContentEncoding'] = content_encoding
            if cache_control:
                mpu_args['CacheControl'] = cache_control
            
            mpu = s3.create_multipart_upload(**mpu_args)
            upload_id = mpu['UploadId']
            
            # Calculate optimal part size (10MB minimum)
            part_size = max(10 * 1024 * 1024, (size // 10000) + 1)
            
            try:
                # Copy parts
                parts = []
                for i, offset in enumerate(range(0, size, part_size), 1):
                    last_byte = min(offset + part_size - 1, size - 1)
                    range_string = f"bytes={offset}-{last_byte}"
                    logger.info(f"  Copying part {i} ({range_string})...")
                    copy_source = {'Bucket': source_bucket, 'Key': source_key}
                    if version_id:
                        copy_source['VersionId'] = version_id
                    
                    part = s3.upload_part_copy(
                        Bucket=dest_bucket,
                        Key=dest_key,
                        UploadId=upload_id,
                        CopySource=copy_source,
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
                
                # Apply tags if any exist
                if tags:
                    s3.put_object_tagging(
                        Bucket=dest_bucket,
                        Key=dest_key,
                        Tagging={'TagSet': tags}
                    )
                
                logger.info(f"Multipart copy completed: {source_key} → {dest_key}")
                return True
            except Exception as e:
                logger.error(f"Error during multipart upload: {e}")
                s3.abort_multipart_upload(Bucket=dest_bucket, Key=dest_key, UploadId=upload_id)
                return False
        except ClientError as e:
            logger.error(f"Error preparing multipart copy: {e}")
            return False
    
    def copy_file(self, source_bucket, source_key, dest_bucket, dest_key, current_version_only=False):
        """
        Copy a file with timestamp preservation.
        Always preserves the timestamp of the most recent version in metadata.
        """
        try:
            # Get the current version's metadata first
            source_metadata = self.get_object_metadata(source_bucket, source_key)
            if not source_metadata:
                return False
            
            # Copy the most recent version with metadata preservation
            if current_version_only:
                # Copy only the current version
                return self.copy_with_metadata_preservation(
                    source_bucket, source_key, dest_bucket, dest_key, source_metadata
                )
            else:
                # Copy all versions including the current one
                logger.info(f"Copying all versions of {source_key} → {dest_key}")
                
                try:
                    # List all versions
                    response = s3.list_object_versions(Bucket=source_bucket, Prefix=source_key)
                    versions = [v for v in response.get('Versions', []) if v.get('Key') == source_key]
                    
                    if not versions:
                        logger.warning(f"Warning: No versions found for {source_key}")
                        return False
                    
                    # Find the current (non-deleted) version first
                    current_version = next((v for v in versions if not v.get('IsDeleted', False)), None)
                    
                    # First copy the current version to preserve metadata
                    if current_version:
                        version_id = current_version.get('VersionId')
                        # Get this version's metadata
                        if version_id:
                            version_metadata = s3.head_object(
                                Bucket=source_bucket, 
                                Key=source_key,
                                VersionId=version_id
                            )
                        else:
                            version_metadata = source_metadata
                        
                        # Copy the current version first with metadata preservation
                        success = self.copy_with_metadata_preservation(
                            source_bucket, source_key, dest_bucket, dest_key, version_metadata
                        )
                        if not success:
                            return False
                    
                    # Copy other versions (if any)
                    for version in versions:
                        version_id = version.get('VersionId')
                        # Skip the current version as we've already copied it
                        if current_version and version_id == current_version.get('VersionId'):
                            continue
                        
                        # Get this version's metadata
                        try:
                            version_metadata = s3.head_object(
                                Bucket=source_bucket, 
                                Key=source_key,
                                VersionId=version_id
                            )
                        except Exception as e:
                            logger.warning(f"Could not get metadata for version {version_id}: {e}")
                            version_metadata = None
                        
                        # Copy this version
                        size = version.get('Size', 0)
                        if size > MAX_DIRECT_COPY_SIZE:
                            logger.info(f"Large file detected: {source_key} (v:{version_id})")
                            copy_source = {'Bucket': source_bucket, 'Key': source_key, 'VersionId': version_id}
                            # For older versions, we're less concerned about metadata preservation
                            s3.copy_object(
                                Bucket=dest_bucket,
                                Key=dest_key,
                                CopySource=copy_source
                            )
                        else:
                            # Standard copy for other versions
                            copy_source = {'Bucket': source_bucket, 'Key': source_key, 'VersionId': version_id}
                            s3.copy_object(
                                Bucket=dest_bucket,
                                Key=dest_key,
                                CopySource=copy_source
                            )
                    
                    return True
                except Exception as e:
                    logger.error(f"Error copying versions for {source_key}: {e}")
                    return False
        except ClientError as e:
            logger.error(f"Error copying {source_key}: {e}")
            return False
    
    def delete_file(self, bucket, key, current_version_only=False):
        """Delete a file (all versions or just current version)."""
        try:
            if current_version_only:
                # Delete only the current version
                logger.info(f"Deleting {key} (current version only)")
                s3.delete_object(Bucket=bucket, Key=key)
                return True
            else:
                # Delete all versions
                logger.info(f"Deleting all versions of {key}")
                
                # Get all versions
                response = s3.list_object_versions(Bucket=bucket, Prefix=key)
                versions = [v for v in response.get('Versions', []) if v.get('Key') == key]
                
                if not versions:
                    logger.warning(f"Warning: No versions found for {key}")
                    return False
                
                # Delete each version
                for version in versions:
                    version_id = version.get('VersionId')
                    s3.delete_object(Bucket=bucket, Key=key, VersionId=version_id)
                
                return True
        except ClientError as e:
            logger.error(f"Error deleting {key}: {e}")
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
                logger.error("Error: Destination bucket and prefix required for copy/move operations")
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
                logger.info("Operation cancelled.")
                return False
        
        # Check if destination exists - treats differently for rename vs. move
        dest_exists = False
        if operation in ['copy', 'move']:
            dest_exists = self.check_prefix_exists(dest_bucket, dest_prefix)
            
            # Create destination if it doesn't exist
            if not self.check_and_create_folder(dest_bucket, dest_prefix):
                logger.error(f"Error: Could not access or create destination {dest_bucket}/{dest_prefix}")
                return False
                
            # Log the operation type
            if dest_exists:
                if merge:
                    logger.info(f"Performing {operation} with merge (files will go directly to destination)")
                else:
                    logger.info(f"Performing {operation} to existing destination (preserving folder structure)")
            else:
                if pattern:
                    logger.info(f"Performing {operation} with pattern (directly to new destination)")
                else:
                    logger.info(f"Performing rename operation (source folder to new destination)")
        
        # Process operations
        success_count = 0
        failed_count = 0
        
        for file in matched_files:
            source_key = file['Key']
            file_name = file['Name']
            
            # Determine path handling mode:
            # 1. If --merge flag is explicitly given, always merge
            # 2. If pattern is specified, always merge
            # 3. If destination does not exist (like a rename), create it directly without subfolder
            # 4. If destination exists, preserve structure with subfolder
            use_merge = merge or (pattern is not None) or (not dest_exists and operation == 'move')
            
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
                
                # Copy the file with our enhanced copy method
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
        
        # Create completion message
        version_str = "current versions only" if current_version_only else "all versions"
        
        if pattern:
            path_str = "directly to destination"
        elif not dest_exists and operation == 'move':
            path_str = "rename operation"
        elif merge:
            path_str = "merged into destination"
        else:
            path_str = "preserving folder structure"
            
        logger.info(f"{operation.capitalize()} operation completed ({version_str}, {path_str}): {success_count} files processed, {failed_count} failed")
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
        logger.error(f"An unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()