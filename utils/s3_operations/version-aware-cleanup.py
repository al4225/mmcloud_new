#!/usr/bin/env python3
"""
S3 Direct File Operations - For copying, moving, or deleting direct files in S3 folders
"""
import boto3
import argparse
import os
import sys
import fnmatch
import re
from botocore.exceptions import ClientError
import logging

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%m-%d %H:%M'
)
logger = logging.getLogger(__name__)

s3 = boto3.client('s3', config=boto3.session.Config(connect_timeout=60, read_timeout=60))
s3_resource = boto3.resource('s3')

MAX_DIRECT_COPY_SIZE = 4 * 1024 * 1024 * 1024

class S3DirectOps:
    def normalize_prefix(self, prefix):
        return prefix.lstrip('/').rstrip('/') + '/'
    
    def get_basename(self, path):
        return os.path.basename(path.rstrip('/'))
    
    def is_recursive_pattern(self, pattern, pattern_type):
        if not pattern:
            return False
            
        if pattern_type == 'glob':
            return '**' in pattern or '/*/' in pattern
        elif pattern_type == 'regex':
            return '/' in pattern and ('.*/' in pattern or '.+/' in pattern)
            
        return False
    
    def match_pattern(self, file_path, pattern, pattern_type, is_full_path=False):
        if not pattern:
            return True
            
        if not is_full_path:
            file_path = os.path.basename(file_path)
        
        pattern_matchers = {
            'glob': lambda f, p: fnmatch.fnmatch(f, p),
            'regex': lambda f, p: bool(re.search(p, f)),
            'exact': lambda f, p: f == p
        }
        
        return pattern_matchers.get(pattern_type, pattern_matchers['glob'])(file_path, pattern)
    
    def check_prefix_exists(self, bucket, prefix):
        norm_prefix = self.normalize_prefix(prefix)
        
        try:
            resp = s3.list_objects_v2(Bucket=bucket, Prefix=norm_prefix, MaxKeys=1)
            return 'Contents' in resp or 'CommonPrefixes' in resp
        except ClientError as e:
            logger.error(f"Error checking prefix '{prefix}': {e}")
            return False
    
    def check_bucket_access(self, bucket):
        try:
            s3.head_bucket(Bucket=bucket)
            return True
        except ClientError as e:
            logger.error(f"Error: Cannot access bucket '{bucket}': {e}")
            return False
    
    def check_and_create_folder(self, bucket, prefix):
        if not self.check_bucket_access(bucket):
            return False
            
        try:
            norm_prefix = self.normalize_prefix(prefix)
            
            if not self.check_prefix_exists(bucket, norm_prefix):
                logger.info(f"Creating folder marker: {norm_prefix}")
                s3.put_object(Bucket=bucket, Key=norm_prefix, Body='')
            
            return True
        except ClientError as e:
            logger.error(f"Error with folder '{prefix}': {e}")
            return False
    
    def list_direct_files(self, bucket, prefix, pattern=None, pattern_type=None):
        norm_prefix = self.normalize_prefix(prefix)
        logger.info(f"Listing direct files in '{norm_prefix}'...")
        
        try:
            response = s3.list_objects_v2(
                Bucket=bucket,
                Prefix=norm_prefix,
                Delimiter='/'
            )
            
            matched_files = []
            
            for obj in response.get('Contents', []):
                key = obj.get('Key')
                
                if key == norm_prefix:
                    continue
                
                file_name = os.path.basename(key)
                if self.match_pattern(file_name, pattern, pattern_type):
                    matched_files.append({
                        'Key': key,
                        'Size': obj.get('Size', 0),
                        'Name': file_name,
                        'LastModified': obj.get('LastModified')
                    })
            
            folder_count = len(response.get('CommonPrefixes', []))
            logger.info(f"Found {len(matched_files)} matching files and {folder_count} folders")
            
            if pattern and matched_files:
                logger.info(f"Files matching pattern '{pattern}':")
                for file in matched_files:
                    logger.info(f"  - {file['Name']}")
            
            if pattern and not matched_files:
                logger.info(f"No files match the pattern '{pattern}'")
                
            return matched_files
            
        except ClientError as e:
            logger.error(f"Error listing objects: {e}")
            return []
            
    def list_folders(self, bucket, prefix):
        norm_prefix = self.normalize_prefix(prefix)
        logger.info(f"Listing folders in '{norm_prefix}'...")
        
        try:
            response = s3.list_objects_v2(
                Bucket=bucket,
                Prefix=norm_prefix,
                Delimiter='/'
            )
            
            folders = []
            
            for prefix_obj in response.get('CommonPrefixes', []):
                prefix_key = prefix_obj.get('Prefix')
                
                if prefix_key == norm_prefix:
                    continue
                
                folder_name = os.path.basename(prefix_key.rstrip('/'))
                folders.append({
                    'Key': prefix_key,
                    'Name': folder_name
                })
            
            return folders
            
        except ClientError as e:
            logger.error(f"Error listing folders: {e}")
            return []
            
    def list_recursive(self, bucket, prefix, pattern=None, pattern_type=None):
        norm_prefix = self.normalize_prefix(prefix)
        logger.info(f"Recursively listing all files in '{norm_prefix}'...")
        
        is_recursive_pattern = False
        if pattern:
            is_recursive_pattern = self.is_recursive_pattern(pattern, pattern_type)
            if is_recursive_pattern:
                logger.info(f"Detected recursive pattern: '{pattern}'. Will match against full paths.")
            else:
                logger.info(f"Using pattern: '{pattern}'. Will match against filenames only.")
        
        try:
            paginator = s3.get_paginator('list_objects_v2')
            all_files = []
            
            for page in paginator.paginate(Bucket=bucket, Prefix=norm_prefix):
                for obj in page.get('Contents', []):
                    key = obj.get('Key')
                    
                    if key == norm_prefix:
                        continue
                    
                    if key.endswith('/') and obj.get('Size', 0) == 0:
                        continue
                    
                    if is_recursive_pattern:
                        relative_path = key[len(norm_prefix):]
                        match = self.match_pattern(relative_path, pattern, pattern_type, is_full_path=True)
                    else:
                        file_name = os.path.basename(key)
                        match = self.match_pattern(file_name, pattern, pattern_type)
                    
                    if match:
                        all_files.append({
                            'Key': key,
                            'Size': obj.get('Size', 0),
                            'Name': os.path.basename(key),
                            'LastModified': obj.get('LastModified')
                        })
            
            logger.info(f"Found {len(all_files)} files recursively")
            
            if pattern and all_files:
                logger.info(f"Files matching pattern '{pattern}':")
                for file in all_files[:10]:
                    logger.info(f"  - {file['Key']}")
                if len(all_files) > 10:
                    logger.info(f"  ... and {len(all_files) - 10} more")
            
            return all_files
            
        except ClientError as e:
            logger.error(f"Error listing objects recursively: {e}")
            return []
    
    def get_object_metadata(self, bucket, key, version_id=None):
        try:
            if version_id:
                return s3.head_object(Bucket=bucket, Key=key, VersionId=version_id)
            else:
                return s3.head_object(Bucket=bucket, Key=key)
        except ClientError as e:
            logger.error(f"Error getting metadata for {key}: {e}")
            return None
    
    def get_object_tags(self, bucket, key, version_id=None):
        try:
            if version_id:
                tags_response = s3.get_object_tagging(
                    Bucket=bucket, Key=key, VersionId=version_id
                )
            else:
                tags_response = s3.get_object_tagging(Bucket=bucket, Key=key)
            return tags_response.get('TagSet', [])
        except Exception as e:
            logger.warning(f"Could not get tags for {key}: {e}")
            return []
    
    def prepare_copy_args(self, metadata, content_type, content_disposition=None, 
                         content_encoding=None, cache_control=None, for_multipart=False):
        copy_args = {
            'Metadata': metadata,
            'ContentType': content_type or 'binary/octet-stream'
        }
        
        if not for_multipart:
            copy_args['MetadataDirective'] = 'REPLACE'
        
        if content_disposition:
            copy_args['ContentDisposition'] = content_disposition
        if content_encoding:
            copy_args['ContentEncoding'] = content_encoding
        if cache_control:
            copy_args['CacheControl'] = cache_control
            
        return copy_args
    
    def apply_tags(self, bucket, key, tags):
        if tags:
            try:
                s3.put_object_tagging(
                    Bucket=bucket,
                    Key=key,
                    Tagging={'TagSet': tags}
                )
                return True
            except Exception as e:
                logger.warning(f"Could not apply tags to {key}: {e}")
                return False
        return True
    
    def copy_with_metadata_preservation(self, source_bucket, source_key, dest_bucket, dest_key, source_metadata=None):
        try:
            if not source_metadata:
                source_metadata = self.get_object_metadata(source_bucket, source_key)
                if not source_metadata:
                    return False
            
            source_timestamp = source_metadata.get('LastModified')
            if not source_timestamp:
                logger.warning(f"Warning: Could not get timestamp for {source_key}")
                return False
            
            metadata = source_metadata.get('Metadata', {})
            metadata['original-last-modified'] = source_timestamp.isoformat()
            
            content_type = source_metadata.get('ContentType', 'binary/octet-stream')
            content_disposition = source_metadata.get('ContentDisposition', '')
            content_encoding = source_metadata.get('ContentEncoding', '')
            cache_control = source_metadata.get('CacheControl', '')
            
            tags = self.get_object_tags(source_bucket, source_key)
            
            size = source_metadata.get('ContentLength', 0)
            logger.info(f"Copying {source_key} → {dest_key} (size: {size} bytes)")
            
            if size > MAX_DIRECT_COPY_SIZE:
                logger.info(f"Large file detected ({size} bytes): using multipart copy")
                success = self._multipart_copy_with_metadata(
                    source_bucket, source_key, None, dest_bucket, dest_key,
                    metadata, content_type, content_disposition, content_encoding, cache_control, tags
                )
            else:
                copy_args = self.prepare_copy_args(
                    metadata, content_type, content_disposition, content_encoding, cache_control
                )
                
                copy_args.update({
                    'Bucket': dest_bucket,
                    'Key': dest_key,
                    'CopySource': {'Bucket': source_bucket, 'Key': source_key}
                })
                
                s3.copy_object(**copy_args)
                
                self.apply_tags(dest_bucket, dest_key, tags)
                
                success = True
                
            if success:
                logger.info(f"Successfully copied object with metadata: {source_key} → {dest_key}")
                
            return success
        except ClientError as e:
            logger.error(f"Error copying {source_key}: {e}")
            return False
    
    def _multipart_copy_with_metadata(self, source_bucket, source_key, version_id, dest_bucket, dest_key,
                                     metadata, content_type, content_disposition, content_encoding, cache_control, tags):
        try:
            response = self.get_object_metadata(source_bucket, source_key, version_id)
            if not response:
                return False
                
            size = response['ContentLength']
            
            mpu_args = self.prepare_copy_args(
                metadata, content_type, content_disposition, content_encoding, cache_control, for_multipart=True
            )
            
            mpu_args.update({
                'Bucket': dest_bucket,
                'Key': dest_key
            })
            
            mpu = s3.create_multipart_upload(**mpu_args)
            upload_id = mpu['UploadId']
            
            target_part_size = 500 * 1024 * 1024  # 500MB
            
            min_part_size = max(5 * 1024 * 1024, size // 10000)  # At least 5MB
            
            part_size = max(target_part_size, min_part_size)
            part_size = min(part_size, MAX_DIRECT_COPY_SIZE - 1)
            
            try:
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
                
                s3.complete_multipart_upload(
                    Bucket=dest_bucket,
                    Key=dest_key,
                    UploadId=upload_id,
                    MultipartUpload={'Parts': parts}
                )
                
                self.apply_tags(dest_bucket, dest_key, tags)
                
                logger.info(f"Multipart copy completed: {source_key} → {dest_key}")
                return True
            except Exception as e:
                logger.error(f"Error during multipart upload: {e}")
                s3.abort_multipart_upload(Bucket=dest_bucket, Key=dest_key, UploadId=upload_id)
                return False
        except Exception as e:
            logger.error(f"Error preparing multipart copy: {e}")
            return False
    
    def get_all_versions(self, bucket, key):
        try:
            response = s3.list_object_versions(Bucket=bucket, Prefix=key)
            return [v for v in response.get('Versions', []) if v.get('Key') == key]
        except Exception as e:
            logger.error(f"Error getting versions for {key}: {e}")
            return []
    
    def copy_file(self, source_bucket, source_key, dest_bucket, dest_key, current_version_only=False):
        try:
            source_metadata = self.get_object_metadata(source_bucket, source_key)
            if not source_metadata:
                return False
            
            if current_version_only:
                return self.copy_with_metadata_preservation(
                    source_bucket, source_key, dest_bucket, dest_key, source_metadata
                )
            else:
                logger.info(f"Copying all versions of {source_key} → {dest_key}")
                
                versions = self.get_all_versions(source_bucket, source_key)
                
                if not versions:
                    logger.warning(f"Warning: No versions found for {source_key}")
                    return False
                
                current_version = next((v for v in versions if not v.get('IsDeleted', False)), None)
                
                if current_version:
                    version_id = current_version.get('VersionId')
                    version_metadata = self.get_object_metadata(
                        source_bucket, source_key, version_id
                    ) or source_metadata
                    
                    success = self.copy_with_metadata_preservation(
                        source_bucket, source_key, dest_bucket, dest_key, version_metadata
                    )
                    if not success:
                        return False
                
                for version in versions:
                    version_id = version.get('VersionId')
                    if current_version and version_id == current_version.get('VersionId'):
                        continue
                    
                    copy_source = {'Bucket': source_bucket, 'Key': source_key, 'VersionId': version_id}
                    s3.copy_object(
                        Bucket=dest_bucket,
                        Key=dest_key,
                        CopySource=copy_source
                    )
                
                return True
        except ClientError as e:
            logger.error(f"Error copying {source_key}: {e}")
            return False
    
    def delete_file(self, bucket, key, current_version_only=False):
        try:
            if current_version_only:
                logger.info(f"Deleting {key} (current version only)")
                s3.delete_object(Bucket=bucket, Key=key)
                return True
            else:
                logger.info(f"Deleting all versions of {key}")
                
                versions = self.get_all_versions(bucket, key)
                
                if not versions:
                    logger.warning(f"Warning: No versions found for {key}")
                    return False
                
                for version in versions:
                    version_id = version.get('VersionId')
                    s3.delete_object(Bucket=bucket, Key=key, VersionId=version_id)
                
                return True
        except ClientError as e:
            logger.error(f"Error deleting {key}: {e}")
            return False
    
    def calculate_destination_path(self, source_prefix, dest_prefix, file_name, use_merge):
        if use_merge:
            return f"{self.normalize_prefix(dest_prefix)}{file_name}"
        else:
            source_base = self.get_basename(source_prefix)
            dest_subfolder = f"{self.normalize_prefix(dest_prefix)}{source_base}"
            return f"{dest_subfolder}/{file_name}"
    
    def process_files(self, operation, source_bucket, source_prefix, dest_bucket=None, 
                     dest_prefix=None, pattern=None, pattern_type=None, current_version_only=False,
                     merge=False):
        if operation == 'list':
            matched_files = self.list_direct_files(source_bucket, source_prefix, pattern, pattern_type)
            return True, matched_files
        
        if operation in ['copy', 'move']:
            if not dest_bucket or not dest_prefix:
                logger.error("Error: Destination bucket and prefix required for copy/move operations")
                return False, None
        
        # By default, use recursive mode except for non-recursive patterns
        is_recursive_pattern = pattern and self.is_recursive_pattern(pattern, pattern_type)
        
        # Handle pattern - if present, determine if it's recursive or not
        if pattern and not is_recursive_pattern:
            logger.info(f"Pattern specified: '{pattern}'. Only searching in current directory.")
            matched_files = self.list_direct_files(source_bucket, source_prefix, pattern, pattern_type)
            use_recursive = False
        elif pattern and is_recursive_pattern:
            logger.info(f"Recursive pattern detected: '{pattern}'. Searching in all subdirectories.")
            matched_files = self.list_recursive(source_bucket, source_prefix, pattern, pattern_type)
            use_recursive = True
        else:
            # No pattern - default to recursive mode
            logger.info(f"Using recursive mode to process files and subfolders")
            matched_files = self.list_recursive(source_bucket, source_prefix, None, None)
            use_recursive = True
            
            # Check for folders
            folders = self.list_folders(source_bucket, source_prefix)
            if folders and not matched_files:
                folder_names = [f['Name'] for f in folders]
                logger.info(f"Found {len(folders)} folders but no direct files: {', '.join(folder_names)}")
        
        # Check if we have any files to process
        if not matched_files:
            if pattern:
                logger.error(f"No files match the pattern '{pattern}'")
                return False, None
            
            # Check for folders when in recursive mode
            folders = self.list_folders(source_bucket, source_prefix)
            if use_recursive and folders and operation in ['copy', 'move']:
                logger.info(f"No matching files found, but {len(folders)} folders exist")
                logger.info(f"Will create corresponding folder structure in destination")
                
                for folder in folders:
                    folder_success, _ = self.process_files(
                        operation, 
                        source_bucket, 
                        folder['Key'], 
                        dest_bucket, 
                        dest_prefix, 
                        pattern, 
                        pattern_type, 
                        current_version_only, 
                        merge
                    )
                    
                    if not folder_success:
                        logger.error(f"Failed to process folder: {folder['Key']}")
                        return False, None
                
                return True, []
            
            logger.warning(f"No files found in '{source_prefix}'")
            return True, []
        
        # Ask confirmation for delete
        if operation in ['delete', 'move'] and pattern:
            version_str = "current versions only" if current_version_only else "all versions"
            confirmation = input(f"Are you sure you want to {operation} {len(matched_files)} files ({version_str})? (y/n): ")
            if confirmation.lower() != 'y':
                logger.info("Operation cancelled.")
                return False, None
        
        # Check destination for copy/move operations
        dest_exists = False
        if operation in ['copy', 'move']:
            dest_exists = self.check_prefix_exists(dest_bucket, dest_prefix)
            
            if not self.check_and_create_folder(dest_bucket, dest_prefix):
                logger.error(f"Error: Could not access or create destination {dest_bucket}/{dest_prefix}")
                return False, None
                
            # Determine merge mode
            use_merge = merge or (pattern is not None) or (not dest_exists and operation == 'move')
            
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
        
        # Define operation functions
        op_functions = {
            'delete': lambda file: self.delete_file(source_bucket, file['Key'], current_version_only),
            'copy': lambda file: self._process_copy_or_move(file, source_bucket, dest_bucket, source_prefix, dest_prefix, 
                                                          use_merge, current_version_only, False),
            'move': lambda file: self._process_copy_or_move(file, source_bucket, dest_bucket, source_prefix, dest_prefix, 
                                                          use_merge, current_version_only, True)
        }
        
        # Process each file
        for file in matched_files:
            if operation in op_functions:
                success = op_functions[operation](file)
                
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
        
        # Return overall success status
        return failed_count == 0, matched_files
    
    def _process_copy_or_move(self, file, source_bucket, dest_bucket, source_prefix, dest_prefix, 
                             use_merge, current_version_only, is_move):
        source_key = file['Key']
        file_name = file['Name']
        
        # For recursive operations, preserve the directory structure
        if source_key.startswith(source_prefix) and '/' in source_key[len(source_prefix):]:
            # This is a file in a subfolder, preserve the path structure
            relative_path = source_key[len(source_prefix):]
            dest_key = f"{self.normalize_prefix(dest_prefix)}{relative_path}"
            
            # Ensure parent folders exist
            parent_folders = os.path.dirname(dest_key)
            if parent_folders:
                self.check_and_create_folder(dest_bucket, parent_folders)
        else:
            # Calculate destination path for direct files
            dest_key = self.calculate_destination_path(source_prefix, dest_prefix, file_name, use_merge)
            
            # Ensure subfolder exists if not merging
            if not use_merge:
                source_base = self.get_basename(source_prefix)
                dest_subfolder = f"{self.normalize_prefix(dest_prefix)}{source_base}"
                self.check_and_create_folder(dest_bucket, dest_subfolder)
        
        # Copy the file
        success = self.copy_file(source_bucket, source_key, dest_bucket, dest_key, current_version_only)
        
        # For move, also delete if copy was successful
        if is_move and success:
            success = self.delete_file(source_bucket, source_key, current_version_only)
            
        return success

def parse_args():
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
    args = parse_args()
    s3ops = S3DirectOps()
    
    # Set defaults
    if not args.dest_bucket:
        args.dest_bucket = args.source_bucket
    
    try:
        # Process files and capture the result
        success, matched_files = s3ops.process_files(
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
        
        # Exit with error if any operation failed
        if not success:
            logger.error("One or more file operations failed. Exiting with error.")
            sys.exit(1)
            
        # For list operation, always return success
        if args.operation == 'list':
            sys.exit(0)
            
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()