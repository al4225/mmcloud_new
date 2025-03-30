#!/usr/bin/env python3
"""
Improved S3 File Operations - For copying, moving, or deleting files in S3 folders
with proper handling of directory structures 
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
    format='%(asctime)s %(message)s',
    datefmt='%m-%d %H:%M'
)
logger = logging.getLogger(__name__)

# Initialize S3 clients
s3 = boto3.client('s3', config=boto3.session.Config(connect_timeout=60, read_timeout=60))
s3_resource = boto3.resource('s3')

# Constants
MAX_DIRECT_COPY_SIZE = 4 * 1024 * 1024 * 1024  # 4GB

# Helper functions
def normalize_prefix(prefix):
    """Normalize S3 prefix by fixing consecutive slashes and ensuring trailing slash"""
    normalized = re.sub(r'/+', '/', prefix.strip('/')) + '/'
    return normalized

def get_basename(path):
    """Get the last path component, handling potential double slashes"""
    return os.path.basename(path.rstrip('/').replace('//', '/'))

def is_recursive_pattern(pattern, pattern_type):
    """Determine if a pattern is recursive (applies to subdirectories)"""
    if not pattern:
        return False
        
    if pattern_type == 'glob':
        return '**' in pattern or '/*/' in pattern
    elif pattern_type == 'regex':
        return '/' in pattern and ('.*/' in pattern or '.+/' in pattern)
        
    return False

def match_pattern(file_path, pattern, pattern_type, is_full_path=False):
    """Match a file path against a pattern"""
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

def check_prefix_exists(bucket, prefix):
    """Check if a prefix exists in the S3 bucket"""
    norm_prefix = normalize_prefix(prefix)
    
    try:
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=norm_prefix, MaxKeys=1)
        return 'Contents' in resp or 'CommonPrefixes' in resp
    except ClientError as e:
        logger.error(f"Error checking prefix '{prefix}': {e}")
        return False

def check_bucket_access(bucket):
    """Check if the bucket exists and is accessible"""
    try:
        s3.head_bucket(Bucket=bucket)
        return True
    except ClientError as e:
        logger.error(f"Error: Cannot access bucket '{bucket}': {e}")
        return False

def check_and_create_folder(bucket, prefix, dryrun=False):
    """Check if a folder exists and create it if not"""
    if not check_bucket_access(bucket):
        return False
        
    try:
        norm_prefix = normalize_prefix(prefix)
        
        if not check_prefix_exists(bucket, norm_prefix):
            if not dryrun:
                logger.info(f"Creating folder marker: {norm_prefix}")
                s3.put_object(Bucket=bucket, Key=norm_prefix, Body='')
            else:
                logger.info(f"[DRYRUN] Would create folder marker: {norm_prefix}")
        
        return True
    except ClientError as e:
        logger.error(f"Error with folder '{prefix}': {e}")
        return False

def list_direct_files(bucket, prefix, pattern=None, pattern_type=None):
    """List files directly in a prefix (not in subfolders)"""
    norm_prefix = normalize_prefix(prefix)
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
            if match_pattern(file_name, pattern, pattern_type):
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

def list_folders(bucket, prefix):
    """List folders in a prefix"""
    norm_prefix = normalize_prefix(prefix)
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

def list_recursive(bucket, prefix, pattern=None, pattern_type=None):
    """Recursively list all files in a prefix including subfolders"""
    norm_prefix = normalize_prefix(prefix)
    logger.info(f"Recursively listing all files in '{norm_prefix}'...")
    
    is_recursive_pattern_flag = False
    if pattern:
        is_recursive_pattern_flag = is_recursive_pattern(pattern, pattern_type)
        if is_recursive_pattern_flag:
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
                
                if is_recursive_pattern_flag:
                    relative_path = key[len(norm_prefix):]
                    match = match_pattern(relative_path, pattern, pattern_type, is_full_path=True)
                else:
                    file_name = os.path.basename(key)
                    match = match_pattern(file_name, pattern, pattern_type)
                
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

def get_object_metadata(bucket, key, version_id=None):
    """Get metadata for an S3 object"""
    try:
        if version_id:
            return s3.head_object(Bucket=bucket, Key=key, VersionId=version_id)
        else:
            return s3.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        logger.error(f"Error getting metadata for {key}: {e}")
        return None

def get_object_tags(bucket, key, version_id=None):
    """Get tags for an S3 object"""
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

def prepare_copy_args(metadata, content_type, content_disposition=None, 
                     content_encoding=None, cache_control=None, for_multipart=False):
    """Prepare arguments for copy operations"""
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

def apply_tags(bucket, key, tags, dryrun=False):
    """Apply tags to an S3 object"""
    if tags:
        try:
            if not dryrun:
                s3.put_object_tagging(
                    Bucket=bucket,
                    Key=key,
                    Tagging={'TagSet': tags}
                )
                return True
            else:
                logger.info(f"[DRYRUN] Would apply {len(tags)} tags to {key}")
                return True
        except Exception as e:
            logger.warning(f"Could not apply tags to {key}: {e}")
            return False
    return True

def multipart_copy_with_metadata(source_bucket, source_key, version_id, dest_bucket, dest_key,
                                metadata, content_type, content_disposition, content_encoding, 
                                cache_control, tags, dryrun=False):
    """Perform a multipart copy for large files"""
    try:
        response = get_object_metadata(source_bucket, source_key, version_id)
        if not response:
            return False
            
        size = response['ContentLength']
        
        if dryrun:
            logger.info(f"[DRYRUN] Would perform multipart copy: {source_key} → {dest_key} ({size} bytes)")
            return True
        
        mpu_args = prepare_copy_args(
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
            
            apply_tags(dest_bucket, dest_key, tags)
            
            logger.info(f"Multipart copy completed: {source_key} → {dest_key}")
            return True
        except Exception as e:
            logger.error(f"Error during multipart upload: {e}")
            s3.abort_multipart_upload(Bucket=dest_bucket, Key=dest_key, UploadId=upload_id)
            return False
    except Exception as e:
        logger.error(f"Error preparing multipart copy: {e}")
        return False

def copy_with_metadata_preservation(source_bucket, source_key, dest_bucket, dest_key, 
                                   source_metadata=None, dryrun=False):
    """Copy a file with preservation of metadata"""
    try:
        if not source_metadata:
            source_metadata = get_object_metadata(source_bucket, source_key)
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
        
        tags = get_object_tags(source_bucket, source_key)
        
        size = source_metadata.get('ContentLength', 0)
        
        if dryrun:
            logger.info(f"[DRYRUN] Would copy {source_key} → {dest_key} (size: {size} bytes)")
            logger.info(f"[DRYRUN] Original timestamp: {source_timestamp}")
            return True
        
        logger.info(f"Copying {source_key} → {dest_key} (size: {size} bytes)")
        
        if size > MAX_DIRECT_COPY_SIZE:
            logger.info(f"Large file detected ({size} bytes): using multipart copy")
            success = multipart_copy_with_metadata(
                source_bucket, source_key, None, dest_bucket, dest_key,
                metadata, content_type, content_disposition, content_encoding, cache_control, tags,
                dryrun
            )
        else:
            if not dryrun:
                copy_args = prepare_copy_args(
                    metadata, content_type, content_disposition, content_encoding, cache_control
                )
                
                copy_args.update({
                    'Bucket': dest_bucket,
                    'Key': dest_key,
                    'CopySource': {'Bucket': source_bucket, 'Key': source_key}
                })
                
                s3.copy_object(**copy_args)
                
                apply_tags(dest_bucket, dest_key, tags)
            
            success = True
            
        if success:
            logger.info(f"{'[DRYRUN] Would copy' if dryrun else 'Successfully copied'} object with metadata: {source_key} → {dest_key}")
            
        return success
    except ClientError as e:
        logger.error(f"Error copying {source_key}: {e}")
        return False

def get_all_versions(bucket, key):
    """Get all versions of an object"""
    try:
        response = s3.list_object_versions(Bucket=bucket, Prefix=key)
        return [v for v in response.get('Versions', []) if v.get('Key') == key]
    except Exception as e:
        logger.error(f"Error getting versions for {key}: {e}")
        return []

def copy_file(source_bucket, source_key, dest_bucket, dest_key, current_version_only=False, dryrun=False):
    """Copy a file with optional version handling"""
    try:
        source_metadata = get_object_metadata(source_bucket, source_key)
        if not source_metadata:
            return False
        
        if current_version_only:
            return copy_with_metadata_preservation(
                source_bucket, source_key, dest_bucket, dest_key, source_metadata, dryrun
            )
        else:
            if dryrun:
                logger.info(f"[DRYRUN] Would copy all versions of {source_key} → {dest_key}")
            else:
                logger.info(f"Copying all versions of {source_key} → {dest_key}")
            
            versions = get_all_versions(source_bucket, source_key)
            
            if not versions:
                logger.warning(f"Warning: No versions found for {source_key}")
                return False
            
            current_version = next((v for v in versions if not v.get('IsDeleted', False)), None)
            
            if current_version:
                version_id = current_version.get('VersionId')
                version_metadata = get_object_metadata(
                    source_bucket, source_key, version_id
                ) or source_metadata
                
                success = copy_with_metadata_preservation(
                    source_bucket, source_key, dest_bucket, dest_key, version_metadata, dryrun
                )
                if not success:
                    return False
            
            if not dryrun:
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
            else:
                logger.info(f"[DRYRUN] Would copy {len(versions)} additional versions")
            
            return True
    except ClientError as e:
        logger.error(f"Error copying {source_key}: {e}")
        return False

def delete_file(bucket, key, current_version_only=False, dryrun=False):
    """Delete a file with optional version handling"""
    try:
        if current_version_only:
            if dryrun:
                logger.info(f"[DRYRUN] Would delete {key} (current version only)")
            else:
                logger.info(f"Deleting {key} (current version only)")
                s3.delete_object(Bucket=bucket, Key=key)
            return True
        else:
            if dryrun:
                logger.info(f"[DRYRUN] Would delete all versions of {key}")
            else:
                logger.info(f"Deleting all versions of {key}")
            
            versions = get_all_versions(bucket, key)
            
            if not versions:
                logger.warning(f"Warning: No versions found for {key}")
                return False
            
            if not dryrun:
                for version in versions:
                    version_id = version.get('VersionId')
                    s3.delete_object(Bucket=bucket, Key=key, VersionId=version_id)
            else:
                logger.info(f"[DRYRUN] Would delete {len(versions)} versions")
            
            return True
    except ClientError as e:
        logger.error(f"Error deleting {key}: {e}")
        return False

def calculate_destination_key(source_key, source_prefix, dest_prefix, merge=False):
    """Calculate the destination key based on the merge option"""
    norm_source_prefix = normalize_prefix(source_prefix)
    norm_dest_prefix = normalize_prefix(dest_prefix)
    
    source_basename = get_basename(source_prefix)
    dest_basename = get_basename(dest_prefix)
    
    # If source folder and destination folder have the same name, we always merge
    # This is the special case mentioned in the requirements
    folders_have_same_name = source_basename == dest_basename and source_basename and dest_basename
    
    # Get the relative path after the source prefix
    if source_key.startswith(norm_source_prefix):
        relative_path = source_key[len(norm_source_prefix):]
        
        if merge or folders_have_same_name:
            # When merging, we put contents directly in the destination folder
            return f"{norm_dest_prefix}{relative_path}"
        else:
            # Regular case: create a subfolder with the source's basename
            return f"{norm_dest_prefix}{source_basename}/{relative_path}"
    else:
        # Fallback if the source key doesn't start with the source prefix
        filename = os.path.basename(source_key)
        return f"{norm_dest_prefix}{filename}"

def process_copy_or_move(file, source_bucket, dest_bucket, source_prefix, dest_prefix,
                        merge, current_version_only, is_move, dryrun=False):
    """Process a copy or move operation for a single file"""
    source_key = file['Key']
    
    # Create the destination key based on the merge option
    dest_key = calculate_destination_key(source_key, source_prefix, dest_prefix, merge)
    
    # Ensure parent folders exist in the destination
    if '/' in dest_key:
        parent_folder = os.path.dirname(dest_key)
        if parent_folder:
            check_and_create_folder(dest_bucket, parent_folder, dryrun)
    
    # Copy the file
    success = copy_file(source_bucket, source_key, dest_bucket, dest_key, current_version_only, dryrun)
    
    # For move operations, delete the source after successful copy
    if is_move and success and not dryrun:
        success = delete_file(source_bucket, source_key, current_version_only)
    elif is_move and success and dryrun:
        logger.info(f"[DRYRUN] Would delete {source_key} after successful copy")
        
    return success

def process_files(operation, source_bucket, source_prefix, dest_bucket=None, 
                dest_prefix=None, pattern=None, pattern_type=None, current_version_only=False,
                merge=False, dryrun=False, dryrun_count=100):
    """Main function to process files based on operation type"""
    # For list operation, just return the matching files
    if operation == 'list':
        matched_files = list_direct_files(source_bucket, source_prefix, pattern, pattern_type)
        return True, matched_files
    
    # For copy/move operations, validate destination parameters
    if operation in ['copy', 'move']:
        if not dest_bucket or not dest_prefix:
            logger.error("Error: Destination bucket and prefix required for copy/move operations")
            return False, None
    
    # Determine if we should use recursive mode
    is_recursive_pattern_flag = pattern and is_recursive_pattern(pattern, pattern_type)
    
    # Get files based on pattern and recursion settings
    if pattern and not is_recursive_pattern_flag:
        logger.info(f"Pattern specified: '{pattern}'. Only searching in current directory.")
        matched_files = list_direct_files(source_bucket, source_prefix, pattern, pattern_type)
        use_recursive = False
    elif pattern and is_recursive_pattern_flag:
        logger.info(f"Recursive pattern detected: '{pattern}'. Searching in all subdirectories.")
        matched_files = list_recursive(source_bucket, source_prefix, pattern, pattern_type)
        use_recursive = True
    else:
        # Default behavior: recursive mode
        logger.info(f"Using recursive mode to process files and subfolders")
        matched_files = list_recursive(source_bucket, source_prefix, None, None)
        use_recursive = True
        
        # Check for folders when no files are found
        if not matched_files:
            folders = list_folders(source_bucket, source_prefix)
            if folders:
                folder_names = [f['Name'] for f in folders]
                logger.info(f"Found {len(folders)} folders but no direct files: {', '.join(folder_names)}")
    
    # Handle case with no matching files
    if not matched_files:
        if pattern:
            logger.error(f"No files match the pattern '{pattern}'")
            return False, None
        
        # For recursive operations with folders but no files
        folders = list_folders(source_bucket, source_prefix)
        if use_recursive and folders and operation in ['copy', 'move']:
            logger.info(f"No matching files found, but {len(folders)} folders exist")
            logger.info(f"Will create corresponding folder structure in destination")
            
            all_folder_success = True
            for folder in folders:
                folder_success, _ = process_files(
                    operation, 
                    source_bucket, 
                    folder['Key'], 
                    dest_bucket, 
                    dest_prefix, 
                    pattern, 
                    pattern_type, 
                    current_version_only, 
                    merge,
                    dryrun,
                    dryrun_count
                )
                
                if not folder_success:
                    logger.error(f"Failed to process folder: {folder['Key']}")
                    all_folder_success = False
            
            return all_folder_success, []
        
        logger.warning(f"No files found in '{source_prefix}'")
        return True, []
    
    if dryrun:
        display_count = len(matched_files) if dryrun_count == -1 else min(dryrun_count, len(matched_files))
        logger.info(f"[DRYRUN] Would {operation} {len(matched_files)} files. Showing {display_count}:")
        for idx, file in enumerate(matched_files):
            if idx >= display_count:
                break
            if operation in ['copy', 'move']:
                source_key = file['Key']
                dest_key = calculate_destination_key(source_key, source_prefix, dest_prefix, merge)
                logger.info(f"  {idx+1}. {source_key} → {dest_key} ({file['Size']} bytes)")
            else:
                logger.info(f"  {idx+1}. {file['Key']} ({file['Size']} bytes)")
        
        if display_count < len(matched_files):
            logger.info(f"  ... and {len(matched_files) - display_count} more files")
    
    # Ask for confirmation for destructive operations unless in dryrun mode
    if not dryrun and operation in ['delete', 'move'] and pattern:
        version_str = "current versions only" if current_version_only else "all versions"
        confirmation = input(f"Are you sure you want to {operation} {len(matched_files)} files ({version_str})? (y/n): ")
        if confirmation.lower() != 'y':
            logger.info("Operation cancelled.")
            return False, None
    
    # Check destination for copy/move operations
    if operation in ['copy', 'move']:
        dest_exists = check_prefix_exists(dest_bucket, dest_prefix)
        
        if not check_and_create_folder(dest_bucket, dest_prefix, dryrun):
            logger.error(f"Error: Could not access or create destination {dest_bucket}/{dest_prefix}")
            return False, None
    
    if dryrun:
        logger.info(f"[DRYRUN] Operation completed. No actual changes were made.")
        return True, matched_files
    
    success_count = 0
    failed_count = 0
    
    for file in matched_files:
        if operation == 'delete':
            success = delete_file(source_bucket, file['Key'], current_version_only, dryrun)
        elif operation in ['copy', 'move']:
            success = process_copy_or_move(
                file, source_bucket, dest_bucket, source_prefix, dest_prefix,
                merge, current_version_only, operation == 'move', dryrun
            )
        else:
            logger.error(f"Unknown operation: {operation}")
            return False, None
            
        if success:
            success_count += 1
        else:
            failed_count += 1
    
    # Create completion message
    version_str = "current versions only" if current_version_only else "all versions"
    merge_str = "merging contents directly" if merge else "preserving folder structure"
    
    msg_prefix = "[DRYRUN] Would complete" if dryrun else ""
    logger.info(f"{msg_prefix} {operation.capitalize()} operation ({version_str}, {merge_str}): {success_count} files processed, {failed_count} failed")
    
    # Return overall success status
    return failed_count == 0, matched_files


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Improved S3 File Operations with Pattern Matching")
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
    
    # Path handling option
    path_group = parser.add_argument_group('Path handling')
    path_group.add_argument('--merge', action='store_true',
                         help="Merge contents directly into destination folder without creating subfolders")
    
    # Dryrun option
    dryrun_group = parser.add_argument_group('Dryrun options')
    dryrun_group.add_argument('--dryrun', type=int, nargs='?', const=100, default=None, metavar='COUNT',
                           help="Print operations without executing them. Optional number of items to display (default: 100, -1 for all)")
    
    args = parser.parse_args()
    
    # Set defaults for destination bucket if not specified
    if args.operation in ['copy', 'move'] and not args.dest_bucket:
        args.dest_bucket = args.source_bucket
    
    return args

def main():
    """Main entry point for the script"""
    args = parse_args()
    
    try:
        # Set dryrun count (default to 100 if dryrun specified with no value)
        dryrun = args.dryrun is not None
        dryrun_count = args.dryrun if dryrun else 100
        
        if dryrun:
            logger.info(f"*** DRY RUN MODE ENABLED - No changes will be made ***")
            if dryrun_count == -1:
                logger.info("Will show all items that would be processed")
            else:
                logger.info(f"Will show up to {dryrun_count} items that would be processed")
        
        # Process files and capture the result
        success, matched_files = process_files(
            args.operation,
            args.source_bucket,
            args.source_prefix,
            args.dest_bucket,
            args.dest_prefix,
            args.pattern,
            args.pattern_type,
            args.current_version_only,
            args.merge,
            dryrun,
            dryrun_count
        )
        
        # Exit with error if any operation failed
        if not success:
            logger.error("One or more file operations failed. Exiting with error.")
            sys.exit(1)
            
        if args.operation == 'list':
            sys.exit(0)
            
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()