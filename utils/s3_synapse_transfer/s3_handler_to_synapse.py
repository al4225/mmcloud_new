#!/usr/bin/env python3

## Usage
# # Transfer only CSV and TXT files
# python3 s3_synapse_transfer.py --synid syn123456 --bucket my-bucket --path data/ --token-file token.txt --extensions csv txt --recursive

# # Transfer only XLSX files
# python3 s3_synapse_transfer.py --synid syn123456 --bucket my-bucket --path data/ --token-file token.txt --extensions xlsx

# # Transfer files with specific patterns
# python3 s3_synapse_transfer.py --synid syn123456 --bucket my-bucket --path data/ --token-file token.txt --patterns cis_qtl.pairs.tsv.gz --recursive

# # Original behavior (transfer all files)
# python3 s3_synapse_transfer.py --synid syn123456 --bucket my-bucket --path data/ --recursive --token-file token.txt

"""
S3-Synapse Transfer Tool
A script for transferring files from S3 to Synapse using file handlers.
"""

import os
import sys
import json
import argparse
import hashlib
import logging
import re
from pathlib import Path

import synapseclient
import s3fs

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s %(message)s',
    datefmt='%m-%d %H:%M'
)
logger = logging.getLogger(__name__)

def read_token_file(token_file):
    """Read token from file"""
    try:
        with open(token_file, 'r') as f:
            return f.read().strip()
    except Exception as e:
        logger.error(f"Error reading token file: {e}")
        sys.exit(1)

def get_s3_file_info(bucket, key):
    """Get file info from S3"""
    try:
        fs = s3fs.S3FileSystem(anon=False)
        path = f'{bucket}/{key}'
        
        if not fs.exists(path):
            logger.error(f"File not found in S3: {path}")
            return None
            
        info = fs.info(path)
        return {
            'size': info.get('size', 0),
            'path': path
        }
    except Exception as e:
        logger.error(f"Error getting S3 file info: {e}")
        return None

def calculate_md5(bucket, key):
    """Calculate MD5 hash of S3 file"""
    try:
        fs = s3fs.S3FileSystem(anon=False)
        path = f'{bucket}/{key}'
        
        md5_hash = hashlib.md5()
        with fs.open(path, 'rb') as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                md5_hash.update(byte_block)
                
        return md5_hash.hexdigest()
    except Exception as e:
        logger.error(f"Error calculating MD5: {e}")
        return None

def guess_content_type(filename):
    """Guess content type based on file extension"""
    ext = os.path.splitext(filename)[1].lower()
    content_types = {
        '.csv': 'text/csv',
        '.tsv': 'text/tab-separated-values',
        '.txt': 'text/plain',
        '.json': 'application/json',
        '.pdf': 'application/pdf',
        '.jpg': 'image/jpeg',
        '.jpeg': 'image/jpeg',
        '.png': 'image/png',
        '.gif': 'image/gif',
        '.xls': 'application/vnd.ms-excel',
        '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        '.gz': 'application/gzip',
        '.zip': 'application/zip'
    }
    return content_types.get(ext, 'application/octet-stream')

def create_external_s3_location(syn, bucket):
    """Create external S3 storage location setting"""
    destination = {
        'uploadType': 'S3',
        'concreteType': 'org.sagebionetworks.repo.model.project.ExternalS3StorageLocationSetting',
        'bucket': bucket
    }
    
    try:
        return syn.restPOST('/storageLocation', body=json.dumps(destination))
    except Exception as e:
        logger.error(f"Error creating storage location: {e}")
        sys.exit(1)

def create_s3_file_handle(syn, bucket, key, filename, file_size, md5, storage_location_id):
    """Create S3 file handle"""
    file_handle = {
        'concreteType': 'org.sagebionetworks.repo.model.file.S3FileHandle',
        'fileName': filename,
        'contentSize': str(file_size),
        'contentType': guess_content_type(filename),
        'contentMd5': md5,
        'bucketName': bucket,
        'key': key,
        'storageLocationId': storage_location_id
    }
    
    try:
        return syn.restPOST('/externalFileHandle/s3', 
                           json.dumps(file_handle), 
                           endpoint=syn.fileHandleEndpoint)
    except Exception as e:
        logger.error(f"Error creating file handle: {e}")
        return None

def store_file_in_synapse(syn, parent_id, file_handle_id, filename):
    """Store file in Synapse using file handle"""
    f = synapseclient.File(parentId=parent_id, 
                          dataFileHandleId=file_handle_id,
                          name=filename)
    
    try:
        return syn.store(f)
    except Exception as e:
        logger.error(f"Error storing file in Synapse: {e}")
        return None

def find_specific_files(bucket, s3_path, extensions, recursive=False):
    """
    Find files with specific extensions in the S3 bucket path
    
    Args:
        bucket (str): S3 bucket name
        s3_path (str): Path in S3 bucket
        extensions (list): List of file extensions to filter by (e.g., ['.csv', '.txt'])
        recursive (bool): Whether to process directories recursively
        
    Returns:
        list: List of keys matching the specified extensions
    """
    # Normalize extensions to lowercase and ensure they start with a dot
    normalized_extensions = []
    for ext in extensions:
        if not ext.startswith('.'):
            ext = '.' + ext
        normalized_extensions.append(ext.lower())
    
    logger.info(f"Searching for files with extensions: {normalized_extensions}")
    
    # Get all files
    all_files = process_s3_path(bucket, s3_path, recursive)
    
    # Filter by extension
    filtered_files = []
    for file_key in all_files:
        file_ext = os.path.splitext(file_key)[1].lower()
        if file_ext in normalized_extensions:
            filtered_files.append(file_key)
    
    logger.info(f"Found {len(filtered_files)} files with specified extensions")
    return filtered_files

def find_files_by_pattern(bucket, s3_path, patterns, recursive=False):
    """
    Find files matching specific patterns in the S3 bucket path
    
    Args:
        bucket (str): S3 bucket name
        s3_path (str): Path in S3 bucket
        patterns (list): List of patterns to match (e.g., ['cis_qtl.pairs.tsv.gz', '.csv'])
        recursive (bool): Whether to process directories recursively
        
    Returns:
        list: List of keys matching the specified patterns
    """
    logger.info(f"Searching for files with patterns: {patterns}")
    
    # Get all files
    all_files = process_s3_path(bucket, s3_path, recursive)
    
    # Prepare patterns - simple extensions (with dot) or regex patterns for complex matches
    regex_patterns = []
    simple_extensions = []
    
    for pattern in patterns:
        if pattern.startswith('.') and pattern.count('.') == 1 and all(c.isalnum() or c == '.' for c in pattern):
            # Simple extension like '.csv'
            simple_extensions.append(pattern.lower())
        else:
            # Convert pattern to regex that matches at the end of the filename
            if not pattern.startswith('.'):
                pattern = pattern
            regex_patterns.append(re.compile(f"{re.escape(pattern)}$"))
    
    # Filter by patterns
    filtered_files = []
    for file_key in all_files:
        filename = os.path.basename(file_key)
        
        # Check simple extensions
        file_ext = os.path.splitext(filename)[1].lower()
        if file_ext in simple_extensions:
            filtered_files.append(file_key)
            continue
            
        # Check regex patterns
        for pattern in regex_patterns:
            if pattern.search(filename):
                filtered_files.append(file_key)
                break
    
    logger.info(f"Found {len(filtered_files)} files matching specified patterns")
    return filtered_files

def process_s3_path(bucket, s3_path, recursive=False):
    """Process S3 path and return list of keys to transfer"""
    try:
        fs = s3fs.S3FileSystem(anon=False)
        path = f'{bucket}/{s3_path}'
        
        # Check if path exists
        if not fs.exists(path):
            logger.error(f"Path not found in S3: {path}")
            return []
            
        # Check if path is a file
        if not s3_path.endswith('/') and fs.isfile(path):
            return [s3_path]
            
        # Process as directory
        if not s3_path.endswith('/'):
            s3_path += '/'
            
        files = []
        
        # List directory
        for entry in fs.ls(path, detail=True):
            # Skip directories unless recursive
            if entry['type'] == 'directory':
                if recursive:
                    subdir = entry['name'][len(f'{bucket}/'):] 
                    files.extend(process_s3_path(bucket, subdir, recursive))
            else:
                key = entry['name'][len(f'{bucket}/'):] 
                files.append(key)
                
        return files
        
    except Exception as e:
        logger.error(f"Error processing S3 path: {e}")
        return []

def transfer_s3_to_synapse(syn, bucket, s3_path, synapse_id, recursive=False, calculate_hashes=True, extensions=None, patterns=None):
    """Transfer files from S3 to Synapse"""
    # Get list of files to transfer
    if patterns:
        keys = find_files_by_pattern(bucket, s3_path, patterns, recursive)
    elif extensions:
        keys = find_specific_files(bucket, s3_path, extensions, recursive)
    else:
        keys = process_s3_path(bucket, s3_path, recursive)
    
    if not keys:
        logger.error("No files found to transfer")
        return 0, 0
        
    logger.info(f"Found {len(keys)} files to transfer")
    
    # Create external storage location
    storage_location = create_external_s3_location(syn, bucket)
    
    success = 0
    total = len(keys)
    
    for i, key in enumerate(keys, 1):
        filename = os.path.basename(key)
        logger.info(f"Processing file {i}/{total}: {filename}")
        
        # Get file info
        file_info = get_s3_file_info(bucket, key)
        if not file_info:
            continue
            
        # Calculate MD5 if requested
        md5 = None
        if calculate_hashes:
            logger.info(f"Calculating MD5 hash for {filename}")
            md5 = calculate_md5(bucket, key)
            if not md5:
                logger.warning(f"Failed to calculate MD5 for {filename}, using dummy value")
                md5 = "d41d8cd98f00b204e9800998ecf8427e"  # MD5 of empty string
        else:
            logger.info("Skipping MD5 calculation (using dummy value)")
            md5 = "d41d8cd98f00b204e9800998ecf8427e"  # MD5 of empty string
            
        # Create file handle
        file_handle = create_s3_file_handle(syn, bucket, key, filename, 
                                           file_info['size'], md5, 
                                           storage_location['storageLocationId'])
        if not file_handle:
            continue
            
        # Store file in Synapse
        result = store_file_in_synapse(syn, synapse_id, file_handle['id'], filename)
        if result:
            logger.info(f"Successfully transferred {filename} to Synapse")
            success += 1
        
    logger.info(f"Transferred {success}/{total} files to Synapse")
    return success, total

def main():
    parser = argparse.ArgumentParser(description='Transfer files from S3 to Synapse using file handlers')
    
    parser.add_argument("--synid", required=True,
                        help="Synapse folder ID (destination)")
    parser.add_argument("--bucket", required=True,
                        help="S3 bucket name")
    parser.add_argument("--path", required=True,
                        help="Path in S3 bucket (file or directory)")
    parser.add_argument("--token-file", required=True,
                        help="File containing Synapse authentication token")
    parser.add_argument("--recursive", action="store_true",
                        help="Process directories recursively")
    parser.add_argument("--skip-md5", action="store_true",
                        help="Skip MD5 hash calculation (faster)")
    parser.add_argument("--extensions", nargs='+',
                        help="List of file extensions to transfer (e.g., csv txt)")
    parser.add_argument("--patterns", nargs='+',
                        help="List of filename patterns to match (e.g., 'cis_qtl.pairs.tsv.gz')")
    parser.add_argument("--verbose", action="store_true",
                        help="Enable verbose logging")
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Read token
    token = read_token_file(args.token_file)
    
    start_time = time.time()
    
    try:
        # Login to Synapse
        logger.info("Logging in to Synapse")
        syn = synapseclient.login(authToken=token)
        
        # Transfer files
        success, total = transfer_s3_to_synapse(
            syn, 
            args.bucket, 
            args.path, 
            args.synid, 
            args.recursive,
            not args.skip_md5,
            args.extensions,
            args.patterns
        )
        
        end_time = time.time()
        logger.info(f"Execution completed in {end_time - start_time:.2f} seconds")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    import time
    main()