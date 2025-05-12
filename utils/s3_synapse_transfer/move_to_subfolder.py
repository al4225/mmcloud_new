#!/usr/bin/env python3
## Usage
# # Move all files to the eQTL subfolder
# python synapse_move_files.py --synid syn123456 --token-file token.txt

# # Move only specific file types
# python synapse_move_files.py --synid syn123456 --token-file token.txt --extensions csv txt

# # Specify a different subfolder name
# python synapse_move_files.py --synid syn123456 --token-file token.txt --subfolder "eQTL_data"

# # Enable verbose logging
# python synapse_move_files.py --synid syn123456 --token-file token.txt --verbose

"""
Synapse Folder Organization Tool
A script for moving files within Synapse to an eQTL subfolder.
"""

import os
import sys
import argparse
import logging
import time

import synapseclient

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

def move_files_to_subfolder(syn, parent_folder_id, subfolder_name, extensions=None):
    """
    Move files from parent folder to a subfolder
    
    Args:
        syn: Synapse client
        parent_folder_id (str): Parent folder Synapse ID
        subfolder_name (str): Name of subfolder to create/use
        extensions (list): Optional list of file extensions to filter by
    
    Returns:
        tuple: (success_count, total_count)
    """
    # Verify parent folder exists
    try:
        parent = syn.get(parent_folder_id, downloadFile=False)
        logger.info(f"Found parent folder: {parent.name} ({parent_folder_id})")
    except Exception as e:
        logger.error(f"Error accessing parent folder {parent_folder_id}: {e}")
        return 0, 0

    # Check if subfolder exists, create it if not
    subfolder = None
    for child in syn.getChildren(parent_folder_id, includeTypes=["folder"]):
        if child['name'] == subfolder_name:
            subfolder = child
            logger.info(f"Found existing subfolder: {subfolder_name} ({subfolder['id']})")
            break

    if not subfolder:
        logger.info(f"Creating new subfolder: {subfolder_name}")
        subfolder = syn.store(synapseclient.Folder(name=subfolder_name, parent=parent_folder_id))
        logger.info(f"Created subfolder with ID: {subfolder.id}")
    
    subfolder_id = subfolder.get('id', None) or subfolder.id
    
    # Get all files in the parent folder
    logger.info(f"Retrieving files from parent folder")
    file_list = list(syn.getChildren(parent_folder_id, includeTypes=["file"]))
    
    # Filter by extensions if specified
    if extensions:
        normalized_extensions = []
        for ext in extensions:
            if not ext.startswith('.'):
                ext = '.' + ext
            normalized_extensions.append(ext.lower())
        
        logger.info(f"Filtering files by extensions: {normalized_extensions}")
        filtered_files = []
        for file_info in file_list:
            file_ext = os.path.splitext(file_info['name'])[1].lower()
            if file_ext in normalized_extensions:
                filtered_files.append(file_info)
        
        file_list = filtered_files
        logger.info(f"Found {len(file_list)} files with specified extensions")
    
    success = 0
    total = len(file_list)
    logger.info(f"Found {total} files to move")
    
    # Move each file
    for i, file_info in enumerate(file_list, 1):
        try:
            # Get the entity
            entity = syn.get(file_info['id'], downloadFile=False)
            
            # Skip if already in target folder
            if entity.properties.get('parentId') == subfolder_id:
                logger.info(f"[{i}/{total}] File {entity.name} already in subfolder, skipping")
                success += 1
                continue
                
            logger.info(f"[{i}/{total}] Moving {entity.name} to {subfolder_name} folder")
            
            # Update the parent ID to move the file
            entity.properties['parentId'] = subfolder_id
            syn.store(entity)
            
            logger.info(f"Successfully moved {entity.name}")
            success += 1
            
        except Exception as e:
            logger.error(f"Error moving file {file_info.get('name', file_info['id'])}: {e}")
    
    logger.info(f"Moved {success}/{total} files to {subfolder_name} subfolder")
    return success, total

def main():
    parser = argparse.ArgumentParser(description='Move files in Synapse to a subfolder')
    
    parser.add_argument("--synid", required=True,
                        help="Synapse folder ID (source folder)")
    parser.add_argument("--subfolder", default="eQTL",
                        help="Name of subfolder to create/use (default: eQTL)")
    parser.add_argument("--token-file", required=True,
                        help="File containing Synapse authentication token")
    parser.add_argument("--extensions", nargs='+',
                        help="List of file extensions to move (e.g., csv txt)")
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
        syn = synapseclient.login(authToken=token, silent=True)
        
        # Move files
        success, total = move_files_to_subfolder(
            syn,
            args.synid,
            args.subfolder,
            args.extensions
        )
        
        end_time = time.time()
        logger.info(f"Execution completed in {end_time - start_time:.2f} seconds")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()