#!/usr/bin/env python3



"""
File Path Extractor to Manifest TSV Generator

This script extracts all file paths from a specified folder and generates
a tab-delimited manifest.tsv file with 'path' and 'parent' columns.

Usage:
    python generate_manifest.py <folder_path> <parent_id> [output_file]

Arguments:
    folder_path: Path to the folder to scan for files
    parent_id: Parent ID (e.g., syn123) to assign to all files
    output_file: Optional output file name (default: manifest.tsv)

Example:
    python generate_manifest.py /path/to/data syn123
    python generate_manifest.py /path/to/data syn456 my_manifest.tsv
"""

import os
import sys
import argparse
from pathlib import Path


def extract_file_paths(folder_path, recursive=True):
    """
    Extract file paths from the given folder.
    
    Args:
        folder_path (str): Path to the folder to scan
        recursive (bool): If True, scan subfolders recursively; if False, scan only current folder
        
    Returns:
        list: List of absolute file paths
    """
    file_paths = []
    folder = Path(folder_path).resolve()
    
    if not folder.exists():
        raise FileNotFoundError(f"Folder not found: {folder_path}")
    
    if not folder.is_dir():
        raise NotADirectoryError(f"Path is not a directory: {folder_path}")
    
    if recursive:
        # Walk through all files recursively (including subfolders)
        for file_path in folder.rglob('*'):
            if file_path.is_file():
                file_paths.append(str(file_path.resolve()))
    else:
        # Only scan files in the current folder (no subfolders)
        for file_path in folder.glob('*'):
            if file_path.is_file():
                file_paths.append(str(file_path.resolve()))
    
    return sorted(file_paths)


def generate_manifest(file_paths, parent_id, output_file="manifest.tsv", append=False):
    """
    Generate a tab-delimited manifest file.
    
    Args:
        file_paths (list): List of file paths
        parent_id (str): Parent ID to assign to all files
        output_file (str): Output file name
        append (bool): Whether to append to existing file or overwrite
    """
    try:
        # Check if file exists and we're appending
        file_exists = os.path.exists(output_file)
        mode = 'a' if append else 'w'
        
        with open(output_file, mode, encoding='utf-8') as f:
            # Write header only if creating new file or file doesn't exist
            if not append or not file_exists:
                f.write("path\tparent\n")
            
            # Write file entries
            for file_path in file_paths:
                f.write(f"{file_path}\t{parent_id}\n")
        
        if append and file_exists:
            print(f"✓ Entries appended to existing manifest: {output_file}")
        else:
            print(f"✓ Manifest generated successfully: {output_file}")
        print(f"✓ Files processed in this run: {len(file_paths)}")
        
    except Exception as e:
        print(f"Error writing manifest file: {e}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Extract file paths and generate manifest.tsv",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s /path/to/data syn123
  %(prog)s /path/to/data syn456 custom_manifest.tsv
  %(prog)s ./data_folder syn789 --append
  %(prog)s /path/to/data syn123 --no-recursive
        """
    )
    
    parser.add_argument(
        "folder_path",
        help="Path to the folder to scan for files"
    )
    
    parser.add_argument(
        "parent_id",
        help="Parent ID to assign to all files (e.g., syn123)"
    )
    
    parser.add_argument(
        "output_file",
        nargs='?',
        default="manifest.tsv",
        help="Output manifest file name (default: manifest.tsv)"
    )
    
    parser.add_argument(
        "--append",
        action="store_true",
        help="Append to existing manifest file instead of overwriting"
    )
    
    parser.add_argument(
        "--no-recursive",
        action="store_true",
        help="Scan only files in the current folder (exclude subfolders)"
    )
    
    args = parser.parse_args()
    
    try:
        print(f"Scanning folder: {args.folder_path}")
        print(f"Parent ID: {args.parent_id}")
        print(f"Output file: {args.output_file}")
        print(f"Mode: {'Append' if args.append else 'Overwrite'}")
        print(f"Scan mode: {'Current folder only' if args.no_recursive else 'Recursive (including subfolders)'}")
        print("-" * 50)
        
        # Extract file paths
        file_paths = extract_file_paths(args.folder_path, recursive=not args.no_recursive)
        
        if not file_paths:
            print("⚠ No files found in the specified folder.")
            return
        
        # Generate manifest
        generate_manifest(file_paths, args.parent_id, args.output_file, args.append)
        
        # Show preview of first few entries
        print("\nPreview of manifest entries:")
        print("path\tparent")
        print("-" * 30)
        for i, path in enumerate(file_paths[:5]):
            print(f"{path}\t{args.parent_id}")
        
        if len(file_paths) > 5:
            print(f"... and {len(file_paths) - 5} more entries")
            
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()