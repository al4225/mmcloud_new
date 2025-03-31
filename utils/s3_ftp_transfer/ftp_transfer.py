#!/usr/bin/env python3

import math
import time
import io
import os
import sys
import argparse
import boto3
import paramiko
import logging
from ftplib import FTP, FTP_TLS

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s %(message)s',
    datefmt='%m-%d %H:%M'
)
logger = logging.getLogger(__name__)

# 100 MB chunk size
DEFAULT_CHUNK_SIZE = 104857600

class S3FTPTransfer:
    def __init__(self, config):
        self.config = config
        self.s3_client = None
        self.ftp_connection = None
        self.protocol = config.get('protocol', 'auto').lower()
        
    def connect_s3(self):
        try:
            self.s3_client = boto3.client('s3')
            self.s3_client.list_buckets()
            logger.info("Connected to S3")
        except Exception as e:
            logger.error(f"S3 connection error: {e}")
            sys.exit(1)

    def connect_ftp(self):
        if self.protocol == 'auto':
            try:
                self._connect_sftp()
                self.protocol = 'sftp'
                logger.info("Connected using SFTP")
            except Exception as e:
                logger.warning(f"SFTP failed: {e}")
                try:
                    self._connect_ftps()
                    self.protocol = 'ftps'
                    logger.info("Connected using FTPS")
                except Exception as e:
                    logger.warning(f"FTPS failed: {e}")
                    try:
                        self._connect_ftp()
                        self.protocol = 'ftp'
                        logger.info("Connected using FTP")
                    except Exception as e:
                        logger.error(f"All connection attempts failed: {e}")
                        sys.exit(1)
        elif self.protocol == 'sftp':
            self._connect_sftp()
        elif self.protocol == 'ftps':
            self._connect_ftps()
        elif self.protocol == 'ftp':
            self._connect_ftp()
        else:
            logger.error(f"Unknown protocol: {self.protocol}")
            sys.exit(1)

    def _connect_sftp(self):
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        transport = paramiko.Transport((self.config['ftp_host'], int(self.config['ftp_port'])))
        transport.connect(username=self.config['ftp_username'], password=self.config['ftp_password'])
        self.ftp_connection = paramiko.SFTPClient.from_transport(transport)

    def _connect_ftps(self):
        ftps = FTP_TLS()
        ftps.connect(self.config['ftp_host'], int(self.config['ftp_port']))
        ftps.login(self.config['ftp_username'], self.config['ftp_password'])
        ftps.prot_p()
        self.ftp_connection = ftps

    def _connect_ftp(self):
        ftp = FTP()
        ftp.connect(self.config['ftp_host'], int(self.config['ftp_port']))
        ftp.login(self.config['ftp_username'], self.config['ftp_password'])
        self.ftp_connection = ftp

    def disconnect(self):
        if self.ftp_connection:
            try:
                if self.protocol == 'sftp':
                    self.ftp_connection.close()
                    transport = self.ftp_connection.get_transport()
                    if transport:
                        transport.close()
                else:  # FTP or FTPS
                    self.ftp_connection.quit()
                logger.info("Disconnected from FTP")
            except Exception as e:
                logger.warning(f"Disconnect error: {e}")

    def is_directory(self, path):
        try:
            if self.protocol == 'sftp':
                try:
                    stat_info = self.ftp_connection.stat(path)
                    return bool(stat_info.st_mode & 0o40000)
                except FileNotFoundError:
                    return False
            else:  # FTP or FTPS
                current_dir = self.ftp_connection.pwd()
                try:
                    self.ftp_connection.cwd(path)
                    self.ftp_connection.cwd(current_dir)
                    return True
                except Exception:
                    return False
        except Exception as e:
            logger.error(f"Error checking path: {e}")
            return False
    
    def list_files(self, directory):
        try:
            if self.protocol == 'sftp':
                return self.ftp_connection.listdir(directory)
            else:  # FTP or FTPS
                self.ftp_connection.cwd(directory)
                return self.ftp_connection.nlst()
        except Exception as e:
            logger.error(f"Error listing files: {e}")
            return []

    def list_s3_files(self, bucket, prefix=''):
        try:
            response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            if 'Contents' in response:
                return [item['Key'] for item in response['Contents']]
            return []
        except Exception as e:
            logger.error(f"Error listing S3 files: {e}")
            return []

    def get_file_size_ftp(self, file_path):
        try:
            if self.protocol == 'sftp':
                return self.ftp_connection.stat(file_path).st_size
            else:  # FTP or FTPS
                return self.ftp_connection.size(file_path)
        except Exception as e:
            logger.error(f"Error getting file size: {e}")
            return None

    def get_file_size_s3(self, bucket, key):
        try:
            response = self.s3_client.head_object(Bucket=bucket, Key=key)
            return response['ContentLength']
        except Exception as e:
            logger.error(f"Error getting S3 file size: {e}")
            return None

    def calc_chunk_size(self, file_size, chunk_size=None):
        min_chunk = 5 * 1024 * 1024  # 5 MB
        max_parts = 10000  # AWS limit
        
        if chunk_size and chunk_size >= min_chunk:
            return chunk_size
            
        chunk_size = max(math.ceil(file_size / max_parts), DEFAULT_CHUNK_SIZE, min_chunk)
        logger.info(f"Using chunk size: {chunk_size / (1024 * 1024):.2f} MB")
        return chunk_size

    def upload(self, ftp_path, s3_bucket, s3_key, chunk_size=None, recursive=False):
        if self.is_directory(ftp_path):
            logger.info(f"Uploading directory {ftp_path} to S3")
            if not s3_key.endswith('/'):
                s3_key = f"{s3_key}/"
            return self._upload_directory(ftp_path, s3_bucket, s3_key, chunk_size, recursive)
        else:
            # For files, handle the s3_key
            if s3_key.endswith('/'):
                s3_key = f"{s3_key}{os.path.basename(ftp_path)}"
                
            success = self._upload_file(ftp_path, s3_bucket, s3_key, chunk_size)
            return (1, 1) if success else (0, 1)

    def _create_ftp_directory(self, directory):
        if not directory or directory == '/':
            return
            
        parent = os.path.dirname(directory)
        if parent:
            self._create_ftp_directory(parent)
            
        try:
            if self.protocol == 'sftp':
                try:
                    self.ftp_connection.stat(directory)
                except FileNotFoundError:
                    logger.info(f"Creating SFTP directory: {directory}")
                    self.ftp_connection.mkdir(directory)
            else:  # FTP or FTPS
                current_dir = self.ftp_connection.pwd()
                try:
                    self.ftp_connection.cwd(directory)
                    self.ftp_connection.cwd(current_dir)
                except Exception:
                    logger.info(f"Creating FTP directory: {directory}")
                    self.ftp_connection.mkd(directory)
        except Exception as e:
            logger.debug(f"Directory creation note: {e}")

    def _upload_directory(self, ftp_dir, s3_bucket, s3_prefix, chunk_size=None, recursive=False):
        files = self.list_files(ftp_dir)
        total = 0
        success = 0
        
        for file_name in files:
            ftp_path = f"{ftp_dir}/{file_name}"
            
            if self.is_directory(ftp_path):
                if recursive:
                    logger.info(f"Processing subdirectory: {ftp_path}")
                    sub_prefix = f"{s3_prefix}{file_name}/"
                    sub_success, sub_total = self._upload_directory(
                        ftp_path, s3_bucket, sub_prefix, chunk_size, recursive
                    )
                    success += sub_success
                    total += sub_total
                else:
                    logger.info(f"Skipping subdirectory (not recursive): {ftp_path}")
            else:
                total += 1
                s3_key = f"{s3_prefix}{file_name}"
                if self._upload_file(ftp_path, s3_bucket, s3_key, chunk_size):
                    success += 1
                    
        logger.info(f"Uploaded {success}/{total} files from {ftp_dir}")
        return success, total

    def _upload_file(self, ftp_path, s3_bucket, s3_key, chunk_size=None):
        logger.info(f"Uploading {ftp_path} to s3://{s3_bucket}/{s3_key}")
        
        file_size = self.get_file_size_ftp(ftp_path)
        if file_size is None:
            logger.error(f"File not found: {ftp_path}")
            return False
            
        chunk_size = self.calc_chunk_size(file_size, chunk_size)
            
        try:
            s3_file = self.s3_client.head_object(Bucket=s3_bucket, Key=s3_key)
            if s3_file['ContentLength'] == file_size:
                logger.info(f"File already exists with same size, skipping")
                return True
        except Exception:
            pass
            
        if self.protocol == 'sftp':
            return self._upload_sftp_file(ftp_path, s3_bucket, s3_key, file_size, chunk_size)
        else:  # FTP or FTPS
            return self._upload_ftp_file(ftp_path, s3_bucket, s3_key, file_size, chunk_size)

    def _upload_sftp_file(self, ftp_path, s3_bucket, s3_key, file_size, chunk_size):
        multipart_upload = None
        try:
            ftp_file = self.ftp_connection.file(ftp_path, 'r')
            
            if file_size <= chunk_size:
                # Upload file in one go
                start_time = time.time()
                
                ftp_file_data = ftp_file.read()
                ftp_file_data_bytes = io.BytesIO(ftp_file_data)
                self.s3_client.upload_fileobj(ftp_file_data_bytes, s3_bucket, s3_key)
                
                end_time = time.time()
                logger.info(f"Transfer completed in {end_time - start_time:.2f} seconds")
                
                ftp_file.close()
                return True
            else:
                # Multipart upload
                chunk_count = int(math.ceil(file_size / float(chunk_size)))
                logger.info(f"Using multipart upload: {chunk_count} chunks")
                
                multipart_upload = self.s3_client.create_multipart_upload(
                    Bucket=s3_bucket, Key=s3_key
                )
                
                parts = []
                for i in range(chunk_count):
                    logger.info(f"Uploading chunk {i + 1}/{chunk_count}")
                    
                    chunk = ftp_file.read(int(chunk_size))
                    part = self.s3_client.upload_part(
                        Bucket=s3_bucket,
                        Key=s3_key,
                        PartNumber=i + 1,
                        UploadId=multipart_upload["UploadId"],
                        Body=chunk,
                    )
                    
                    parts.append({
                        "PartNumber": i + 1,
                        "ETag": part["ETag"]
                    })
                
                self.s3_client.complete_multipart_upload(
                    Bucket=s3_bucket,
                    Key=s3_key,
                    UploadId=multipart_upload["UploadId"],
                    MultipartUpload={"Parts": parts},
                )
                
                logger.info("Multipart upload completed")
                ftp_file.close()
                return True
                
        except Exception as e:
            logger.error(f"Upload error: {e}")
            if multipart_upload:
                try:
                    self.s3_client.abort_multipart_upload(
                        Bucket=s3_bucket,
                        Key=s3_key,
                        UploadId=multipart_upload["UploadId"]
                    )
                except Exception as abort_error:
                    logger.error(f"Error aborting multipart upload: {abort_error}")
            return False

    def _upload_ftp_file(self, ftp_path, s3_bucket, s3_key, file_size, chunk_size):
        multipart_upload = None
        try:
            if file_size <= chunk_size:
                # Upload file in one go
                start_time = time.time()
                
                file_data_buffer = io.BytesIO()
                self.ftp_connection.retrbinary(f'RETR {ftp_path}', file_data_buffer.write)
                
                file_data_buffer.seek(0)
                self.s3_client.upload_fileobj(file_data_buffer, s3_bucket, s3_key)
                
                end_time = time.time()
                logger.info(f"Transfer completed in {end_time - start_time:.2f} seconds")
                
                return True
            else:
                # Multipart upload
                chunk_count = int(math.ceil(file_size / float(chunk_size)))
                logger.info(f"Using multipart upload: {chunk_count} chunks")
                
                multipart_upload = self.s3_client.create_multipart_upload(
                    Bucket=s3_bucket, Key=s3_key
                )
                
                parts = []
                for i in range(chunk_count):
                    logger.info(f"Uploading chunk {i + 1}/{chunk_count}")
                    file_data_buffer = io.BytesIO()
                    
                    try:
                        self.ftp_connection.retrbinary(
                            f'RETR {ftp_path}',
                            file_data_buffer.write,
                            blocksize=chunk_size,
                            rest=i * chunk_size
                        )
                    except Exception as e:
                        logger.error(f"Error retrieving chunk: {e}")
                        raise
                    
                    file_data_buffer.seek(0)
                    chunk = file_data_buffer.read(chunk_size)
                    
                    part = self.s3_client.upload_part(
                        Bucket=s3_bucket,
                        Key=s3_key,
                        PartNumber=i + 1,
                        UploadId=multipart_upload["UploadId"],
                        Body=chunk,
                    )
                    
                    parts.append({
                        "PartNumber": i + 1,
                        "ETag": part["ETag"]
                    })
                
                self.s3_client.complete_multipart_upload(
                    Bucket=s3_bucket,
                    Key=s3_key,
                    UploadId=multipart_upload["UploadId"],
                    MultipartUpload={"Parts": parts},
                )
                
                logger.info("Multipart upload completed")
                return True
                
        except Exception as e:
            logger.error(f"Upload error: {e}")
            if multipart_upload:
                try:
                    self.s3_client.abort_multipart_upload(
                        Bucket=s3_bucket,
                        Key=s3_key,
                        UploadId=multipart_upload["UploadId"]
                    )
                except Exception as abort_error:
                    logger.error(f"Error aborting multipart upload: {abort_error}")
            return False

    def download(self, s3_bucket, s3_key, ftp_path, chunk_size=None, recursive=False):
        # Check if this is a directory-like prefix in S3
        objects = self.list_s3_files(s3_bucket, s3_key)
        
        if len(objects) == 0:
            logger.error(f"No objects found at s3://{s3_bucket}/{s3_key}")
            return 0, 0
        elif len(objects) == 1 and objects[0] == s3_key:
            # Single file, direct download
            success = self._download_file(s3_bucket, s3_key, ftp_path, chunk_size)
            return (1, 1) if success else (0, 1)
        else:
            # This is a prefix with multiple objects
            if not s3_key.endswith('/'):
                s3_key = f"{s3_key}/"
                
            return self._download_directory(s3_bucket, s3_key, ftp_path, chunk_size, recursive)

    def _download_directory(self, s3_bucket, s3_prefix, ftp_dir, chunk_size=None, recursive=False):
        # Ensure ftp_dir exists
        self._create_ftp_directory(ftp_dir)
        
        objects = self.list_s3_files(s3_bucket, s3_prefix)
        success = 0
        total = 0
        
        for s3_key in objects:
            # Skip if this is the directory placeholder itself
            if s3_key == s3_prefix:
                continue
                
            # Skip directory placeholders
            if s3_key.endswith('/'):
                if recursive:
                    rel_path = s3_key[len(s3_prefix):].rstrip('/')
                    sub_dir = f"{ftp_dir}/{rel_path}"
                    
                    logger.info(f"Processing S3 subdirectory: {s3_key}")
                    sub_success, sub_total = self._download_directory(
                        s3_bucket, s3_key, sub_dir, chunk_size, recursive
                    )
                    success += sub_success
                    total += sub_total
                else:
                    logger.info(f"Skipping S3 subdirectory (not recursive): {s3_key}")
                continue
                
            # For files within the prefix
            if s3_key.startswith(s3_prefix):
                # Get relative path within the prefix
                rel_path = s3_key[len(s3_prefix):]
                
                # Skip files in subdirectories if not recursive
                if not recursive and '/' in rel_path:
                    continue
                    
                total += 1
                ftp_file_path = f"{ftp_dir}/{rel_path}"
                
                if self._download_file(s3_bucket, s3_key, ftp_file_path, chunk_size):
                    success += 1
        
        logger.info(f"Downloaded {success}/{total} files to {ftp_dir}")
        return success, total

    def _download_file(self, s3_bucket, s3_key, ftp_path, chunk_size=None):
        logger.info(f"Downloading s3://{s3_bucket}/{s3_key} to {ftp_path}")
        
        file_size = self.get_file_size_s3(s3_bucket, s3_key)
        if file_size is None:
            logger.error(f"S3 file not found: {s3_key}")
            return False
            
        chunk_size = self.calc_chunk_size(file_size, chunk_size)
            
        ftp_dir = os.path.dirname(ftp_path)
        if ftp_dir:
            self._create_ftp_directory(ftp_dir)
            
        if self.protocol == 'sftp':
            return self._download_to_sftp(s3_bucket, s3_key, ftp_path, file_size, chunk_size)
        else:  # FTP or FTPS
            return self._download_to_ftp(s3_bucket, s3_key, ftp_path, file_size, chunk_size)

    def _download_to_sftp(self, s3_bucket, s3_key, ftp_path, file_size, chunk_size):
        try:
            if file_size <= chunk_size:
                # Download in one go
                start_time = time.time()
                
                s3_obj = self.s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
                file_data = s3_obj['Body'].read()
                
                with self.ftp_connection.file(ftp_path, 'w') as ftp_file:
                    ftp_file.write(file_data)
                    
                end_time = time.time()
                logger.info(f"Transfer completed in {end_time - start_time:.2f} seconds")
                
                return True
            else:
                # Download in chunks
                chunk_count = int(math.ceil(file_size / float(chunk_size)))
                logger.info(f"Using chunked download: {chunk_count} chunks")
                
                with self.ftp_connection.file(ftp_path, 'w') as ftp_file:
                    for i in range(chunk_count):
                        logger.info(f"Downloading chunk {i + 1}/{chunk_count}")
                        start_byte = i * chunk_size
                        end_byte = min(start_byte + chunk_size - 1, file_size - 1)
                        
                        s3_obj = self.s3_client.get_object(
                            Bucket=s3_bucket,
                            Key=s3_key,
                            Range=f'bytes={start_byte}-{end_byte}'
                        )
                        chunk = s3_obj['Body'].read()
                        
                        ftp_file.write(chunk)
                
                logger.info("Chunked download completed")
                return True
                
        except Exception as e:
            logger.error(f"Download error: {e}")
            return False

    def _download_to_ftp(self, s3_bucket, s3_key, ftp_path, file_size, chunk_size):
        try:
            temp_file = io.BytesIO()
            
            if file_size <= chunk_size:
                # Download in one go
                start_time = time.time()
                
                self.s3_client.download_fileobj(s3_bucket, s3_key, temp_file)
                temp_file.seek(0)
                
                self.ftp_connection.storbinary(f'STOR {ftp_path}', temp_file)
                
                end_time = time.time()
                logger.info(f"Transfer completed in {end_time - start_time:.2f} seconds")
                
                return True
            else:
                # Download in chunks and combine
                chunk_count = int(math.ceil(file_size / float(chunk_size)))
                logger.info(f"Using chunked download: {chunk_count} chunks")
                
                total_downloaded = 0
                for i in range(chunk_count):
                    logger.info(f"Downloading chunk {i + 1}/{chunk_count}")
                    start_byte = i * chunk_size
                    end_byte = min(start_byte + chunk_size - 1, file_size - 1)
                    
                    s3_obj = self.s3_client.get_object(
                        Bucket=s3_bucket,
                        Key=s3_key,
                        Range=f'bytes={start_byte}-{end_byte}'
                    )
                    chunk = s3_obj['Body'].read()
                    
                    temp_file.write(chunk)
                    total_downloaded += len(chunk)
                    
                    progress = total_downloaded / file_size * 100
                    logger.info(f"Progress: {progress:.1f}%")
                
                logger.info("Uploading to FTP...")
                temp_file.seek(0)
                self.ftp_connection.storbinary(f'STOR {ftp_path}', temp_file)
                
                logger.info("Chunked download completed")
                return True
                
        except Exception as e:
            logger.error(f"Download error: {e}")
            return False


def main():
    parser = argparse.ArgumentParser(description='Transfer files between S3 and FTP/FTPS/SFTP servers')
    
    parser.add_argument('--mode', choices=['upload', 'download'], required=True,
                        help='Transfer direction: upload (FTP to S3) or download (S3 to FTP)')
    
    parser.add_argument('--protocol', choices=['auto', 'ftp', 'ftps', 'sftp'], default='auto',
                        help='FTP protocol to use (default: auto)')
    
    parser.add_argument('--ftp-host', required=True, help='FTP server hostname or IP')
    parser.add_argument('--ftp-port', type=int, help='FTP server port (default: 21 for FTP/FTPS, 22 for SFTP)')
    parser.add_argument('--ftp-user', required=True, help='FTP username')
    parser.add_argument('--ftp-password', required=True, help='FTP password')
    parser.add_argument('--ftp-path', required=True, help='Path on FTP server')
    
    parser.add_argument('--s3-bucket', required=True, help='S3 bucket name')
    parser.add_argument('--s3-path', required=True, help='Path/key prefix in S3 bucket')
    
    parser.add_argument('--chunk-size', type=int, default=DEFAULT_CHUNK_SIZE,
                        help='Chunk size in bytes (default: 100MB)')
    parser.add_argument('--recursive', action='store_true',
                        help='Process directories recursively')
    parser.add_argument('--verbose', action='store_true',
                        help='Enable verbose logging')
                        
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    if not args.ftp_port:
        if args.protocol == 'sftp':
            args.ftp_port = 22
        else:
            args.ftp_port = 21
    
    config = {
        'protocol': args.protocol,
        'ftp_host': args.ftp_host,
        'ftp_port': args.ftp_port,
        'ftp_username': args.ftp_user,
        'ftp_password': args.ftp_password
    }
    
    transfer = S3FTPTransfer(config)
    start_time = time.time()
    
    try:
        transfer.connect_s3()
        transfer.connect_ftp()
        
        if args.mode == 'upload':
            success, total = transfer.upload(
                args.ftp_path, 
                args.s3_bucket, 
                args.s3_path, 
                args.chunk_size,
                args.recursive
            )
            logger.info(f"Upload completed: {success}/{total} files transferred")
        
        elif args.mode == 'download':
            success, total = transfer.download(
                args.s3_bucket,
                args.s3_path,
                args.ftp_path,
                args.chunk_size,
                args.recursive
            )
            logger.info(f"Download completed: {success}/{total} files transferred")
        
        end_time = time.time()
        logger.info(f"Total execution time: {end_time - start_time:.2f} seconds")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)
    finally:
        transfer.disconnect()


if __name__ == "__main__":
    main()
