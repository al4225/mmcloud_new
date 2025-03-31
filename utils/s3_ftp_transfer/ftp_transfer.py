#!/usr/bin/env python3

"""
S3-FTP Transfer Tool
A versatile script for transferring files between S3 and various FTP protocols (FTP, FTPS, SFTP).
"""

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
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%m-%d %H:%M'
)
logger = logging.getLogger(__name__)

# Default chunk size for large file transfers (100 MB)
DEFAULT_CHUNK_SIZE = 104857600  # 100 MB (100 * 1024 * 1024)


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
            logger.info("Successfully connected to S3")
        except Exception as e:
            logger.error(f"Failed to connect to S3: {e}")
            sys.exit(1)

    def connect_ftp(self):
        if self.protocol == 'auto':
            try:
                self._connect_sftp()
                self.protocol = 'sftp'
                logger.info("Connected using SFTP protocol")
            except Exception as sftp_error:
                logger.warning(f"SFTP connection failed: {sftp_error}")
                try:
                    self._connect_ftps()
                    self.protocol = 'ftps'
                    logger.info("Connected using FTPS protocol")
                except Exception as ftps_error:
                    logger.warning(f"FTPS connection failed: {ftps_error}")
                    try:
                        self._connect_ftp()
                        self.protocol = 'ftp'
                        logger.info("Connected using FTP protocol")
                    except Exception as ftp_error:
                        logger.error(f"FTP connection failed: {ftp_error}")
                        logger.error("All connection attempts failed")
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
        
        try:
            transport = paramiko.Transport((self.config['ftp_host'], int(self.config['ftp_port'])))
            transport.connect(username=self.config['ftp_username'], password=self.config['ftp_password'])
            self.ftp_connection = paramiko.SFTPClient.from_transport(transport)
            logger.info("Successfully connected to SFTP server")
        except Exception as e:
            logger.error(f"SFTP connection error: {e}")
            raise

    def _connect_ftps(self):
        ftps = FTP_TLS()
        try:
            ftps.connect(self.config['ftp_host'], int(self.config['ftp_port']))
            ftps.login(self.config['ftp_username'], self.config['ftp_password'])
            ftps.prot_p()
            self.ftp_connection = ftps
            logger.info("Successfully connected to FTPS server")
        except Exception as e:
            logger.error(f"FTPS connection error: {e}")
            raise

    def _connect_ftp(self):
        ftp = FTP()
        try:
            ftp.connect(self.config['ftp_host'], int(self.config['ftp_port']))
            ftp.login(self.config['ftp_username'], self.config['ftp_password'])
            self.ftp_connection = ftp
            logger.info("Successfully connected to FTP server")
        except Exception as e:
            logger.error(f"FTP connection error: {e}")
            raise

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
                logger.info("Disconnected from FTP server")
            except Exception as e:
                logger.warning(f"Error during FTP disconnection: {e}")

    def list_files(self, directory):
        try:
            if self.protocol == 'sftp':
                return self.ftp_connection.listdir(directory)
            else:  # FTP or FTPS
                self.ftp_connection.cwd(directory)
                return self.ftp_connection.nlst()
        except Exception as e:
            logger.error(f"Error listing files in {directory}: {e}")
            return []

    def list_s3_files(self, bucket, prefix=''):
        try:
            response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            if 'Contents' in response:
                return [item['Key'] for item in response['Contents']]
            return []
        except Exception as e:
            logger.error(f"Error listing S3 files in {bucket}/{prefix}: {e}")
            return []

    def get_file_size_ftp(self, file_path):
        try:
            if self.protocol == 'sftp':
                return self.ftp_connection.stat(file_path).st_size
            elif self.protocol == 'ftps' or self.protocol == 'ftp':
                return self.ftp_connection.size(file_path)
        except Exception as e:
            logger.error(f"Error getting file size for {file_path}: {e}")
            return None

    def get_file_size_s3(self, bucket, key):
        try:
            response = self.s3_client.head_object(Bucket=bucket, Key=key)
            return response['ContentLength']
        except Exception as e:
            logger.error(f"Error getting S3 file size for {bucket}/{key}: {e}")
            return None

    def _calculate_optimal_chunk_size(self, file_size, user_chunk_size=None):
        MIN_CHUNK_SIZE = 5 * 1024 * 1024  # 5 MB (AWS minimum requirement)
        MAX_PARTS = 10000  # AWS maximum parts for multipart upload
        
        if user_chunk_size and user_chunk_size >= MIN_CHUNK_SIZE:
            return user_chunk_size
            
        min_required_chunk_size = math.ceil(file_size / MAX_PARTS)
        optimal_chunk_size = max(min_required_chunk_size, DEFAULT_CHUNK_SIZE)
        optimal_chunk_size = max(optimal_chunk_size, MIN_CHUNK_SIZE)
        
        logger.info(f"Using chunk size of {optimal_chunk_size / (1024 * 1024):.2f} MB")
        return optimal_chunk_size

    def _log_transfer_stats(self, chunk_size, total_seconds):
        if total_seconds > 0:
            speed_mbps = (chunk_size / (1024 * 1024)) / total_seconds
            logger.info(f"Transfer speed: {speed_mbps:.2f} MB/s, time: {total_seconds:.2f} seconds")
        else:
            logger.info("Transfer complete (too fast to measure speed)")

    def upload_to_s3(self, ftp_path, s3_bucket, s3_key, chunk_size=None):
        logger.info(f"Uploading {ftp_path} to s3://{s3_bucket}/{s3_key}")
        
        ftp_file_size = self.get_file_size_ftp(ftp_path)
        if ftp_file_size is None:
            logger.error(f"File {ftp_path} does not exist on FTP server")
            return False
            
        optimal_chunk_size = self._calculate_optimal_chunk_size(ftp_file_size, chunk_size)
            
        try:
            s3_file = self.s3_client.head_object(Bucket=s3_bucket, Key=s3_key)
            if s3_file['ContentLength'] == ftp_file_size:
                logger.info(f"File already exists in S3 bucket with same size, skipping upload")
                return True
        except Exception:
            pass
            
        if self.protocol == 'sftp':
            return self._upload_sftp_to_s3(ftp_path, s3_bucket, s3_key, ftp_file_size, optimal_chunk_size)
        else:  # FTP or FTPS
            return self._upload_ftp_to_s3(ftp_path, s3_bucket, s3_key, ftp_file_size, optimal_chunk_size)

    def _upload_sftp_to_s3(self, ftp_path, s3_bucket, s3_key, ftp_file_size, chunk_size):
        multipart_upload = None
        try:
            ftp_file = self.ftp_connection.file(ftp_path, 'r')
            
            if ftp_file_size <= chunk_size:
                logger.info("Transferring complete file from SFTP to S3...")
                start_time = time.time()
                
                ftp_file_data = ftp_file.read()
                ftp_file_data_bytes = io.BytesIO(ftp_file_data)
                self.s3_client.upload_fileobj(ftp_file_data_bytes, s3_bucket, s3_key)
                
                end_time = time.time()
                total_seconds = end_time - start_time
                self._log_transfer_stats(ftp_file_size, total_seconds)
                
                logger.info("Successfully transferred file from SFTP to S3!")
                ftp_file.close()
                return True
            else:
                logger.info("Transferring file from SFTP to S3 in chunks...")
                chunk_count = int(math.ceil(ftp_file_size / float(chunk_size)))
                logger.info(f"File will be uploaded in {chunk_count} chunks of {chunk_size / (1024 * 1024):.2f} MB each")
                
                multipart_upload = self.s3_client.create_multipart_upload(
                    Bucket=s3_bucket, Key=s3_key
                )
                
                parts = []
                for i in range(chunk_count):
                    logger.info(f"Transferring chunk {i + 1}/{chunk_count}...")
                    start_time = time.time()
                    
                    chunk = ftp_file.read(int(chunk_size))
                    part = self.s3_client.upload_part(
                        Bucket=s3_bucket,
                        Key=s3_key,
                        PartNumber=i + 1,
                        UploadId=multipart_upload["UploadId"],
                        Body=chunk,
                    )
                    
                    end_time = time.time()
                    total_seconds = end_time - start_time
                    self._log_transfer_stats(len(chunk), total_seconds)
                    
                    parts.append({
                        "PartNumber": i + 1,
                        "ETag": part["ETag"]
                    })
                    logger.info(f"Chunk {i + 1}/{chunk_count} transferred successfully!")
                
                logger.info("Completing multipart upload...")
                self.s3_client.complete_multipart_upload(
                    Bucket=s3_bucket,
                    Key=s3_key,
                    UploadId=multipart_upload["UploadId"],
                    MultipartUpload={"Parts": parts},
                )
                
                logger.info("All chunks transferred to S3 bucket! File transfer successful!")
                ftp_file.close()
                return True
                
        except Exception as e:
            logger.error(f"Error uploading file from SFTP to S3: {e}")
            if multipart_upload:
                try:
                    logger.info("Aborting multipart upload due to failure...")
                    self.s3_client.abort_multipart_upload(
                        Bucket=s3_bucket,
                        Key=s3_key,
                        UploadId=multipart_upload["UploadId"]
                    )
                except Exception as abort_error:
                    logger.error(f"Error aborting multipart upload: {abort_error}")
            return False

    def _upload_ftp_to_s3(self, ftp_path, s3_bucket, s3_key, ftp_file_size, chunk_size):
        multipart_upload = None
        try:
            if ftp_file_size <= chunk_size:
                logger.info("Transferring complete file from FTP to S3...")
                start_time = time.time()
                
                file_data_buffer = io.BytesIO()
                self.ftp_connection.retrbinary(f'RETR {ftp_path}', file_data_buffer.write)
                
                file_data_buffer.seek(0)
                self.s3_client.upload_fileobj(file_data_buffer, s3_bucket, s3_key)
                
                end_time = time.time()
                total_seconds = end_time - start_time
                self._log_transfer_stats(ftp_file_size, total_seconds)
                
                logger.info("Successfully transferred file from FTP to S3!")
                return True
                
            else:
                logger.info("Transferring file from FTP to S3 in chunks...")
                chunk_count = int(math.ceil(ftp_file_size / float(chunk_size)))
                logger.info(f"File will be uploaded in {chunk_count} chunks of {chunk_size / (1024 * 1024):.2f} MB each")
                
                multipart_upload = self.s3_client.create_multipart_upload(
                    Bucket=s3_bucket, Key=s3_key
                )
                
                parts = []
                for i in range(chunk_count):
                    logger.info(f"Transferring chunk {i + 1}/{chunk_count}...")
                    file_data_buffer = io.BytesIO()
                    
                    try:
                        self.ftp_connection.retrbinary(
                            f'RETR {ftp_path}',
                            file_data_buffer.write,
                            blocksize=chunk_size,
                            rest=i * chunk_size
                        )
                    except Exception as e:
                        logger.error(f"Error retrieving chunk {i + 1}: {e}")
                        raise
                    
                    file_data_buffer.seek(0)
                    chunk = file_data_buffer.read(chunk_size)
                    
                    start_time = time.time()
                    part = self.s3_client.upload_part(
                        Bucket=s3_bucket,
                        Key=s3_key,
                        PartNumber=i + 1,
                        UploadId=multipart_upload["UploadId"],
                        Body=chunk,
                    )
                    end_time = time.time()
                    
                    total_seconds = end_time - start_time
                    self._log_transfer_stats(len(chunk), total_seconds)
                    
                    parts.append({
                        "PartNumber": i + 1,
                        "ETag": part["ETag"]
                    })
                    logger.info(f"Chunk {i + 1}/{chunk_count} transferred successfully!")
                
                logger.info("Completing multipart upload...")
                self.s3_client.complete_multipart_upload(
                    Bucket=s3_bucket,
                    Key=s3_key,
                    UploadId=multipart_upload["UploadId"],
                    MultipartUpload={"Parts": parts},
                )
                
                logger.info("All chunks transferred to S3 bucket! File transfer successful!")
                return True
                
        except Exception as e:
            logger.error(f"Error uploading file from FTP to S3: {e}")
            if multipart_upload:
                try:
                    logger.info("Aborting multipart upload due to failure...")
                    self.s3_client.abort_multipart_upload(
                        Bucket=s3_bucket,
                        Key=s3_key,
                        UploadId=multipart_upload["UploadId"]
                    )
                except Exception as abort_error:
                    logger.error(f"Error aborting multipart upload: {abort_error}")
            return False

    def download_from_s3(self, s3_bucket, s3_key, ftp_path, chunk_size=None):
        logger.info(f"Downloading s3://{s3_bucket}/{s3_key} to {ftp_path}")
        
        s3_file_size = self.get_file_size_s3(s3_bucket, s3_key)
        if s3_file_size is None:
            logger.error(f"File s3://{s3_bucket}/{s3_key} does not exist")
            return False
            
        optimal_chunk_size = self._calculate_optimal_chunk_size(s3_file_size, chunk_size)
            
        ftp_dir = os.path.dirname(ftp_path)
        if ftp_dir:
            self._ensure_ftp_directory(ftp_dir)
            
        if self.protocol == 'sftp':
            return self._download_s3_to_sftp(s3_bucket, s3_key, ftp_path, s3_file_size, optimal_chunk_size)
        else:  # FTP or FTPS
            return self._download_s3_to_ftp(s3_bucket, s3_key, ftp_path, s3_file_size, optimal_chunk_size)

    def _ensure_ftp_directory(self, directory):
        if not directory or directory == '/':
            return
            
        parent = os.path.dirname(directory)
        if parent:
            self._ensure_ftp_directory(parent)
            
        try:
            if self.protocol == 'sftp':
                try:
                    self.ftp_connection.stat(directory)
                except FileNotFoundError:
                    logger.info(f"Creating directory on SFTP: {directory}")
                    self.ftp_connection.mkdir(directory)
            else:  # FTP or FTPS
                current_dir = self.ftp_connection.pwd()
                try:
                    self.ftp_connection.cwd(directory)
                    self.ftp_connection.cwd(current_dir)  # Go back to original directory
                except Exception:
                    logger.info(f"Creating directory on FTP: {directory}")
                    self.ftp_connection.mkd(directory)
        except Exception as e:
            logger.debug(f"Directory creation note: {e}")

    def _download_s3_to_sftp(self, s3_bucket, s3_key, ftp_path, s3_file_size, chunk_size):
        try:
            if s3_file_size <= chunk_size:
                logger.info("Transferring complete file from S3 to SFTP...")
                start_time = time.time()
                
                s3_obj = self.s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
                file_data = s3_obj['Body'].read()
                
                with self.ftp_connection.file(ftp_path, 'w') as ftp_file:
                    ftp_file.write(file_data)
                    
                end_time = time.time()
                total_seconds = end_time - start_time
                self._log_transfer_stats(s3_file_size, total_seconds)
                
                logger.info("Successfully transferred file from S3 to SFTP!")
                return True
            else:
                logger.info("Transferring file from S3 to SFTP in chunks...")
                chunk_count = int(math.ceil(s3_file_size / float(chunk_size)))
                logger.info(f"File will be downloaded in {chunk_count} chunks of {chunk_size / (1024 * 1024):.2f} MB each")
                
                with self.ftp_connection.file(ftp_path, 'w') as ftp_file:
                    for i in range(chunk_count):
                        logger.info(f"Transferring chunk {i + 1}/{chunk_count}...")
                        start_byte = i * chunk_size
                        end_byte = min(start_byte + chunk_size - 1, s3_file_size - 1)
                        
                        start_time = time.time()
                        s3_obj = self.s3_client.get_object(
                            Bucket=s3_bucket,
                            Key=s3_key,
                            Range=f'bytes={start_byte}-{end_byte}'
                        )
                        chunk = s3_obj['Body'].read()
                        
                        ftp_file.write(chunk)
                        
                        end_time = time.time()
                        total_seconds = end_time - start_time
                        self._log_transfer_stats(len(chunk), total_seconds)
                        
                        logger.info(f"Chunk {i + 1}/{chunk_count} transferred successfully!")
                
                logger.info("All chunks transferred to SFTP server! File transfer successful!")
                return True
                
        except Exception as e:
            logger.error(f"Error downloading file from S3 to SFTP: {e}")
            return False

    def _download_s3_to_ftp(self, s3_bucket, s3_key, ftp_path, s3_file_size, chunk_size):
        try:
            temp_file = io.BytesIO()
            
            if s3_file_size <= chunk_size:
                logger.info("Transferring complete file from S3 to FTP...")
                start_time = time.time()
                
                self.s3_client.download_fileobj(s3_bucket, s3_key, temp_file)
                temp_file.seek(0)
                
                self.ftp_connection.storbinary(f'STOR {ftp_path}', temp_file)
                
                end_time = time.time()
                total_seconds = end_time - start_time
                self._log_transfer_stats(s3_file_size, total_seconds)
                
                logger.info("Successfully transferred file from S3 to FTP!")
                return True
            else:
                logger.info("Transferring file from S3 to FTP in chunks...")
                chunk_count = int(math.ceil(s3_file_size / float(chunk_size)))
                logger.info(f"File will be downloaded in {chunk_count} chunks of {chunk_size / (1024 * 1024):.2f} MB each")
                
                total_downloaded = 0
                for i in range(chunk_count):
                    logger.info(f"Downloading chunk {i + 1}/{chunk_count} from S3...")
                    start_byte = i * chunk_size
                    end_byte = min(start_byte + chunk_size - 1, s3_file_size - 1)
                    
                    start_time = time.time()
                    s3_obj = self.s3_client.get_object(
                        Bucket=s3_bucket,
                        Key=s3_key,
                        Range=f'bytes={start_byte}-{end_byte}'
                    )
                    chunk = s3_obj['Body'].read()
                    
                    temp_file.write(chunk)
                    
                    end_time = time.time()
                    total_seconds = end_time - start_time
                    self._log_transfer_stats(len(chunk), total_seconds)
                    total_downloaded += len(chunk)
                    
                    logger.info(f"Downloaded {total_downloaded / (1024 * 1024):.2f} MB of {s3_file_size / (1024 * 1024):.2f} MB ({(total_downloaded / s3_file_size * 100):.1f}%)")
                
                logger.info("Uploading complete file to FTP...")
                temp_file.seek(0)
                self.ftp_connection.storbinary(f'STOR {ftp_path}', temp_file)
                
                logger.info("All chunks transferred to FTP server! File transfer successful!")
                return True
                
        except Exception as e:
            logger.error(f"Error downloading file from S3 to FTP: {e}")
            return False

    def bulk_upload_to_s3(self, ftp_dir, s3_bucket, s3_prefix, chunk_size=None):
        logger.info(f"Bulk uploading from {ftp_dir} to s3://{s3_bucket}/{s3_prefix}")
        files = self.list_files(ftp_dir)
        total = len(files)
        
        if total == 0:
            logger.warning(f"No files found in {ftp_dir}")
            return 0, 0
            
        logger.info(f"Found {total} files to upload")
        success = 0
        
        for i, file_name in enumerate(files, 1):
            try:
                if self.protocol == 'sftp':
                    stat_info = self.ftp_connection.stat(f"{ftp_dir}/{file_name}")
                    is_dir = stat_info.st_mode & 0o40000
                    if is_dir:
                        logger.info(f"Skipping directory: {file_name}")
                        continue
            except Exception as e:
                logger.warning(f"Error checking if {file_name} is a directory: {e}")
            
            ftp_path = f"{ftp_dir}/{file_name}"
            s3_key = f"{s3_prefix}/{file_name}" if s3_prefix else file_name
            
            logger.info(f"Processing file {i}/{total}: {file_name}")
            if self.upload_to_s3(ftp_path, s3_bucket, s3_key, chunk_size):
                success += 1
                
        logger.info(f"Bulk upload completed: {success}/{total} files transferred successfully")
        return success, total

    def bulk_download_from_s3(self, s3_bucket, s3_prefix, ftp_dir, chunk_size=None):
        logger.info(f"Bulk downloading from s3://{s3_bucket}/{s3_prefix} to {ftp_dir}")
        files = self.list_s3_files(s3_bucket, s3_prefix)
        total = len(files)
        
        if total == 0:
            logger.warning(f"No files found in s3://{s3_bucket}/{s3_prefix}")
            return 0, 0
            
        logger.info(f"Found {total} files to download")
        success = 0
        
        for i, s3_key in enumerate(files, 1):
            if s3_key.endswith('/'):
                logger.info(f"Skipping directory placeholder: {s3_key}")
                continue
                
            if s3_prefix and s3_key.startswith(s3_prefix):
                relative_path = s3_key[len(s3_prefix):].lstrip('/')
                ftp_path = f"{ftp_dir}/{relative_path}" if ftp_dir else relative_path
            else:
                ftp_path = f"{ftp_dir}/{os.path.basename(s3_key)}" if ftp_dir else os.path.basename(s3_key)
                
            logger.info(f"Processing file {i}/{total}: {s3_key}")
            if self.download_from_s3(s3_bucket, s3_key, ftp_path, chunk_size):
                success += 1
                
        logger.info(f"Bulk download completed: {success}/{total} files transferred successfully")
        return success, total


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
                        help=f'Chunk size for multipart transfers in bytes (default: {DEFAULT_CHUNK_SIZE} - {DEFAULT_CHUNK_SIZE/(1024*1024):.0f} MB)')
    parser.add_argument('--bulk', action='store_true',
                        help='Process entire directory instead of single file')
    parser.add_argument('--recursive', action='store_true',
                        help='Process directories recursively (with --bulk)')
    parser.add_argument('--verbose', action='store_true',
                        help='Enable verbose logging')
                        
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Verbose logging enabled")
    
    if not args.ftp_port:
        if args.protocol == 'sftp':
            args.ftp_port = 22
            logger.debug(f"Using default SFTP port: {args.ftp_port}")
        else:
            args.ftp_port = 21
            logger.debug(f"Using default FTP/FTPS port: {args.ftp_port}")
    
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
            if args.bulk:
                success, total = transfer.bulk_upload_to_s3(
                    args.ftp_path, 
                    args.s3_bucket, 
                    args.s3_path, 
                    args.chunk_size
                )
                logger.info(f"Bulk upload completed: {success}/{total} files transferred successfully")
            else:
                success = transfer.upload_to_s3(
                    args.ftp_path,
                    args.s3_bucket,
                    args.s3_path,
                    args.chunk_size
                )
                logger.info(f"Upload {'completed successfully' if success else 'failed'}")
        
        elif args.mode == 'download':
            if args.bulk:
                success, total = transfer.bulk_download_from_s3(
                    args.s3_bucket,
                    args.s3_path,
                    args.ftp_path,
                    args.chunk_size
                )
                logger.info(f"Bulk download completed: {success}/{total} files transferred successfully")
            else:
                success = transfer.download_from_s3(
                    args.s3_bucket,
                    args.s3_path,
                    args.ftp_path,
                    args.chunk_size
                )
                logger.info(f"Download {'completed successfully' if success else 'failed'}")
        
        end_time = time.time()
        total_time = end_time - start_time
        logger.info(f"Total execution time: {total_time:.2f} seconds")
        
    except Exception as e:
        logger.error(f"Error during transfer operation: {e}", exc_info=True)
        sys.exit(1)
    finally:
        transfer.disconnect()


if __name__ == "__main__":
    main()
