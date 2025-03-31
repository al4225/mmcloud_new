#!/usr/bin/python

import sys,getopt,pdb,os,glob,pandas as pd,argparse,csv,subprocess
import hashlib
# login to synapse
import synapseclient
import json
import argparse
import Crypto 
import s3fs

def hash_file(bucket, key):
    md5_hash = Crypto.createHash('md5')
    fs = s3fs.S3FileSystem(anon=False)
    with fs.open(f'{bucket}/{key}', 'rb') as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            md5_hash.update(byte_block)
        return md5_hash.hexdigest()
# Gets MD5 from file 
def getmd5(filename):
    return hashlib.md5(open(filename,'rb').read()).hexdigest()

def main():
    parser = argparse.ArgumentParser(description='Description')
    parser.add_argument("--synid", help="provide the synapse folder id, which is the destination", required=True)
    parser.add_argument("--filename", help="provide the file name to be transfered", required=True)
    parser.add_argument("--bucket", help="provide the bucket name to be transfered from", required=True)
    parser.add_argument("--path", help="provide the relative path on aws for the file to be transfered", required=True)
    
    args = parser.parse_args()
    syn = synapseclient.login(authToken="...")
# Set the project you want to link the files to
#    PROJECT = 'syn54087777'
    PROJECT = args.synid
    filename = args.filename
    relative_path = args.path
    #file_size = os.path.getsize(args.filename)
    bucket_name = args.bucket
    destination = {'uploadType':'S3',
               'concreteType':'org.sagebionetworks.repo.model.project.ExternalS3StorageLocationSetting',
#               'bucket':'test-synapse-aws'}
               'bucket':bucket_name}
    destination = syn.restPOST('/storageLocation', body=json.dumps(destination))
    if relative_path == "root":
        value_to_key = filename
    else:
        value_to_key = "".join([ relative_path, filename ])
    #md5 = getmd5(args.filename)
#    md5 = hash_file(bucket_name, value_to_key)    
    # create filehandle
    fileHandle = {'concreteType': 'org.sagebionetworks.repo.model.file.S3FileHandle',
#       'fileName'  : 'test_aws_to_synapse3.py',
        'fileName'  : filename,
        'contentSize' : "10240",
#       'contentType' : 'text/csv',    
        'contentMd5' : '8b8db3dfa426f6bdb1798d578f5239ae',
#       'bucketName' : destination['bucket'],
#       'key' : 'test_aws_to_synapse3.py',
#        'contentSize' : file_size,
        'contentType' : 'text/csv',    
#        'contentMd5' : md5,
        'bucketName' : destination['bucket'],
        'key' : value_to_key,
       'storageLocationId': destination['storageLocationId']}
    fileHandle = syn.restPOST('/externalFileHandle/s3', json.dumps(fileHandle), endpoint=syn.fileHandleEndpoint)
    f = synapseclient.File(parentId=PROJECT, dataFileHandleId = fileHandle['id'],name=fileHandle['fileName'])
    f = syn.store(f)

if __name__ == "__main__":
    main()
