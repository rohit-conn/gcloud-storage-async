#!/usr/bin/env python3
from google.cloud import storage
import logging
import os
from itertools import tee
from pathlib import Path
import sys
import asyncio
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)


class download_blob():
    bucketName = ''
    bucketPath = '/'
    localDirectory = os.getcwd()
    loop = asyncio.get_event_loop()

    def __init__(self, bucketName, bucketPath, localDir):
        self.bucketName = bucketName
        self.localDirectory = self.localDirectory + '/' + localDir
        self.bucketPath = bucketPath


    def folder_create(self, blob):
        """
            Creates folder and returns blob iterator.
            This fucntion can be optimized for also running in async if proper concurrency param is taken care of.
        """
        dir, objects = tee(blob)
        for filePath in dir:
            directory = self.localDirectory + '/'+ os.path.dirname(filePath.name)
            Path(directory).mkdir(parents=True, exist_ok=True)
        return objects

    def blobs(self):
        client = storage.Client()
        bucket = client.get_bucket(self.bucketName)
        blobs = bucket.list_blobs(prefix=self.bucketPath)
        return self.folder_create(blobs)

    def _blob_list(self):
        client = storage.Client()
        bucket = client.get_bucket(self.bucketName)
        blobs = bucket.list_blobs(prefix=self.bucketPath)
        return [blob.name for blob in blobs]

    def message(self, uri):
        remote_name = "gs://{}/{}".format(self.bucketName, uri)
        logging.info('Copying {}'.format(remote_name))


    def seq_download(self):
        """
            Simple sequential downloader.
            Loops over total objects and copies to local.
        """
        objectsTobecreated = self.blobs()
        num = 0
        for blobFile in objectsTobecreated:
            desLoction = self.localDirectory + '/' + blobFile.name
            self.message(blobFile.name)
            blobFile.download_to_filename(desLoction)
            num+=1
        logging.info("{} files".format(num))

    async def _download_coro(self, uri):
        """
            Coroutine function for running as a task object
            uri - Path to be downloaded.
        """
        local_path = self.localDirectory + '/' + uri
        try:
            client = storage.Client()
            params = [client, uri, local_path]
            filename = await self.loop.run_in_executor(None, self._download_object, *params)
        except Exception as e:
            raise e
        return filename

    def _download_object(self, client, uri, filename):
        """
            Downloader of object.
            Creates local file for async coro.
            client -  Session object.
            uri - Path of each object.
            filename - Local file path.
        """
        try:
            data = client.bucket(self.bucketName).get_blob(uri)
        except Exception as e:
            raise e
        if not data:
            return None
        self.message(uri)
        data.download_to_filename(filename)
        return filename

    def async_download(self):
        """
            Runner function.
            Collects all coroutines for each object and runs in an event loop
        """
        folders=self.blobs()
        all_files = asyncio.gather(*[self._download_coro(path) for path in self._blob_list()])
        results = self.loop.run_until_complete(all_files)
        logging.info("{} files in parallel".format(len(results)))
        self.loop.close()


if __name__ == '__main__':
    def help_txt():
        print("""Usage: ./gcloud-download.py gs://<bucket>/mydir /some/path
optional arguments:
        --concur Concurrent async download.
        -h or --help This output.
        """)
    if ("-h" in sys.argv) or ("--help" in sys.argv):
        help_txt()
        sys.exit(0)
    if len(sys.argv) < 3:
        help_txt()
        sys.exit('Please provide bucketname and download path.')

    bucketLink = sys.argv[1].lstrip("gs://")
    bucket_name = bucketLink.split('/')[0]
    bucket_path = '/'.join(bucketLink.split("/")[1:])
    storageDownload = download_blob(bucket_name, bucket_path, sys.argv[2])

    if  ("--concur" in sys.argv):
        storageDownload.async_download()
    else:
        storageDownload.seq_download()
