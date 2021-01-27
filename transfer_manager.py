import sys
import threading

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config


MB = 1024 * 1024
s3 = boto3.resource('s3')


class TransferCallback:
    """
    Handle callbacks from the transfer manager.

    The transfer manager periodically calls the __call__ method throughout
    the upload and download process so that it can take action, such as
    displaying progress to the user and collecting data about the transfer.
    """

    def __init__(self, target_size):
        self._target_size = target_size
        self._total_transferred = 0
        self._lock = threading.Lock()
        self.thread_info = {}

    def __call__(self, bytes_transferred):
        """
        The callback method that is called by the transfer manager.

        Display progress during file transfer and collect per-thread transfer
        data. This method can be called by multiple threads, so shared instance
        data is protected by a thread lock.
        """
        thread = threading.current_thread()
        with self._lock:
            self._total_transferred += bytes_transferred
            if thread.ident not in self.thread_info.keys():
                self.thread_info[thread.ident] = bytes_transferred
            else:
                self.thread_info[thread.ident] += bytes_transferred

            target = self._target_size * MB
            sys.stdout.write(
                f"\r{self._total_transferred} of {target} transferred "
                f"({(self._total_transferred / target) * 100:.2f}%).")
            sys.stdout.flush()

class FileUploadAPI:
    def __init__(self):
        self.s3 = boto3.client('s3')

    def upload_with_default_configuration(self, local_file_path, bucket_name,
                                        s3_filename, file_size_mb):
        """
        Upload a file from a local folder to an Amazon S3 bucket, using the default
        configuration.
        """
        print(f"Uploading a file of {file_size_mb}MB with default configurations.")
        transfer_callback = TransferCallback(file_size_mb)
        self.s3.upload_file(local_file_path, bucket_name, s3_filename, callback=transfer_callback)
        
        return transfer_callback.thread_info


    def upload_with_multipart_chunksize(self, local_file_path, bucket_name, s3_filename,
                                    file_size_mb):
        """
        Upload a file from a local folder to an Amazon S3 bucket, setting a
        multipart chunk size and adding metadata to the Amazon S3 object.

        The multipart chunk size controls the size of the chunks of data that are
        sent in the request. A smaller chunk size typically results in the transfer
        manager using more threads for the upload.

        The metadata is a set of key-value pairs that are stored with the object
        in Amazon S3.
        """
        print(f"Uploading a file of {file_size_mb}MB with multipart chunks.")
        transfer_callback = TransferCallback(file_size_mb)

        config = TransferConfig(multipart_chunksize=1 * MB, max_concurrency=12, use_threads=True)
        self.s3.upload_file(
            local_file_path,
            bucket_name,
            s3_filename,
            Config=config,
            callback=transfer_callback)
        return transfer_callback.thread_info


    def upload_with_high_threshold(self, local_file_path, bucket_name, s3_filename,
                                file_size_mb):
        """
        Upload a file from a local folder to an Amazon S3 bucket, setting a
        multipart threshold larger than the size of the file.

        Setting a multipart threshold larger than the size of the file results
        in the transfer manager sending the file as a standard upload instead of
        a multipart upload.
        """
        print(f"Uploading a file of {file_size_mb}MB with multipart chunks.")

        transfer_callback = TransferCallback(file_size_mb)
        config = TransferConfig(multipart_threshold=file_size_mb * 2 * MB)
        self.s3.upload_file(
            local_file_path,
            bucket_name,
            s3_filename,
            Config=config,
            callback=transfer_callback)
        return transfer_callback.thread_info

    def upload_with_transfer_acceleration(self, local_file_path, bucket_name, s3_filename,
                                            file_size_mb):
        """
        Upload a file from a local folder to an Amazon S3 bucket with transfer
        acceleration function enabled.
        """
        print(f"Uploading a file of {file_size_mb}MB with transfer acceleration.")
        s3_accelerated = boto3.client('s3', endpoint_url='http://big-data-project-eu.s3-accelerate.amazonaws.com',
                        config=Config(s3={'use_accelerate_endpoint': True}))
        transfer_callback = TransferCallback(file_size_mb)
        s3_accelerated.upload_file(
            local_file_path,
            bucket_name,
            s3_filename,
            callback=transfer_callback)
        return transfer_callback.thread_info


class FileDownloadAPI:
    def __init__(self):
        self.s3 = boto3.resource('s3')

    def download_with_default_configuration(self, bucket_name, s3_filename,
                                            download_file_path, file_size_mb):
        """
        Download a file from an Amazon S3 bucket to a local folder, using the
        default configuration.
        """
        print(f"Downloading a file of {file_size_mb}MB with derfault configuration.")
        transfer_callback = TransferCallback(file_size_mb)
        s3.Bucket(bucket_name).download_file(
            s3_filename,
            download_file_path)
        return transfer_callback.thread_info


    def download_with_single_thread(self, bucket_name, s3_filename,
                                    download_file_path, file_size_mb):
        """
        Download a file from an Amazon S3 bucket to a local folder, using a
        single thread.
        """
        print(f"Downloading a file of {file_size_mb}MB with a single thread.")
        transfer_callback = TransferCallback(file_size_mb)
        config = TransferConfig(use_threads=False)
        s3.Bucket(bucket_name).download_file(
            s3_filename,
            download_file_path,
            Config=config)
        return transfer_callback.thread_info


    def download_with_multiple_threads(self, bucket_name, s3_filename,
                                    download_file_path, file_size_mb, threads_no=8):
        """
        Download a file from an Amazon S3 bucket to a local folder, using a
        single thread.
        """
        print(f"Downloading a file of {file_size_mb}MB with multi threads.")
        transfer_callback = TransferCallback(file_size_mb)
        config = TransferConfig(max_concurrency=threads_no, use_threads=True)
        s3.Bucket(bucket_name).download_file(
            s3_filename,
            download_file_path,
            Config=config)
        return transfer_callback.thread_info


    def download_with_high_threshold(self, bucket_name, s3_filename,
                                    download_file_path, file_size_mb):
        """
        Download a file from an Amazon S3 bucket to a local folder, setting a
        multipart threshold larger than the size of the file.

        Setting a multipart threshold larger than the size of the file results
        in the transfer manager sending the file as a standard download instead
        of a multipart download.
        """
        print(f"Downloading a file of {file_size_mb}MB with high threshold.")
        transfer_callback = TransferCallback(file_size_mb)
        config = TransferConfig(multipart_threshold=file_size_mb * 64 * MB)
        s3.Bucket(bucket_name).download_file(
            s3_filename,
            download_file_path,
            Config=config)
        return transfer_callback.thread_info

    def download_with_chunksize(self, bucket_name, s3_filename,
                                    download_file_path, file_size_mb):
        """
        Download a file from an Amazon S3 bucket to a local folder, setting a
        multipart threshold larger than the size of the file.

        Setting a multipart threshold larger than the size of the file results
        in the transfer manager sending the file as a standard download instead
        of a multipart download.
        """
        print(f"Downloading a file of {file_size_mb}MB with multipart chunksize.")
        transfer_callback = TransferCallback(file_size_mb)
        config = TransferConfig(multipart_chunksize=1 * MB, max_concurrency=12, use_threads=True)
        s3.Bucket(bucket_name).download_file(
            s3_filename,
            download_file_path,
            Config=config)
        return transfer_callback.thread_info

    def download_with_transfer_acceleration(self, bucket_name, s3_filename, download_file_path, file_size_mb):
        """
        Download a file from an Amazon S3 bucket to a local folder, setting a
        multipart threshold larger than the size of the file.

        Setting a multipart threshold larger than the size of the file results
        in the transfer manager sending the file as a standard download instead
        of a multipart download.
        """
        print(f"Downloading a file of {file_size_mb}MB with transfer acceleration.")
        s3_accelerated = boto3.resource('s3', endpoint_url='http://big-data-project-eu.s3-accelerate.amazonaws.com', config=Config(s3={'use_accelerate_endpoint': True}))
        transfer_callback = TransferCallback(file_size_mb)
        config = TransferConfig(multipart_chunksize=1 * MB)
        s3_accelerated.Bucket(bucket_name).download_file(
            s3_filename,
            download_file_path,
            Config=config)
        return transfer_callback.thread_info
