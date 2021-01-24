from transfer_manager import *
import file_generator
import time
from multiprocessing.pool import ThreadPool
import threading
import os


MB = 1024 * 1024

DIRECTORY = './demo_files'
DOWNLOADS_DIR= './demo_files/downloads'
FILES = {}
MAX_FILE_SIZE = 512

def get_full_file_name(file):
    return f"{DIRECTORY}/{file}"

def generate_files():
    global FILES
    size_mb = 1
    ratio = 2
    while (size_mb <= 128):
        filename = get_full_file_name(f"file_{size_mb}MB")
        file_generator.generate_big_random_bin_file(filename, size_mb * MB)
        FILES[filename] = size_mb
    size_mb = ratio * size_mb
    ratio = ratio * 2


def get_files_from_directory():
    global FILES
    FILES = {get_full_file_name(f): os.path.getsize(get_full_file_name(f)) for f in os.listdir(DIRECTORY) if os.path.isfile(get_full_file_name(f))}

def print_tranfer_result(thread_info, elapsed):
    """Report the result of a transfer, including per-thread data."""
    print(f"\nUsed {len(thread_info)} threads.")
    for ident, byte_count in thread_info.items():
        print(f"{'':4}Thread {ident} copied {byte_count} bytes.")
    print(f"Your transfer took {elapsed:.2f} seconds.")

def upload_files_serial(upload_function, bucket_name, meta=None):
    global_start_time = time.perf_counter()
    for filename, file_size in FILES.items():
        start_time = time.perf_counter()
        data = upload_function(filename, bucket_name, filename, file_size)
        end_time = time.perf_counter()
        print(f"{file_size} { end_time - start_time}")
    global_end_time = time.perf_counter()

    print(f"[Serial Upload] Total elapsed time: {global_end_time - global_start_time}")

def upload_files_with_thread_pool(upload_function, bucket_name, threads_no=8, meta=None):
    pool = ThreadPool(processes=threads_no)
    arguments = []
    for filename, file_size in FILES.items():
        arguments.append((filename, bucket_name, filename, file_size))
    global_start_time = time.perf_counter()
    pool.starmap(upload_function, arguments)
    global_end_time = time.perf_counter()

    print(f"[Thread Pool Upload] Total elapsed time: {global_end_time - global_start_time}")

def upload_files_with_thread_for_each_file(upload_function, bucket_name, meta=None):
    threads = []
    for filename, file_size in FILES.items():
        threads.append(threading.Thread(target=upload_function, args=(filename, bucket_name, filename, file_size)))

    global_start_time = time.perf_counter()
    for t in threads:
        t.start()

    for t in threads:
        t.join()
    global_end_time = time.perf_counter()

    print(f"[Threads Upload] Total elapsed time: {global_end_time - global_start_time}")


def download_files_serial(download_function, bucket_name, meta=None):
    global_start_time = time.perf_counter()
    for filename, file_size in FILES.items():
        start_time = time.perf_counter()
        data = download_function(bucket_name, filename, filename, file_size)
        end_time = time.perf_counter()
        print_tranfer_result(data, end_time - start_time)
    global_end_time = time.perf_counter()

    print(f"[Serial Download] Total elapsed time: {global_end_time - global_start_time}")


def download_files_with_thread_pool(download_function, bucket_name, threads_no=8, meta=None):
    pool = ThreadPool(processes=threads_no)
    arguments = []
    for filename, file_size in FILES.items():
        arguments.append((bucket_name, filename, filename, file_size))
    global_start_time = time.perf_counter()
    pool.starmap(download_function, arguments)
    global_end_time = time.perf_counter()

    print(f"[Thread Pool Download] Total elapsed time: {global_end_time - global_start_time}")

def download_files_with_thread_for_each_file(download_function, bucket_name, meta=None):
    threads = []
    for filename, file_size in FILES.items():
        threads.append(threading.Thread(target = download_function, args=(bucket_name, filename, filename, file_size)))

    global_start_time = time.perf_counter()
    for t in threads:
        t.start()

    for t in threads:
        t.join()
    global_end_time = time.perf_counter()

    print(f"[Threads Download] Total elapsed time: {global_end_time - global_start_time}")


def main():
    #get_files_from_directory()
    generate_files()	
    bucket_name = 'big-data-project-eu'

    file_download = FileDownloadAPI()
    file_upload = FileUploadAPI()

    upload_functions = [
        file_upload.upload_with_default_configuration,
        file_upload.upload_with_transfer_acceleration,
        file_upload.upload_with_high_threshold,
        file_upload.upload_with_multipart_chunksize
    ]

    for f in upload_functions:
        upload_files_serial(f, bucket_name)
        #upload_files_with_thread_for_each_file(f, bucket_name)
        #upload_files_with_thread_pool(f, bucket_name)


    download_functions = [
        file_download.download_with_default_configuration,
        file_download.download_with_high_threshold,
        file_download.download_with_single_thread,
        file_download.download_with_multiple_threads,
        file_download.download_with_chunksize,
        file_download.download_with_transfer_acceleration
    ]

    #for f in download_functions:
       # download_files_serial(f, bucket_name)
       # download_files_with_thread_for_each_file(f, bucket_name)
       # download_files_with_thread_pool(f, bucket_name)

    #for filename in FILES:
        #os.remove(filename)

    
if __name__ == '__main__':
    main()

