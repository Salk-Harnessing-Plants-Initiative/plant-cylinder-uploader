import logging
import json
from datetime import datetime
# Path-related
import os
import shutil
import ntpath
# Getting file creation timestamp
import platform
# Threading
import time
import threading
# AWS S3
import boto3
# Detecting new files
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileCreatedEvent
# Remote logging to AWS CloudWatch
import watchtower
# How many seconds to wait to process after a new file is moved into `unprocessed_dir`
SECONDS_DELAY = 10.0
lock = threading.Lock()
t = None
    
def creation_date(file_path):
    """
    Try to get the date that a file was created, falling back to when it was
    last modified if that isn't possible.
    See http://stackoverflow.com/a/39501288/1709587 for explanation.
    """
    if platform.system() == 'Windows':
        return os.path.getctime(file_path)
    else:
        stat = os.stat(file_path)
        try:
            return stat.st_birthtime
        except AttributeError:
            # We're probably on Linux. No easy way to get creation dates here,
            # so we'll settle for when its content was last modified.
            return stat.st_mtime

def get_file_created(file_path):
    """Gets the file's creation timestamp from the filesystem and returns it as a string
    Errors upon failure
    """
    return datetime.fromtimestamp(creation_date(file_path)).astimezone().isoformat()

def get_files_alphabetical_order(directory):
    """Returns paths. Avoids hidden files
    """
    paths = []
    for file in os.listdir(directory):
        path = os.path.join(directory, file)
        if os.path.isfile(path) and not file[0] == '.':
            paths.append(path)
    return sorted(paths)

def get_leaf_directories(root):
    """ Given root directory (path), recursively crawls through all children directories
    until it finds all of the directories that do not have children directories
    (finds all the leaf directories). Returns this as a list of paths in alphabetical order.
    """
    folders = []
    for cur, dirs, files in os.walk(root):
        if not dirs:
            folders.append(cur)
    return sorted(folders)

def get_files_for_leaf_directories(root):
    """Returns paths in alphabetical order
    """
    paths = []
    for directory in get_leaf_directories(root):
        paths.extend(get_files_alphabetical_order(directory))
    return sorted(paths)

def make_parallel_path(src_dir, dst_dir, src_path, add_date_subdir=True):
    """Creates a parallel path of src_path using dst_dir instead of src_dir
    as the prefix. If add_date_subdir is True, uses dst_dir/(today's date in "YYYY-MM-DD" format)/
    as the new prefix instead.

    src_dir should be a prefix of src_path, else error
    """
    # Remove prefix
    prefix = src_dir
    if src_path.startswith(prefix):
        suffix = src_path[len(prefix)+1:]
    else:
        raise Exception("src_dir {} was not a prefix of src_path {}".format(src_dir, src_path))

    # Add prefix
    result = dst_dir
    if add_date_subdir:
        result = os.path.join(result, datetime.today().strftime('%Y-%m-%d'))
    result = os.path.join(result, suffix)
    return result

def delete_directory_if_empty_or_hidden(directory):
    all_hidden = True
    files = os.listdir(directory)
    for file in files:
        if file[0] != '.':
            all_hidden = False
    if all_hidden:
        try:
            for file in files:
                os.remove(os.path.join(directory, file))
            os.rmdir(directory)
        except:
            pass

def move(src_path, dst_path, src_root=None):
    """Move file from src_path to dst_path, creating new directories from dst_path 
    along the way if they don't already exist.     
    Avoids collisions if file already exists at dst_path by adding "(#)" if necessary, where # is a number
    (Formatted the same way filename collisions are resolved in Google Chrome downloads)

    If src_root is specified, then the function will try to delete the directory of src_path
    if it is not src_root and it is empty or only contains hidden files. (The hidden files will be deleted).
    If src_root is not actually a parent directory of src_path, deletion will never take place. 
    """
    root_ext = os.path.splitext(dst_path)
    i = 0
    while os.path.isfile(dst_path):
        # Recursively avoid the collision
        i += 1
        dst_path = root_ext[0] + " ({})".format(i) + root_ext[1]
    # Move file, make directories if needed
    os.makedirs(os.path.dirname(dst_path), exist_ok=True)
    shutil.move(src_path, dst_path)
    # Delete directory if necessary
    directory = os.path.dirname(src_path)
    if (src_root is not None and src_path.startswith(src_root) and directory != src_root):
        delete_directory_if_empty_or_hidden(directory)

def assert_directories_configured(config):
    # None of the dirs should be the same as another
    unprocessed_dir = config['unprocessed_dir']
    error_dir = config['error_dir']
    done_dir = config['done_dir']
    assert (len([unprocessed_dir, error_dir, done_dir]) == len(set([unprocessed_dir, error_dir, done_dir])))

def assert_s3_working(config):
    s3 = boto3.resource('s3', 
        region_name=config['aws_region_name'], 
        aws_access_key_id=config['aws_access_key_id'], 
        aws_secret_access_key=config['aws_secret_access_key'])
    assert s3.Bucket(config['s3']['bucket']) in s3.buckets.all()

def setup_remote_logging(config):
    logger = logging.getLogger(__name__)
    cloudwatch = config['cloudwatch']
    if cloudwatch['use_cloudwatch']:
        boto3_session = boto3.Session(aws_access_key_id=config['aws_access_key_id'],
            aws_secret_access_key=config['aws_secret_access_key'],
            region_name=config['aws_region_name'])
        watchtower_handler = watchtower.CloudWatchLogHandler(
            log_group=cloudwatch["log_group"],
            stream_name=cloudwatch["stream_name"],
            send_interval=cloudwatch["send_interval"],
            create_log_group=True,
            boto3_session=boto3_session
        )
        logger.addHandler(watchtower_handler)

def init(config):
    print("Initializing...")
    assert_directories_configured(config)
    assert_postgres_working(config)
    assert_s3_working(config)
    setup_remote_logging(config)

def run_indefinitely(config):
    logger = logging.getLogger(__name__)
    # Setup the watchdog handler for new files that are added while the script is running
    observer = Observer()
    observer.schedule(UploaderEventHandler(config), config['unprocessed_dir'], recursive=True)
    observer.start()
    # run process() with countdown indefinitely
    # process() will run after the countdown if not interrupted during countdown
    try:
        s = 0
        while True:
            time.sleep(1)
            s += 1
            if config['log_heartbeat'] and s >= config['heartbeat_seconds']:
                logger.info("HEARTBEAT")
                s = 0
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt: shutting down...")
        observer.stop()
        observer.join()

def get_config():
    config_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config.json')
    with open(config_path) as f:
        config = json.load(f)
    return config

class UploaderEventHandler(FileSystemEventHandler):
    """Handler for what to do if watchdog detects a filesystem change
    """
    def __init__(self, config):
        self.config = config

    def on_created(self, event):
        global lock
        global t
        is_file = not event.is_directory
        if is_file:
            # Attempt to cancel the thread if in countdown mode
            # And start up a new one with countdown
            with lock:
                t.cancel()
                t.join()
                t = threading.Timer(SECONDS_DELAY, process, args=(self.config,))
                t.start()

def generate_bucket_key(file_path, s3_directory, plant_id, subfolder_name):
    """Keep things nice and random to prevent collisions
    "/Users/russell/Documents/taco_tuesday.jpg" becomes "raw/taco_tuesday-b94b0793-6c74-44a9-94e0-00420711130d.jpg"
    Note: We still like to keep the basename because some files' only timestamp is in the filename

    Also removes spaces, parenthesis in the filename
    """
    root_ext = os.path.splitext(ntpath.basename(file_path));
    filename = root_ext[0] + "-" + str(uuid.uuid4()) + root_ext[1]
    filename = filename.replace(" ", "").replace("(", "").replace(")", "")
    return s3_directory + filename


def get_metadata(file_path, config, last_reference, qr_code, qr_codes):
    metadata = {"Metadata": {}}
    metadata["Metadata"]["user_input_filename"] = os.path.basename(file_path)
    metadata["Metadata"]["upload_device_id"] = config['upload_device_id']
    metadata["Metadata"]["qr_code"] = qr_code
    metadata["Metadata"]["qr_codes"] = str(qr_codes)
    try:
        metadata["Metadata"]["file_created"] = get_file_created(file_path)
    except:
        pass
    return metadata

def process(config):
    logger = logging.getLogger(__name__)
    unprocessed_dir, error_dir, done_dir = config['unprocessed_dir'], config['error_dir'], config['done_dir']
    



    s3_client = boto3.client('s3', region_name=config['aws_region_name'], 
        aws_access_key_id=config['aws_access_key_id'], aws_secret_access_key=config['aws_secret_access_key'])

    files = get_files_alphabetical_order(unprocessed_dir)



    if len(files) > 0:
        logger.info("Processing files in the order: {}".format(files))
    for file in files:
        path = os.path.join(unprocessed_dir, file)
        # QR code if present
        


        # Process
        try:
            bucket = config['s3']['bucket']
            key = generate_bucket_key(path, config['s3']['bucket_dir'])
            qr_code = last_reference["qr_code"]
            metadata = get_metadata(path, config, last_reference, qr_code, qr_codes)
            s3_client.upload_file(path, bucket, key, ExtraArgs=metadata)
        except Exception as e:
            logger.error(e)
            error_path = make_parallel_path(unprocessed_dir, error_dir, path)
            move(path, error_path)
        else:
            done_path = make_parallel_path(unprocessed_dir, done_dir, path)
            move(path, done_path)
    if len(files) > 0:
        logger.info("Done processing the batch")

def main():
    global lock
    global t
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    config = get_config()
    init(config)

    # Startup a single processing thread in case there are any images preexisting in `unprocessed_dir`
    logger.info("Running Plant Cylinder Uploader...")
    with lock:
        t = threading.Timer(SECONDS_DELAY, process, args=(config,))
        t.start()
    # Run forever
    run_indefinitely(config)

if __name__ == "__main__":
    main()
