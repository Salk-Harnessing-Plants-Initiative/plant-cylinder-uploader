import os
import shutil
import ntpath

def get_files_alphabetical_order(directory):
    files = sorted([file for file in os.listdir(directory) if not file[0] == '.'])
    return files

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

def move(src_path, dst_path):
    """ Move file from src_path to dst_path, creating new directories from dst_path 
    along the way if they don't already exist.     
    Avoids collisions if file already exists at dst_path by adding "(#)" if necessary
    (Formatted the same way filename collisions are resolved in Google Chrome downloads)
    """
    root_ext = os.path.splitext(dst_path)
    i = 0
    while os.path.isfile(dst_path):
        # Recursively avoid the collision
        i += 1
        dst_path = root_ext[0] + " ({})".format(i) + root_ext[1]
    # Finally move file, make directories if needed
    os.makedirs(os.path.dirname(dst_path), exist_ok=True)
    shutil.move(src_path, dst_path)

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