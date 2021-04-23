import hashlib

BLOCK_SIZE = 65536


def get_sha256(data):
    if not data:
        return ''
    return hashlib.sha256(data).hexdigest()


def get_hash_of_file(file_path):
    hash_obj = hashlib.sha256()
    with open(file_path, 'rb') as f:
        fb = f.read(BLOCK_SIZE)
        while len(fb) > 0:
            hash_obj.update(fb)
            fb = f.read(BLOCK_SIZE)
    return hash_obj.hexdigest()
