import os, stat, mmap


def open_buffer(path, mode=os.O_RDWR | os.O_CREAT, size=0):
    fh = os.open(path, mode)
    stat_result = os.stat(fh)
    assert not stat.S_ISFIFO(stat_result.st_mode), "Can not open pipe as buffer."
    if size:
        os.truncate(fh, size)
    else:
        assert stat_result.st_size, "File size can not be 0."

    if mode & os.O_WRONLY or mode & os.O_RDWR:
        return mmap.mmap(fh, size, access=mmap.ACCESS_WRITE)
    else:
        return mmap.mmap(fh, size, access=mmap.ACCESS_COPY)
