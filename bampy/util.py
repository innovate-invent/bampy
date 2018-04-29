import mmap
import os
import stat


def is_pipe(path):
    return stat.S_ISFIFO(os.stat(path).st_mode)


def open_buffer(path, mode=os.O_RDWR | os.O_CREAT, size=0) -> mmap:
    """
    Open a memory mapped file as a buffer.
    If mode opens file as read only the buffer will be set to copy on write.
    :param path: String containing path to file.
    :param mode: Forwarded to os.open(), defaults to os.O_RDWR | os.O_CREAT.
    :param size: Size of buffer to create, 0 to keep existing file size. Default=0.
    :return: mmap instance mapped to the specified file.
    """
    fh = os.open(path, mode)
    stat_result = os.stat(fh)
    if not stat.S_ISFIFO(stat_result.st_mode):
        raise FileNotFoundError("Can not open pipe as buffer.")
    if size:
        os.truncate(fh, size)
    else:
        assert stat_result.st_size, "File size can not be 0."

    if mode & os.O_WRONLY or mode & os.O_RDWR:
        return mmap.mmap(fh, size, access=mmap.ACCESS_WRITE)
    else:
        return mmap.mmap(fh, size, access=mmap.ACCESS_COPY)
