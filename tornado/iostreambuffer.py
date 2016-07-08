import collections

WRITE_BUFFER_CHUNK_SIZE = 128 * 1024

class IOStreamBuffer(object):

    def __init__(self, max_write_buffer_size):
        self._max_write_buffer_size = max_write_buffer_size
        self._read_buffer = collections.deque()
        self._write_buffer = collections.deque()
        self._write_buffer_frozen = False
        self._read_buffer_size = 0
        self._write_buffer_size = 0
        self._read_max_bytes = None

    def consume(self, loc):
        if loc == 0:
            return b""
        _merge_prefix(self._read_buffer, loc)
        self._read_buffer_size -= loc
        return self._read_buffer.popleft()

    def find_read_pos(self, read_delimiter, read_bytes, read_partial, read_regex):
        """Attempts to find a position in the read buffer that satisfies
        the currently-pending read.

        Returns a position in the buffer if the current read can be satisfied,
        or None if it cannot.
        """
        if (read_bytes is not None and
            (self._read_buffer_size >= read_bytes or
             (read_partial and self._read_buffer_size > 0))):
            num_bytes = min(read_bytes, self._read_buffer_size)
            return num_bytes
        elif read_delimiter is not None:
            # Multi-byte delimiters (e.g. '\r\n') may straddle two
            # chunks in the read buffer, so we can't easily find them
            # without collapsing the buffer.  However, since protocols
            # using delimited reads (as opposed to reads of a known
            # length) tend to be "line" oriented, the delimiter is likely
            # to be in the first few chunks.  Merge the buffer gradually
            # since large merges are relatively expensive and get undone in
            # _consume().
            if self._read_buffer:
                while True:
                    loc = self._read_buffer[0].find(read_delimiter)
                    if loc != -1:
                        delimiter_len = len(read_delimiter)
                        self._check_max_bytes(read_delimiter,
                                              loc + delimiter_len)
                        return loc + delimiter_len
                    if len(self._read_buffer) == 1:
                        break
                    _double_prefix(self._read_buffer)
                self._check_max_bytes(read_delimiter,
                                      len(self._read_buffer[0]))
        elif read_regex is not None:
            if self._read_buffer:
                while True:
                    m = read_regex.search(self._read_buffer[0])
                    if m is not None:
                        self._check_max_bytes(read_regex, m.end())
                        return m.end()
                    if len(self._read_buffer) == 1:
                        break
                    _double_prefix(self._read_buffer)
                self._check_max_bytes(read_regex,
                                      len(self._read_buffer[0]))
        return None


    def set_write_buffer_frozen(self):
        self._write_buffer_frozen = True


    @property
    def read_max_bytes(self):
        return self._read_max_bytes

    @read_max_bytes.setter
    def read_max_bytes(self, b):
        self._read_max_bytes = b



    def _check_max_bytes(self, delimiter, size):
        if (self._read_max_bytes is not None and
                size > self._read_max_bytes):
            raise UnsatisfiableReadError(
                "delimiter %r not found within %d bytes" % (
                    delimiter, self._read_max_bytes))

    def write_to_stream(self, stream):
        if not self._write_buffer_frozen:
            # On windows, socket.send blows up if given a
            # write buffer that's too large, instead of just
            # returning the number of bytes it was able to
            # process.  Therefore we must not call socket.send
            # with more than 128KB at a time.
            _merge_prefix(self._write_buffer, 128 * 1024)
        num_bytes = stream.write_to_fd(self._write_buffer[0])
        if num_bytes == 0:
            # With OpenSSL, if we couldn't write the entire buffer,
            # the very same string object must be used on the
            # next call to send.  Therefore we suppress
            # merging the write buffer after an incomplete send.
            # A cleaner solution would be to set
            # SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER, but this is
            # not yet accessible from python
            # (http://bugs.python.org/issue8240)
            self._write_buffer_frozen = True
            return False
        self._write_buffer_frozen = False
        _merge_prefix(self._write_buffer, num_bytes)
        self._write_buffer.popleft()
        self._write_buffer_size -= num_bytes
        return True

    def read_from_stream(self, stream):
        chunk = stream.read_from_fd()
        if chunk is None:
            return 0
        self._read_buffer.append(chunk)
        self._read_buffer_size += len(chunk)
        return len(chunk)

    def add_to_buffer(self, data):
        if data:
            if (self._max_write_buffer_size is not None and
                    self._write_buffer_size + len(data) > self._max_write_buffer_size):
                raise StreamBufferFullError("Reached maximum write buffer size")
            # Break up large contiguous strings before inserting them in the
            # write buffer, so we don't have to recopy the entire thing
            # as we slice off pieces to send to the socket.
            for i in range(0, len(data), WRITE_BUFFER_CHUNK_SIZE):
                self._write_buffer.append(data[i:i + WRITE_BUFFER_CHUNK_SIZE])
            self._write_buffer_size += len(data)


def _double_prefix(deque):
    """Grow by doubling, but don't split the second chunk just because the
    first one is small.
    """
    new_len = max(len(deque[0]) * 2,
                  (len(deque[0]) + len(deque[1])))
    _merge_prefix(deque, new_len)


def _merge_prefix(deque, size):
    """Replace the first entries in a deque of strings with a single
    string of up to size bytes.

    >>> d = collections.deque(['abc', 'de', 'fghi', 'j'])
    >>> _merge_prefix(d, 5); print(d)
    deque(['abcde', 'fghi', 'j'])

    Strings will be split as necessary to reach the desired size.
    >>> _merge_prefix(d, 7); print(d)
    deque(['abcdefg', 'hi', 'j'])

    >>> _merge_prefix(d, 3); print(d)
    deque(['abc', 'defg', 'hi', 'j'])

    >>> _merge_prefix(d, 100); print(d)
    deque(['abcdefghij'])
    """
    if len(deque) == 1 and len(deque[0]) <= size:
        return
    prefix = []
    remaining = size
    while deque and remaining > 0:
        chunk = deque.popleft()
        if len(chunk) > remaining:
            deque.appendleft(chunk[remaining:])
            chunk = chunk[:remaining]
        prefix.append(chunk)
        remaining -= len(chunk)
    # This data structure normally just contains byte strings, but
    # the unittest gets messy if it doesn't use the default str() type,
    # so do the merge based on the type of data that's actually present.
    if prefix:
        deque.appendleft(type(prefix[0])().join(prefix))
    if not deque:
        deque.appendleft(b"")


def doctests():
    import doctest
    return doctest.DocTestSuite()
