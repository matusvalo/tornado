#define PY_SSIZE_T_CLEAN

#include "speedups.h"


#define WRITE_BUFFER_CHUNK_SIZE = (128 * 1024)

#define MAX(a,b)                   \
       ({ __typeof__ (a) _a = (a); \
          __typeof__ (b) _b = (b); \
          _a > _b ? _a : _b; })

#define MIN(a,b)                   \
       ({ __typeof__ (a) _a = (a); \
          __typeof__ (b) _b = (b); \
          _a < _b ? _a : _b; })

static inline int
check_max_bytes(int read_max_bytes, int size)
{
    int _read_max_bytes = (read_max_bytes);
    if(_read_max_bytes != -1 && (size) > _read_max_bytes) {
        return 0;
    }
    return 1;
}


static PyObject* websocket_mask(PyObject* self, PyObject* args) {
    const char* mask;
    Py_ssize_t mask_len;
    const char* data;
    Py_ssize_t data_len;
    Py_ssize_t i;
    PyObject* result;
    char* buf;

    if (!PyArg_ParseTuple(args, "s#s#", &mask, &mask_len, &data, &data_len)) {
        return NULL;
    }

    result = PyBytes_FromStringAndSize(NULL, data_len);
    if (!result) {
        return NULL;
    }
    buf = PyBytes_AsString(result);
    for (i = 0; i < data_len; i++) {
        buf[i] = data[i] ^ mask[i % 4];
    }

    return result;
}

static void
merge_prefix(PyObject* deque, int size)
{
    if(PySequence_Size(deque) == 1) {
        PyObject *first = PySequence_ITEM(deque, 0);
        if(PySequence_Size(first) <= size) {
            Py_DECREF(first);
            return;
        }
        Py_DECREF(first);
    }

    PyObject *prefix = PyList_New(0);
    int remaining = size;
    while(PySequence_Size(deque) > 0 && remaining > 0) {
        PyObject *chunk = PyObject_CallMethod(deque, "popleft", NULL);

        if(PySequence_Size(chunk) > remaining) {
            PyObject *s = PySequence_GetSlice(chunk, remaining, PySequence_Size(chunk));
            PyObject_CallMethod(deque, "appendleft", "O", s);
            Py_DECREF(s);

            PyObject *new_chunk = PySequence_GetSlice(chunk, 0, remaining);
            Py_DECREF(chunk);
            chunk = new_chunk;

        }
        PyList_Append(prefix, chunk);
        remaining -= PySequence_Size(chunk);

        Py_DECREF(chunk);
    }

    if(PySequence_Size(prefix) > 0) {
        PyObject *item = PySequence_ITEM(prefix, 0);
        PyObject *type = PyObject_Type(item);
        PyObject *obj = PyObject_CallObject(type, NULL);
        PyObject *joined_prefix = PyObject_CallMethod(obj,"join", "O", prefix);
        PyObject_CallMethod(deque, "appendleft", "O", joined_prefix);

        Py_DECREF(joined_prefix);
        Py_DECREF(item);
        Py_DECREF(type);
        Py_DECREF(obj);
    }
    if(PySequence_Size(deque) == 0)
        PyObject_CallMethod(deque, "appendleft", "s", "");

    Py_DECREF(prefix);
}

static void
double_prefix(PyObject *deque)
{

    PyObject *first_item = PySequence_ITEM(deque, 0);
    PyObject *second_item = PySequence_ITEM(deque, 1);
    int first_len = PySequence_Size(first_item);
    int second_len = PySequence_Size(second_item);
    int new_len = MAX(first_len * 2, first_len + second_len);
    merge_prefix(deque, new_len);
    Py_DECREF(first_item);
    Py_DECREF(second_item);
}

static void
IOStreamBuffer_dealloc(IOStreamBufferObject* self)
{
    Py_XDECREF(self->read_buffer);
    Py_XDECREF(self->write_buffer);
    Py_XDECREF(self->stream);
    self->ob_type->tp_free((PyObject*)self);
}

static PyObject *
IOStreamBuffer_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    IOStreamBufferObject *self;
    self = (IOStreamBufferObject *)type->tp_alloc(type, 0);
    self->read_buffer_size = 0;
    self->write_buffer_size = 0;
    self->write_buffer_offset = 0;
    self->write_pending = NULL;
    Py_INCREF(Py_None);
    self->write_buffer = Py_None;
    Py_INCREF(Py_None);
    self->read_buffer = Py_None;
    Py_INCREF(Py_None);
    self->stream = Py_None;
    self->max_write_buffer_size = -1;
    self->write_buffer_frozen = 0;
    self->read_max_bytes = -1;
    return (PyObject *) self;
}

static int
IOStreamBuffer_init(IOStreamBufferObject *self, PyObject *args, PyObject *kwds)
{
    Py_DECREF(Py_None);
    self->read_buffer = PyObject_CallMethod(collections_module, "deque", NULL);
    if (self->read_buffer == NULL)
        return -1;
    Py_DECREF(Py_None);
    self->write_buffer = PyObject_CallMethod(collections_module, "deque", NULL);
    if (self->write_buffer == NULL)
        return -1;
    PyObject *max_write_buffer_size;
    PyObject *stream;
    if (!PyArg_ParseTuple(args, "OO", &stream, &max_write_buffer_size)) {
        return -1;
    }
    Py_INCREF(max_write_buffer_size);
    Py_INCREF(stream);
    self->stream = stream;
    if (max_write_buffer_size == Py_None) {
        self->max_write_buffer_size = -1;
    } else {
        self->max_write_buffer_size = PyLong_AsLong(max_write_buffer_size);
    }
    Py_DECREF(max_write_buffer_size);

    return 0;
}

static PyObject *
IOStreamBuffer_get_read_max_bytes(IOStreamBufferObject *self, void *closure)
{
    if (self->read_max_bytes == -1) {
        Py_RETURN_NONE;
    } else {
        return PyLong_FromLong(self->read_max_bytes);
    }
}

static int
IOStreamBuffer_set_read_max_bytes(IOStreamBufferObject *self, PyObject *value, void *closure)
{
    if (value == Py_None) {
        self->read_max_bytes = -1;
    } else {
        self->read_max_bytes = PyLong_AsLong(value);
    }
    return 0;
}

static PyObject *
IOStreamBuffer_find_read_pos(IOStreamBufferObject* self, PyObject* args)
{
    PyObject *read_delimiter;
    PyObject *read_bytes;
    PyObject *read_partial;
    PyObject *read_regex;
    if (!PyArg_ParseTuple(args, "OOOO",
                          &read_delimiter,
                          &read_bytes,
                          &read_partial,
                          &read_regex)) {
        return NULL;
    }
    Py_INCREF(read_bytes);
    Py_INCREF(read_delimiter);
    Py_INCREF(read_partial);
    Py_INCREF(read_regex);
    if (read_bytes != Py_None) {
        int cread_bytes = PyInt_AS_LONG(read_bytes);
        if (self->read_buffer_size >= cread_bytes ||
            (PyObject_IsTrue(read_partial) && self->read_buffer_size > 0)) {
            int num_bytes = MIN(cread_bytes, self->read_buffer_size);

            Py_DECREF(read_bytes);
            Py_DECREF(read_delimiter);
            Py_DECREF(read_partial);
            Py_DECREF(read_regex);
            return PyLong_FromLong(num_bytes);
        }
    } else if (read_delimiter != Py_None) {
        if (PyObject_IsTrue(self->read_buffer)) {
            while (1) {
                PyObject *item = PySequence_ITEM(self->read_buffer, 0);
                PyObject *find_method_name = PyString_FromString("find");
                PyObject *py_loc = PyObject_CallMethodObjArgs(item,
                                                              find_method_name,
                                                              read_delimiter, NULL);
                Py_DECREF(find_method_name);
                Py_DECREF(item);
                int loc = PyInt_AS_LONG(py_loc);
                Py_DECREF(py_loc);
                if (loc != -1) {
                    int delimiter_len = PySequence_Size(read_delimiter);

                    if (! check_max_bytes(self->read_max_bytes, loc + delimiter_len)) {
                        goto MAX_BYTES_ERROR;
                    }
                    Py_DECREF(read_bytes);
                    Py_DECREF(read_delimiter);
                    Py_DECREF(read_partial);
                    Py_DECREF(read_regex);
                    return PyLong_FromLong(loc + delimiter_len);
                }
                if (PySequence_Size(self->read_buffer) == 1)
                    break;
                double_prefix(self->read_buffer);
            }
            PyObject *item = PySequence_ITEM(self->read_buffer, 0);
            if (! check_max_bytes(self->read_max_bytes, PySequence_Size(item))) {
                Py_DECREF(item);
                goto MAX_BYTES_ERROR;
            }
            Py_DECREF(item);
        }
    } else if (read_regex != Py_None) {
        if (PyObject_IsTrue(self->read_buffer)) {
            while (1) {
                PyObject *item = PySequence_ITEM(self->read_buffer, 0);
                PyObject *search_method_name = PyString_FromString("search");
                PyObject *m = PyObject_CallMethodObjArgs(read_regex, search_method_name, item, NULL);
                Py_DECREF(item);
                Py_DECREF(search_method_name);
                if (m != Py_None) {
                    PyObject *mend = PyObject_CallMethod(m, "end", NULL);
                    Py_DECREF(m);
                    int cmend = PyLong_AsLong(mend);
                    Py_DECREF(mend);
                    if (! check_max_bytes(self->read_max_bytes, cmend)) {
                        goto MAX_BYTES_ERROR;
                    }
                    Py_DECREF(read_bytes);
                    Py_DECREF(read_delimiter);
                    Py_DECREF(read_partial);
                    Py_DECREF(read_regex);
                    return PyLong_FromLong(cmend);
                }
                Py_DECREF(m);
                if (PySequence_Size(self->read_buffer) == 1) {
                    break;
                }
                double_prefix(self->read_buffer);
            }
            PyObject *item = PySequence_ITEM(self->read_buffer, 0);
            if (! check_max_bytes(self->read_max_bytes, PySequence_Size(item))) {
                Py_DECREF(item);
                goto MAX_BYTES_ERROR;
            }
            Py_DECREF(item);
        }
    }
    Py_DECREF(read_bytes);
    Py_DECREF(read_delimiter);
    Py_DECREF(read_partial);
    Py_DECREF(read_regex);
    Py_RETURN_NONE;

MAX_BYTES_ERROR:
    Py_DECREF(read_bytes);
    Py_DECREF(read_delimiter);
    Py_DECREF(read_partial);
    Py_DECREF(read_regex);
    PyErr_SetString(unsatisfiable_read_error, "delimiter %r not found within %d bytes");
    return NULL;

}

static unsigned int
prepare_write_data(PyObject *buffer, PyObject **out_bytes, Py_ssize_t len, unsigned int offset)
{
    if (len <= 0) {
        *out_bytes = NULL;
        return 0;
    }

    *out_bytes = PyByteArray_FromStringAndSize(NULL, len);

    Py_buffer out_bytes_buffer;
    PyObject_GetBuffer(*out_bytes, &out_bytes_buffer, PyBUF_WRITABLE|PyBUF_C_CONTIGUOUS);
    unsigned int item_offset = offset;
    unsigned int copied = 0;

    PyObject *iter = PySequence_Fast(buffer, "");
    int iter_size = PySequence_Fast_GET_SIZE(iter);
    for(int i = 0; i < iter_size && copied < len; i++, item_offset = 0) {
        PyObject *item = PySequence_Fast_GET_ITEM(iter, i);

        Py_buffer item_buffer;
        PyObject_GetBuffer(item, &item_buffer, PyBUF_C_CONTIGUOUS);

        Py_ssize_t item_len = item_buffer.len;

        int item_remaining = item_len - item_offset;


        if (item_remaining == 0) {
            // Buffer Item is copied -> POP ITEM from buffer
            PyObject *item = PyObject_CallMethod(buffer, "popleft", NULL);
            Py_DECREF(item);
        } else {
            int to_be_copied = len - copied;
            // Buffer Item contains data to be copied
            if(to_be_copied > 0 && item_len <= to_be_copied) {
                // remaining data to be copied are larger than remaining item -> copy rest of the buffer item
                memcpy(&((char *) out_bytes_buffer.buf)[copied],
                       &((char *) item_buffer.buf)[item_offset],
                       item_len);
                copied += item_len;
                // POP ITEM from buffer
                PyObject *item = PyObject_CallMethod(buffer, "popleft", NULL);
                Py_DECREF(item);
            } else if (to_be_copied > 0 &&  item_len > to_be_copied) {
                // remaining item is larger then amount of data to be copied -> copy the data remaining only.
                memcpy(&((char *) out_bytes_buffer.buf)[copied],
                       &((char *) item_buffer.buf)[item_offset],
                       to_be_copied);
                copied += to_be_copied;
            }
        }
        PyBuffer_Release(&item_buffer);

    }
    Py_DECREF(iter);
    out_bytes_buffer.len = copied + 1;
    PyBuffer_Release(&out_bytes_buffer);
    return item_offset;
}

static PyObject *
IOStreamBuffer_write_to_stream(IOStreamBufferObject* self, PyObject* args)
{
    int write_len;
    PyObject *out_bytes = NULL;
    if (! self->write_buffer_frozen) {
        write_len = MIN(127 * 1024, self->write_buffer_size);
        if (write_len == 0) {
            Py_RETURN_TRUE;
        }
        self->write_buffer_offset = prepare_write_data(self->write_buffer,
                                                       &out_bytes, write_len,
                                                       self->write_buffer_offset);

    } else {
        out_bytes = self->write_pending;
        write_len = PySequence_Size(out_bytes);
        self->write_pending = NULL;
    }

    PyObject *pynum_bytes = PyObject_CallMethodObjArgs(self->stream,
                                                       write_to_fd_method_name,
                                                       out_bytes, NULL);
    if (pynum_bytes == NULL) {
        self->write_pending = out_bytes;
        return NULL;
    }
    int num_bytes = PyInt_AS_LONG(pynum_bytes);
    Py_DECREF(pynum_bytes);

    if (num_bytes == 0) {
        self->write_pending = out_bytes;
        self->write_buffer_frozen = 1;
        Py_RETURN_FALSE;
    } else {
        self->write_buffer_frozen = 0;

        if(write_len > num_bytes) {

            self->write_pending = PySequence_GetSlice(out_bytes, num_bytes,
                                                      PySequence_Size(out_bytes));
            // PUT BACK TO BUFFER
            PyObject_CallMethodObjArgs(self->write_buffer, appendleft_method_name,
                                       self->write_pending, NULL);
            Py_DECREF(self->write_pending);
            self->write_pending = NULL;
        }

        self->write_buffer_size -= num_bytes;
        Py_DECREF(out_bytes);
        Py_RETURN_TRUE;
    }
}

static PyObject *
IOStreamBuffer_read_from_stream(IOStreamBufferObject* self, PyObject* args)
{
    PyObject *chunk = PyObject_CallMethod(self->stream, "read_from_fd", NULL);
    if (chunk == NULL)
        return NULL;
    if (chunk == Py_None) {
        Py_DECREF(chunk);
        return PyLong_FromLong(0);
    }
    PyObject_CallMethodObjArgs(self->read_buffer, append_method_name, chunk, NULL);
    size_t size = PySequence_Size(chunk);

    Py_DECREF(chunk);
    self->read_buffer_size += size;
    return PyLong_FromSize_t(size);
}



static PyObject *
IOStreamBuffer_consume(IOStreamBufferObject* self, PyObject *args)
{
    int loc;
    if (!PyArg_ParseTuple(args, "i", &loc)) {
        return NULL;
    }
    if(loc == 0) return PyBytes_FromString("");
    merge_prefix(self->read_buffer, loc);
    self->read_buffer_size -= loc;
    return PyObject_CallMethod(self->read_buffer, "popleft", NULL);
}


static PyObject *
IOStreamBuffer_add_to_buffer(IOStreamBufferObject *self, PyObject *args)
{
    PyObject *data;
    if (!PyArg_ParseTuple(args, "O", &data)) {
        return NULL;
    }
    Py_INCREF(data);
    if (data != Py_None && PySequence_Size(data) > 0) {
        int data_len = PySequence_Size(data);
        if (self->max_write_buffer_size != -1 &&
            self->write_buffer_size + data_len > self->max_write_buffer_size) {
            PyErr_SetString(stream_buffer_full_error, "Reached maximum write buffer size");
            Py_DECREF(data);
            return NULL;
        }
        PyObject_CallMethodObjArgs(self->write_buffer, append_method_name, data, NULL);
        self->write_buffer_size += data_len;
    }
    Py_DECREF(data);
    Py_RETURN_NONE;

}

static PyObject *
IOStreamBuffer_set_write_buffer_frozen(IOStreamBufferObject *self, PyObject *args)
{
    self->write_buffer_frozen = 1;
    Py_RETURN_NONE;
}
