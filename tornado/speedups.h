#include <Python.h>
#include <structmember.h>

typedef struct {
    PyObject_HEAD
    /* Type-specific fields go here. */
    int read_buffer_size;
    int write_buffer_size;
    unsigned int write_buffer_offset;
    PyObject *write_pending;
    PyObject *stream;
    PyObject *read_buffer;
    PyObject *write_buffer;
    int max_write_buffer_size;
    int write_buffer_frozen;
    int read_max_bytes;
} IOStreamBufferObject;

static PyObject *collections_module;
static PyObject *unsatisfiable_read_error;
static PyObject *stream_buffer_full_error;

static PyObject *read_from_fd_method_name;
static PyObject *write_to_fd_method_name;
static PyObject *appendleft_method_name;
static PyObject *append_method_name;

static inline int
check_max_bytes(int read_max_bytes, int size);

static PyObject* websocket_mask(PyObject* self, PyObject* args);

static void
merge_prefix(PyObject* deque, int size);


static void double_prefix(PyObject *deque);

static void
IOStreamBuffer_dealloc(IOStreamBufferObject* self);

static void
IOStreamBuffer_dealloc(IOStreamBufferObject* self);

static PyObject *
IOStreamBuffer_new(PyTypeObject *type, PyObject *args, PyObject *kwds);

static int
IOStreamBuffer_init(IOStreamBufferObject *self, PyObject *args, PyObject *kwds);

static PyObject *
IOStreamBuffer_get_read_max_bytes(IOStreamBufferObject *self, void *closure);

static int
IOStreamBuffer_set_read_max_bytes(IOStreamBufferObject *self, PyObject *value, void *closure);

static PyObject *
IOStreamBuffer_find_read_pos(IOStreamBufferObject* self, PyObject* args);

static PyObject *
IOStreamBuffer_write_to_stream(IOStreamBufferObject* self, PyObject* args);

static PyObject *
IOStreamBuffer_read_from_stream(IOStreamBufferObject* self, PyObject* args);

static PyObject *
IOStreamBuffer_consume(IOStreamBufferObject* self, PyObject *args);

static PyObject *
IOStreamBuffer_add_to_buffer(IOStreamBufferObject *self, PyObject *args);

static PyObject *
IOStreamBuffer_set_write_buffer_frozen(IOStreamBufferObject *self, PyObject *args);

static PyMethodDef methods[] = {
    {"websocket_mask",  websocket_mask, METH_VARARGS, ""},
    {NULL, NULL, 0, NULL}
};

static PyGetSetDef IOStreamBuffer_getseters[] = {
    {"read_max_bytes",
     (getter)IOStreamBuffer_get_read_max_bytes, (setter)IOStreamBuffer_set_read_max_bytes,
     "", NULL},
    {NULL}  /* Sentinel */
};

static PyMethodDef IOStreamBuffer_methods[] = {
    {"consume", (PyCFunction) IOStreamBuffer_consume, METH_VARARGS, ""},
    {"write_to_stream", (PyCFunction) IOStreamBuffer_write_to_stream, METH_NOARGS, ""},
    {"read_from_stream", (PyCFunction) IOStreamBuffer_read_from_stream, METH_NOARGS, ""},
    {"add_to_buffer", (PyCFunction) IOStreamBuffer_add_to_buffer, METH_VARARGS, ""},
    {"find_read_pos", (PyCFunction) IOStreamBuffer_find_read_pos, METH_VARARGS, ""},
    {"set_write_buffer_frozen", (PyCFunction) IOStreamBuffer_set_write_buffer_frozen, METH_NOARGS, ""},
    {NULL}  /* Sentinel */
};

static PyMemberDef IOStreamBuffer_members[] = {
    {"_max_write_buffer_size", T_INT, offsetof(IOStreamBufferObject, max_write_buffer_size), 0, ""},
    {"_read_buffer", T_OBJECT_EX, offsetof(IOStreamBufferObject, read_buffer), 0,""},
    {"_write_buffer", T_OBJECT_EX, offsetof(IOStreamBufferObject, write_buffer), 0,""},
    {"_write_buffer_frozen", T_INT, offsetof(IOStreamBufferObject, write_buffer_frozen), 0, ""},
    {"_read_buffer_size", T_INT, offsetof(IOStreamBufferObject, read_buffer_size), 0, ""},
    {"_write_buffer_size", T_INT, offsetof(IOStreamBufferObject, write_buffer_size), 0, ""},
    //{"read_max_bytes", T_INT, offsetof(IOStreamBufferObject, read_max_bytes), 0, ""},
    {NULL}  /* Sentinel */
};

static PyTypeObject speedups_IOStreamBufferType = {
    PyObject_HEAD_INIT(NULL)
    0,                         /*ob_size*/
    "speedups.IOStreamBuffer",             /*tp_name*/
    sizeof(IOStreamBufferObject),             /*tp_basicsize*/
    0,                         /*tp_itemsize*/
    (destructor)IOStreamBuffer_dealloc, /*tp_dealloc*/
    0,                         /*tp_print*/
    0,                         /*tp_getattr*/
    0,                         /*tp_setattr*/
    0,                         /*tp_compare*/
    0,                         /*tp_repr*/
    0,                         /*tp_as_number*/
    0,                         /*tp_as_sequence*/
    0,                         /*tp_as_mapping*/
    0,                         /*tp_hash */
    0,                         /*tp_call*/
    0,                         /*tp_str*/
    0,                         /*tp_getattro*/
    0,                         /*tp_setattro*/
    0,                         /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE, /*tp_flags*/
    "IOStream object",           /* tp_doc */
    0,                     /* tp_traverse */
    0,                     /* tp_clear */
    0,                     /* tp_richcompare */
    0,                     /* tp_weaklistoffset */
    0,                     /* tp_iter */
    0,                     /* tp_iternext */
    IOStreamBuffer_methods,             /* tp_methods */
    IOStreamBuffer_members,                         /* tp_members */
    IOStreamBuffer_getseters,                         /* tp_getset */
    0,                         /* tp_base */
    0,                         /* tp_dict */
    0,                         /* tp_descr_get */
    0,                         /* tp_descr_set */
    0,                         /* tp_dictoffset */
    (initproc)IOStreamBuffer_init,      /* tp_init */
    0,                         /* tp_alloc */
    IOStreamBuffer_new,                 /* tp_new */
};

/* -----------------------------------------------------------*/


#if PY_MAJOR_VERSION >= 3
static struct PyModuleDef speedupsmodule = {
   PyModuleDef_HEAD_INIT,
   "speedups",
   NULL,
   -1,
   methods
};

PyMODINIT_FUNC
PyInit_speedups(void) {
    return PyModule_Create(&speedupsmodule);
}
#else  // Python 2.x
PyMODINIT_FUNC
initspeedups(void) {

    PyObject* m;
    speedups_IOStreamBufferType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&speedups_IOStreamBufferType) < 0)
        return;

    m = Py_InitModule("tornado.speedups", methods);
    collections_module = PyImport_ImportModule("collections");
    PyObject *iostreamexceptions_module = PyImport_ImportModule("tornado.iostreamexceptions");
    unsatisfiable_read_error = PyObject_GetAttrString(iostreamexceptions_module, "UnsatisfiableReadError");
    stream_buffer_full_error = PyObject_GetAttrString(iostreamexceptions_module, "StreamBufferFullError");

    if(collections_module == NULL)
        return;

    read_from_fd_method_name = PyString_FromString("read_from_fd");
    write_to_fd_method_name = PyString_FromString("write_to_fd");
    appendleft_method_name = PyString_FromString("appendleft");
    append_method_name = PyString_FromString("append");

    Py_INCREF(&speedups_IOStreamBufferType);
    PyModule_AddObject(m, "IOStreamBuffer", (PyObject *)&speedups_IOStreamBufferType);
    PyModule_AddObject(m, "collections", collections_module);
    PyModule_AddObject(m, "UnsatisfiableReadError", unsatisfiable_read_error);
    PyModule_AddObject(m, "StreamBufferFullError", stream_buffer_full_error);
    PyModule_AddObject(m, "append_method_name", append_method_name);
    PyModule_AddObject(m, "appendleft_method_name", appendleft_method_name);
    PyModule_AddObject(m, "write_to_fd_method_name", write_to_fd_method_name);
    PyModule_AddObject(m, "read_from_fd_method_name", read_from_fd_method_name);

}
#endif

