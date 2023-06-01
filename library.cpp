#include "library.h"

/* Iterator status structure */

typedef struct {
    PyObject_HEAD                                                                               // header
    int m;                                                                                      // current chunk length
    int i;                                                                                      // current position
    bool stop;                                                                                  // stop cursor flag
    int nFields;                                                                                // fields counter
    PGresult *res;                                                                              // select result
    PyObject *it;                                                                               // for garbage collector
    PyObject *tmp;                                                                              // result
    pg_conn *conn;                                                                              // db connection
    int iteration;                                                                              // cursor iteration step
    bool column_names;                                                                          // display column names
    bool debug;                                                                                 // debug mode
    char *cursor_name;                                                                          // cursor name
    char *format;                                                                               // format (TODO)
} pg_select_iter;

/* Iterator */

PyObject* cursor_iter(PyObject *self)
{
    Py_INCREF(self);
    return self;
}

/* Used garbage collector */

static int pg_select_iter_traverse(pg_select_iter *self, visitproc visit, void *arg)
{
    Py_VISIT(self->it);
    return 0;
}

static int pg_select_iter_clear(pg_select_iter *self)
{
    Py_CLEAR(self->it);
    return 0;
}

static void pg_select_iter_dealloc(pg_select_iter *self)
{
    PyObject_GC_UnTrack(self);
    pg_select_iter_clear(self);
    PyObject_GC_Del(self);
}

/* Clear PostgreSQL structures */

void clear_cursor(pg_select_iter *p, bool stopIteration=true) {
    PQclear(p->res);
    std::string cursor_name(p->cursor_name);
    std::string command = "CLOSE " + cursor_name;
    debug_print("__next__: command execution " + command)
    PQclear(PQexec(p->conn, command.c_str()));
    PQclear(PQexec(p->conn, "END"));
    debug_print("__next__: transaction ended")
    PQfinish(p->conn);
    if(stopIteration)
        PyErr_SetNone(PyExc_StopIteration);
    delete [] p->cursor_name;
    debug_print("__next__: connection cleared")
}

/* Next function */

PyObject* cursor_next(PyObject *self)
{
    auto *p = (pg_select_iter *)self;
    if ((p->i < 0) || (p->i >= p->m)) {
        if(p->stop) {
            debug_print("__next__: cursor stopped")
            clear_cursor(p);
            return (PyObject*) nullptr;
        }
        std::string cursor_name(p->cursor_name);
        std::string command = "FETCH " + std::to_string(p->iteration) + " " + cursor_name;
        debug_print("__next__: command execution " + command)
        if(p->res)
            PQclear(p->res);
        p->res = PQexec(p->conn, command.c_str());
        if (PQresultStatus(p->res) != PGRES_TUPLES_OK)
        {
            PyErr_SetString(PyExc_ConnectionError, PQerrorMessage(p->conn));
            clear_cursor(p, false);
            return (PyObject*) nullptr;
        }
        p->m = PQntuples(p->res);
        if(p->m < p->iteration) {
            debug_print("__next__: set finalization flag")
            p->stop = true;
        }
        if(p->i < 0) {
            p->nFields = PQnfields(p->res);
            if(p->column_names) {
                p->i = 0;
                p->tmp = PyList_New(p->nFields);
                for (int i = 0; i < p->nFields; i++)
                    PyList_SetItem(p->tmp, i, PyUnicode_FromString(PQfname(p->res, i)));
                return p->tmp;
            }
        }
        p->i = 0;
        p->m = PQntuples(p->res);
        if(!p->m) {
            debug_print("__next__: cursor stopped")
            clear_cursor(p);
            return (PyObject*) nullptr;
        }
    }
    p->tmp = PyList_New(p->nFields);
    for (int i = 0; i < p->nFields; i++)
        PyList_SetItem(p->tmp, i, PyUnicode_FromString(PQgetvalue(p->res, p->i, i)));
    p->i++;
    return p->tmp;
}

/* Python object for module */

static PyTypeObject pg_select_iterType = {
        PyObject_HEAD_INIT(nullptr)
        "pg_select._iter",
        sizeof(pg_select_iter),
        0,
        reinterpret_cast<destructor>(pg_select_iter_dealloc),
        0,
        nullptr,
        nullptr,
        nullptr,
        nullptr,
        nullptr,
        nullptr,
        nullptr,
        nullptr,
        nullptr,
        nullptr,
        nullptr,
        nullptr,
        nullptr,
        Py_TPFLAGS_DEFAULT,
        "Internal myiter iterator object.",
        reinterpret_cast<traverseproc>(pg_select_iter_traverse),
        reinterpret_cast<inquiry>(pg_select_iter_clear),
        nullptr,
        0,
        cursor_iter,
        cursor_next,
        nullptr
};

/* Generating random string */

std::string random_string()
{
    std::string str("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz");
    std::random_device rd;
    std::mt19937 generator(rd());
    std::shuffle(str.begin(), str.end(), generator);
    return "a" + str.substr(0, 32);
}

/* Create iterator method */

PyObject *cursor(PyObject *self, PyObject *args, PyObject *kwargs) {
    pg_select_iter *p;
    PGresult *res;
    char *connection = nullptr, *sql = nullptr;
    static char *kwlist[] = {
            (char *) "connection",
            (char *) "sql",
            (char *) "format",
            (char *) "iteration",
            (char *) "column_names",
            (char *) "debug",
            nullptr
    };
    p = PyObject_GC_New(pg_select_iter, &pg_select_iterType);
    if (!p) return nullptr;
    p->iteration = 20000;
    p->column_names = true;
    p->debug = false;
    std::string random_str = random_string();
    p->format = nullptr;
    p->cursor_name = new char[random_str.length() + 1];
    strcpy(p->cursor_name, random_str.c_str());
    std::string cursor_name(p->cursor_name);
    std::string cursor = "DECLARE " + cursor_name + " CURSOR FOR ", end_cursor = ";";
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "ss|slbb", kwlist, &connection, &sql, &p->format,
                                     &p->iteration, &p->column_names, &p->debug))
        return nullptr;
    p->conn = PQconnectdb(connection);
    if (PQstatus(p->conn) != CONNECTION_OK) {
        PyErr_SetString(PyExc_ConnectionError, PQerrorMessage(p->conn));
        return (PyObject*) nullptr;
    }
    debug_print("__iter__: connected to database")
    res = PQexec(p->conn, "BEGIN");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        PyErr_SetString(PyExc_ConnectionError, PQerrorMessage(p->conn));
        PQclear(res);
        return (PyObject*) nullptr;
    }
    debug_print("__iter__: transaction started")
    PQclear(res);
    std::string s(sql);
    std::string command = cursor + s + end_cursor;
    debug_print("__iter__: command execution " + command)
    res = PQexec(p->conn, command.c_str());
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        PyErr_SetString(PyExc_ConnectionError, PQerrorMessage(p->conn));
        PQclear(res);
        return (PyObject*) nullptr;
    }
    PQclear(res);
    p->it = nullptr;
    p->tmp = nullptr;
    PyObject_GC_Track(p);
    if (!PyObject_Init((PyObject *)p, &pg_select_iterType)) {
        Py_DECREF(p);
        return nullptr;
    }
    p->m = p->iteration;
    p->i = -1;
    p->res = nullptr;
    p->stop = false;
    return (PyObject *)p;
}

/* Methods definition structure*/

static PyMethodDef PG_SelectMethods[] = {
        {
            "cursor",                                                                           // method name
            (PyCFunction)cursor,                                                                // implementation
            METH_VARARGS | METH_KEYWORDS,                                                       // parameters flags
            "Iterate database cursor"                                                           // documentation
        },
        {nullptr, nullptr, 0, nullptr}
};

/* Module definition structure */

static struct PyModuleDef pg_select = {
        PyModuleDef_HEAD_INIT,                                                                  // header
        "pg_select",                                                                            // module name
        "usage: import pg_select\n\n for record in pg_select.cursor(...):\n    # action\n",     // documentation
        -1,                                                                                     // keeps globals
        PG_SelectMethods                                                                         // methods
};

/* Module init function */

PyMODINIT_FUNC PyInit_pg_select(void) {
    return PyModule_Create(&pg_select);
}
