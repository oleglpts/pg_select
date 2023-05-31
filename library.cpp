#include "library.h"

#include <iostream>

/* Iterator status structure */

typedef struct {
    PyObject_HEAD                                                                               // header
    int m;                                                                                      // current chunk length
    int i;                                                                                      // current position
    bool stop;                                                                                  // stop cursor flag
    int nFields;                                                                                // fields counter
    PGresult *res;                                                                              // select result
    PyObject *it;                                                                               // for garbage collector
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
    command = close_command + cursor_name;
    debug_print("__next__: command execution " + command)
    PQclear(PQexec(conn, command.c_str()));
    PQclear(PQexec(conn, "END"));
    debug_print("__next__: transaction ended")
    PQfinish(conn);
    if(stopIteration)
        PyErr_SetNone(PyExc_StopIteration);
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
        command = fetch_command + std::to_string(iteration) + " " + cursor_name;
        debug_print("__next__: command execution " + command)
        p->res = PQexec(conn, command.c_str());
        if (PQresultStatus(p->res) != PGRES_TUPLES_OK)
        {
            PyErr_SetString(PyExc_ConnectionError, PQerrorMessage(conn));
            clear_cursor(p, false);
            return (PyObject*) nullptr;
        }
        p->m = PQntuples(p->res);
        if(p->m < iteration) {
            debug_print("__next__: set finalization flag")
            p->stop = true;
        }
        if(p->i < 0) {
            p->nFields = PQnfields(p->res);
            if(column_names) {
                p->i = 0;
                PyObject *tmp = PyList_New(0);
                for (int i = 0; i < p->nFields; i++)
                    PyList_Append(tmp, PyUnicode_FromString(PQfname(p->res, i)));
                return tmp;
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
    PyObject *tmp = PyList_New(0);
    for (int i = 0; i < p->nFields; i++)
        PyList_Append(tmp, PyUnicode_FromString(PQgetvalue(p->res, p->i, i)));
    p->i++;
    return tmp;
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
    cursor_name = random_string();
    std::string cursor = "DECLARE " + cursor_name + " CURSOR FOR ", end_cursor = ";";
    char *connection, *sql, *format = (char *) "tab";
    static char *kwlist[] = {
            (char *) "connection",
            (char *) "sql",
            (char *) "format",
            (char *) "iteration",
            (char *) "column_names",
            (char *) "debug",
            nullptr
    };
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "ss|slbb", kwlist, &connection, &sql, &format,
                                     &iteration, &column_names, &debug))
        return nullptr;
    conn = PQconnectdb(connection);
    if (PQstatus(conn) != CONNECTION_OK) {
        PyErr_SetString(PyExc_ConnectionError, PQerrorMessage(conn));
        return (PyObject*) nullptr;
    }
    debug_print("__iter__: connected to database")
    res = PQexec(conn, "BEGIN");
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        PyErr_SetString(PyExc_ConnectionError, PQerrorMessage(conn));
        PQclear(res);
        return (PyObject*) nullptr;
    }
    debug_print("__iter__: transaction started")
    PQclear(res);
    std::string s(sql);
    command = cursor + s + end_cursor;
    debug_print("__iter__: command execution " + command)
    res = PQexec(conn, command.c_str());
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        PyErr_SetString(PyExc_ConnectionError, PQerrorMessage(conn));
        PQclear(res);
        return (PyObject*) nullptr;
    }
    PQclear(res);
    p = PyObject_GC_New(pg_select_iter, &pg_select_iterType);
    if (!p) return nullptr;
    p->it = nullptr;
    PyObject_GC_Track(p);
    if (!PyObject_Init((PyObject *)p, &pg_select_iterType)) {
        Py_DECREF(p);
        return nullptr;
    }
    p->m = iteration;
    p->i = -1;
    p->stop = false;
    return (PyObject *)p;
}

/* Methods definition structure*/

static PyMethodDef PGSelectMethods[] = {
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
        PGSelectMethods                                                                         // methods
};

/* Module init function */

PyMODINIT_FUNC PyInit_pg_select(void) {
    return PyModule_Create(&pg_select);
}
