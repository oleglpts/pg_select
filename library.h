#ifndef PG_SELECT_LIBRARY_H
#define PG_SELECT_LIBRARY_H

#include <python3.8/Python.h>
#include <postgresql/libpq-fe.h>
#include <bits/stdc++.h>

#define debug_print(message) if(debug){std::cout << message << std::endl;}

pg_conn *conn;
int iteration = 20000;
bool column_names = true, debug = false;
std::string command, close_command = "CLOSE ", fetch_command = "FETCH ", cursor_name = "my_portal";

#endif //PG_SELECT_LIBRARY_H
