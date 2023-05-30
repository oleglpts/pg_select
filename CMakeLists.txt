cmake_minimum_required(VERSION 3.25)
project(pg_select)

set(CMAKE_CXX_STANDARD 17)

add_library(pg_select SHARED library.cpp)
