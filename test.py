import psutil
from pg_select import cursor
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED

"""

See memory leaks:

$ sudo apt-get install valgrind
$ pip install pytest-valgrind
$ PYTHONMALLOC=malloc valgrind --leak-check=full --show-leak-kinds=definite --log-file=/tmp/valgrind-output \
python test.py -vv --valgrind --valgrind-log=/tmp/valgrind-output

"""


def execute_sql(connection, sentence):
    for row in cursor(connection, sentence, iteration=15, column_names=True, debug=False, format='tab'):
        print(row)


if __name__ == '__main__':
    print(psutil.virtual_memory().available)
    pool = ThreadPoolExecutor(max_workers=2)
    conn = "host=localhost port=5433 dbname=arena3 connect_timeout=10 user=postgres password=wS5boxC2ea"
    sql = [
        "select * from gateway_country order by country_code",
        "select * from gateway_currency"
    ]
    # # 1 thread
    execute_sql(conn, sql[0])
    execute_sql(conn, sql[1])
    # # 2 threads
    futures = [pool.submit(execute_sql, conn, sentence) for sentence in sql]
    wait(futures, return_when=ALL_COMPLETED)
    pool.shutdown()
    print(psutil.virtual_memory().available)
    pass
