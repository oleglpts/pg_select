from pg_select import cursor
from concurrent.futures import ThreadPoolExecutor


def execute_sql(connection, sentence):
    for row in cursor(connection, sentence, iteration=15, column_names=True, debug=False):
        print(row)


if __name__ == '__main__':
    pool = ThreadPoolExecutor(max_workers=10)
    conn = "host=localhost port=5433 dbname=arena3 connect_timeout=10 user=postgres password=wS5boxC2ea"
    sql = "select * from gateway_country"

    for i in range(30):
        pool.submit(execute_sql, conn, sql)
