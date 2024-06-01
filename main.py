from datetime import datetime
import mysql.connector
import pytest

def mysql_connection():
    connection = mysql.connector.connect(
        host='192.168.31.113',
        user='root',
        password='P@ssw0rd',
        database='test'
    )
    return connection


def func_check_results(connection):
    cursor = connection.cursor()
    #cursor.execute("ALTER TABLE test DROP INDEX text_index")
    cursor.execute("SELECT * FROM test WHERE text LIKE 'password: %'")
    result = [i[0] for i in cursor.fetchall()]

    cursor.close()
    connection.close()

    result.sort()
    return result


def func_check_with_and_without_index(connection):
    cursor = connection.cursor()

    cursor.execute("SELECT * FROM test WHERE text LIKE 'password: %'")
    result_wo_ind = [i[0] for i in cursor.fetchall()]

    cursor.execute("CREATE INDEX text_index ON test (text)")
    cursor.execute("SELECT * FROM test WHERE text LIKE 'password: %'")
    result_wi_ind = [i[0] for i in cursor.fetchall()]
    cursor.execute("ALTER TABLE test DROP INDEX text_index;")

    cursor.close()
    connection.close()

    result_wi_ind.sort()
    result_wo_ind.sort()
    return result_wo_ind == result_wi_ind


def performance(connection):
    cursor = connection.cursor()

    start = datetime.now()
    cursor.execute("SELECT SQL_NO_CACHE * FROM test WHERE text LIKE 'some'")
    no_index_time = datetime.now() - start
    cursor.fetchall()

    cursor.execute("CREATE INDEX text_index ON test (text)")
    start = datetime.now()
    cursor.execute("SELECT SQL_NO_CACHE * FROM test USE INDEX (text_index) WHERE text LIKE 'some'")
    with_index_time = datetime.now() - start
    cursor.fetchall()
    cursor.execute("ALTER TABLE test DROP INDEX text_index")

    cursor.close()
    connection.close()

    return with_index_time < no_index_time


def insert(connection):
    cursor = connection.cursor()

    for i in range(10000):
        cursor.execute("INSERT INTO test (text, text2) VALUES ('somesdfsadfasfd', 'lsdfk');")
    connection.commit()
    cursor.close()
    connection.close()


def test_results():
    assert func_check_results(mysql_connection()) == ['password: 123456783874', 'password: askdfsaakdfas', 'password: askdfsaakdfas', 'password: secksldfkjllk', 'password: secret']

def test_equation_of_with_and_wo_index():
    assert func_check_with_and_without_index(mysql_connection()) == True

def test_performance():
    assert performance(mysql_connection()) == True

if __name__ == '__main__':
    print(func_check_results(mysql_connection()))
    print(func_check_with_and_without_index(mysql_connection()))
    print(performance(mysql_connection()))

    #insert(mysql_connection())
