import os
import traceback

import cx_Oracle

import config

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

def fetch_dict(db, sql):
    results = []
    try:
        cursor = db.cursor()
        cursor.execute(sql)
        columns = [i[0] for i in cursor.description]
        rows = cursor.fetchall()
        for row in rows:
            ele = {}
            for i in range(len(row)):
                ele[columns[i]] = row[i]
            results.append(ele)
        cursor.close()
    except Exception as e:
        traceback.print_exc()
        print(Exception, ":", e)
    finally:
        return results


def creat_if_not_exist(db, table_name):
    sql = "select count(*) as count from user_tables where table_name = upper('%s')" % table_name
    cursor = db.cursor()
    cursor.execute(sql)
    count = cursor.fetchone()[0]
    if not count > 0:
        print("表%s不存在，需创建表" % table_name, end="...")
        create_table_sql = '''
            create table %s (
                WOFFURL VARCHAR2(100),
                SOUR_NUM VARCHAR2(100),
                DEST_NUM VARCHAR2(100),
                SOUR_WORD VARCHAR2(4000),
                DEST_WORD VARCHAR2(4000),
                INSERT_TIME VARCHAR2(100)
            )
        ''' % table_name
        cursor.execute(create_table_sql)
        db.commit()
        print('OK')
    cursor.close()


def insert_or_update_font(db, woff_dicts, table_name):
    insert_table_sql = '''
               MERGE INTO %s T1
               USING (SELECT :WOFFURL AS WOFFURL FROM dual) T2
               ON ( T1.WOFFURL=T2.WOFFURL)
               WHEN NOT MATCHED THEN
               INSERT (WOFFURL,SOUR_NUM,DEST_NUM,SOUR_WORD,DEST_WORD,INSERT_TIME) values(:WOFFURL,:SOUR_NUM,:DEST_NUM,:SOUR_WORD,:DEST_WORD,:INSERT_TIME)
               ''' % table_name
    cursor = db.cursor()
    cursor.prepare(insert_table_sql)
    cursor.executemany(None, woff_dicts)
    cursor.close()
    db.commit()


if __name__ == '__main__':
    ip = config.ip
    port = config.port
    database = config.db
    username = config.username
    pwd = config.pwd

    dsn = cx_Oracle.makedsn(ip, port, db)
    db = cx_Oracle.connect(username, pwd, dsn)
    results = fetch_dict(db, 'SELECT * FROM TYC_MISS')

    print(len(results), db.version)
