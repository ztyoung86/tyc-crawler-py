import asyncio
import time
import traceback
from datetime import datetime

import cx_Oracle
from rediscluster import StrictRedisCluster

import config
import oracle_db
from utils import FontHandler, MD5, download_woff

DECODE_KEY = config.DECODE_KEY
REVERSE_DECODE_KEY = config.REVERSE_DECODE_KEY
redis_nodes = config.redis_nodes
rc = StrictRedisCluster(startup_nodes=redis_nodes, decode_responses=True)

total = 0
index = 0


def init_db():
    ip = config.ip
    port = config.port
    database = config.db
    username = config.username
    pwd = config.pwd

    dsn = cx_Oracle.makedsn(ip, port, database)
    db = cx_Oracle.connect(username, pwd, dsn)
    return db

def decode_num(num_order_list):
    return num_order_list, sorted(num_order_list)

def decode_char(font_handler, char, use_image=False):
    json_md5 = MD5(font_handler.get_font_json(char))
    decoded_word = rc.hget(DECODE_KEY, json_md5)
    if decoded_word is None and use_image:
        image_path = font_handler.save_image(char)
        print(('发现新字体，生成图片：%s' % image_path), end=', ')
        decoded_word = input('请根据图片手动输入解码字符：')
        rc.hset(DECODE_KEY, json_md5, decoded_word)
        rc.hset(REVERSE_DECODE_KEY, decoded_word, json_md5)
    return decoded_word

def get_font_hash(dst_word):
    return rc.hget(REVERSE_DECODE_KEY, dst_word)

def add_font_map(font_handler, src_word, dst_word):
    json_md5 = MD5(font_handler.get_font_json(src_word))
    rc.hset(DECODE_KEY, json_md5, dst_word)
    rc.hset(REVERSE_DECODE_KEY, dst_word, json_md5)

def decode_woff(woff_name):
    font_handler = FontHandler(woff_name)

    global index, total
    index += 1
    print('%d/%d: %s解码' % (index, total, woff_name), end='...')

    woff_dict = {'WOFFURL': woff_name}
    src_nums, dst_nums = decode_num(font_handler.get_num_order())
    woff_dict['SOUR_NUM'] = ','.join(src_nums)
    woff_dict['DEST_NUM'] = ','.join(dst_nums)

    src_words = font_handler.get_char_order()
    dst_words = []

    for src_word in src_words:
        dst_word = decode_char(font_handler, src_word, use_image=True)
        dst_words.append(dst_word)
    woff_dict['SOUR_WORD'] = ','.join(src_words)
    woff_dict['DEST_WORD'] = ','.join(dst_words)
    woff_dict['INSERT_TIME'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    print('OK')
    return woff_dict

async def decode_job(db, woffs, table_name):
    global total
    total = len(woffs)
    batch_size = 50
    start = 0
    while start < total:
        end = total if start + batch_size > total else start + batch_size
        download_tasks = [download_woff(woff['WOFFURL']) for woff in woffs[start:end]]
        await asyncio.gather(*download_tasks)
        woff_dicts = []
        try:
            for woff in woffs[start:end]:
                woff_dicts.append(decode_woff(woff['WOFFURL']))
        except Exception as e:
            traceback.print_exc()
            print(Exception, ":", e)
        print('将该批次插入数据库', end='...')
        oracle_db.insert_or_update_font(db, woff_dicts, table_name)
        start = end
        print('OK')


if __name__ == "__main__":
    db = init_db()
    table_name = 'TYC_WOFF_CODE_' + datetime.now().strftime('%Y%m%d')
    oracle_db.creat_if_not_exist(db, table_name)

    woffs = oracle_db.fetch_dict(db, 'SELECT * FROM TYC_MISS T WHERE NOT EXISTS(SELECT 1 FROM %s T1 WHERE T1.WOFFURL = T.WOFFURL)' % table_name)

    start = time.time()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(decode_job(db, woffs, table_name))
    loop.close()
    end = time.time()
    print('Cost {} seconds'.format(end - start))

    db.close()