import asyncio
import time
from datetime import datetime

import cx_Oracle
from rediscluster import StrictRedisCluster

import oracle_db,config
from utils import FontHandler, MD5

DECODE_KEY = config.DECODE_KEY
REVERSE_DECODE_KEY = config.REVERSE_DECODE_KEY
redis_nodes = config.redis_nodes
rc = StrictRedisCluster(startup_nodes=redis_nodes, decode_responses=True)

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


async def init_handler(woff_name):
    return await FontHandler.create(woff_name)


async def decode_woff(font_handler):
    woff_name = font_handler.get_woff_name()

    global index
    index += 1
    print('%d: %s解码' % (index, woff_name), end='...')

    woff_dict = {'WOFFURL': woff_name}
    src_nums, dst_nums = decode_num(font_handler.get_num_order())
    # print(src_nums)
    # print(dst_nums)
    woff_dict['SOUR_NUM'] = ','.join(src_nums)
    woff_dict['DEST_NUM'] = ','.join(dst_nums)

    src_words = font_handler.get_char_order()
    dst_words = []

    for src_word in src_words:
        #
        # print('%s: %s ==>' % (index, src_word), end=' ')
        dst_word = decode_char(font_handler, src_word, use_image=True)
        dst_words.append(dst_word)
        # print(dst_word)
    # end = datetime.now()
    # print((start-end).seconds)
    # print(src_words)
    # print(dst_words)
    woff_dict['SOUR_WORD'] = ','.join(src_words)
    woff_dict['DEST_WORD'] = ','.join(dst_words)
    woff_dict['INSERT_TIME'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    print('OK')
    return woff_dict


async def decode_job(db, woffs, table_name):
    total = len(woffs)
    batch_size = 50
    start = 0
    while start < total:
        end = total if start + batch_size > total else start + batch_size
        init_tasks = [init_handler(woff['WOFFURL']) for woff in woffs[start:end]]
        handlers = await asyncio.gather(*init_tasks)
        decode_tasks = [decode_woff(handler) for handler in handlers]
        woff_dicts = await asyncio.gather(*decode_tasks)
        print('将该批次插入数据库', end='...')
        oracle_db.insert_or_update_font(db, woff_dicts, table_name)
        start = end
        print('OK')


if __name__ == "__main__":
    db = init_db()
    woffs = oracle_db.fetch_dict(db, 'SELECT * FROM TYC_MISS')

    table_name = 'TYC_WOFF_CODE_' + datetime.now().strftime('%Y%m%d') + '_TEST'
    oracle_db.creat_if_not_exist(db, table_name)

    start = time.time()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(decode_job(db, woffs, table_name))
    loop.close()
    end = time.time()
    print('Cost {} seconds'.format(end - start))

    db.close()