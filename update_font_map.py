from fontTools.ttLib import TTFont
import requests
import os
import json
from bs4 import BeautifulSoup
from rediscluster import StrictRedisCluster

import font_decode
from utils import FontHandler

URL_TEMPLATE = 'https://static.tianyancha.com/fonts-styles/fonts/%s/%s/tyc-num.woff'
DECODE_KEY = 'tyc_decode'
REVERSE_DECODE_KEY = 'tyc_decode_reverse'

startup_nodes = [{"host": "172.22.5.147", "port": "7000"}]
rc = StrictRedisCluster(startup_nodes=startup_nodes, decode_responses=True)

def download_woff(woff_name):
    woff_file_path = 'fonts_woff/%s.woff' % woff_name
    if os.path.exists(woff_file_path):
        return woff_file_path
    woff_url = URL_TEMPLATE % (woff_name[:2], woff_name)
    print('下载woff文件: ' + woff_url)
    with open(woff_file_path, 'wb') as fd:
        r = requests.get(woff_url)
        fd.write(r.content)
        print('缓存woff文件到' + woff_file_path)
    return woff_file_path


if __name__ == "__main__":  
    with open('mapping.xml',encoding='utf-8') as f:
        mappings = BeautifulSoup(f, 'xml')
    i = 0
    for record in mappings.find_all('RECORD'):
        woff_name = record.WOFFURL.string
        print(i, woff_name)
        src_list = record.SOUR_WORD.string.split(',')
        src_words = set(src_list)
        dst_list = record.DEST_WORD.string.split(',')
        dst_words = set(dst_list)
        flag = False
        for word in dst_words:
            if not word in src_words:
                print('识别错误：', word)
                # flag = True
        for word in src_words:
            if not word in dst_words:
                print('未识别：', word)
                # flag = True
        count = 0
        if not flag:
            if i < 443:
                i += 1
                continue
            download_woff(woff_name)
            font_handler = FontHandler(woff_name)
            char_order = font_handler.get_char_order()
            for j in range(len(char_order)):
                code_word = char_order[j]
                decoded_word = font_decode.decode_char(font_handler, code_word)
                if j < len(src_list):
                    src_word = src_list[j]
                    dst_word = dst_list[j]
                else:
                    src_word = code_word
                    dst_word = decoded_word
                    print('映射文件不全，直接验证解码：%s ==> %s' % (src_word, dst_word))
                if code_word != src_word:
                    print("原字符'%s'不正确，使用实际字符'%s'" % (src_word, code_word))
                    src_word = code_word
                if decoded_word is not None:
                    if decoded_word != dst_word and not dst_word in ['曰','日','千','干']:
                        print('映射与识别结果不符：%s ==> %s, %s' % (src_word, dst_word, decoded_word))
                        signal = input('是否继续？')
                        if signal is not None and len(signal) > 0:
                            exit(1)
                elif dst_word is not None and font_decode.get_font_hash(dst_word) is not None:
                    print("%s ==> %s 中，解码字符已与其他字体映射，请排查" % (src_word,dst_word))
                    exit(1)
                else:
                    count += 1
                    print('新增：%s ==> %s' % (src_word, dst_word))
                    image_path = font_handler.save_image(src_word)
                    if dst_word not in src_words:
                        print('识别错误，%s 在编码文字中不存在！' % dst_word)
                        input_word = input('请根据图片%s输入正确字符,以$结束：' % image_path)
                        if input_word is None or not input_word.endswith('$'):
                            exit(1)
                        dst_word = input_word[:-1]
                    else:
                        input_word = input('请查看图片%s确认：' % image_path)
                        if input_word is None or input_word.lower() != 'yes':
                            exit(1)
                    font_decode.add_font_map(font_handler, src_word, dst_word)
        else:
            break
        print('新增字体映射%d个' % count)
        i += 1
        if i > 500:
            break
    print('total:', i)