import hashlib
import json
import os
from io import BytesIO

import aiohttp
import requests
from PIL import Image, ImageDraw, ImageFont
from bs4 import BeautifulSoup
from fontTools.ttLib import TTFont

URL_TEMPLATE = 'https://static.tianyancha.com/fonts-styles/fonts/%s/%s/tyc-num.woff'
IMAGE_DIR = 'images/'
WOFF_DIR = 'fonts_woff/'
TTX_DIR = 'fonts/'


async def fetch_content(url):
    async with aiohttp.ClientSession(read_timeout=None) as session:
        async with session.get(url) as response:
            return await response.read()


async def download_woff(woff_name):
    woff_file_path = 'fonts_woff/%s.woff' % woff_name
    # if os.path.exists(woff_file_path):
    #     return woff_file_path
    woff_url = URL_TEMPLATE % (woff_name[:2], woff_name)
    print('下载woff文件: ' + woff_url)
    with open(woff_file_path, 'wb') as fd:
        content = await fetch_content(woff_url)
        fd.write(content)
        # r = requests.get(woff_url)
        # fd.write(r.content)
        print('缓存woff文件到' + woff_file_path)
    return woff_file_path


class FontHandler:
    @classmethod
    async def create(cls, woff_name):
        woff_file_path = await download_woff(woff_name)
        return FontHandler(woff_name, woff_file_path)

    def __init__(self, woff_name, woff_file_path):
        print('handler of %s initializing...' % woff_name)
        self.woff_name = woff_name
        self.woff_file_path = woff_file_path
        self.__font = self.__fetch_font()
        self.__char_start_index = self.__font.getGlyphID('_')
        self.__reverse_cmap = {v: k for k, v in self.__font.getBestCmap().items()}
        self.__font_dict = self.__get_font_dict()
        print('handler initialized.')

    def get_woff_name(self):
        return self.woff_name

    def get_num_order(self):
        glyphOrder = self.__font.getGlyphOrder()
        return glyphOrder[2:self.__char_start_index]

    def get_char_order(self):
        char_glyphOrder = self.__font.getGlyphOrder()[self.__char_start_index:]
        return [chr(self.__reverse_cmap.get(glyphName)) for glyphName in char_glyphOrder]

    def get_font_json(self, char):
        char_code = ord(char)
        char_font = self.__font_dict.get(char_code)
        char_dict = char_font.attrs.copy()
        del char_dict['name']
        char_dict['contours'] = []
        for contour in char_font.find_all('contour'):
            pt_list = []
            for pt in contour.find_all('pt'):
                pt_list.append(pt.attrs)
            char_dict['contours'].append(pt_list)
        return json.dumps(char_dict, ensure_ascii=False, sort_keys=True)

    def save_image(self, word):
        font_path = os.path.join(WOFF_DIR, self.woff_name + '.woff')
        font = ImageFont.truetype(font_path, 150)
        image = Image.new('RGB', (200, 200), (255, 255, 255))
        draw = ImageDraw.Draw(image)
        draw.text((25, 0), word, font=font, fill=(0, 0, 0))
        image_name = self.woff_name + '_' + word + '.jpg'
        image_path = os.path.join(IMAGE_DIR, image_name)
        image.save(image_path)
        return image_path

    def __fetch_font(self, save_xml=False):
        # if os.path.exists(ttx_file_path):
        #     font = TTFont()
        #     font.importXML(ttx_file_path)
        #     return font
        font = TTFont(self.woff_file_path)
        if save_xml:
            ttx_file_path = 'fonts/%s.ttx' % self.woff_name
            font.saveXML(ttx_file_path)
            print('缓存ttx文件到' + ttx_file_path)
        return font

    def __get_font_dict(self):
        ttx_file = BytesIO()
        self.__font.saveXML(ttx_file)
        soup = BeautifulSoup(BytesIO(ttx_file.getvalue()), 'xml')
        font_dict = {}
        for char_font in soup.find_all('TTGlyph'):
            glyphName = char_font['name']
            char_code = self.__reverse_cmap.get(glyphName)
            font_dict[char_code] = char_font
        ttx_file.close()
        return font_dict


def SHA256(content):
    sha256 = hashlib.sha256()
    sha256.update(content.encode('utf-8'))
    return sha256.hexdigest()


def MD5(content):
    md5 = hashlib.md5()
    md5.update(content.encode('utf-8'))
    return md5.hexdigest()


if __name__ == '__main__':
    print(FontHandler('c4605357').save_image('心'))
