import configparser
import logging
import os
import urllib.request

import sys
from bs4 import BeautifulSoup
from sqlalchemy import Column, Integer, String, create_engine, BLOB
from sqlalchemy.ext import baked
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session

Base = declarative_base()
Bakery = baked.bakery()
Logger = logging.getLogger('publish.log')


class PublisherConfig:
    """
    global config
    """
    __config = None

    def __new__(cls, *args, **kwargs):
        if PublisherConfig.__config:
            return PublisherConfig.__config
        super().__new__(*args, **kwargs)

    def __init__(self, config_type: str=None):
        self.__config_instance = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
        path = os.path.split(os.path.realpath(__file__))[0]
        self.__config_instance.read(os.path.join(path, 'config.ini'), encoding='utf-8')
        if config_type in self.__config_instance:
            self.__config_type = config_type
        else:
            raise FileNotFoundError('config type not found in config file')

        PublisherConfig.__config = self

    def __getattr__(self, item: str):
        """
        get config value in config.ini
        :param item:
        :return:
        """
        if not item:
            return None
        if item.endswith('_int'):
            return self.__config_instance[self.__config_type].getint(item)
        elif item.endswith('_bool'):
            return self.__config_instance[self.__config_type].getboolean(item)
        return self.__config_instance[self.__config_type].get(item)

    @classmethod
    def current(cls):
        if not cls.__config:
            raise SyntaxError('You cannot use config before init!')
        return cls.__config


class PublishLog(Base):
    """
    DB Schema
    """
    __tablename__ = 'publish_log'
    publish_id = Column(Integer, primary_key=True, autoincrement=True)  # item id
    publish_title = Column(String, index=True, nullable=False)  # content title
    publish_url = Column(String, index=True, unique=True, nullable=False)  # content url, unique
    publish_content = Column(String(length=100000), nullable=False, default='')
    publish_pic = Column(BLOB, default='', nullable=False)  # content picture, may be empty
    publish_pic_name = Column(String, default='', nullable=False)  # picture name
    publish_process_flag = Column(Integer, default=0, nullable=False)  # process flag,
    # 0=first, 1~9=fail times 10=no more retry -1=done


class Publisher:
    def __init__(self, config_type: str):
        PublisherConfig(config_type)
        self.db_session = Session(bind=create_engine(PublisherConfig.current().db_url, echo=False))

    @staticmethod
    def __get_full_content(item: PublishLog):
        """
        get full published content
        :param item: item instance
        :return:
        """
        if not item:
            Logger.warning('update item not exists!')
            return
        url = item.publish_url
        if not url:
            Logger.warning('URL missing!')
            return

        req = urllib.request.Request(url)
        first_img = ''
        image_binary = ''
        full_content = ''
        with urllib.request.urlopen(req, timeout=30) as response:
            soup_content = BeautifulSoup(response.read(), 'lxml')
            full_content_node = soup_content.select('.topic-nfo')
            if full_content_node:
                full_content = str(full_content_node[0])
            img_content = soup_content.select('.topic-nfo img')
            if img_content:
                first_img = img_content[0].attrs.get('src')
            if first_img:
                with urllib.request.urlopen(first_img, timeout=30) as image_response:
                    image_binary = image_response.read()
        item = None
        item_changed = False
        if full_content:
            item.publish_content = full_content
            item_changed = True
        if item and first_img and image_binary:
            item.publish_pic = image_binary
            item.publish_pic_name = first_img.rsplit('/', 1)[1]
            item_changed = True
        if not item_changed:
            # process flag add 1 when error
            item.publish_process_flag += 1

    def __check_rss(self):
        """
        check rss content save new content to db
        :return:
        """
        self.rss_content = None
        req = urllib.request.Request(PublisherConfig.current().request_url)
        with urllib.request.urlopen(req, timeout=30) as response:
            soup_content = BeautifulSoup(response.read(), 'lxml-xml')
            for title in soup_content.select('item title'):
                title_content = title.string
                # \n will be known as a node
                url_content = title.next_sibling.next_sibling.string
                # check db exists
                query = Bakery(lambda session: session.query(PublishLog).filter(PublishLog.publish_url == url_content))
                exists_content = query(self.db_session).first()
                if exists_content:
                    Logger.info('end flag detected!')
                    break
                # save to db
                publish_log = PublishLog(publish_title=title_content, publish_url=url_content)
                self.db_session.add(publish_log)
            self.db_session.commit()

    @staticmethod
    def encode_multipart_form_data(fields, files):
        """
        fields is a sequence of (name, value) elements for regular form fields.
        files is a sequence of (name, filename, value) elements for data to be
        uploaded as files.
        Return (content_type, body) ready for httplib.HTTP instance
        """
        boundary = b'----------ThIs_Is_tHe_bouNdaRY_$'
        crlf = b'\r\n'
        line = []
        for (key, value) in fields:
            line.append(b'--%s' % boundary)
            line.append(b'Content-Disposition: form-data; name="%s"' % str(key).encode())
            line.append(b'')
            line.append(str(value).encode())
        for (key, filename, value) in files:
            # filename = filename.encode()
            line.append(b'--%s' % boundary)
            line.append(
                b'Content-Disposition: form-data; name="%s"; filename="%s"' % (str(key).encode(), filename.encode())
            )
            line.append(b'Content-Type: application/octet-stream')
            line.append(b'')
            line.append(value)  # this is file binary, DO not encode
        line.append(b'--%s--' % boundary)
        line.append(b'')
        body = crlf.join(line)
        content_type = 'multipart/form-data; boundary=%s' % boundary.decode()
        return content_type, body

    def __publish_db_content_to_weibo(self):
        """
        check db and publish all content to weibo
        :return:
        """
        query = Bakery(lambda session: session.query(PublishLog).filter(PublishLog.publish_process_flag >= 0).filter(
            PublishLog.publish_process_flag < 10))
        exists_content = query(self.db_session).all()
        for content in exists_content:
            content_type, request_body = self.encode_multipart_form_data(
                [('access_token', PublisherConfig.current().access_token), ('status', content.publish_title), ()],
                [('pic', content.publish_pic_name, content.publish_pic)])
            header = {
                'Content-Type': content_type,
                'Content-Length': str(len(request_body))
            }
            req = urllib.request.Request(PublisherConfig.current().weibo_request_url, data=request_body, headers=header,
                                         method='POST')
            try:
                response = urllib.request.urlopen(req)
                res_data = response.read()
            except Exception as e:
                content.publish_process_flag += 1
                logging.error("Post to weibo encounter error! {}, detail: {}".format(e, sys.exc_info()[2]))
                continue
            content.publish_process_flag = -1
        logging.info('posted to weibo complete')
        self.db_session.commit()

    def run(self):
        self.__check_rss()
        query = Bakery(lambda session: session.query(PublishLog).filter(
            PublishLog.publish_content == '').filter(PublishLog.publish_process_flag < 10))
        all_content_need_detail = query(self.db_session).all()
        for content in all_content_need_detail:
            self.__get_full_content(content)
        self.db_session.commit()
        self.__publish_db_content_to_weibo()


if __name__ == '__main__':
    publish = Publisher('pro')
    publish.run()


