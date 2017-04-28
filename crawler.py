#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__: FireEgret

from string import find
import socket
import urllib2
import multiprocessing
from urlparse import urljoin, urlparse
from bs4 import BeautifulSoup
import logging

logging.basicConfig(
    filename='crawler.log',
    format="%(name)s    %(asctime)s   %(message)s",
    level=logging.INFO
)

console = logging.StreamHandler()
log = logging.getLogger('log')
log.addHandler(console)


class Crawler(object):
    def __init__(self, q, seen, lock, dom=None, maxdep=10):
        self.q = q
        self.seen = seen
        self.lock = lock
        self.dom = dom
        self.depth = maxdep
        self.current_depth = 1
        self.count = 1
        self.ac_count = 1

    def getPage(self, url, timeout=2):
        # 获取内容
        try:
            socket.setdefaulttimeout(timeout)
            req = urllib2.Request(url)
            req.add_header('User-Agent', 'IE')
            res = urllib2.urlopen(req)
            log.info('Connecting  %s  Success', url)
        except Exception, e:
            log.info('Connecting  %s  Error:%s', url, str(e))
            return ""
        try:
            # 只抓HTML
            if res.headers['Content-Type'] != 'text/html':
                log.info('Fetching  %s  Error: Not HTML', url)
                return ""
            else:
                page = res.read()
                log.info('Fetching  %s  Success', url)
                return page
        except Exception, e:
            log.info('Fetching  %s  Error:%s', url, str(e))
            return ""

    def parseAndGetLinks(self, data, url):
        # 抽取链接
        links = []
        try:
            if data == "":
                return links
            soup = BeautifulSoup(data, "lxml")
            for link in soup.find_all("a", href=True):
                links.append(link.attrs['href'])
            log.info('Parsing  %s  Success', url)
            return links
        except Exception, e:
            log.info('Parsing  %s  Error:%s', url, str(e))

    def go(self, i):
        while self.current_depth < self.depth:
            # 提取链接
            urls = self.q.get()
            url = urls[0]
            dep = urls[1]
            if self.current_depth < dep:
                self.current_depth = dep

            content = self.getPage(url)
            self.seen[url] = ''
            links = self.parseAndGetLinks(content, url)

            for link in links:
                # 特殊端口过滤
                if len(urlparse(link, 'http').netloc.split(':')) > 1:
                    continue
                # 相对路径处理
                if len(link) > 0 and link[0] == '/':
                    if url[-1] == '/':
                        url = url[:-1]
                    link = urljoin(url, link)

                # 去重处理、域处理
                self.lock.acquire()
                self.count += 1
                if link not in self.seen:
                    if self.dom and find(link, self.dom) == -1:
                        self.lock.release()
                        continue
                    else:
                        self.q.put([link, self.current_depth + 1])
                        self.seen[link] = ''
                        self.ac_count += 1
                        self.lock.release()
                else:
                    self.lock.release()
                    continue
            f = open('url.txt', 'a')
            f.write(url + ' ' + str(dep) + '\n')
            f.close()
        g = open('count.txt', 'a')
        g.write('Thread ' + str(i) + '发现链接:' + str(self.count) + '\n')
        g.write('Thread ' + str(i) + '实际链接:' + str(self.ac_count) + '\n')
        g.close()


def main(crawler, i):
    crawler.go(i)


if __name__ == '__main__':
    seen = multiprocessing.Manager().dict()
    lock = multiprocessing.Manager().Lock()
    q = multiprocessing.Manager().Queue()
    q.put(["http://www.xxx", 1])
    crawler = Crawler(q, seen, lock, "xxx")

    pool = multiprocessing.Pool()

    for i in range(multiprocessing.cpu_count()):
        pool.apply_async(main, args=(crawler, i))
    pool.close()
    pool.join()

