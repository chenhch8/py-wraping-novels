# -*- coding: utf-8 -*-

'管理十个协程workers的状态'

__author__ = 'chenhch8'

import re
reg = re.compile(r'</*br/*>')

# html parse
from bs4 import BeautifulSoup

from collections import defaultdict

import config_http
import aiohttp
import asyncio
import os

# asynio队列，用于存储任务
try:
    from asyncio import JoinableQueue as Queue
except ImportError:
    # In Python 3.5, asyncio.JoinableQueue is
    # merged into Queue.
    from asyncio import Queue

class Crawler(object):

  def __init__(self, headers, loop):
    self.tasks = Queue()
    self.max_tasks = 10
    self.headers = headers
    self.novels = {}
    # 创建一个session对象，并keep-alive，即长连接
    self.session = aiohttp.ClientSession(loop = loop)


  async def setRootUrls(self, urls):
    for url in urls:
      await self.tasks.put((None, '', url))


  async def crawl(self, urls):
    await self.setRootUrls(urls)
    # 创建十个子协程
    workers = [asyncio.Task(self.work()) for _ in range(self.max_tasks)]
    # 阻塞直至tasks为空
    await self.tasks.join()
    # 保存成txt
    await self.save_txt()
    # 取消所有workers后，触发error从而结束事件循环
    for w in workers:
      w.cancel()
    self.session.close()


  async def work(self):
    while True:
      # 阻塞直至获取到一个值
      id, url, book_url = await self.tasks.get()
      # [1] 抓取 html
      html, url = await self.fetch(book_url + url)
      # [2] 解析 html
      await self.parseHtml(html, url, id, book_url)
      # 告诉队列所获取的url已处理完毕
      self.tasks.task_done()


  async def fetch(self, url):
    print('抓取<%s>' % url)
    res = await self.session.get(url, headers = self.headers)
    assert res.status == 200
    html = await res.text(encoding='gbk')
    print('<%s>抓取成功！' % url)
    return html, url


  async def parseHtml(self, html, url, id, book_url):
    print('解析<%s>网页' % url)
    html = BeautifulSoup(html, 'html.parser')
    print('<%s>解析成功' % url)
    if id is None: # 解析小说章节列表页
      await self.__parseHome(html, url)
    else: # 解析小说内容页
      await self.__parseContent(html, url, id, book_url)


  async def __parseHome(self, html, url):
    html = html.find('div', attrs = { 'id': 'list' })
    # 获取小说名
    name = html.dl.dt.string
    name = name[:name.find('》') + 1]
    self.novels[url] = { 'name': name, 'content': defaultdict(lambda: None) }
    print('开始下载%s' % name)
    list = html.find_all('a')
    for index, value in enumerate(list):
      self.tasks.put_nowait((index, value['href'], url))


  async def __parseContent(self, html, url, id, book_url):
    chapter = html.find('div', attrs = { 'class': 'bookname' }).h1.string
    content = html.find('div', attrs = { 'id': 'content' })

    content_str = ''
    for a in content.contents:
      if a.string is None:
        content_str += re.sub(reg, '\n', str(a))
      else:
        content_str += re.sub(reg, '\n', a.string)

    # print(content_str)

    if self.novels[book_url]['content'][id] is None:
      self.novels[book_url]['content'][id] = {}
    self.novels[book_url]['content'][id]['chapter'] = chapter
    self.novels[book_url]['content'][id]['content'] = content_str


  async def save_txt(self):
    for item in self.novels.values():
      name = os.path.join('novels', item['name'] + '.txt')
      content = item['content']
      with open(name, 'w') as f:
        for i in range(len(content)):
          f.write(content.get(i)['chapter'])
          f.write('\n')
          f.write(content.get(i)['content'])
          f.write('\n\n')



if __name__ == '__main__':
  loop = asyncio.get_event_loop()
  # asyncio.Semaphore(),限制同时运行协程数量  
  # sem = asyncio.Semaphore(5)
  config = config_http.config
  crawler = Crawler(config['headers'], loop)
  loop.run_until_complete(crawler.crawl(config['urls']))
  loop.close()