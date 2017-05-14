# -*- coding: utf-8 -*-

'管理十个协程workers的状态'

__author__ = 'chenhch8'

import re
reg = re.compile(r'</*br/*>')

# html parse
from bs4 import BeautifulSoup

from collections import defaultdict
from multiprocessing import Pool

import config_http
import aiohttp
import asyncio
import os
import time

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
    # 创建一个session对象，并keep-alive，即长连接
    self.session = aiohttp.ClientSession(loop = loop)


  async def setRootUrl(self, url):
    await self.tasks.put((None, '', url))


  async def crawl(self, url):
    await self.setRootUrl(url)
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
      if html is None:
        self.tasks.task_done()
        continue
      # [2] 解析 html
      await self.parseHtml(html, url, id, book_url)
      # 告诉队列所获取的url已处理完毕
      self.tasks.task_done()


  async def fetch(self, url):
    print('抓取<%s>' % url)
    res = await self.session.get(url, headers = self.headers)
    if res.status == 200:
      html = await res.text(encoding='gbk')
      print('<%s>抓取成功！' % url)
      return html, url
    else:
      assert res.status == 200
      print('<%s>抓取失败！' % url)
      return None, url


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
    self.novels = { 'name': name, 'content': defaultdict(lambda: None) }
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

    if self.novels['content'][id] is None:
      self.novels['content'][id] = {}
    self.novels['content'][id]['chapter'] = chapter
    self.novels['content'][id]['content'] = content_str


  async def save_txt(self):
    name = os.path.join('novels', self.novels['name'] + '.txt')
    content = self.novels['content']
    print('保存%s...' % self.novels['name'])
    with open(name, 'w') as f:
      for i in range(len(content)):
        f.write(content.get(i)['chapter'])
        f.write(content.get(i)['content'])
    print('%s保存成功！' % self.novels['name'])



def process_start(url, headers):
  loop = asyncio.get_event_loop()
  # asyncio.Semaphore(),限制同时运行协程数量  
  # sem = asyncio.Semaphore(5)
  crawler = Crawler(headers, loop)
  loop.run_until_complete(crawler.crawl(url))
  loop.close()
  print('close')


def start():
  config = config_http.config
  urls, headers = config['urls'], config['headers']

  first = 0; rear = 4 if len(urls) > 4 else None
  time_count = 0
  while True:
    count = rear - first if rear is not None else len(urls) - first
    print('创建%d个子进程...' % count)
    pool = Pool(count)
    start = time.time()
    # 进程分配
    for url in urls[first:rear]:
      pool.apply_async(
        process_start,
        args = (url, headers),
        error_callback = lambda err: print(err)
      )
    print('等待所有进程结束')
    pool.close()
    pool.join()
    end = time.time()
    time_count += end - start
    print('所有进程已结束')
    if rear == None or rear >= len(urls):
      break
    first = rear
    rear = first + 4
    rear = rear if rear < len(urls) else None

  print('共 %s 本小说，总用时：%s秒，平均用时：%s秒' % (len(urls), time_count, time_count / len(urls)))

if __name__ == '__main__':
  start()