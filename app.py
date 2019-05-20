import logging;logging.basicConfig(level=logging.INFO)#INFO：确认一切按预期运行

import asyncio ,os,json,time
from datetime import datetime

from aiohttp import web

async def index(request):
    # 与老师的源码相比，最重要的是要加content_type这个参数，否则会变成下载文件
    return web.Response(body='<h1>Awesome</h1>'.encode('utf-8'),content_type='text/html')

def init():
    # 创建一个Application实例
    app=web.Application()
    # 并在特定的HTTP方法和路径上注册请求处理程序
    app.add_routes([web.get('/',index)])
    logging.info('Server started at http://127.0.0.1:8080')
    #通过run_app()调用运行应用程序
    web.run_app(app,host="127.0.0.1",port=8080)

init()