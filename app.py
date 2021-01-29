import os
import re
import json
import weakref
import logging
import traceback

from datetime import datetime
from bs4 import BeautifulSoup
from aiohttp import web
import aiohttp
import asyncio
import requests

from utils import parse_content


LOG_LEVEL = logging.DEBUG
FORMAT = '%(asctime)s-%(funcName)-10s(%(lineno)d) %(levelname)-5s: %(message)s'
logging.basicConfig(
    level=LOG_LEVEL,
    format=FORMAT,
)

logger = logging.getLogger('aiohttp.access')


WS_DOMAIN = os.getenv("WS_DOMAIN", "localhost")
WS_HOST = os.getenv("WS_HOST", "127.0.0.1")
WS_PORT = int(os.getenv("WS_PORT", 5000))
MAX_THREADS = 5

PTT_URL = 'https://www.ptt.cc'


async def cancel_tasks(request):
    try:
        channel_id = request.match_info.get("channel_id")
        logger.debug(f'ws: {channel_id}')
        ws = request.app["websockets"].pop(channel_id, None)
        await ws.close()
        rep = f'id: {channel_id}, websocket canceled'

    except:
        rep = f'id: {channel_id}, websocket cancel failed'

    return web.Response(text=rep)


async def socket_handler(request):
    channel_id = request.match_info.get("channel_id")
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    request.app["websockets"][channel_id] = ws

    logger.info(f"Client connected: {channel_id}")

    try:
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                req = json.loads(msg.data)
                if req['type'] == 'PTT':
                    board = req['board']
                    datetime.strptime("20210108", "%Y%m%d")
                    target_start_date = datetime.strptime(req['start_date'], "%Y%m%d")
                    target_end_date = datetime.strptime(req['end_date'], "%Y%m%d")
                    key_word = req['key_word']

                    start_page = 1
                    last_page = await get_last_page(board)
                    logger.info(f'Board: {board}, Last page: {last_page}')

                    page_list = [p for p in range(start_page, last_page + 1)]
                    logger.info(f'Page list length: {len(page_list)}')

                    target_start_page = await search_target_date_page(
                        page_list, target_start_date, page_list.index(start_page), len(page_list) - 1, board
                    )
                    logger.info(f'Target start page: {target_start_page}')

                    page_queue = asyncio.Queue()
                    div_queue = asyncio.Queue()
                    [page_queue.put_nowait(i) for i in range(target_start_page, last_page + 1)]

                    article_ids_tasks = [
                        handle_article_ids_tasks(task_id, page_queue, div_queue, ws, board)
                        for task_id in range(MAX_THREADS)
                    ]
                    divs_tasks = [
                        handle_divs_tasks(task_id, div_queue, ws, key_word, target_end_date, board)
                        for task_id in range(MAX_THREADS)
                    ]
                    logger.info('Start crawler tasks ...')
                    # loop.run_until_complete(asyncio.wait(article_ids_tasks + divs_tasks))

                    for t in article_ids_tasks + divs_tasks:
                        asyncio.ensure_future(t, loop=loop)

    except:
        logger.error(str(traceback.format_exc()))

    finally:
        request.app["websockets"].pop(channel_id, None)
        logger.info(f"Client connection closed: {channel_id}")
        raise web.HTTPOk()


async def handle_article_ids_tasks(task_id, page_queue, div_queue, ws, board):
    while not page_queue.empty():
        if ws.closed:
            logger.debug(f'Websocket connection closed, article_ids_task left, task id: [{task_id}]')
            return

        current_req = await page_queue.get()
        try:
            await get_article_ids(current_req, div_queue, board)

        except ConnectionResetError:
            logger.error(f'Websocket connection error, article_ids_task left, task id: [{task_id}]')

        except:
            logger.debug(traceback.format_exc())


async def get_article_ids(req, div_queue, board):
    async with aiohttp.ClientSession() as session:
        logger.debug(f'Processing index: {str(req)}')
        async with session.get(url=PTT_URL + '/bbs/' + board + '/index' + str(req) + '.html', timeout=30) as response:
            assert response.status == 200
            csv_content = await response.text()
            soup = BeautifulSoup(csv_content, 'html.parser')
            divs = soup.find_all("div", "r-ent")

            [div_queue.put_nowait(div) for div in divs]
            return True


async def handle_divs_tasks(task_id, div_queue, ws, key_word, target_end_date, board):
    while True:
        if ws.closed:
            logger.debug(f'Websocket connection closed, divs_task left, task id: [{task_id}]')
            return
        current_div = await div_queue.get()
        try:
            success, content = await parse_the_article(current_div, key_word, target_end_date, board)

            if success:
                # await ws.send_str(str(content))
                await ws.send_json(content)
            else:
                logger.debug(f'Content message: {content}')

        except ConnectionResetError:
            logger.error(f'Websocket connection error, divs_task left, task id: [{task_id}]')

        except:
            logger.debug(traceback.format_exc())

        if div_queue.empty():
            if task_id == MAX_THREADS - 1:
                await ws.send_str('done')
            return


async def parse_the_article(div, key_word, target_end_date, board):
    #  ex. link would be <a href="/bbs/PublicServan/M.1127742013.A.240.html">Re: [問題] 職等</a>
    href = div.find('a')['href']
    link = PTT_URL + href
    article_id = re.sub('\.html', '', href.split('/')[-1])

    async with aiohttp.ClientSession() as session:
        async with session.get(url=link, cookies={'over18': '1'}, timeout=30) as response:
            assert response.status == 200
            rep = await response.text()
            soup = BeautifulSoup(rep, 'html.parser')
            content = parse_content(soup.find(id="main-content"), link, article_id, board)

            if key_word:
                article_date = datetime.strptime(str(content['date']), "%a %b %d %H:%M:%S %Y")
                if article_date > target_end_date:
                    return False, 'Out of date.'

                if key_word not in str(content):
                    return False, 'No key word in content'

                messages = content.get('messages')
                content['messages'] = []
                for m in messages:
                    if key_word in str(m):
                        content['messages'].append(m)

                content['content'] = content.get('content') if key_word in str(content.get('content')) else ''

            return True, content


async def get_last_page(board, timeout=3):
    async with aiohttp.ClientSession() as session:
        req_url = PTT_URL + '/bbs/' + board + '/index.html'
        async with session.get(url=req_url, cookies={'over18': '1'}, timeout=timeout) as response:
            assert response.status == 200
            content = await response.text()
            first_page = re.search(r'href="/bbs/' + board + r'/index(\d+).html">&lsaquo;', content)
            if first_page is None:
                return 1
            return int(first_page.group(1)) + 1


async def search_target_date_page(page_list, target_date, start_index, end_index, board):
    while end_index >= start_index:
        middle_index = round((start_index + end_index) / 2)

        date_article_first, date_article_last = await get_article_first_last_date(page_list, middle_index, board)
        logger.debug(f'{date_article_first} | < {target_date.date()} > | {date_article_last}')

        if date_article_first.date() <= target_date.date() <= date_article_last.date():
            return page_list[middle_index]

        elif date_article_first.date() < target_date.date():
            start_index = middle_index + 1
        else:
            end_index = middle_index - 1

    logger.info(
        f'Can not find target date, the nearest date page: {page_list[start_index]}, start index: {start_index}, end_index: {end_index}'
    )

    return page_list[end_index]


async def get_article_first_last_date(page_list, page_index, board):
    async with aiohttp.ClientSession() as session:
        req_url = PTT_URL + '/bbs/' + board + '/index' + str(page_list[page_index]) + '.html'

        async with session.get(url=req_url, cookies={'over18': '1'}, timeout=30) as response:
            assert response.status == 200
            content = await response.text()

            soup = BeautifulSoup(content, 'html.parser')
            divs = soup.find_all("div", "r-ent")

            if len(page_list) - 1 == page_index:
                # 最新頁過濾掉置頂文
                filtered_divs = [
                    div
                    for div in divs
                    if div.find('div', 'mark').text != 'M' and '(本文已被刪除)' not in div.find('div', 'title').text
                ]
            else:
                filtered_divs = [div for div in divs if '(本文已被刪除)' not in div.find('div', 'title').text]

            date_article_first = ''
            date_article_last = ''

            reverse_index = -1
            for i in range(len(filtered_divs)):
                try:
                    if not date_article_first:
                        link = PTT_URL + filtered_divs[i].find('a')['href']
                        resp = requests.get(url=link, cookies={'over18': '1'})
                        soup = BeautifulSoup(resp.text, 'html.parser')
                        main_content = soup.find(id="main-content")
                        metas = main_content.select('div.article-metaline')
                        date = metas[2].select('span.article-meta-value')[0].string
                        date_article_first = datetime.strptime(date, "%a %b %d %H:%M:%S %Y")

                    if not date_article_last:
                        link = PTT_URL + filtered_divs[reverse_index].find('a')['href']
                        resp = requests.get(url=link, cookies={'over18': '1'})
                        soup = BeautifulSoup(resp.text, 'html.parser')
                        main_content = soup.find(id="main-content")
                        metas = main_content.select('div.article-metaline')
                        date = metas[2].select('span.article-meta-value')[0].string
                        date_article_last = datetime.strptime(date, "%a %b %d %H:%M:%S %Y")
                except Exception as e:
                    logger.debug(e)

                if date_article_first and date_article_last:
                    break
                reverse_index -= 1

            return date_article_first, date_article_last


async def index(request):
    return web.FileResponse('./index.html')


async def criteria(request):
    return web.FileResponse('./static/criteria.html')


async def result_page(request):
    return web.FileResponse('./static/result.html')


def main():
    aio_app = web.Application()

    # 建立一個 reference dict 準備關聯全部 ws 連線物件, key 為 {channel_id}
    aio_app["websockets"] = weakref.WeakValueDictionary()
    aio_app.router.add_route('GET', '/index', criteria)
    aio_app.router.add_route('GET', '/result', result_page)
    # aio_app.router.add_route('GET', '/index2', index)
    aio_app.add_routes([web.static('/static', './static')])

    aio_app.router.add_route('GET', '/socket/{channel_id}', socket_handler)
    aio_app.router.add_route('GET', '/socket/cancel/{channel_id}', cancel_tasks)
    runner = web.AppRunner(aio_app, access_log=logger)

    loop.run_until_complete(runner.setup())
    site = web.TCPSite(runner, WS_HOST, WS_PORT)
    loop.run_until_complete(site.start())

    loop.run_forever()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()

    main()
