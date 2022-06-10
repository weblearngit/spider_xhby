# -*- coding: utf-8 -*-
"""
@desc: 新华报业
@version: python3
@author: shhx
@time: 2022-05-24 15:40:02

# 使用规则方式，爬取有规律的网站内容
# 这个爬虫会自动捕捉全部的链接，然后通过rules筛选需要跟进的链接

使用 布隆过滤器 进行去重，需要 重复下载需要删除redis中的数据
"""
from loguru import logger
import datetime
import re
from urllib.parse import urlsplit, urljoin
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from pyquery import PyQuery as pq
from apputils.yw_common import get_rq_list, get_now_filename


def get_settings():
    settings = {
        "DOWNLOADER_MIDDLEWARES": {
            "spiderapp.middlewares.proxy_request.ProxyMiddleware": 125,
        },
        "ITEM_PIPELINES": {"spiderapp.pipelines.file_save.TxtPipeline": 1},
        "TXT_SAVE": {
            "output_path": f"E:/Z_ES_DATA/xhby-{get_now_filename()}.txt",
            "flush_data_length": 100,
        },
        "HTTPERROR_ALLOWED_CODES": [500, 503, 504],
        "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.63 Safari/537.36",
    }
    return settings


class XhbySpider(CrawlSpider):
    name = "xhby"
    allowed_domains = [
        "xhby.net",
        "yzwb.net",
        "dxscg.com.cn",
    ]

    # 网站，最早日期， url模板 {} 对应 YYYYMM/DD
    website_list = [
        ("http://xh.xhby.net", "2021-11-01", "/pc/layout/{}/node_1.html",),
        (
            "http://epaper.yzwb.net",
            "2020-08-01",
            "/pc/layout/{}/node_A01.html",
        ),
        (
            "http://epaper.dxscg.com.cn",
            "2020-04-03",
            "/pc/layout/{}/node_1.html",
        ),
        ("http://njcb.xhby.net", "2020-08-01", "/pc/layout/{}/node_A01.html",),
        (
            "http://jsjjb.xhby.net",
            "2020-08-01",
            "/pc/layout/{}/node_A01.html",
        ),
        (
            "http://jsfzb.xhby.net",
            "2020-07-28",
            "/pc/layout/{}/node_A01.html",
        ),
        ("http://jnsb.xhby.net", "2020-01-21", "/pc/layout/{}/node_A01.html",),
        (
            "http://xinsushang.xhby.net",
            "2019-11-28",
            "/pc/layout/{}/node_A01.html",
        ),
    ]

    start_urls = []

    for website in website_list:
        search_date = datetime.datetime.strptime(website[1], "%Y-%m-%d")
        while search_date <= datetime.datetime.today():
            url_tpl = website[2]
            url = urljoin(
                website[0], url_tpl.format(search_date.strftime("%Y%m/%d"))
            )
            start_urls.append(url)
            search_date += datetime.timedelta(days=1)

    custom_settings = get_settings()

    rules = (
        Rule(LinkExtractor(allow=r"/.*/node_[0-9A-Za-z]+.html"), follow=False),
        Rule(
            LinkExtractor(allow=r"/pc/con/.*"),
            callback="parse_item",
            follow=False,
        ),
    )

    def parse_item(self, response):
        doc = pq(response.body)

        title_text = doc("div[class=newsdetatit]").text()
        if not title_text:
            return

        author = ""
        year = ""
        for each in re.split(r"\s+", title_text):
            if each.startswith("来源："):
                author = each[3:]

            if each.endswith("日"):
                rq_list = get_rq_list(each)
                if not rq_list:
                    continue
                year = rq_list[0].replace("-", "")

        row_dict = {
            "type": 9,
            "title": doc("div[class=newsdetatit] >h3").text(),
            "content": doc("div[class=newsdetatext]").text(),
            "author": author,
            "year": year,
            "url": response.url,
            "from": urlsplit(response.url).netloc,
        }
        yield row_dict
