import scrapy
from scrapy import Selector

from douban.items import DoubanItem


class MainSpider(scrapy.Spider):
    name = "main"
    allowed_domains = ["movie.douban.com"]

    # start_urls = ["https://movie.douban.com/top250"]

    def start_requests(self):
        for i in range(10):
            yield scrapy.Request(url=f'https://movie.douban.com/top250?start={25*i}&filter=')

    def parse(self, response):
        sel = Selector(response)
        li_list = sel.xpath('//ol[@class="grid_view"]/li')
        item = DoubanItem()
        for li in li_list:
            item['title'] = li.xpath('.//span[@class="title"]/text()').extract_first()

            item['rating_num'] = li.xpath('.//span[@class="rating_num"]/text()').extract_first()
            item['inq'] = li.xpath('.//span[@class="inq"]/text()').extract_first()
            yield {
                'title': item['title'],
                'rating_num': item['rating_num'],
                'inq': item['inq']
            }
        pass
