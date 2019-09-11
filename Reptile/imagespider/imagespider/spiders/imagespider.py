import scrapy
from scrapy import Request
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.wait import WebDriverWait
from imagespider.items import ImagespiderItem
from scrapy.http import HtmlResponse
from selenium.common.exceptions import TimeoutException
import time
from PIL import Image
from selenium.webdriver.support import  expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.by import By
import lxml.html
etree=lxml.html.etree

class JdSpider(scrapy.Spider):
    name = 'myspider'
    def start_requests(self):
        header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:65.0) Gecko/20100101 Firefox/65.0', }
        yield scrapy.Request('https://girlimg.epio.app/article', headers=header)

    def __init__(self):
        """
        在爬虫内初始化 selenium
        减少 selenium 的开关次数
        """
        super().__init__()
        # 无界面模式
        self.firefox = Options()
        self.firefox.add_argument('--headless')
        self.browser = webdriver.Firefox(firefox_options=self.firefox)
        # 有界面
        # self.browser = webdriver.Firefox()
        # 超时设置
        self.browser.set_page_load_timeout(60)

    #def closed(self, spider):
        """ 爬虫结束自动关闭 selenium """
        #self.browser.close()
        #self.browser.quit()

    def parse(self, response):
        wait = WebDriverWait(self.browser, 8)
        #设置显示等待
        i = 2
        item = ImagespiderItem()
        #导入item
        try:
            while True:
                xx = 'div.ant-col-xs-24:nth-child({}) > div:nth-child(1) > div:nth-child(1) > div:nth-child(1) > img:nth-child(1)'
                xx = xx.format(i)
                wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, xx))
                ).click()
                time.sleep(1)
                html = self.browser.page_source
                ff = etree.HTML(html)
                cc = ff.xpath('/html/body/div[2]/div/div[2]/div/div[1]/div[2]/div/div[1]/article/div[1]/p/img/@src')
                item['image_urls']=cc
                #通过一系列操作拿到图片地址并添加到item
                wait.until(
                    EC.presence_of_element_located(
                        (By.XPATH, '/html/body/div[2]/div/div[2]/div/div[1]/button'))
                ).click()
                js = 'var q=document.documentElement.scrollTop=100000'
                self.browser.execute_script(js)
                i = i + 1
                time.sleep(1)
                yield item
                #返回items
        except TimeoutException as e:
            print('超时')
            self.browser.execute_script('window.stop()')