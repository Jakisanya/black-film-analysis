import utils
import scrapy
from scrapy.crawler import CrawlerProcess
import re
import warnings
import pandas as pd

def run_grammy_awards_crawler():
    """Scrape grammy awards nominee and winners data from 4
       pages."""

    grammy_urls = pd.read_csv("data_files/grammy_urls.csv")["url"]
    grammy_awards_data = []

    class GrammyAwardsScraper(scrapy.Spider):
        name = "grammy_awards_scraper"

        def start_requests(self):
            count = 0
            for url in grammy_urls[:10]:
                count += 1
                print(f'Currently on page {count} / {len(grammy_urls)}')
                yield scrapy.Request(url=url, callback=self.parse)

        def parse(self, response):
            category_elements = []
            for i in range(93):
                element_string = f'/html/body/div[3]/div/main/section/section[2]/section[{i}]'
                element = response.xpath(element_string)
                category_elements.append(element)

            for category in category_elements:
                print("I get here.")
                award_data = {
                    "awards_year": category.xpath('//h1[@class="text-predominant-color font-polaris uppercase text-30 md-xl:text-42 font-thin leading-tight mb-25px"]/text()').get()[:4],
                    "ceremony": (category.xpath('//h2[@class="font-polaris text-23 font-medium flex flex-row items-center relative"]/text()').get()),
                    "category": category.xpath('.//div[@class="w-full text-left md-xl:text-right mb-1 md-xl:mb-20px text-14 md-xl:text-22 font-polaris uppercase"]/text()').get(),
                    "nominee": category.xpath('.//div[@class="w-full text-center md-xl:text-left text-17 md-xl:text-22 mr-10px md-xl:mr-30px font-polaris font-bold md-xl:leading-8 tracking-wider"]/text()').extract_all(),
                    }
                print(award_data["awards_year"])
                print(award_data["ceremony"])
                print(award_data["category"])
                print(award_data["nominee"])
                if category.xpath('.//div[@class="awards-category-link"]/text()').extract_all() is not None:
                    award_data["artist"] = category.xpath('.//div[@class="awards-category-link"]/text()').extract_all()

                if (category.xpath('.//div[@class="w-full text-left text-14 font-polaris md-xl:leading-normal"]/text()')
                        is not None):
                    award_data["workers"] = (category.xpath(
                            './/div[@class="w-full text-left text-14 font-polaris md-xl:leading-normal"]/text()')
                                         .extract_all())

                award_data["winner"] = [award_data["nominee"][0], award_data["artist"][0], award_data["workers"][0]]

                grammy_awards_data.append(award_data)

    process = CrawlerProcess()
    process.crawl(GrammyAwardsScraper)
    process.start()

    utils.save_data_as_json("grammy_awards_data.json", grammy_awards_data)
