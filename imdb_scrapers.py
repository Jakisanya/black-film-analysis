"""File that contains the functions that scrape and store actor/film data from film websites.
    Scraper 1: Black actor names and imdb IDs from imdb
    Scraper 2: Movie box office data (worldwide gross, box office gross) from imdb
    Scraper 3: Soundtrack credits data from imdb
"""
import utils
import scrapy
from scrapy.crawler import CrawlerProcess
import re
import warnings

warnings.filterwarnings("ignore", category=scrapy.crawler.ScrapyDeprecationWarning)

# Dictionary for scraped actor names and IDs from imdb
actor_names_and_ids = {"imdb_ids": [], "names": []}


def run_actor_name_crawler():
    """Create a list of imdb URLs and scrape actor names and ids."""
    actor_list_urls = []
    for i in range(1, 9):
        # get url which displays list of actor names
        imdb_url = f'https://www.imdb.com/list/ls006050174/?sort=list_order,asc&mode=detail&page={i}'
        actor_list_urls.append(imdb_url)

    for i in range(1, 16):
        imdb_url = f'https://www.imdb.com/list/ls066061932/?sort=list_order,asc&mode=detail&page={i}'
        actor_list_urls.append(imdb_url)

    # Create the Spider class for imdb_actor_names
    class ScrapeActors(scrapy.Spider):
        name = 'imdb_actor_names'

        def start_requests(self):
            for url in actor_list_urls:
                yield scrapy.Request(url=url, callback=self.parse)

        def parse(self, response):
            # Extract actor IMDb IDs and names
            # Create a SelectorList of the IMDb html links (which contain the IDs)
            act_links = response.xpath(
                '//h3[@class = "lister-item-header"]/a/@href')
            for link in act_links.extract():
                actor_names_and_ids["imdb_ids"].append(re.sub("(/name/)", "", link))

            # Create a SelectorList of the actor name headers (which is the actor name in text format)
            act_names = response.xpath(
                '//h3[@class = "lister-item-header"]/a/text()')
            for name in act_names.extract():
                actor_names_and_ids["names"].append(name.strip(" \n"))  # extract, clean, and append names

    process = CrawlerProcess()
    process.crawl(ScrapeActors)
    process.start()


def verify_actor_name_data():
    """Check that all actors were scraped, each imdb ID has a corresponding actor name, and
       both the names and IDs have been cleaned (extracted from html source code correctly)."""

    # check lengths of lists (as of 03/01/2024 the total should be 2225)
    if len(actor_names_and_ids["imdb_ids"]) == len(actor_names_and_ids["names"]):
        print(f'There are an equal number of IDs and names in the list {len(actor_names_and_ids["names"])}.\n')
    else:
        print(f'There are not an equal number of IDs and names in the list.\n'
              f'There are {len(actor_names_and_ids["names"])} names and {len(actor_names_and_ids["imdb_ids"])} IDs.\n')

    # check both names and IDs have been extracted correctly
    print(f'The first 10 names are in the format:\n{actor_names_and_ids["names"][:10]}\n\n'
          f'The first 10 IDs are in the format:\n{actor_names_and_ids["imdb_ids"][:10]}\n\n')


def save_actor_names_and_ids():
    """Save actor name and id dictionary as json."""
    utils.save_data_as_json("data_files/actor_names_and_ids.json", actor_names_and_ids)


# List for imdb box office data dictionaries
box_office_data_list = []


def run_imdb_box_office_data_summary_crawler():
    """Scrape the movie's IMDb summary page to retrieve the US Opening Weekend Gross 
       and Worldwide Box Office Gross figures."""
    # Load the two tmdb movie data lists to use.
    tmdb_movie_data_list = utils.load_json_data("data_files/concatenated_tmdb_movie_data_list.json")

    imdb_summary_urls = []

    for movie_data in tmdb_movie_data_list:
        if movie_data["IMDb_ID"] is not None:
            imdb_id = movie_data["IMDb_ID"]
            summary_url = f'https://www.imdb.com/title/{imdb_id}/'
            imdb_summary_urls.append(summary_url)

    # Create the Spider class
    class ScrapeMovieSummary(scrapy.Spider):
        name = 'movie_summary_scraper'

        def start_requests(self):
            for url in imdb_summary_urls:
                yield scrapy.Request(url=url, callback=self.parse)

        def parse(self, response):
            # Extract first billed cast and names
            imdb_movie_id = (response.xpath('//meta[contains(@property, "pageConst")]/@content').extract_first())
            opening_weekend_gross = (response.xpath(
                '//li[contains(@data-testid, "weekend")]//span[contains(@class, "content-item")]/text()')
                                  .extract_first())
            worldwide_gross = (response.xpath(
                '//li[contains(@data-testid, "worldwide")]//span[contains(@class, "content-item")]/text()')
                               .extract_first())
            box_office_data = dict(IMDb_ID=imdb_movie_id, Opening_Weekend_Gross=opening_weekend_gross,
                                   Worldwide_Gross=worldwide_gross)
            box_office_data_list.append(box_office_data)

    process = CrawlerProcess()
    process.crawl(ScrapeMovieSummary)
    process.start()


def save_imdb_box_office_data_summary_crawler():
    utils.save_data_as_json("box_office_data_list.json", box_office_data_list)


soundtrack_credits_data_list = []


def run_soundtrack_credits_crawler():
    print("I get here.")
    tmdb_movie_data_list = utils.load_json_data("data_files/concatenated_tmdb_movie_data_list.json")
    imdb_soundtrack_urls = []

    for movie_data in tmdb_movie_data_list:
        imdb_id = movie_data["IMDb_ID"]
        soundtrack_url = f'https://www.imdb.com/title/{imdb_id}/soundtrack/'
        imdb_soundtrack_urls.append(soundtrack_url)

    class ScrapeSoundtrackCredits(scrapy.Spider):
        name = 'soundtrack_creds_scraper'
        custom_settings = {
            'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        def start_requests(self):
            count = 0
            for url in imdb_soundtrack_urls:
                count += 1
                print(f"Currently on url {count} / {len(imdb_soundtrack_urls)}.")
                yield scrapy.Request(url=url, callback=self.parse)

        def parse(self, response):
            # Iterate through each <li> element
            imdb_movie_id = response.xpath(
                'substring-before(substring-after(//meta[@property="og:url"]/@content, "/title/"), "/soundtrack/")').extract_first()
            soundtrack_credits = {"imdb_ID": imdb_movie_id, "credits": []}
            for track_element in response.xpath('//li[@class="ipc-metadata-list__item ipc-metadata-list__item--stacked"]'):
                track_credit = {"title": track_element.xpath('.//span[@class="ipc-metadata-list-item__label"]/text()').get()}
                for person_element in track_element.xpath('.//div[@class="ipc-html-content-inner-div"]'):
                    text_p1 = person_element.xpath('./text()[1]').get()
                    if text_p1 == 'Written by ':
                        track_credit["writers"] = self.extract_people_info(person_element)
                    elif (text_p1[:11] == 'Written by ') and (len(text_p1) > 11):
                        track_credit["writers"] = text_p1
                    if text_p1 == "Performed by ":
                        track_credit["performers"] = self.extract_people_info(person_element)
                    elif (text_p1[:13] == 'Performed by ') and (len(text_p1) > 13):
                        track_credit["performers"] = text_p1
                    if text_p1 == "Arranged by ":
                        track_credit["arrangers"] = self.extract_people_info(person_element)
                    elif (text_p1[:12] == 'Arranged by') and (len(text_p1) > 12):
                        track_credit["arrangers"] = text_p1

                soundtrack_credits["credits"].append(track_credit)
            soundtrack_credits_data_list.append(soundtrack_credits)

        def extract_people_info(self, person_element):
            people_info = []
            # Extract the links inside the div
            links = person_element.xpath('.//a[@class="ipc-md-link ipc-md-link--entity"]')

            # Extract the name and IMDb ID for each link
            for link in links:
                name = link.xpath('text()').get()
                person_imdb_id = link.xpath('@href').re_first(r'/name/(nm\d+)/')
                person_info = {'name': name, 'imdb_id': person_imdb_id}
                people_info.append(person_info)

            return people_info

    process = CrawlerProcess()
    process.crawl(ScrapeSoundtrackCredits)
    process.start()

def save_soundtrack_credits_data():
    utils.save_data_as_json("soundtrack_credits_data_list.json", soundtrack_credits_data_list)
