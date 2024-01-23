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
    utils.save_data_as_json("actor_names_and_ids.json", actor_names_and_ids)


# List for imdb box office data dictionaries
box_office_data_list = []


def run_imdb_box_office_data_summary_crawler():
    """Scrape the movie's IMDb summary page to retrieve the US Opening Weekend Gross 
       and Worldwide Box Office Gross figures."""
    # Load the two tmdb movie data lists to use.
    tmdb_movie_data_list = utils.load_json_data("concatenated_tmdb_movie_data_list.json")

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
    tmdb_movie_data_list = utils.load_json_data("concatenated_tmdb_movie_data_list.json")
    imdb_soundtrack_urls = []

    for movie_data in tmdb_movie_data_list:
        imdb_id = movie_data["IMDb_ID"]
        soundtrack_url = f'https://www.imdb.com/title/{imdb_id}/soundtrack'
        imdb_soundtrack_urls.append(soundtrack_url)

    class ScrapeSoundtrackCredits(scrapy.Spider):
        name = 'soundtrack_creds_scraper'

        def start_requests(self):
            for url in imdb_soundtrack_urls:
                yield scrapy.Request(url=url, callback=self.parse)

        def parse(self, response):
            imdb_movie_id = response.xpath('//meta[contains(@property, "pageId")]/@content').extract_first()
            writer_performer_id_links = response.xpath(
                '//div[@id = "soundtracks_content"]//a/@href').extract()  # Extract writer/performer IMDb ID links
            writer_performer_names = response.xpath(
                '//div[@id = "soundtracks_content"]//a/text()').extract()  # Extract writer/performer names
            credits_data = dict(IMDb_ID=imdb_movie_id, Soundtrack_Credits=[])
            for link_and_text in list(zip(writer_performer_id_links, writer_performer_names)):
                writer_performer_id = re.sub("(/name/)", "", link_and_text[0]).strip("/")
                writer_performer_name = link_and_text[1].strip(" \n")
                credits_data["Soundtrack_Credits"].append((writer_performer_id, writer_performer_name))
            soundtrack_credits_data_list.append(credits_data)

    process = CrawlerProcess()
    process.crawl(ScrapeSoundtrackCredits)
    process.start()


def save_soundtrack_credits_data():
    utils.save_data_as_json("soundtrack_credits_data_list.json", soundtrack_credits_data_list)
