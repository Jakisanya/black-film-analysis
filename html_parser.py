from bs4 import BeautifulSoup
import os
import utils


def extract_data_from_category_section(category_section):
    """Extract the data from each category section and store it in a dictionary."""
    category = category_section.find('div', class_='w-full text-left md-xl:text-right mb-1 md-xl:mb-20px text-14 '
                                                   'md-xl:text-22 font-polaris uppercase').get_text(strip=True)
    nominee_section = category_section.find_all('div',
                                                class_='w-full text-left md-xl:text-22 text-17 mr-10px md-xl:mr-30px '
                                                       'font-polaris font-bold md-xl:leading-8 tracking-wider flex '
                                                       'flex-row justify-between')
    nominees = [nominee.get_text()[:-1].strip('"').replace('"', '').replace('\\', '') for nominee in nominee_section]
    artist_section = category_section.find_all('div', class_="awards-nominees-link")
    artists = [artist.get_text().replace('\n', '') for artist in artist_section]
    worker_section = category_section.find_all('div', class_="text-left text-14 font-polaris")
    workers = [workers.find('p', class_="pt-8 pb-4").get_text().replace('\n', '') if workers.find('p', class_="pt-8 pb-4") else list()
               for workers in worker_section]
    return [category, nominees, artists, workers]


annual_grammy_awards_data_list = []


def run_html_parser():
    """Parse each grammy webpage and extract and store data in a dictionary."""
    grammy_html_docs = []
    grammy_award_years = [year for year in range(2023, 1957, -1)]

    for file in os.listdir('data_files/Grammy Award HTMLs'):
        grammy_html_docs.append("data_files/Grammy Award HTMLs/" + file)
        print(file)

    html_doc_no = 0
    for html_doc, award_year in zip(grammy_html_docs, grammy_award_years):
        html_doc_no += 1
        print(f'Currently on html doc {html_doc_no} / {len(grammy_html_docs)}.')
        with open(html_doc, 'r', encoding='utf-8') as file:
            html_doc = file.read()
            soup = BeautifulSoup(html_doc, 'html.parser')
            awards_data = {"ceremony": [], "awards_year": [], "category": [], "nominee": [], "artist": [],
                           "workers": [], "winner": []}
            category_sections = soup.find_all('section',
                                              class_="h-full w-full flex flex-col items-center mt-6 md-xl:mt-8")
            for category_section in category_sections:
                category_data = extract_data_from_category_section(category_section)
                ceremony = soup.find('h2',
                                     class_='font-polaris text-23 font-medium flex flex-row items-center '
                                            'relative').get_text(strip=True)
                ceremony_list = ((ceremony + "&&") * len(category_data[1])).split("&&")[:-1]
                awards_year = str(award_year)
                awards_year_list = ((awards_year + "&&") * len(category_data[1])).split("&&")[:-1]
                awards_data["ceremony"].append(ceremony_list)
                awards_data["awards_year"].append(awards_year_list)
                category_list = ((category_data[0] + "&&") * len(category_data[1])).split("&&")[:-1]
                awards_data["category"].append(category_list)
                awards_data["nominee"].append(category_data[1])
                awards_data["artist"].append(category_data[2])
                awards_data["workers"].append(category_data[3])
                winner_bool_list = [True if nominee == 0 else False for nominee in range(len(category_data[1]))]
                awards_data["winner"].append(winner_bool_list)

            annual_grammy_awards_data_list.append(awards_data)

    utils.save_data_as_json("data_files/annual_grammy_awards.json", annual_grammy_awards_data_list)
