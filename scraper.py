import requests
import os
import time
import pandas as pd
import numpy as np
import sqlite3
from tqdm import tqdm
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException


# Specify webdriver options
options = webdriver.ChromeOptions()
options.add_argument('headless')  # set to headerless windows
options.add_argument('window-size=1200x600')  # set the window size

# Initiate headerless scraping
driver = webdriver.Chrome(executable_path = os.getcwd() + '/linux-drivers' + '/chromedriver',
                         options=options)

main_categories = [
    'Economics', 'Quantitative Biology', 'Quantitative Finance', 'Statistics', 'Electrical Engineering'
]

arxiv_names = [
    'econ', 'q-bio', 'q-fin', 'stat', 'eess'
]


"""
# TODO: Fix bug when there's no 'all' button to click (Testing fix)
main_categories = ['Economics']
arxiv_names = ['econ']
"""

# Initiate master dataframe
main_df = pd.DataFrame()

for cat, link_name in tqdm(zip(main_categories, arxiv_names)):

    url = f'https://arxiv.org/list/{link_name}/recent'

    driver.get(url)

    try:
        # Wait until the 'all' link is accessible, get this link and click it
        all_link = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="dlpage"]/small[2]/a[3]'))
        )
        all_link.click()

    except TimeoutException:
        # Subjects with no all click have all their data ready to be scraped
        pass


    # Get the html for the current url
    #html = requests.get(driver.current_url).text
    time.sleep(2)
    html = driver.page_source

    time.sleep(1)
    # Parse the html with BeautifulSoup
    soup = BeautifulSoup(html, 'html.parser')
    time.sleep(2)
    # Find the main containers
    all_dl = soup.find_all('dl')

    for dl in all_dl:

        # Initiate empty list to contain metadata
        all_titles = []
        abstract_links = []
        download_links = []
        abstract_data = []
        authors_data = []
        submission_data = []
        subjects_data = []

        # Titles
        for x in dl.find_all('dd'):
            # list of all titles
            titles = [x.text.replace('Title: ', '').replace('\n', '') for x in x.find_all('div', {'class': 'list-title mathjax'})]

            # Append titles to all titles list
            for t in titles:
                all_titles.append(t)

        # Links for abstract, pdf
        for x in dl.find_all('dt'):

            all_article_links = x.find_all('a', href=True)
            link_list = ['https://arxiv.org' + link['href'] for link in all_article_links][0:2]

            # Append abstract url to abstract links
            abstract_url = link_list[0]
            abstract_links.append(abstract_url)

            # Append download url to abstract link list
            download_url = link_list[1]
            download_links.append(download_url)

        # Subjects
        for x in dl.find_all('div', {'class': 'list-subjects'}):
            subjects = x.text.strip().replace('Subjects: ', '')
            subjects_data.append(subjects)

        # Scrape the abstract meta-data
        for link in abstract_links:

            try:

                driver.get(link)

                # Abstract text
                abstract_block = WebDriverWait(driver, 60).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="abs"]/blockquote'))
                )
                abstract_text = abstract_block.text
                abstract_text = abstract_text.replace('Abstract:  ', '')

                # Authors text
                WebDriverWait(driver, 60).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, '#abs > div.authors'))
                )

                authors_text = driver.find_element_by_css_selector('#abs > div.authors').text

                # Submission date text
                WebDriverWait(driver, 60).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, '#abs > div.dateline'))
                )

                submission_date_text = driver.find_element_by_css_selector('#abs > div.dateline').text


            except Exception as e:

                # Set authors, abstract and submission info to NaN if scraping fails
                authors_text = np.NaN
                abstract_text = np.NaN
                submission_date_text = np.NaN

            # Append metadata info to the main data lists
            abstract_data.append(abstract_text)
            authors_data.append(authors_text)
            submission_data.append(submission_date_text)

        # Convert meta-data into a dataframe
        df = pd.DataFrame({'title': all_titles,
                           'download_url': download_links,
                           'abstract_link': abstract_links,
                           'abstract_text': abstract_data,
                           'authors': authors_data,
                           'submission_date': submission_data,
                           'subjects': subjects_data})

        # Tag the current subject
        df['subject_tag'] = cat

        # Append the subject dataframe to the main dataframe
        main_df = main_df.append(df)

    time.sleep(3)

# Reset index and export data
main_df = main_df.reset_index(drop=True)
main_df.to_csv('data/test.csv', index=False)
main_df.to_excel('data/test.xlsx', index=False)

# Push scraped data to db
with sqlite3.connect('data/arxiv.sqlite') as conn:
    main_df.to_sql('raw_data', if_exists='append', con=conn, index=False)


# Drop duplicate data
with sqlite3.connect('data/arxiv.sqlite') as conn:
    # read raw data and drop duplicates
    df = pd.read_sql_query(sql='SELECT * FROM raw_data', con=conn)
    df = df.drop_duplicates(subset='abstract_link')

    # replace duplicated data with clean data
    df.to_sql('raw_data', con=conn, if_exists='replace', index=False)

# Exit application
driver.quit()
