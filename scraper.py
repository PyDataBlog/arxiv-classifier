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


# Specify webdriver options
options = webdriver.ChromeOptions()
options.add_argument('headless')  # set to headerless windows
options.add_argument('window-size=1200x600')  # set the window size

# Initiate headerless scraping
driver = webdriver.Chrome(executable_path = os.getcwd() + '/linux-drivers' + '/chromedriver',
                         options=options)

#driver = webdriver.Chrome(executable_path = os.getcwd() + '/mac-drivers' + '/chromedriver')


main_categories = [
    'Quantitative Biology', 'Quantitative Finance', 'Statistics', 'Electrical Engineering', 'Economics'
]

arxiv_names = [
    'q-bio', 'q-fin', 'stat', 'eess', 'econ'
]


'''
main_categories = ['Quantitative Finance']
arxiv_names = ['q-fin']
'''

# Initiate master dataframe
main_df = pd.DataFrame()

for cat, link_name in tqdm(zip(main_categories, arxiv_names)):

    url = f'https://arxiv.org/list/{link_name}/recent'

    driver.get(url)

    try:
        # Wait until the 'all' link is accessible, get this link and click it
        all_link = WebDriverWait(driver, 7).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="dlpage"]/small[2]/a[3]'))
        )
        all_link.click()
    except Exception as e:
        continue


    # Get the html for the current url
    html = requests.get(driver.current_url).text

    # Parse the html with BeautifulSoup
    soup = BeautifulSoup(html, 'html.parser')

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


        # Scrape the abstract meta-data
        for link in abstract_links:

            try:

                driver.get(link)

                # Abstract text
                abstract_block = WebDriverWait(driver, 45).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="abs"]/blockquote'))
                )
                abstract_text = abstract_block.text

                # Authors text
                WebDriverWait(driver, 45).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, '#abs > div.authors'))
                )

                authors_text = driver.find_element_by_css_selector('#abs > div.authors').text

                # Submission date text
                WebDriverWait(driver, 45).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, '#abs > div.dateline'))
                )

                submission_date_text = driver.find_element_by_css_selector('#abs > div.dateline').text

                abstract_text = abstract_text.replace('Abstract:  ', '')

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
                           'submission_date': submission_data})

        # Tag the current subject
        df['subject_tag'] = cat

        # Append the subject dataframe to the main dataframe
        main_df = main_df.append(df)


    time.sleep(2)

# Reset index and export data
main_df = main_df.reset_index(drop=True)
main_df.to_csv('data/test.csv', index=False)
main_df.to_excel('data/test.xlsx', index=False)

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
