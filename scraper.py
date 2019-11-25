import requests
import os
import time
import pandas as pd
import numpy as np
from tqdm import tqdm
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


#driver = webdriver.Firefox(executable_path = os.getcwd() + '/mac-drivers' + '/geckodriver')
driver = webdriver.Chrome(executable_path = os.getcwd() + '/linux-drivers' + '/chromedriver')

'''
main_categories = [
    'Quantitative Biology', 'Quantitative Finance', 'Statistics', 'Electrical Engineering', 'Economics'
]

arxiv_names = [
    'q-bio', 'q-fin', 'stat', 'eess', 'econ'
]
'''

main_categories = ['Economics']
arxiv_names = ['econ']


# Initiate master dataframe
main_df = pd.DataFrame()

for cat, link_name in tqdm(zip(main_categories, arxiv_names), desc='Loading data from arxiv'):

    url = f'https://arxiv.org/list/{link_name}/recent'

    driver.get(url)

    try:
        # Wait until the 'all' link is accessible, get this link and click it
        all_link = WebDriverWait(driver, 7).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="dlpage"]/small[2]/a[3]'))
        )
        all_link.click()
    except Exception as e:
        pass

    print(driver.current_url)

    # Get the html for the current url
    html = requests.get(driver.current_url).text

    # Parse the html with BeautifulSoup
    soup = BeautifulSoup(html, 'html.parser')

    # Find the main containers
    all_dl = soup.find_all('dl')

    for dl in tqdm(all_dl, desc=f'Dowloading metadata for {cat}'):

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


        # Scrape the abstract data
        for link in tqdm(abstract_links, desc='Attempting To Scrape Abstract Data'):

            # TODO: Scrape the abstract data using the abstract link, authors, submission_date
            #print(x)
            try:

                print('Attempting to scrape the abstract data')

                driver.get(link)

                abstract_block = WebDriverWait(driver, 7).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="abs"]/blockquote'))
                )
                abstract_text = abstract_block.text

                authors_text = driver.find_element_by_xpath('//*[@id="abs"]/div[1]').text

                submission_date_text = driver.find_element_by_xpath('//*[@id="abs"]/div[2]').text

                print(f'Successfully Scraped the abstract metadata from {link}')

            except Exception as e:

                print(f'Failed to scrape abstract data for {link}')

                abstract_text = np.NaN
                authors_text = np.NaN
                submission_data = np.NaN

            abstract_text = abstract_text.replace('Abstract:  ', '')
            submission_date_text = submission_date_text.replace('Submitted on ', '')


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

main_df = main_df.reset_index(drop=True)

print(main_df.info())
print(main_df.head(10))

main_df.to_excel('test.xlsx', index=False)
driver.quit()

# Dimensions: date_scraped, url, title, abstract, authors, tag