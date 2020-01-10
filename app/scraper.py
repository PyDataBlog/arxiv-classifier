import os
import time
import pandas as pd
import numpy as np
import sqlite3
import platform
from tqdm import tqdm
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException



def scrape_data(driver, categories, arxiv_identifier):
    """
    """

    # Initiate master dataframe
    main_df = pd.DataFrame()

    for cat, link_name in tqdm(zip(main_categories, arxiv_names)):

        url = f'https://export.arxiv.org/list/{link_name}/recent'

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
        time.sleep(2)
        html = driver.page_source

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
                titles = [x.text.replace('Title: ', '').strip() for x in x.find_all('div', {'class': 'list-title mathjax'})]

                # Append titles to all titles list
                for t in titles:
                    all_titles.append(t)

            # Links for abstract, pdf
            for x in dl.find_all('dt'):

                all_article_links = x.find_all('a', href=True)
                link_list = ['https://export.arxiv.org' + link['href'] for link in all_article_links][0:2]

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
                    abstract_block = WebDriverWait(driver, 90).until(
                        EC.presence_of_element_located((By.XPATH, '//*[@id="abs"]/div[2]/blockquote'))
                    )
                    abstract_text = abstract_block.text
                    abstract_text = abstract_text.replace('Abstract:  ', '')

                    # Authors text
                    WebDriverWait(driver, 90).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, '#abs > div.leftcolumn > div.authors'))
                    )

                    authors_text = driver.find_element_by_css_selector('#abs > div.leftcolumn > div.authors').text

                    # Submission date text
                    WebDriverWait(driver, 90).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, '#abs > div.leftcolumn > div.dateline'))
                    )

                    submission_date_text = driver.find_element_by_css_selector('#abs > div.leftcolumn > div.dateline').text


                except Exception as e:
                    print(e)
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

    # Push scraped data to db
    with sqlite3.connect(os.path.join('app', 'data', 'arxiv.sqlite')) as conn:
        main_df.to_sql('raw_data', if_exists='replace', con=conn, index=False)

    # Exit application
    driver.quit()

    print('Done scraping all the data')


if __name__ == "__main__":

    # Specify webdriver options
    options = webdriver.FirefoxOptions()
    #options.headless = True  # set to headerless windows
    options.add_argument('window-size=1200x600')  # set the window size

    os_platform = platform.system()

    if os_platform == 'Linux':
        # Specify Linux path for the webdriver executable
        linux_path = os.path.join('app', 'linux-drivers', 'geckodriver')

        # Initiate headerless scraping in a linux environment
        driver = webdriver.Firefox(executable_path = linux_path,
                                   options=options)

    elif os_platform == 'Darwin':
        # Specify Mac path for the chromedriver executable
        mac_path = os.path.join('app', 'mac-drivers', 'geckodriver')

        # Initiate headerless scraping in a darwin/mac environment
        driver = webdriver.Firefox(executable_path = mac_path,
                                   options=options)

    elif os_platform == 'Windows':
        # Specify Mac path for the chromedriver executable
        windows_path = os.path.join('app', 'windows-drivers', 'geckodriver.exe')

        # Initiate headerless scraping in a darwin/mac environment
        driver = webdriver.Firefox(executable_path = windows_path,
                                   options=options)

    else:
        raise OSError('Unsupported OS Platform. Only Linux/Mac/Windows firefox drivers supported!')

    main_categories = [
        'Economics', 'Quantitative Biology', 'Quantitative Finance',
        'Statistics', 'Electrical Engineering', 'Mathematics',
        'Computer Science', 'Physics', 'Astrophysics', 'Condensed Matter',
        'General Relativity & Quantum Cosmology', 'High Energy Physics - Experiment',
        'High Energy Physics - Lattice', 'High Energy Physics - Phenomenology',
        'High Energy Physics - Theory', 'Mathematical Physics',
        'Nonlinear Sciences', 'Nuclear Experiment', 'Nuclear Theory',
        'Quantum Physics'
    ]

    arxiv_names = [
        'econ', 'q-bio', 'q-fin',
        'stat', 'eess', 'math',
        'cs', 'physics', 'astro-ph',
        'cond-mat', 'gr-qc', 'hep-ex',
        'hep-lat', 'hep-ph', 'hep-th', 
        'math-ph', 'nlin', 'nucl-ex',
        'nucl-th', 'quant-ph'
    ]

    """
    # Only used for testing
    main_categories = ['Economics']
    arxiv_names = ['econ']
    """

    scrape_data(driver = driver, categories = main_categories, arxiv_identifier = arxiv_names)
