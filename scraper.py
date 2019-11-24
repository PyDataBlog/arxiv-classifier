import requests
import os
import time
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


#driver = webdriver.Firefox(executable_path = os.getcwd() + '/mac-drivers' + '/geckodriver')
driver = webdriver.Chrome(executable_path = os.getcwd() + '/mac-drivers' + '/chromedriver')


main_categories = [
    'Quantitative Biology', 'Quantitative Finance', 'Statistics', 'Electrical Engineering', 'Economics'
]

arxiv_names = [
    'q-bio', 'q-fin', 'stat', 'eess', 'econ'
]

'''
main_categories = ['Economics']
arxiv_names = ['econ']
'''
# Initiate master dataframe
main_df = pd.DataFrame()

for cat, link_name in tqdm(zip(main_categories, arxiv_names)):
    
    url = f'https://arxiv.org/list/{link_name}/recent'

    driver.get(url)

    try:
        # Wait until the 'all' link is accessible, get this link and click it
        all_link = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="dlpage"]/small[2]/a[3]'))
        )
        all_link.click()
    except Exception as e:
        pass
    
    print(driver.current_url)
    # Get the current html from the current url 
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
        for x in abstract_links:
            # TODO: Scrape the abstract data using the abstract link
            #print(x)
            pass

        # Convert meta-data into a dataframe
        df = pd.DataFrame({'title': all_titles,
                           'download_url': download_links,
                           'abstract_link': abstract_links})

        # Tag the current subject
        df['subject_tag'] = cat

        # Append the subject dataframe to the main dataframe
        main_df = main_df.append(df)

    time.sleep(2)

main_df = main_df.reset_index(drop=True)

print(main_df.info())

driver.quit()

# Dimensions: date_scraped, url, title, abstract, authors, tag