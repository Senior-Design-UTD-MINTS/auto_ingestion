from bs4 import BeautifulSoup
from selenium import webdriver


def getSensors():

    # Update location with where you downloaded PhantomJS (https://phantomjs.org/download.html)
    driver = webdriver.PhantomJS(
        '/Users/kameron/Downloads/phantomjs-2.1.1-macosx/bin/phantomjs')
    driver.get('http://mintsdata.utdallas.edu:4200/files#/')

    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')

    test = soup.find_all('div')

    fin = []

    # find every new sensor added to mintsdata.utdallas.edu
    for div in test:
        if div.table is not None:
            for tr in div.table.tbody:
                try:
                    fin.append(tr.td.input['value'])
                except:
                    pass

    return fin
