import requests
from bs4 import BeautifulSoup
import pandas as pd
import html
import hashlib
import threading
from concurrent.futures import ThreadPoolExecutor
lock = threading.Lock()

# creating a GET request to the API https://www.cuitonline.com/search.php?q=el
base_link = 'https://www.cuitonline.com/search.php?q=trabajo&f2[]=iva%3Aiva_exento&pn=1&f1[]=iva%3Aiva_exento&f5[]=persona%3Ajuridica&f6[]=nacionalidad%3Aargentina&f0[]=ganancias%3Aganancias_sociedades&f4[]=empleador%3Ano&&pn=1'
# creating dataframe to store the data


def hashCaluculator(text):
    hash_object = hashlib.sha256(text.encode())
    hex_dig = hash_object.hexdigest()
    return hex_dig

def textParser(text):
    df = pd.DataFrame(columns=['title', 'cuit', 'reg_link'])
    # getting the all the elements with class hit
    cuit_text = ''
    title_text = ''
    reg_link_text = ''
    soup = BeautifulSoup(text, 'html.parser')
    hits = soup.find_all('div', class_='hit')
    # iterating thourgh the hits and getting the elements with class denominacion
    for hit in hits:
        denominacion = hit.find('div', class_='denominacion')
        # adding the denominacion.text to the dataframe title column
        #df.loc[len(df)] = [denominacion.text]
        title_text = denominacion.text
        # getting the elements of class doc-facets
        doc_facets = hit.find_all('div', class_='doc-facets')
        # iterating through the doc_facets and getting the elements with class linea-cuit-persona
        for doc_facet in doc_facets:
            # finding spans with class linea-cuit-persona
            linea = doc_facet.find('span', class_='linea-cuit-persona')
            for lin in linea:
                cuits = doc_facet.find_all('span', class_='cuit')
                cuit_text = cuits[0].text
                reg_links = doc_facet.find_all('a')
                # adding the reg_links[0].text to the dataframe reg_link column
                reg_link_text = reg_links[0]['href']
                # removing the first 2 characters from the reg_link_text
                reg_link_text = reg_link_text[2:]
                # making the link clickable
                reg_link_text = 'https://' + reg_link_text
                link_html = html.escape(reg_link_text)
                link_html = '=HYPERLINK("{}")'.format(link_html)
        df.loc[len(df)] = [title_text, cuit_text, link_html]
    return df

def nextPage(text):
    soup = BeautifulSoup(text, 'html.parser')
    next_page = soup.find('div', class_='paginator-next')
    try:
        return next_page.find('a')['href']
    except:
        return None


def exportToExcel(fileName, df):
    if fileName[-3:] != '.csv':
        df.to_csv(fileName, index=False)
    else:
        while True:
            print('File name must contain .csv extension')
            fileName = input('Enter the file name: ')
            if fileName[-3:] != '.csv':
                df.to_csv(fileName, index=False)
                break

def allPages(base_link):
    df1 = pd.DataFrame(columns=['title', 'cuit', 'reg_link'])
    page = 1
    while True:
        print('Page: ', page)
        page += 1
        req = requests.get(base_link)
        currentDF = textParser(req.text)
        data = [df1, currentDF]
        df1 = pd.concat(data)
        next_url = nextPage(req.text)
        if next_url == None:
            break
        else:
            base_link = 'https://www.cuitonline.com/' + next_url
    fileN = hashCaluculator(base_link)
    exportToExcel(fileN + '.csv', df1)


def fetchLinks(fileName):
    df = pd.read_csv(fileName)
    links = df['URLS by keywords']
    return links


# Define a function to fetch data for a group of links
def fetch_links_group(links):
        for link in links:
            print(link)
            allPages(link)



links = fetchLinks('links.csv')

# Create a thread pool executor
executor = ThreadPoolExecutor(max_workers=32)

# Submit the function to the executor
for i in range(1018,len(links),10):
    start = i
    end = i+10 if i+10 < len(links) else len(links)
    executor.submit(fetch_links_group, links[start:end])

# Wait for all threads to finish
executor.shutdown(wait=True)





