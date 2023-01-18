from bs4 import BeautifulSoup
import requests

url = """https://www.worldometers.info/coronavirus/"""

def scrape(url=url):
    info = {}
    res = requests.get(url=url)

    if res.status_code != 200:
        print(f"Access Denied!!\n Erro {res.status_code}")

    else:
        soup = BeautifulSoup(res.content)  
        data = soup.select('body')[0]
        content = data.select('div.container')[1].select('div.row')[1].select('div.col-md-8')[0].select('div.content-inner')[0].\
        select('div')

        # Get time number of cases was updated 
        update_time = content[1].text.strip()

        # Get the number of cases
        number_cases = content[3].select('div.maincounter-number')[0].select('span')[0].text.strip()
        #number_cases = number_cases.replace(',','')

        info = dict(time = update_time, cases = number_cases)

        return info
