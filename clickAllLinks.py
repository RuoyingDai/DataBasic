from bs4 import BeautifulSoup, SoupStrainer
import requests
import webbrowser

#url = "http://stackoverflow.com/"
url = "https://fme.discomap.eea.europa.eu/fmedatastreaming/AirQualityDownload/AQData_Extract.fmw?CountryCode="
url = url + "FI&CityName=&Pollutant=8&Year_from=2017&Year_to=2017&Station=&Samplingpoint=&Source=E1a&Output=HTML&UpdateDate=&TimeCoverage=Year"


def openurl(url):
    page = requests.get(url)
    data = page.text
    soup = BeautifulSoup(data)
    for link in soup.find_all('a'):
    #    print(link.get('href'))
    #    page = requests.get(link.get('href'))  
        webbrowser.open(link.get('href'))  #
