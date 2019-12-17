import requests
from lxml import etree

response = requests.get("https://maoyan.com/")
html = etree.HTML(response.text)
info = html.xpath('//*[@id="app"]/div/div[2]/div/div[1]/div[2]/dl/dd[2]/div/a/div/div/div/div[1]')[0].xpath(
    'string(.)').extract()[0]
print(info)
