from bs4 import BeautifulSoup
import urllib.request
import requests
import sys
import re


def get_url(url):
    headers = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36'
    }
    result = requests.get(url, headers=headers)
    return result.text


def parser_url(text):
    soup = BeautifulSoup.get(text, "html.parser")



if __name__ == "__main__":
    result = get_url("https://sz.lianjia.com/xiaoqu/")
    parser_url(text=result)
