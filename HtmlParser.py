import random
import threading

from bs4 import BeautifulSoup
import urllib.request
import requests
import sys
import re
import Main

from SqlUtils import gen_xiaoqu_insert_command, gen_zaishou_insert_command, gen_chengjiao_insert_command

hds = [{'User-Agent': 'Mozilla/5.0 (Windows U Windows NT 6.1 en-US rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'},
       {'User-Agent': 'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.12 '
                      'Safari/535.11'},
       {'User-Agent': 'Mozilla/5.0 (compatible MSIE 10.0 Windows NT 6.2 Trident/6.0)'},
       {'User-Agent': 'Mozilla/5.0 (X11 Ubuntu Linux x86_64 rv:34.0) Gecko/20100101 Firefox/34.0'},
       {'User-Agent': 'Mozilla/5.0 (X11 Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu '
                      'Chromium/44.0.2403.89 Chrome/44.0.2403.89 Safari/537.36'},
       {'User-Agent': 'Mozilla/5.0 (Macintosh U Intel Mac OS X 10_6_8 en-us) AppleWebKit/534.50 (KHTML, '
                      'like Gecko) Version/5.1 Safari/534.50'},
       {'User-Agent': 'Mozilla/5.0 (Windows U Windows NT 6.1 en-us) AppleWebKit/534.50 (KHTML, like Gecko) '
                      'Version/5.1 Safari/534.50'},
       {'User-Agent': 'Mozilla/5.0 (compatible MSIE 9.0 Windows NT 6.1 Trident/5.0'},
       {'User-Agent': 'Mozilla/5.0 (Macintosh Intel Mac OS X 10.6 rv:2.0.1) Gecko/20100101 Firefox/4.0.1'},
       {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1 rv:2.0.1) Gecko/20100101 Firefox/4.0.1'},
       {'User-Agent': 'Mozilla/5.0 (Macintosh Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) '
                      'Chrome/17.0.963.56 Safari/535.11'},
       {'User-Agent': 'Opera/9.80 (Macintosh Intel Mac OS X 10.6.8 U en) Presto/2.8.131 Version/11.11'},
       {'User-Agent': 'Opera/9.80 (Windows NT 6.1 U en) Presto/2.8.131 Version/11.11'}]


def get_qu_url_spider():
    regions = []
    regionurls = []
    """
    爬取大区信息
    """
    url = u"https://nj.lianjia.com/xiaoqu/"
    try:
        req = urllib.request.Request(url, headers=hds[random.randint(0, len(hds) - 1)])
        source_code = urllib.request.urlopen(req, timeout=5).read()
        plain_text = source_code.decode('utf-8')
        soup = BeautifulSoup(plain_text, "html.parser")
    except (urllib.request.HTTPError, urllib.request.URLError) as e:
        print(e)
        return
    except Exception as e:
        print(e)
        return

    d = soup.find('div', {'data-role': "ershoufang"}).findAll('a')
    for item in d:
        href = item['href']
        region = item.contents[0]
        fullhref = u"https://nj.lianjia.com" + href
        print(region + fullhref)
        regions.append(region)
        regionurls.append(fullhref)

    for regionurl in regionurls:
        do_xiaoqu_spider(Main.db_xq, regionurl)


def do_xiaoqu_spider(db_xq, url=u"https://nj.lianjia.com/xiaoqu/gulou/"):
    """
    爬取大区域中的所有小区信息
    """
    try:
        req = urllib.request.Request(url, headers=hds[random.randint(0, len(hds) - 1)])
        source_code = urllib.request.urlopen(req, timeout=5).read()
        plain_text = source_code.decode('utf-8')
        soup = BeautifulSoup(plain_text, "html.parser")
    except (urllib.request.HTTPError, urllib.request.URLError) as e:
        print(e)
        return
    except Exception as e:
        print(e)
        return
    d = "d=" + soup.find('div', {'class': 'page-box house-lst-page-box'}).get('page-data')
    loc = {}
    glb = {}
    exec(d, glb, loc)
    total_pages = loc['d']['totalPage']
    threads = []
    for i in range(total_pages):
        url_page = url + u"pg%d/" % (i + 1)
        print(url_page)
        t = threading.Thread(target=xiaoqu_spider, args=(db_xq, url_page))
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print(u"爬下了 %s 区全部的小区信息" % url)


def xiaoqu_spider(db_xq, url_page=u"https://nj.lianjia.com/xiaoqu/gulou/pg1/"):
    """
    爬取页面链接中的小区信息
    """
    try:
        req = urllib.request.Request(url_page, headers=hds[random.randint(0, len(hds) - 1)])
        source_code = urllib.request.urlopen(req, timeout=10).read()
        plain_text = source_code.decode('utf-8')
        soup = BeautifulSoup(plain_text, "html.parser")
    except (urllib.request.HTTPError, urllib.request.URLError) as e:
        print(e)
        exit(-1)
    except Exception as e:
        print(e)
        exit(-1)

    xiaoqu_list = soup.findAll('li', {'class': 'clear xiaoquListItem'})
    for xq in xiaoqu_list:
        info_dict = {}
        title = xq.find('div', {'class': 'title'})
        info_dict.update({u'小区名称': title.text})
        d = title.findAll('a')
        for item in d:
            href = item['href']
            info_dict.update({u'url': href})

        postioninfo = xq.find('div', {'class': 'positionInfo'}).renderContents().strip().decode('utf-8')
        content = "".join(postioninfo.split())
        info = re.match(r".+district.+>(.+)</a>.+bizcircle.+>(.+)</a>(.+)", content)
        if info:
            info = info.groups()
            info_dict.update({u'大区域': info[0]})
            info_dict.update({u'小区域': info[1]})
            info_dict.update({u'建造时间': info[2]})
        command = gen_xiaoqu_insert_command(info_dict)
        db_xq.execute(command, 1)


def do_xiaoqu_zaishou_spider(db_xq, db_zs):
    """
    批量爬取小区在售
    """
    count = 0
    xq_list = db_xq.fetchall()
    for xq in xq_list:
        xiaoqu_zaishou_spider(Main.db_cj, xq[0])
        count += 1
        print('have spidered zaishou %d xiaoqu %s' % (count, xq[0]))
    print('done')


def xiaoqu_zaishou_spider(db_cj, xq_url=u"https://nj.lianjia.com/xiaoqu/1411000000391/"):
    """
    爬取小区在售
    """
    url = xq_url.replace('xiaoqu/', 'ershoufang/c')
    try:
        req = urllib.request.Request(url, headers=hds[random.randint(0, len(hds) - 1)])
        source_code = urllib.request.urlopen(req, timeout=10).read()
        plain_text = source_code.decode('utf-8')
        soup = BeautifulSoup(plain_text, "html.parser")
    except (urllib.request.HTTPError, urllib.request.URLError) as e:
        print(e)
        exception_write('xiaoqu_zaishou_spider', xq_url)
        return
    except Exception as e:
        print(e)
        exception_write('xiaoqu_zaishou_spider', xq_url)
        return
    content = soup.find('div', {'class': 'page-box house-lst-page-box'})
    total_pages = 0
    if content:
        d = "d=" + content.get('page-data')
        loc = {}
        glb = {}
        exec(d, glb, loc)
        total_pages = loc['d']['totalPage']

    threads = []
    for i in range(total_pages):
        tmp = u'ershoufang/pg%dc' % (i + 1)
        url_page = url.replace('ershoufang/c', tmp)
        t = threading.Thread(target=zaishou_spider, args=(db_cj, url_page))
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()


def zaishou_spider(db_cj, url_page=u"https://nj.lianjia.com/chengjiao/pg4c1411000000142/"):
    """
    爬取页面链接中的在售
    """
    try:
        req = urllib.request.Request(url_page, headers=hds[random.randint(0, len(hds) - 1)])
        source_code = urllib.request.urlopen(req, timeout=10).read()
        plain_text = source_code.decode('utf-8')
        soup = BeautifulSoup(plain_text, "html.parser")
    except (urllib.request.HTTPError, urllib.request.URLError) as e:
        print(e)
        exception_write('zaishou_spider', url_page)
        return
    except Exception as e:
        print(e)
        exception_write('zaishou_spider', url_page)
        return

    cjs = soup.find('ul', {'class': 'sellListContent'})
    cj_list = cjs.findAll('li', {})
    for cj in cj_list:
        info_dict = {}
        title = cj.find('div', {'class': 'title'})
        houseInfo = cj.find('div', {'class': 'houseInfo'})
        positionInfo = cj.find('div', {'class': 'positionInfo'})
        followInfo = cj.find('div', {'class': 'followInfo'})
        tag = cj.find('div', {'class': 'tag'})
        totalPrice = cj.find('div', {'class': 'totalPrice'})
        unitPrice = cj.find('div', {'class': 'unitPrice'})

        href = title.find('a')
        if not href:
            continue
        info_dict.update({u'链接': href.attrs['href']})
        content = title.text
        info_dict.update({u'房子描述': content})

        content = houseInfo.text.split('|')
        # 有可能有别墅项，有可能无部分项
        if content:
            info_dict.update({u'小区名称': content[0].strip()})
            if content[1].find(u'墅') != -1:
                i = 1
            else:
                i = 0
            if len(content) >= 2 + i:
                info_dict.update({u'户型': content[1 + i].strip()})
            if len(content) >= 3 + i:
                info_dict.update({u'面积': content[2 + i].strip()})
            if len(content) >= 4 + i:
                info_dict.update({u'朝向': content[3 + i].strip()})
            if len(content) >= 5 + i:
                info_dict.update({u'装修': content[4 + i].strip()})
            if len(content) >= 6 + i:
                info_dict.update({u'电梯': content[5 + i].strip()})

        content = positionInfo.text.split('-')
        if len(content) >= 2:
            info_dict.update({u'楼层年代楼型': content[0].strip()})
            info_dict.update({u'区域': content[1].strip()})
        else:
            info_dict.update({u'区域': content[0].strip()})

        content = followInfo.text.split('/')
        for cont in content:
            if cont.find(u'关注') != -1:
                info_dict.update({u'关注': cont.strip()})
            elif cont.find(u'带看') != -1:
                info_dict.update({u'带看': cont.strip()})
            elif cont.find(u'发布') != -1:
                info_dict.update({u'发布时间': cont.strip()})

        if tag != None:
            tagall = tag.findAll('span')
            for span in tagall:
                if span.attrs['class'][0] == u'taxfree':
                    info_dict.update({u'税费': span.text})  # 满几年
                elif span.attrs['class'][0] == u'subway':
                    info_dict.update({u'地铁': span.text})
                elif span.attrs['class'][0] == u'haskey':
                    info_dict.update({u'限制': span.text})

        info_dict.update({u'挂牌价': totalPrice.text})
        info_dict.update({u'挂牌单价': unitPrice.text})

        command = gen_zaishou_insert_command(info_dict)
        Main.db_zs.execute(command, 1)


def do_xiaoqu_chengjiao_spider(db_xq, db_cj):
    """
    批量爬取小区成交记录
    """
    count = 0
    xq_list = db_xq.fetchall()
    for xq in xq_list:
        xiaoqu_chengjiao_spider(db_cj, xq[0], xq[1])
        count += 1
        print('have spidered %d xiaoqu %s' % (count, xq[0]))
    print('done')


def chengjiao_spider(db_cj, url_page=u"https://nj.lianjia.com/chengjiao/pg4c1411000000142/"):
    """
    爬取页面链接中的成交记录
    """
    print(u"爬取页面%s成交记录" % url_page)
    try:
        req = urllib.request.Request(url_page, headers=hds[random.randint(0, len(hds) - 1)])
        source_code = urllib.request.urlopen(req, timeout=10).read()
        plain_text = source_code.decode('utf-8')
        soup = BeautifulSoup(plain_text, "html.parser")
    except (urllib.request.HTTPError, urllib.request.URLError) as e:
        print(e)
        exception_write('chengjiao_spider', url_page)
        return
    except Exception as e:
        print(e)
        exception_write('chengjiao_spider', url_page)
        return

    recodenum = 0
    cjs = soup.find('ul', {'class': 'listContent'})
    cj_list = cjs.findAll('li', {})
    for cj in cj_list:
        info_dict = {}
        title = cj.find('div', {'class': 'title'})
        houseInfo = cj.find('div', {'class': 'houseInfo'})
        dealDate = cj.find('div', {'class': 'dealDate'})
        totalPrice = cj.find('div', {'class': 'totalPrice'})
        positionInfo = cj.find('div', {'class': 'positionInfo'})
        source = cj.find('div', {'class': 'source'})
        unitPrice = cj.find('div', {'class': 'unitPrice'})
        dealHouseInfo = cj.find('div', {'class': 'dealHouseInfo'})
        dealCycleeInfo = cj.find('div', {'class': 'dealCycleeInfo'})

        href = title.find('a')
        if not href:
            continue
        info_dict.update({u'链接': href.attrs['href']})
        content = title.text.split()
        if content:
            info_dict.update({u'小区名称': content[0]})
            info_dict.update({u'户型': content[1]})
            info_dict.update({u'面积': content[2]})

        content = houseInfo.text.split('|')  # unicode(cj.find('div', {'class': 'con'}).renderContents().strip())
        if content:
            info_dict.update({u'朝向': content[0].strip()})
            if len(content) >= 2:
                info_dict.update({u'装修': content[1].strip()})
            if len(content) >= 3:
                info_dict.update({u'电梯': content[2].strip()})

        info_dict.update({u'签约时间': dealDate.text})
        info_dict.update({u'签约总价': totalPrice.text})  # 注意值
        content = positionInfo.text.split()
        if len(content) >= 2:
            info_dict.update({u'楼层': content[0].strip()})
            info_dict.update({u'年代楼型': content[1].strip()})
        else:
            info_dict.update({u'楼层': content[0].strip()})

        info_dict.update({u'来源': source.text})
        info_dict.update({u'签约单价': unitPrice.text})  # 可能为*
        # content = dealHouseInfo.text.split()
        if dealHouseInfo != None:
            for span in dealHouseInfo.find('span', {'class': 'dealHouseTxt'}).findAll('span'):
                if span.text.find(u'房屋') != -1:
                    info_dict.update({u'税费': span.text})  # 满几年
                elif span.text.find(u'距') != -1:
                    info_dict.update({u'地铁': span.text})

        # content = dealCycleeInfo.text.split()
        if dealCycleeInfo != None:
            for span in dealCycleeInfo.find('span', {'class': 'dealCycleTxt'}).findAll('span'):
                if span.text.find(u'挂牌') != -1:
                    info_dict.update({u'挂牌价': span.text})
                elif span.text.find(u'成交周期') != -1:
                    info_dict.update({u'成交周期': span.text})

        command = gen_chengjiao_insert_command(info_dict)

        db_cj.execute(command, 1)
        recodenum += 1
    print(u"爬取页面%s成交记录%d条" % (url_page, recodenum))


def xiaoqu_chengjiao_spider(db_cj, xq_url=u"https://nj.lianjia.com/xiaoqu/1411000000391/", xq_name=u"default"):
    """
    爬取小区成交记录
    """
    url = xq_url.replace('xiaoqu/', 'chengjiao/c')
    try:
        req = urllib.request.Request(url, headers=hds[random.randint(0, len(hds) - 1)])
        source_code = urllib.request.urlopen(req, timeout=10).read()
        plain_text = source_code.decode('utf-8')
        soup = BeautifulSoup(plain_text, "html.parser")
    except (urllib.request.HTTPError, urllib.request.URLError) as e:
        print(e)
        exception_write('xiaoqu_chengjiao_spider', xq_url)
        return
    except Exception as e:
        print(e)
        exception_write('xiaoqu_chengjiao_spider', xq_url)
        return
    content = soup.find('div', {'class': 'page-box house-lst-page-box'})
    total_pages = 0
    if content:
        d = "d=" + content.get('page-data')
        loc = {}
        glb = {}
        exec(d, glb, loc)
        total_pages = loc['d']['totalPage']

    print(u"xiaoqu %s chengjiao totalpage %d" % (xq_name, total_pages))
    threads = []
    for i in range(total_pages):
        tmp = u'chengjiao/pg%dc' % (i + 1)
        url_page = url.replace('chengjiao/c', tmp)
        t = threading.Thread(target=chengjiao_spider, args=(db_cj, url_page))
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()



