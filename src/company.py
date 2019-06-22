# -*- coding:utf-8 -*-

import pandas as pd
import requests
import re
from bs4 import BeautifulSoup
import io
from datetime import datetime
from src.database import Holder,Company
from src.utils import get_session_and_engine
import logging

# 设置日志文件
logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
handler = logging.FileHandler("log.txt",encoding="utf-8")
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# 设置企查查爬虫headers
headers = {
    'user-agent': "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36"
}


def get_cookies():
    # 将cookies字符串转化为dict
    f = open(r'../settings/qichacha_cookies.txt', 'r')  # 打开所保存的cookies内容文件
    cookies = {}  # 初始化cookies字典变量
    for line in f.read().split(';'):  # 按照字符：进行划分读取
        # 其设置为1就会把字符串拆分成2份
        name, value = line.strip().split('=', 1)
        cookies[name] = value  # 为字典cookies添加内容
    return cookies


def get_company_detail_url(company_name):
    # 获取公司企查查详情页url
    payload = {'key': company_name}
    cookies = get_cookies()
    origin_url = 'https://www.qichacha.com/search'
    r = requests.get(origin_url, params=payload, headers=headers, cookies=cookies)
    content = r.text
    soup = BeautifulSoup(content, 'html.parser')
    trs = soup.find_all("tr")
    for tr in trs:
        a = tr.find(href=re.compile("firm"))
        if company_name == a.get_text():
            href = a['href']
            return href
    return ""


def get_company_info(soup,company_name,session):
    '''
    获取公司基本信息
    :param soup: beautifulsoup对象
    :param company_name: 公司名称
    :return: company对象
    '''

    # （1）获取注册资本
    pattern1 = re.compile("注册资本")
    element1 = soup.find(text=pattern1).parent.find_next_sibling("td")
    registered_capital = element1.text.strip()
    # （2）获取成立日期
    pattern2 = re.compile("成立日期")
    element2 = soup.find(text=pattern2).parent.find_next_sibling("td")
    establish_date_str = element2.text.strip()
    establish_date = datetime.strptime(establish_date_str, '%Y-%m-%d')
    # （3）获取经营范围
    pattern3 = re.compile("经营范围")
    element3 = soup.find(text=pattern3).parent.find_next_sibling("td")
    business_scope = element3.text.strip()
    # （4）获取企业信用编码
    pattern4 = re.compile("统一社会信用代码")
    element4 = soup.find(text=pattern4).parent.find_next_sibling("td")
    code = element4.text.strip()
    # (5)获取企业地址
    pattern5 = re.compile("企业地址")
    element5 = soup.find(text=pattern5).parent.find_next_sibling("td")
    # 移除查看地图和附近企业两个标签
    for link in element5.find_all('a'):
        link.replace_with('')
    address = element5.text.strip()
    # 获取企业法定代表人
    element6 = soup.find(attrs={"class": "bname"})
    legal_representative = element6.text.strip()
    company = Company(name=company_name, legal_representative=legal_representative, establish_date=establish_date,
                      address=address, code=code, registered_capital=registered_capital,business_scope=business_scope)
    session.add(company)
    session.commit()
    return company


def get_company_holders(soup,company,company_name,session):
    # 获取网页中股东部分
    partners = soup.find("section", id="partnerslist")
    if partners is None:
        logger.info("{}详情页没有股东部分".format(company_name))
        return
    # 获取股东表格
    table = partners.find('table')
    if table is None:
        logger.info("{}股东部分没有表格".format(company_name))
        return
    # 处理股东表格，使得其成为标准的html的table格式
    # 去除掉表头的a标签
    for th in table.find_all('th'):
        a = th.find('a')
        if a:
            a.replace_with('')
    # 去除掉单元格中的表格，用表格中的h3标签内容代替
    for tab in table.find_all('table'):
        name = tab.find('h3').string
        tab.replace_with(name)
    # 获取表格中所有的行
    rows = table.find_all('tr')
    # 利用StringIO制作df
    csv_io = io.StringIO()
    for row in rows:
        row_texts = []
        for cell in row.findAll(['th', 'td']):
            text = cell.get_text().strip()
            text = text.replace(',', '')
            res = text.split('\n')
            if len(res) > 1:
                text = res[0].strip()
            row_texts.append(text)
        row_string = ','.join(row_texts) + '\n'
        csv_io.write(row_string)
    csv_io.seek(0)
    df = pd.read_csv(csv_io)
    pattern = re.compile(r'(.*)[\(（].*?')
    df = df.rename(index=str, columns=lambda x: re.match(pattern, x)[1] if re.match(pattern, x) else x)
    df = df.rename(index=str, columns={
        "序号": "no",
        "股东": "holder_name",
        "持股比例": "ratio",
        "认缴出资额": "promise_to_pay_amount",
        "认缴出资日期": "promise_to_pay_date",
        "实缴出资额": "pay_amount",
        "实缴出资日期": "pay_date",
    })
    df = df[['holder_name', 'ratio']]
    # # 保存到数据库
    for i in range(len(df)):
        holder_name = df.iat[i, 0]
        print(holder_name)
        ratio = df.iat[i, 1]
        ratio = ratio.replace('%', "")
        print(ratio)
        holder = Holder(name=holder_name, ratio=float(ratio), company=company)
        session.add(holder)
    session.commit()


def download_company(company_name,session,engine):
    '''
    下载公司基本信息和股东信息
    :param company_name:公司名称
    :return:None
    '''
    # 判断是否已经存储了该公司，存储则返回
    try:
        df = pd.read_sql_table('company', con=engine)
    except ValueError as e:
        df = pd.DataFrame()
    if not df.empty:
        df = df[df['name'].str.match(company_name)]
        if df.shape[0] > 0:
            print('该公司已经存储')
            company = session.query(Company).filter(Company.name==company_name).first()
            return company

    #  没有存储则到企查查爬取
    #  获取详情页页面
    url = get_company_detail_url(company_name)
    if url.isspace():
        logger.info("{}没有获得详情页url".format(company_name))
        return df
    origin_url = 'https://www.qichacha.com'
    # 请求详情页面
    r = requests.get(origin_url + url, headers=headers)
    content = r.text
    soup = BeautifulSoup(content, 'html.parser')
    # 获取公司基本信息
    try:
        company = get_company_info(soup,company_name,session)
    except Exception as e:
        print(e)
        # 请用户输入公司基本信息
        logger.info("{}获取公司基本信息失败".format(company_name))
        raise Exception("下载公司失败")
    # 获取公司股东信息
    try:
        get_company_holders(soup,company,company_name,session)
    except Exception as e:
        print(e)
        logger.info("{}获取公司股东信息失败".format(company_name))
        raise Exception("下载公司股东信息失败")

    companies = session.query(Company).filter(Company.name==company_name).all()
    if len(companies)>0:
        return companies[0]
    else:
        raise Exception("下载公司失败")

def manual_input(name,legal_representative,establish_date,address,code,registered_capital,business_scope):
    companies = session.query(Company).filter(Company.name==name).all()
    if len(companies)>0:
        raise Exception("公司信息已经存储，无需人工存储")
    company = Company(name=name,legal_representative=legal_representative,establish_date=establish_date,
                      address=address,code=code,registered_capital=registered_capital,business_scope=business_scope)
    session.add(company)
    session.commit()



if __name__ == '__main__':
    session,engine = get_session_and_engine()
    download_company("湛江千红麦角甾醇有限公司",session,engine)