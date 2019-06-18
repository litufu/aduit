# -*- coding:utf-8 -*-

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from database import Base,FSSubject,FirstClassSubject,AccountingFirm
import pandas as pd
import time
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
#create capabilities
capabilities = DesiredCapabilities.INTERNETEXPLORER

#delete platform and version keys
capabilities.pop("platform", None)
capabilities.pop("version", None)

#start an instance of IE
driver = webdriver.Ie(executable_path="C:\\Users\\litufu\\IEDriverServer_x64_3.141.0\\IEDriverServer.exe", capabilities=capabilities)
driver.maximize_window()



engine = create_engine('sqlite:///audit.sqlite?check_same_thread=False')
Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)
session = DBSession()

def add_fs_subject():
    '''
    添加报表标准项目
    :return:
    '''
    df = pd.read_csv('./data/fs_subject.csv', header=None)
    for i in df[0]:
        subject = FSSubject(name=i)
        session.add(subject)
    session.commit()


def add_first_class_subject():
    df = pd.read_csv('./data/first_clsss_subject.csv')
    for i in range(len(df)):
        code = str(df.iat[i,0])
        name = df.iat[i,1]
        print(code)
        print(name)
        subject = FirstClassSubject(code=code,name=name)
        session.add(subject)
    session.commit()

def get_account_firm():
    driver.get("http://cmispub.cicpa.org.cn/cicpa2_web/public/query0/1/00.shtml")
    time.sleep(3)
    driver.switch_to.frame('mainbody')
    time.sleep(3)
    td = driver.find_element_by_class_name('search_td')
    img = td.find_element_by_tag_name('img')
    driver.execute_script("arguments[0].scrollIntoView();", img)
    ActionChains(driver).move_to_element(img).perform()
    # img.send_keys(Keys.ENTER)
    time.sleep(2)
    num = 0
    while num < 607:
        dfs = pd.read_html(driver.page_source,attrs = {'id': 'tabDetail'})
        df = dfs[0]
        for i in range(len(df)):
            code = str(df.iat[i,1])
            name = df.iat[i,2]
            address = df.iat[i,3]
            contact = df.iat[i,4]
            phone = df.iat[i,5]
            account_firms = session.query(AccountingFirm).filter(AccountingFirm.name==name).all()
            if len(account_firms)>0:
                continue
            account_firm = AccountingFirm(code=code,name=name,address=address,contact=contact,phone=phone)
            session.add(account_firm)
            # print(code, name, address, contact, phone)
        session.commit()
        print(df)
        next_page = driver.find_element_by_link_text('上一页')
        driver.execute_script("arguments[0].scrollIntoView();", next_page)
        ActionChains(driver).move_to_element(next_page).perform()
        next_page.send_keys(Keys.ENTER)
        time.sleep(1)
        num += 1


if __name__ == '__main__':
    # add_fs_subject()
    # add_first_class_subject()
    get_account_firm()

