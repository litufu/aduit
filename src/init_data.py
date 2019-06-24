# -*- coding:utf-8 -*-

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.database import Base,FSSubject,FirstClassSubject,AccountingFirm,SubjectContrast,TBSubject
import pandas as pd
import time
from src.utils import get_session_and_engine
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities


def add_fs_subject(session):
    '''
    添加报表标准项目/TB标准科目/科目对照表
    :return:
    '''
    dfs = pd.read_excel('../data/subject_contrast.xlsx',sheet_name=None)
    for sheet_name in dfs:
        # 处理科目对照表：科目余额表=》TB=>报表之间的对应关系
        if sheet_name == "constrast":
            continue
            df = dfs[sheet_name]
            df = df[["origin",'tb','fs','confidence','direction','first_class','second_class']]
            for i in range(len(df)):
                origin_subject = df.iat[i,0]
                tb_subject = df.iat[i,1]
                fs_subject = df.iat[i,2]
                coefficient = df.iat[i,3]
                direction = df.iat[i,4]
                first_class = df.iat[i, 5]
                second_class = df.iat[i, 6]
                subjectcontrast = SubjectContrast(origin_subject=origin_subject,tb_subject=tb_subject,fs_subject=fs_subject,
                                coefficient=coefficient,direction=direction,first_class=first_class,second_class=second_class
                                )
                session.add(subjectcontrast)
            session.commit()
        elif sheet_name == "tb":
            df = dfs[sheet_name]
            df = df[["tb_show", 'tb_subject', 'direction','order']]
            for i in range(len(df)):
                show = df.iat[i,0]
                subject = df.iat[i,1]
                direction = df.iat[i,2]
                order = int(df.iat[i,3])
                tbsubject = TBSubject(show=show,subject=subject,direction=direction,order=order)
                session.add(tbsubject)
            session.commit()
        elif sheet_name == 'fs1':
            continue
            df = dfs[sheet_name]
            df = df[["fs_show", 'fs_subject', 'direction']]
            for i in range(len(df)):
                show = df.iat[i, 0]
                subject = df.iat[i, 1]
                direction = df.iat[i, 2]
                fssubject = FSSubject(name="2019已执行三个新准则",show=show, subject=subject, direction=direction)
                session.add(fssubject)
            session.commit()
        elif sheet_name == 'fs2':
            continue
            df = dfs[sheet_name]
            df = df[["fs_show", 'fs_subject', 'direction']]
            for i in range(len(df)):
                show = df.iat[i, 0]
                subject = df.iat[i, 1]
                direction = df.iat[i, 2]
                fssubject = FSSubject(name="2019未执行三个新准则",show=show, subject=subject, direction=direction)
                session.add(fssubject)
            session.commit()
        else:
            pass



def add_first_class_subject(session):
    df = pd.read_csv('./data/first_clsss_subject.csv')
    for i in range(len(df)):
        code = str(df.iat[i,0])
        name = df.iat[i,1]
        print(code)
        print(name)
        subject = FirstClassSubject(code=code,name=name)
        session.add(subject)
    session.commit()

def get_account_firm(session):
    # create capabilities
    capabilities = DesiredCapabilities.INTERNETEXPLORER

    # delete platform and version keys
    capabilities.pop("platform", None)
    capabilities.pop("version", None)
    # start an instance of IE
    driver = webdriver.Ie(executable_path="C:\\Users\\litufu\\IEDriverServer_x64_3.141.0\\IEDriverServer.exe",
                          capabilities=capabilities)
    driver.maximize_window()

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
    session,engine = get_session_and_engine()
    add_fs_subject(session)
    # add_first_class_subject(session)
    # get_account_firm(session)

