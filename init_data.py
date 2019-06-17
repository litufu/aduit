# -*- coding:utf-8 -*-

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from database import Base,FSSubject,FirstClassSubject
import pandas as pd


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

if __name__ == '__main__':
    # add_fs_subject()
    add_first_class_subject()
