import pandas as pd
import numpy as np
import json
import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.database import Base

def gen_df_line(df):
    '''
    将一个dataframe转换未一个行对象生成器
    :param df: dataframe
    :return: 生成一个行对象
    '''
    for i in range(len(df)):
        line_json = df.iloc[i].to_json(orient='columns',force_ascii=False)
        yield json.loads(line_json)


def str_to_float(str1):
    '''
    字符串数字转换为float
    :param str1:
    :return:
    '''
    if isinstance(str1, str):
        if str1=="":
            return 0.00
        elif str == "nan":
            return 0.00
        str1 = str1.replace(',', '')
        return round(float(str1),2)
    elif np.isnan(str1):
        return 0.00
    elif pd.isnull(str1):
        return 0.00
    elif isinstance(str1, np.float64):
        return str1
    elif isinstance(str1,float):
        return str1
    elif isinstance(str1,int):
        print(" int")
        return float(str1)



def check_start_end_date(start_time, end_time):
    '''
    检查导入数据的起止日期是否正确，
    起止日期必须为同一年
    起止日期的开始日期必须为1日
    开始日期必须小于截至日期

    :param start_time:
    :param end_time:
    :return:
    '''
    if not isinstance(start_time, datetime.datetime):
        raise Exception("起始日期类型错误")
    if not isinstance(end_time, datetime.datetime):
        raise Exception("截至日期类型错误")

    start_year = start_time.year
    end_year = end_time.year
    if start_time.day != 1:
        raise Exception('开始时间必须为某月的第一天')
    if start_year != end_year:
        raise Exception("开始时间和结束时间不在一个年度")
    start_month = start_time.month
    end_month = end_time.month
    if start_month > end_month:
        raise Exception("开始时间大于截止日期")

def get_session_and_engine():
    # 创建session
    engine = create_engine('sqlite:///audit.sqlite?check_same_thread=False')
    Base.metadata.bind = engine
    DBSession = sessionmaker(bind=engine)
    session = DBSession()
    return session,engine

if __name__ == '__main__':
    str_to_float(np.nan)
