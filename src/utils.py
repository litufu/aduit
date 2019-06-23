import pandas as pd
import numpy as np
import json
import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.database import Base,Suggestion

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

def add_suggestion(kind,content):
    session,engine = get_session_and_engine()
    suggestion = Suggestion(kind=kind, content=content)
    session.add(suggestion)
    session.commit()

def get_subject_num_by_name(subject_name, df_km):
    '''
    根据科目名称获取科目编码，如果没有找到返回None
    :param subject_name:
    :param df_km:
    :return:subject_num
    '''
    df_km_subject = df_km[df_km["subject_name"] == subject_name]
    if len(df_km_subject) == 1:
        return df_km_subject["subject_num"].values[0]
    else:
        return None

def get_subject_num_by_similar_name(subject_name, df_km):
    '''
    根据科目名称获取科目编码，如果没有找到返回None
    :param subject_name:
    :param df_km:
    :return:subject_num
    '''
    df_km_subject = df_km[df_km["subject_name"].str.contains(subject_name)]
    if len(df_km_subject) == 1:
        return df_km_subject["subject_num"].values[0]
    else:
        return None

def get_subject_value_by_name(subject_name, df_km, value_type):
    '''
    获取科目金额包括期初/借方/贷方/期末,如果没有找到返回0.00
    :param subject_name:
    :param df_km:
    :param value_type:
    :return:
    '''
    if not (value_type in ["initial_amount", "debit_amount", "credit_amount", "terminal_amount"]):
        raise Exception("value_type必须为initial_amount/debit_amount/credit_amount/terminal_amount之一")
    df_km_subject = df_km[df_km["subject_name"] == subject_name]
    if len(df_km_subject) == 1:
        return df_km_subject[value_type].values[0]
    else:
        return 0.00

def get_detail_subject_df(subject_name, df_km):
    '''
    获取科目的所有下级科目，
    :param subject_name:
    :param df_km:
    :return:dataFrame
    '''
    subject_num = get_subject_num_by_name(subject_name, df_km)
    if not subject_num:
        return pd.DataFrame()
    df = df_km[(df_km["subject_num"].str.startswith(subject_num)) & (
            df_km["subject_num"] != subject_num)]
    return df.copy()

def get_xsz_by_subject_num(df_xsz, grade, subject_num):
    '''
    获取某个科目编码的所有凭证，包含借贷方
    :param subject_num:科目编码或科目编码列表
    :param grade:科目级别
    :param df_xsz:
    :return: df
    '''
    subject_num_grade = "subject_num_{}".format(grade)
    if  isinstance(subject_num,str) :
        df_suject_xsz = df_xsz[df_xsz[subject_num_grade] == subject_num]
    elif isinstance(subject_num,list):
        df_suject_xsz = df_xsz[df_xsz[subject_num_grade].isin(subject_num)]
    else:
        raise Exception("你必须输入科目编码")
    df_suject_xsz_record = df_suject_xsz[["month", "vocher_num", "vocher_type"]].drop_duplicates()
    # 获取完整借贷方凭证
    df_subject_xsz = pd.merge(df_xsz, df_suject_xsz_record, how="inner",
                              on=["month", "vocher_num", "vocher_type"])
    return df_subject_xsz

if __name__ == '__main__':
    str_to_float(np.nan)
