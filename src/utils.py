import pandas as pd
import re
import numpy as np
import json
from datetime import datetime,date
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
    if not isinstance(start_time, datetime):
        raise Exception("起始日期类型错误")
    if not isinstance(end_time, datetime):
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

def add_suggestion(kind,content,start_time,end_time,company_name):
    session,engine = get_session_and_engine()
    suggestions = session.query(Suggestion).filter(Suggestion.company_name==company_name,
                                                   Suggestion.start_time==start_time,
                                                   Suggestion.end_time == end_time,
                                                   Suggestion.kind ==kind,
                                                   Suggestion.content==content
                                                   ).all()
    if len(suggestions)>0:
        return
    suggestion = Suggestion(company_name=company_name,start_time=start_time,end_time=end_time,kind=kind, content=content)
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

def get_subject_value_by_num(subject_num, df_km, value_type):
    '''
    获取科目金额包括期初/借方/贷方/期末,如果没有找到返回0.00
    :param subject_name:
    :param df_km:
    :param value_type:
    :return:
    '''
    if not (value_type in ["initial_amount", "debit_amount", "credit_amount", "terminal_amount"]):
        raise Exception("value_type必须为initial_amount/debit_amount/credit_amount/terminal_amount之一")
    df_km_subject = df_km[df_km["subject_num"] == subject_num]
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

def get_not_null_df_km(df_km,grade):
    '''
    获取非空的科目余额表
    :param df_km: 科目余额表
    :param grade: 科目等级
    :return: 返回期初数/借方/贷方/期末至少有一个不为0的科目余额表
    '''
    df_km_not_null = df_km[
        (df_km['subject_gradation'] == grade) &
        (
                (df_km['initial_amount'].abs() > 1e-5) |
                (df_km['debit_amount'].abs() > 1e-5) |
                (df_km['credit_amount'].abs() > 1e-5) |
                (df_km['terminal_amount'].abs() > 1e-5)
        )
        ]
    return df_km_not_null


class CJsonEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d')
        elif isinstance(obj, date):
            return obj.strftime('%Y-%m-%d')
        else:
            return json.JSONEncoder.default(self, obj)

def get_tb_origin_value(origin_text,df_km_new,df_xsz_new,):
    if origin_text:
        origins = json.loads(origin_text)
        sum_value = 0.00
        for origin in origins:
            if origin["table_name"] == "km":
                subject_num = origin["values"]["location"]["subject_num"]
                value_type = origin["values"]["value_type"]
                if origin["values"]["sign"] == "+":
                    value = get_subject_value_by_num(subject_num,df_km_new,value_type)
                else:
                    value = -get_subject_value_by_num(subject_num,df_km_new,value_type)
            elif origin["table_name"] == "xsz":
                month = origin["values"]["location"]["month"]
                vocher_type = origin["values"]["location"]["vocher_type"]
                vocher_num = origin["values"]["location"]["vocher_num"]
                subentry_num = origin["values"]["location"]["subentry_num"]
                value_type = origin["values"]["value_type"]
                origin_value = df_xsz_new[(df_xsz_new["month"]==month) &
                           (df_xsz_new["vocher_type"]==vocher_type) &
                           (df_xsz_new["vocher_num"] == vocher_num) &
                           (df_xsz_new["subentry_num"] == subentry_num)
                ][value_type].values[0]
                if origin["values"]["sign"] == "+":
                    value = origin_value
                else:
                    value = - origin_value
            sum_value = sum_value + value
        return sum_value
    return 0.00


def parse_auxiliary(auxiliary_str):
    '''

    :param auxiliary_str: 辅助核算字符串 "【供应商:惠州乐庭电子线缆有限公司 借 9699.00】"
    :return: dict {'供应商': '惠州乐庭电子线缆有限公司'}
    '''
    if auxiliary_str:
        res = {}
        res0 = auxiliary_str.split("【")
        pattern = re.compile('\s+[借贷].*?】')
        for i in res0:
            if i == "":
                pass
            else:
                res1 = re.sub(pattern,"",i.strip())
                res2 = res1.split(":")
                res[res2[0]] = res2[1]
        return res






if __name__ == '__main__':
    # str_to_float(np.nan)

    str = "【供应商:惠州乐庭电子线缆有限公司 借 9699.00】"
    parse_auxiliary(str)