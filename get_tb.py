# -*- coding:utf-8 -*-

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from database import Base,FSSubject,FirstClassSubject,AccountingFirm,SubjectContrast,TBSubject
import pandas as pd
import numpy as np
from datetime import datetime

engine = create_engine('sqlite:///audit.sqlite?check_same_thread=False')
Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)
session = DBSession()

#通过科目余额表和序时账生成TB
# 步骤：
# 1/通过科目余额表期初数和序时账重新计算期末数和损益发生额
# 2/将重新计算生成的科目余额表按照科目与TB对照表生成TB

def check_start_end_date(start_time, end_time):
    start_year = start_time.year
    end_year = end_time.year
    if start_time.day != 1:
        raise Exception('开始时间必须为某月的第一天')
    if start_year != end_year:
        raise Exception("开始时间和结束时间不在一个年度")
    start_month = start_time.month
    end_month = end_time.month
    if start_month > end_month:
        raise Exception("开始时间日期截止日期")

def append_all_gradation_subjects(df_km,df_xsz):
    '''
    为序时账添加所有级别的会计科目编码和名称
    :param df_km: 科目余额表
    :param df_xsz: 序时账
    :return: 添加过所有级别会计科目编码和名称的序时账
    '''
    # 添加科目编码长度列
    df_km['subject_length'] = df_km['subject_num'].str.len()
    df_km_subject = df_km[['subject_num', 'subject_name']]
    # 获取各科目级次和长度
    df_km_gradation = df_km.drop_duplicates('subject_gradation', keep='first')
    df_km_gradation = df_km_gradation[['subject_gradation', 'subject_length']]
    # 给序时账添加所有的科目级别编码和科目名称
    gradation_subject_length = dict(zip(df_km_gradation['subject_gradation'], df_km_gradation['subject_length']))
    for key in gradation_subject_length:
        subject_num_length = gradation_subject_length[key]
        subject_num_name = 'subject_num_{}'.format(key)
        df_xsz[subject_num_name] = df_xsz['subject_num'].str.slice(0, subject_num_length)
        df_xsz = pd.merge(df_xsz, df_km_subject, how='left', left_on=subject_num_name, right_on="subject_num",
                          suffixes=('', '_{}'.format(key)))
    return df_xsz

def get_not_exist_subject_names(df_km):
    '''
    通过比较科目余额表中的一级科目和标准会计科目对照表，检查是否存在未识别的一级会计科目
    :param df_km: 科目余额表
    :return: 所有未在标准科目余额表=》tb=>报表中识别的一级会计科目，返回一个列表
    '''
    std_not_match_subject = {"坏账准备", "本年利润", "利润分配", "以前年度损益调整"}
    # 获取一级科目余额表，用于检查是否存在未识别的一级科目
    df_km_first_subject = df_km[df_km['subject_gradation'] == 1]
    df_km_first_subject = df_km_first_subject[['subject_num', 'subject_name']]
    # 检查是否有未识别的一级会计科目
    df_std = pd.read_sql_table('subjectcontrast', engine)
    df_km_first_subject = pd.merge(df_km_first_subject, df_std, how="left", left_on='subject_name',
                                   right_on="origin_subject")
    df_km_first_subject_not_match = df_km_first_subject[df_km_first_subject['coefficient'].isna()]
    diff_subject_names = set(df_km_first_subject_not_match['subject_name'])
    res_diff_subject_names = diff_subject_names - std_not_match_subject
    return list(res_diff_subject_names)

def check_profit_subject_dirction(df_km,df_xsz):
    # 检查是否所有的损益类项目的核算都正确
    # 第一步：获取所有损益类核算项目
    # 第二部：获取所有损益类核算项目的标准方向
    # 第三部：检查是否存在相反方向核算且不属于损益结转的情况
    # 第四步：将核算方向相反的凭证予以调整
    # 第一步：获取所有损益类核算项目
    df_km_first_subject = df_km[df_km['subject_gradation'] == 1]
    # 获取所有开头未6的损益类一级会计科目
    df_km_first_subject_profit = df_km_first_subject[df_km_first_subject["subject_num"].str.startswith('6')]
    df_km_first_subject_profit = df_km_first_subject_profit[['subject_num', 'subject_name']]
    # 第二部：获取所有损益类核算项目的标准方向
    df_std = pd.read_sql_table('subjectcontrast', engine)
    df_km_first_subject_profit_direction = pd.merge(df_km_first_subject_profit, df_std, how="inner", left_on='subject_name',
                                   right_on="origin_subject")
    df_km_first_subject_profit_direction = df_km_first_subject_profit_direction[["subject_num","subject_name","direction"]]
    # 所有的损益类凭证
    df_xsz_profit = df_xsz[df_xsz['subject_name_1'].isin(df_km_first_subject_profit_direction['subject_name'])]
    # 合并损益类凭证列表和损益类核算项目的标准方向
    df_xsz_profit = pd.merge(df_xsz_profit,df_km_first_subject_profit_direction,how="left",left_on="subject_name_1",right_on="subject_name")
    # 获取核算相反的凭证
    df_xsz_profit_reverse = df_xsz_profit[((df_xsz_profit["direction"]=="借") & (df_xsz_profit["credit"]>0.00))|
                                          ((df_xsz_profit["direction"] == "贷") & (df_xsz_profit["debit"] > 0.00))]
    df_xsz_profit_reverse = df_xsz_profit_reverse[["month","vocher_num","vocher_type"]].drop_duplicates()
    # 获取相反凭证的记录
    reverse_records = df_xsz_profit_reverse.to_dict('records')
    # 获取相反凭证的完整借贷方
    df_xsz_profit_reverse = pd.merge(df_xsz,df_xsz_profit_reverse,how="inner",on=["month","vocher_num","vocher_type"])
    # 第三部：检查是否存在相反方向核算且不属于损益结转的情况
    error_records = []
    # 检查每一笔相反凭证中是否包含本年利润，如果包含则是正常的结转损益，不用理会
    for record in reverse_records:
        df_tmp = df_xsz_profit_reverse[(df_xsz_profit_reverse["month"]==record["month"])
                                       &(df_xsz_profit_reverse["vocher_num"]==record["vocher_num"])
                                       &(df_xsz_profit_reverse["vocher_type"]==record["vocher_type"])
                                        ]
        if df_tmp["subject_name_1"].str.contains('本年利润', regex=False).all():
            error_records.append(record)
        else:
            print("包含本年利润")
    if len(error_records)>0:
        # 调整序时账
        pass

    else:
        print("没有错误的凭证")
        return df_xsz






def recalculation(company_name,start_time, end_time):
    start_time = datetime.strptime(start_time, '%Y-%m-%d')
    end_time = datetime.strptime(end_time, '%Y-%m-%d')
    check_start_end_date(start_time, end_time)
    year = start_time.year
    start_month = start_time.month
    end_month = end_time.month
    # 获取科目余额表
    df_km = pd.read_sql_table('subjectbalance', engine)
    df_km = df_km[
        (df_km['start_time'] == start_time) & (df_km['end_time'] == end_time) & (df_km['company_name'] == company_name)]
    # 检查是否存在未识别的一级会计科目
    subject_names = get_not_exist_subject_names(df_km)
    if len(subject_names)>0:
        print(subject_names)
        raise Exception("含有未识别的一级会计科目")
    # # 获取序时账
    df_xsz = pd.read_sql_table('chronologicalaccount', engine)
    df_xsz = df_xsz[(df_xsz['year'] == year) & (df_xsz['month'] >= start_month) & (df_xsz['month'] <= end_month) & (
            df_xsz['company_name'] == company_name)]
#     为序时账添加所有级别的会计科目编码和名称
    df_xsz = append_all_gradation_subjects(df_km,df_xsz)

    # 检查是否所有的损益类项目的核算都正确
    check_profit_subject_dirction(df_km,df_xsz)









if __name__ == '__main__':
    recalculation("深圳市众恒世讯科技股份有限公司",start_time="2016-1-1", end_time="2016-12-31")
