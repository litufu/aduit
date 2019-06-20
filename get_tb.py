# -*- coding:utf-8 -*-

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from database import Base, FSSubject, FirstClassSubject, AccountingFirm, SubjectContrast, TBSubject, Suggestion
import pandas as pd
import json
import numpy as np
from datetime import datetime

from utils import gen_df_line

engine = create_engine('sqlite:///audit.sqlite?check_same_thread=False')
Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)
session = DBSession()


# 通过科目余额表和序时账生成TB
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

def append_all_gradation_subjects(df_km, df_xsz):
    '''
    为序时账添加所有级别的会计科目编码和名称
    :param df_km: 科目余额表
    :param df_xsz: 序时账
    :return: 添加过所有级别会计科目编码和名称的序时账
    '''
    # 添加科目编码长度列
    df_km['subject_length'] = df_km['subject_num'].str.len()
    df_km_subject = df_km[['subject_num', 'subject_name']]
    df_km_subject = df_km_subject.rename({'subject_num': 'std_subject_num'}, axis='columns')
    # 获取各科目级次和长度
    df_km_gradation = df_km.drop_duplicates('subject_gradation', keep='first')
    df_km_gradation = df_km_gradation[['subject_gradation', 'subject_length']]
    # 给序时账添加所有的科目级别编码和科目名称
    gradation_subject_length = dict(zip(df_km_gradation['subject_gradation'], df_km_gradation['subject_length']))
    for key in gradation_subject_length:
        subject_num_length = gradation_subject_length[key]
        subject_num_name = 'subject_num_{}'.format(key)
        df_xsz[subject_num_name] = df_xsz['subject_num'].str.slice(0, subject_num_length)
        df_xsz = pd.merge(df_xsz, df_km_subject, how='left', left_on=subject_num_name, right_on="std_subject_num",
                          suffixes=('', '_{}'.format(key)))
        df_xsz = df_xsz.drop(columns=['std_subject_num'])
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

def check_profit_subject_dirction(df_km, df_xsz):
    '''
    检查序时账中的损益科目是否核算方向正确，不正确的调整方向
    :param df_km: 科目余额表
    :param df_xsz: 待修改的序时账
    :return: 返回调整后的序时账
    '''
    # 检查是否所有的损益类项目的核算都正确
    # 第一步：获取所有损益类核算项目
    # 第二部：获取所有损益类核算项目的标准方向
    # 第三部：检查是否存在相反方向核算且不属于损益结转的情况
    # 第四步：将核算方向相反的凭证予以调整
    df_xsz_new = df_xsz.copy().set_index(['month', 'vocher_type', 'vocher_num', 'subentry_num'])
    # 第一步：获取所有损益类核算项目
    print(df_xsz.keys())
    df_km_first_subject = df_km[df_km['subject_gradation'] == 1]
    # 获取所有开头未6的损益类一级会计科目
    df_km_first_subject_profit = df_km_first_subject[df_km_first_subject["subject_num"].str.startswith('6')]
    df_km_first_subject_profit = df_km_first_subject_profit[['subject_num', 'subject_name']]
    # 第二部：获取所有损益类核算项目的标准方向
    df_std = pd.read_sql_table('subjectcontrast', engine)
    df_km_first_subject_profit_direction = pd.merge(df_km_first_subject_profit, df_std, how="inner",
                                                    left_on='subject_name',
                                                    right_on="origin_subject")
    df_km_first_subject_profit_direction = df_km_first_subject_profit_direction[
        ["subject_num", "subject_name", "direction"]]
    # 所有的损益类凭证
    df_xsz_profit = df_xsz[df_xsz['subject_name_1'].isin(df_km_first_subject_profit_direction['subject_name'])]
    # 合并损益类凭证列表和损益类核算项目的标准方向
    df_xsz_profit = pd.merge(df_xsz_profit, df_km_first_subject_profit_direction, how="left", left_on="subject_name_1",
                             right_on="subject_name")
    # 获取核算相反的凭证
    df_xsz_profit_reverse = df_xsz_profit[((df_xsz_profit["direction"] == "借") & (df_xsz_profit["credit"] > 0.00)) |
                                          ((df_xsz_profit["direction"] == "贷") & (df_xsz_profit["debit"] > 0.00))]
    df_xsz_profit_reverse_record = df_xsz_profit_reverse[["month", "vocher_num", "vocher_type"]].drop_duplicates()
    # 获取相反凭证的记录
    reverse_records = df_xsz_profit_reverse_record.to_dict('records')
    # 获取相反凭证的完整借贷方
    df_xsz_profit_reverse = pd.merge(df_xsz, df_xsz_profit_reverse_record, how="inner",
                                     on=["month", "vocher_num", "vocher_type"])
    # 第三部：检查是否存在相反方向核算且不属于损益结转的情况

    # 检查每一笔相反凭证中是否包含本年利润，如果包含则是正常的结转损益，不用理会
    for record in reverse_records:
        df_tmp = df_xsz_profit_reverse[(df_xsz_profit_reverse["month"] == record["month"])
                                       & (df_xsz_profit_reverse["vocher_num"] == record["vocher_num"])
                                       & (df_xsz_profit_reverse["vocher_type"] == record["vocher_type"])
                                       ]
        # 检查凭证中是否包含本年利润，如果不包含则调整序时账
        if not df_tmp["subject_name_1"].str.contains('本年利润', regex=False).any():
            # 第四部：修改序时账
            # 合并标准方向
            df_tmp = pd.merge(df_tmp, df_km_first_subject_profit_direction, left_on="subject_num_1",
                              right_on="subject_num", how="left", suffixes=("", "_y"))
            for obj in gen_df_line(df_tmp):
                if obj['direction'] == "借" and obj['credit'] > 0:
                    # 借贷方金额互换
                    tmp = df_xsz_new.loc[
                        (obj['month'], obj['vocher_type'], obj['vocher_num'], obj['subentry_num']), 'credit']
                    df_xsz_new.loc[
                        (obj['month'], obj['vocher_type'], obj['vocher_num'], obj['subentry_num']), 'debit'] = -tmp
                    df_xsz_new.loc[
                        (obj['month'], obj['vocher_type'], obj['vocher_num'], obj['subentry_num']), 'credit'] = 0.00

                    tmp1 = df_xsz_new.loc[
                        (obj['month'], obj['vocher_type'], obj['vocher_num'],
                         obj['subentry_num']), 'credit_foreign_currency']
                    df_xsz_new.loc[
                        (obj['month'], obj['vocher_type'], obj['vocher_num'],
                         obj['subentry_num']), 'debit_foreign_currency'] = -tmp1
                    df_xsz_new.loc[
                        (obj['month'], obj['vocher_type'], obj['vocher_num'],
                         obj['subentry_num']), 'credit_foreign_currency'] = 0.00
                    #    提建议
                    suggestion = Suggestion(kind="会计处理",
                                            content="{}年{}月{}-{}号凭证损益类项目记账不符合规范，建议收入类科目发生时计入贷方，费用类项目发生时计入借方".format(
                                                obj['year'],obj['month'],obj['vocher_type'],obj['vocher_num']))
                    session.add(suggestion)
                    session.commit()
                elif obj['direction'] == "贷" and obj['debit'] > 0:
                    # 借贷方金额互换
                    tmp = df_xsz_new.loc[
                        (obj['month'], obj['vocher_type'], obj['vocher_num'], obj['subentry_num']), 'debit']
                    df_xsz_new.loc[
                        (obj['month'], obj['vocher_type'], obj['vocher_num'], obj['subentry_num']), 'credit'] = -tmp
                    df_xsz_new.loc[
                        (obj['month'], obj['vocher_type'], obj['vocher_num'], obj['subentry_num']), 'debit'] = 0.00
                    tmp1 = df_xsz_new.loc[
                        (obj['month'], obj['vocher_type'], obj['vocher_num'],
                         obj['subentry_num']), 'debit_foreign_currency']
                    df_xsz_new.loc[
                        (obj['month'], obj['vocher_type'], obj['vocher_num'],
                         obj['subentry_num']), 'credit_foreign_currency'] = -tmp1
                    df_xsz_new.loc[
                        (obj['month'], obj['vocher_type'], obj['vocher_num'],
                         obj['subentry_num']), 'debit_foreign_currency'] = 0.00
                    #    提建议
                    suggestion = Suggestion(kind="会计处理",
                                            content="{}年{}月{}-{}号凭证损益类项目记账不符合规范，建议收入类科目发生时计入贷方，费用类项目发生时计入借方".format(
                                                obj['year'], obj['month'], obj['vocher_type'], obj['vocher_num']))
                    session.add(suggestion)
                    session.commit()
    df_xsz_new = df_xsz_new.reset_index()
    return df_xsz_new

def recaculate_km(df_km,df_xsz):
    # 获取科目余额表期初数
    df_km_new = df_km.copy()
    df_km_new = df_km_new[["id","company_code","company_name","start_time","end_time","subject_num","subject_name",
    "subject_type","direction","is_specific","subject_gradation","initial_amount"
    ]]
    df_km_new['debit_amount'] = 0.00
    df_km_new['credit_amount'] = 0.00
    df_km_new['terminal_amount'] = 0.00
    df_km_new = df_km_new.set_index('subject_num')
    # 计算序时账发生额
    df_xsz_pivot = df_xsz.pivot_table(values=['debit', 'credit'], index='subject_num', aggfunc='sum')
    # 重新计算科目余额表
    for i in range(len(df_km_new)):
        subject_num = df_km_new.index[i]
        df_xsz_pivot_tmp = df_xsz_pivot.loc[df_xsz_pivot.index.str.startswith(subject_num)]
        debit = df_xsz_pivot_tmp['debit'].sum()
        credit = df_xsz_pivot_tmp['credit'].sum()
        df_km_new.loc[subject_num, "debit_amount"] = debit
        df_km_new.loc[subject_num, "credit_amount"] = credit
        if df_km_new.loc[subject_num,"direction"] == "借":
            df_km_new.loc[subject_num, "terminal_amount"] = df_km_new.loc[subject_num, "initial_amount"] + debit - credit
        elif df_km_new.loc[subject_num,"direction"] == "贷":
            df_km_new.loc[subject_num, "terminal_amount"] = df_km_new.loc[
                                                                subject_num, "initial_amount"] - debit + credit
    return df_km_new





def recalculation(company_name, start_time, end_time):
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
    if len(subject_names) > 0:
        print(subject_names)
        raise Exception("含有未识别的一级会计科目")
    # # 获取序时账
    df_xsz = pd.read_sql_table('chronologicalaccount', engine)
    df_xsz = df_xsz[(df_xsz['year'] == year) & (df_xsz['month'] >= start_month) & (df_xsz['month'] <= end_month) & (
            df_xsz['company_name'] == company_name)]
    # 为序时账添加所有级别的会计科目编码和名称
    df_xsz = append_all_gradation_subjects(df_km, df_xsz)
    # 检查是否所有的损益类项目的核算都正确,不正确则修改序时账
    df_xsz_new = check_profit_subject_dirction(df_km, df_xsz)
    # 根据序时账重新计算科目余额表
    df_km_new = recaculate_km(df_km,df_xsz_new)
    # 根据新的科目余额表计算tb



if __name__ == '__main__':
    recalculation("深圳市众恒世讯科技股份有限公司", start_time="2016-1-1", end_time="2016-12-31")
