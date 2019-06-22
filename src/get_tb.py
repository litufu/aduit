# -*- coding:utf-8 -*-

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.database import Base, Suggestion
import pandas as pd
import math
from datetime import datetime

from src.utils import gen_df_line

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
                                                obj['year'], obj['month'], obj['vocher_type'], obj['vocher_num']))
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


def recaculate_km(df_km, df_xsz):
    '''
    :param df_km: 科目余额表
    :param df_xsz: 序时账
    :return: 新的科目余额表
    '''
    # 获取科目余额表期初数
    df_km_new = df_km.copy()
    df_km_new = df_km_new[
        ["id", "company_code", "company_name", "start_time", "end_time", "subject_num", "subject_name",
         "subject_type", "direction", "is_specific", "subject_gradation", "initial_amount"
         ]
    ]

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
        if df_km_new.loc[subject_num, "direction"] == "借":
            df_km_new.loc[subject_num, "terminal_amount"] = df_km_new.loc[
                                                                subject_num, "initial_amount"] + debit - credit
        elif df_km_new.loc[subject_num, "direction"] == "贷":
            df_km_new.loc[subject_num, "terminal_amount"] = df_km_new.loc[
                                                                subject_num, "initial_amount"] - debit + credit
    df_km_new = df_km_new.reset_index()
    return df_km_new


def get_subject_num_by_name(subject_name, df_km):
    '''
    根据科目名称获取科目编码，如果没有找到返回None
    :param subject_name:
    :param df_km:
    :return:
    '''
    df_km_subject = df_km[df_km["subject_name"] == subject_name]
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


def get_bad_debt(df_km):
    '''
    bad debts in accounts receivable
    获取应收账款坏账准备和其他应收款坏账准备期末数
    :return: (应收账款坏账准备，其他应收款坏账准备)
    '''

    # 获取坏账准备期末金额，如果等于0，则应收账款坏账准备和其他应收款坏账准备都为0
    bad_debt = get_subject_value_by_name("坏账准备", df_km, "terminal_amount")
    if math.isclose(bad_debt, 0.00, rel_tol=1e-5):
        return 0.00, 0.00
    else:
        #     获取坏账准备明细科目，分别分析应收账款坏账准备和其他应收款坏账准备
        df_km_bad_debt = get_detail_subject_df("坏账准备", df_km)
        if len(df_km_bad_debt) > 0:
            # 应收账款坏账准备
            df_km_bad_debt_ar = df_km_bad_debt[(df_km_bad_debt["subject_name"].str.contains("应收")) & (
                ~df_km_bad_debt["subject_name"].str.contains("其他"))]
            # 其他应收款坏账准备
            df_km_bad_debt_or = df_km_bad_debt[df_km_bad_debt["subject_name"].str.contains("其他应收")]
            if len(df_km_bad_debt_ar) == 1:
                bad_debt_ar = df_km_bad_debt_ar["terminal_amount"].values[0]
            else:
                bad_debt_ar = 0.00
            if len(df_km_bad_debt_or) == 1:
                bad_debt_or = df_km_bad_debt_or["terminal_amount"].values[0]
            else:
                bad_debt_or = 0.00
            if bad_debt_ar + bad_debt_or == bad_debt:
                return bad_debt_ar, bad_debt_or
            else:
                return bad_debt - bad_debt_or, bad_debt_or
        else:
            suggestion = Suggestion(kind="会计处理", content="建议在坏账准备科目下设“应收账款坏账准备”和“其他应收款坏账准备”两个科目")
            session.add(suggestion)
            session.commit()
            return bad_debt, 0.00


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


def get_reserve_fund_provision(df_km):
    '''
    获取本期计提的盈余公积
    :param df_km:科目余额表
    :return:本期计提的各盈余公积明细
    '''
    # 法定盈余公积
    legal_reserve = 0.00
    # 任意盈余公积
    discretionary_surplus_reserve = 0.00
    # 法定公益金
    legal_public_welfare_fund = 0.00
    # 储备基金
    reserve_fund = 0.00
    # 企业发展基金
    enterprise_development_fund = 0.00
    # 利润归还投资
    profit_return_investment = 0.00

    df_reserve_fund = df_km[df_km["subject_name"] == "盈余公积"]
    # 如果公司没有设置盈余公积科目
    if len(df_reserve_fund) == 0:
        return legal_reserve, discretionary_surplus_reserve, legal_public_welfare_fund, reserve_fund, enterprise_development_fund, profit_return_investment

    df_reserve_fund_detail = get_detail_subject_df("盈余公积", df_km)
    # 如果设置了盈余公积，检查有没有明细科目
    if len(df_reserve_fund_detail) == 0:
        # 如果盈余公积没有明细核算，检查利润分配是否有明细核算，根据利润分配来确认各个明细
        if df_reserve_fund["terminal_amount"].values[0] > 0.00:
            suggestion = Suggestion(kind="会计处理",
                                    content="盈余公积账户下应当分别设“法定盈余公积”“任意盈余公积”“法定公益金”“储备基金”“企业发展基金”和“利润归还投资”等进行明细核算")
            session.add(suggestion)
            session.commit()
        # 通过利润分配确认盈余公积明细表
        df_profit_distribution_detail = get_detail_subject_df("利润分配", df_km)
        if len(df_profit_distribution_detail) == 0:
            legal_reserve = df_reserve_fund["credit_amount"].values[0]
            return legal_reserve, discretionary_surplus_reserve, legal_public_welfare_fund, reserve_fund, enterprise_development_fund, profit_return_investment
        else:
            for obj in gen_df_line(df_profit_distribution_detail):
                if "法定盈余公积" in obj["subject_name"]:
                    legal_reserve = obj["debit_amount"]
                elif "任意盈余公积" in obj["subject_name"]:
                    discretionary_surplus_reserve = obj["subject_name"]
                elif "法定公益金" in obj["subject_name"]:
                    legal_public_welfare_fund = obj["debit_amount"]
                elif "储备基金" in obj["subject_name"]:
                    reserve_fund = obj["debit_amount"]
                elif "企业发展基金" in obj["subject_name"]:
                    enterprise_development_fund = obj["debit_amount"]
                elif "利润归还投资" in obj["subject_name"]:
                    profit_return_investment = obj["debit_amount"]
    else:
        for obj in gen_df_line(df_reserve_fund_detail):
            if "法定盈余公积" in obj["subject_name"]:
                legal_reserve = obj["credit_amount"]
            elif "任意盈余公积" in obj["subject_name"]:
                discretionary_surplus_reserve = obj["credit_amount"]
            elif "法定公益金" in obj["subject_name"]:
                legal_public_welfare_fund = obj["credit_amount"]
            elif "储备基金" in obj["subject_name"]:
                reserve_fund = obj["credit_amount"]
            elif "企业发展基金" in obj["subject_name"]:
                enterprise_development_fund = obj["credit_amount"]
            elif "利润归还投资" in obj["subject_name"]:
                profit_return_investment = obj["credit_amount"]
    return legal_reserve, discretionary_surplus_reserve, legal_public_welfare_fund, reserve_fund, enterprise_development_fund, profit_return_investment


def get_profit_distribution(df_km, df_xsz):
    '''
    分析利润分配科目
    :param df_km:
    :param df_xsz:
    :return:
    '''
    # 转作资本（或股本）的普通股股利
    convert_to_capital = 0.00
    # 优先股股利
    preferred_dividend = 0.00
    # 普通股股利
    dividend = 0.00
    # 年初未分配利润调整
    adjustment_profit = 0.00
    # 盈余公积
    legal_reserve, discretionary_surplus_reserve, legal_public_welfare_fund, reserve_fund, enterprise_development_fund, profit_return_investment = get_reserve_fund_provision(
        df_km)

    #     检查利润分配是否有明细科目
    df_profit_distribution_detail = get_detail_subject_df("利润分配", df_km)
    # 利润分配没有设置明细科目
    if len(df_profit_distribution_detail) == 0:
        suggestion = Suggestion(kind="会计处理",
                                content="利润分配账户下应当分别设“提取法定盈余公积”“提取任意盈余公积”“应付现金股利(或利润)”“转作股本的股利”“盈余公积补亏”和“未分配利润”等进行明细核算")
        session.add(suggestion)
        session.commit()
    #   分析序时账分别填列
    # 获取利润分配的所有明细账
    # 获取利润分配的科目编码
    subject_num_profit = get_subject_num_by_name("利润分配", df_km)
    # 获取利润分配的所有凭证包含借贷方
    if subject_num_profit:
        df_profit_xsz = get_xsz_by_subject_num(df_xsz, grade=1, subject_num=subject_num_profit)

        # 检查利润分配-提取盈余公积
        # 获取利润分配凭证中的提取盈余公积的凭证
        subject_num_reserve = get_subject_num_by_name("盈余公积", df_km)
        df_xsz_reserve = pd.DataFrame()
        if subject_num_reserve:
            df_xsz_reserve = get_xsz_by_subject_num(df_profit_xsz, grade=1, subject_num=subject_num_reserve)
            # 检查序时账提取盈余公积与科目余额表是否一致
            df_profit_xsz_reserve = df_xsz_reserve[df_xsz_reserve["subject_num_1"] == subject_num_profit]
            if (df_profit_xsz_reserve["debit"].sum() - df_profit_xsz_reserve["credit"].sum()) != (legal_reserve+ discretionary_surplus_reserve
                                                                                             + legal_public_welfare_fund + reserve_fund + enterprise_development_fund + profit_return_investment):
                raise Exception("利润分配提取盈余公积与科目余额表计算盈余公积提取不一致")


        # 检查利润分配-转为资本
        subject_num_paid_up_capital = get_subject_num_by_name("实收资本", df_km)
        subject_num_equity = get_subject_num_by_name("股本", df_km)
        subject_num_captial_reserve= get_subject_num_by_name("资本公积", df_km)
        subject_num_capital = []
        for item in [subject_num_paid_up_capital,subject_num_equity,subject_num_captial_reserve]:
            if item:
                subject_num_capital.append(item)
        df_xsz_capital = get_xsz_by_subject_num(df_profit_xsz, grade=1, subject_num=subject_num_capital)
        df_profit_xsz_capital = df_xsz_capital[df_xsz_capital["subject_name_1"]=="利润分配"]
        convert_to_capital = df_profit_xsz_capital["debit"].sum() - df_profit_xsz_capital["credit"].sum()

        # 分配股利
        subject_num_ividend_payable = get_subject_num_by_name("应付股利", df_km)
        subject_num_cash1 = get_subject_num_by_name("现金", df_km)
        subject_num_cash2 = get_subject_num_by_name("库存现金", df_km)
        subject_num_bank_deposit = get_subject_num_by_name("银行存款", df_km)
        subject_num_dividend = []
        for item in [subject_num_ividend_payable,subject_num_cash1,subject_num_cash2,subject_num_bank_deposit]:
            if item:
                subject_num_dividend.append(item)
        df_xsz_dividend = get_xsz_by_subject_num(df_profit_xsz, grade=1, subject_num=subject_num_dividend)
    #     优先股股利
        df_preferred_dividend = df_xsz_dividend[
            (df_xsz_dividend["subject_name_1"]=="利润分配") &
            (df_xsz_dividend["subject_name_2"].str.contains("优先股"))]
        preferred_dividend = df_preferred_dividend["debit"].sum()-df_preferred_dividend["credit"].sum()
    #     普通股股利
        df_profit_xsz_dividend = df_xsz_dividend[df_xsz_dividend["subject_name_1"] == "利润分配"]
        dividend = df_profit_xsz_dividend["debit"].sum() - df_profit_xsz_dividend["credit"].sum() - preferred_dividend

    #     以前年度损益调整
        subject_num_prior_year_income_adjustment = get_subject_num_by_name("以前年度损益调整", df_km)
        df_xsz_adjustment = pd.DataFrame()
        if subject_num_prior_year_income_adjustment:
            df_xsz_adjustment = get_xsz_by_subject_num(df_profit_xsz, grade=1, subject_num=subject_num_prior_year_income_adjustment)
            df_profit_xsz_adjustment = df_xsz_adjustment[df_xsz_adjustment["subject_name_1"]=="以前年度损益调整"]
            adjustment_profit = df_profit_xsz_adjustment["debit"].sum() - df_profit_xsz_adjustment["credit"].sum()

        # 本年利润
        subject_num_this_year_profit = get_subject_num_by_name("本年利润", df_km)
        df_xsz_this_year_profit = pd.DataFrame()
        if subject_num_this_year_profit:
            df_xsz_this_year_profit = get_xsz_by_subject_num(df_profit_xsz, grade=1,
                                                       subject_num=subject_num_this_year_profit)
        df_has_check = pd.concat([df_xsz_this_year_profit,df_xsz_adjustment,df_xsz_dividend,df_xsz_capital,df_xsz_reserve])
        df_has_check = df_has_check[["month", "vocher_num", "vocher_type"]].drop_duplicates()

        df_tmp = df_profit_xsz.copy()
        for obj in gen_df_line(df_has_check):
            df_tmp = df_tmp[~((df_tmp["month"]==obj["month"]) &(df_tmp["vocher_num"]==obj["vocher_num"]) &(df_tmp["vocher_type"]==obj["vocher_type"]))]

        if len(df_tmp)>0:
            raise Exception("还有未识别的利润分配，查看一下吧{}".format(df_tmp))
    else:
        raise Exception("公司没有设置利润分配科目")


    return convert_to_capital,preferred_dividend,dividend,adjustment_profit,legal_reserve, discretionary_surplus_reserve, legal_public_welfare_fund, reserve_fund, enterprise_development_fund, profit_return_investment

def get_tb(df_km,df_xsz):
    # 标准科目对照表和标准tb科目表

    df_subject_contrast = pd.read_sql_table('subjectcontrast', engine)
    df_tb_subject = pd.read_sql_table('tbsubject', engine)
    df_tb_subject['amount'] = 0.00
    df_tb = df_tb_subject.set_index("subject")

    # 获取坏账准备
    bad_debt_ar, bad_debt_or = get_bad_debt(df_km)
    # 获取利润分配项目
    convert_to_capital, preferred_dividend, dividend, adjustment_profit, legal_reserve, discretionary_surplus_reserve, legal_public_welfare_fund, reserve_fund, enterprise_development_fund, profit_return_investment = get_profit_distribution(df_km,df_xsz)
    subject_match = {
        "坏账准备--应收账款":bad_debt_ar,
        "坏账准备--其他应收款":bad_debt_or,
        "利润分配--转作资本的普通股股利":convert_to_capital,
        "利润分配--应付优先股股利":preferred_dividend,
        "利润分配--应付普通股股利":dividend,
        "利润分配--提取法定盈余公积":legal_reserve,
        "利润分配--提取任意盈余公积":discretionary_surplus_reserve,
        "利润分配--提取法定公益金":legal_public_welfare_fund,
        "利润分配--提取储备基金":reserve_fund,
        "利润分配--提取企业发展基金":enterprise_development_fund,
        "利润分配--利润归还投资":profit_return_investment
    }
    # 获取一级科目余额表，用于检查是否存在未识别的一级科目
    df_km_first_subject = df_km[df_km['subject_gradation'] == 1]
    df_km_first_subject_not_match = df_km_first_subject[
        ~df_km_first_subject['subject_name'].isin(df_subject_contrast['origin_subject'])]
    df_km_first_subject_not_match = df_km_first_subject_not_match.set_index("subject_name")

    df_km_first_subject_match = pd.merge(df_km_first_subject, df_subject_contrast, left_on="subject_name",
                                         right_on="origin_subject", how="inner")
    df_km_first_subject_match = df_km_first_subject_match.set_index('tb_subject')
    # 遍历TB项目
    for subject in df_tb.index:
        # 如果subject为空则continue
        if not subject:
            continue
        # 如果tb项目在科目余额表中，直接取数
        elif subject in df_km_first_subject_match.index:
            # 如果是资产负债表项目，并且科目余额表方向和TB科目方向一致，则直接取期末数
            subject_num = df_km_first_subject_match.at[subject, 'subject_num']
            if not subject_num.startswith('6'):
                # 判断科目方向是否一致
                if df_km_first_subject_match.at[subject, 'direction_x'] == df_km_first_subject_match.at[
                    subject, 'direction_y']:
                    df_tb.at[subject, "amount"] = df_km_first_subject_match.at[subject, 'terminal_amount']
                else:
                    df_tb.at[subject, "amount"] = -df_km_first_subject_match.at[subject, 'terminal_amount']
            else:
                # 如果是利润表项目，则取发生额
                df_tb.at[subject, "amount"] = df_km_first_subject_match.at[subject, 'debit_amount']
        elif subject in subject_match:
            df_tb.at[subject, "amount"] = subject_match[subject]
        elif subject == "年初未分配利润":
            df_tb.at[subject, "amount"] = df_km_first_subject_not_match.at["利润分配", 'initial_amount'] + \
                                          df_km_first_subject_not_match.at["本年利润", 'initial_amount'] + adjustment_profit

    # 最后计算公式
    # 如果是公式的话，则按照公式进行匹配
    for subject in df_tb.index:
        if not subject:
            continue
        if subject.startswith("%"):
            formula = subject.split("%")
            formula_items = formula[1:len(formula) - 1]
            formula_str = ""
            for item in formula_items:
                if item in ["+", "-", "*", "/"]:
                    formula_str = formula_str + item
                else:
                    value = df_tb.at[item, "amount"]
                    formula_str = formula_str + "{}".format(value)
            df_tb.at[subject, "amount"] = eval(formula_str)
    # 检查TB是否已经平了
    total_assets = df_tb[df_tb["show"]==" 资产总计"]["amount"].values[0]
    liabilities_and_shareholders_equity = df_tb[df_tb["show"]==" 负债和股东权益总计"]["amount"].values[0]
    if math.isclose(total_assets,liabilities_and_shareholders_equity,rel_tol=1e-5):
        return df_tb
    else:
        raise Exception("试算平衡表不平，请重新检查")

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
    df_km_new = recaculate_km(df_km, df_xsz_new)
    # 根据新的科目余额表计算tb
    get_tb(df_km_new,df_xsz_new)


if __name__ == '__main__':
    recalculation("深圳市众恒世讯科技股份有限公司", start_time="2016-1-1", end_time="2016-12-31")
