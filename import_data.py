# -*- coding:utf-8 -*-

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import math
from database import Base, Company, Auxiliary, SubjectBalance, ChronologicalAccount

# 创建session
engine = create_engine('sqlite:///audit.sqlite?check_same_thread=False')
Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)
session = DBSession()


def str_to_float(str1):
    if isinstance(str1, str):
        str1 = str1.replace(',', '')
        return float(str1)
    elif isinstance(str1, np.float64):
        return str1


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


def save_km(company_name, start_time, end_time, km_path):
    '''
    科目编号	科目名称	科目类别	借贷方向	是否明细科目	科目级次
    账面期初数	账面借方发生额	账面贷方发生额	账面期末数
    :param company_name:
    :param start_time:科目余额表开始时间，如20019-1-1
    :param end_time:科目余额表结束时间，如20019-12-31
    :return:
    '''
    # 获取公司信息
    company = session.query(Company).filter(Company.name == company_name).first()
    # 转换起止时间
    start_time = datetime.strptime(start_time, '%Y-%m-%d')
    end_time = datetime.strptime(end_time, '%Y-%m-%d')
    check_start_end_date(start_time, end_time)
    # 检查是否已经存储科目余额表
    kms = session.query(SubjectBalance).filter(SubjectBalance.company_name == company_name,
                                               SubjectBalance.start_time == start_time,
                                               SubjectBalance.end_time == end_time).all()
    if len(kms) > 0:
        choice = input('已经存在科目余额表，是否要替换')
        if choice == "yes":
            print('开始删除')
            for km in kms:
                session.delete(km)
            session.commit()
        else:
            return
    # 读取科目余额表
    print('开始存储')
    df = pd.read_excel(km_path, index_col=0)
    # 重新索引
    df = df.rename(index=str, columns={
        "科目编号": "subject_num",
        "科目名称": "subject_name",
        "科目类别": "subject_type",
        "借贷方向": "direction",
        "是否明细科目": "is_specific",
        "科目级次": "subject_gradation",
        "账面期初数": "initial_amount",
        "账面借方发生额": "debit_amount",
        "账面贷方发生额": "credit_amount",
        "账面期末数": "terminal_amount",
    })
    df = df[['subject_num', 'subject_name', 'subject_type', 'direction',
             'is_specific', 'subject_gradation', 'initial_amount', 'debit_amount',
             'credit_amount', 'terminal_amount'
             ]]
    # 存入数据库
    for i in range(len(df)):
        subject_num = str(df.iat[i, 0])
        subject_name = df.iat[i, 1]
        subject_type = df.iat[i, 2]
        direction = df.iat[i, 3]
        is_specific = df.iat[i, 4] == "是"
        subject_gradation = str(df.iat[i, 5])
        initial_amount = str_to_float(df.iat[i, 6])
        debit_amount = str_to_float(df.iat[i, 7])
        credit_amount = str_to_float(df.iat[i, 8])
        terminal_amount = str_to_float(df.iat[i, 9])
        # 判断科目余额表是否正确
        if subject_type in ["资产", "成本"]:
            if direction == "借":
                if not math.isclose(initial_amount + debit_amount - credit_amount, terminal_amount, rel_tol=1e-5):
                    print(initial_amount)
                    print(debit_amount)
                    print(credit_amount)
                    print(terminal_amount)
                    raise Exception("{}的期初+本期借方-本期贷方不等于期末数".format(subject_name))
            else:
                if not math.isclose(initial_amount - debit_amount + credit_amount, terminal_amount, rel_tol=1e-5):
                    raise Exception("{}的期初+本期借方-本期贷方不等于期末数".format(subject_name))
        else:
            if not math.isclose(initial_amount - debit_amount + credit_amount, terminal_amount, rel_tol=1e-5):
                raise Exception("{}的期初-本期借方+本期贷方不等于期末数".format(subject_name))

        km = SubjectBalance(company_code=company.code, company_name=company.name, start_time=start_time,
                            end_time=end_time,
                            subject_num=subject_num, subject_name=subject_name, subject_type=subject_type,
                            direction=direction, is_specific=is_specific, subject_gradation=subject_gradation,
                            initial_amount=initial_amount, debit_amount=debit_amount, credit_amount=credit_amount,
                            terminal_amount=terminal_amount
                            )
        session.add(km)
    session.commit()


def save_xsz(company_name, start_time, end_time, xsz_path):
    '''
    会计年	会计月	记账时间	凭证编号	凭证种类	编号	业务说明
    	科目编号	科目名称	借方发生额	贷方发生额	借方发生额_外币	贷方发生额_外币	借方数量	贷方数量	借方单价	贷方单价
    			货币种类		核算项目名称

    :param company_name:
    :param xsz_path:
    :return:
    '''
    # 获取公司信息
    company = session.query(Company).filter(Company.name == company_name).first()
    # 转换起止时间
    start_time = datetime.strptime(start_time, '%Y-%m-%d')
    end_time = datetime.strptime(end_time, '%Y-%m-%d')
    check_start_end_date(start_time, end_time)

    # 读取序时账
    df = pd.read_excel(xsz_path, index_col=0)
    # 重新索引
    df = df.rename(index=str, columns={
        "会计年": "year",
        "会计月": "month",
        "记账时间": "record_time",
        "凭证编号": "vocher_num",
        "凭证种类": "vocher_type",
        "编号": "subentry_num",
        "业务说明": "description",
        "科目编号": "subject_num",
        "科目名称": "subject_name",
        "借方发生额": "debit",
        "贷方发生额": "credit",
        "借方发生额_外币": "debit_foreign_currency",
        "贷方发生额_外币": "credit_foreign_currency",
        "借方数量": "debit_number",
        "贷方数量": "credit_number",
        "借方单价": "debit_price",
        "贷方单价": "credit_price",
        "货币种类": "currency_type",
        "核算项目名称": "auxiliary",
    })
    df = df[['year', 'month', 'record_time', 'vocher_num', 'vocher_type', 'subentry_num', 'description', 'subject_num',
             'subject_name', 'debit', 'credit', 'debit_foreign_currency', 'credit_foreign_currency', 'debit_number',
             'credit_number', 'debit_price', 'credit_price', 'currency_type', 'auxiliary'
             ]]

    # 检查是否已经存储序时账
    for year in df['year'].unique():
        xszs = session.query(ChronologicalAccount).filter(ChronologicalAccount.company_name == company_name,
                                                          ChronologicalAccount.year == year).all()
        if len(xszs) > 0:
            choice = input('{}年度已经存在序时账，是否要替换原有数据'.format(year))
            if choice == 'yes':
                for xsz in xszs:
                    session.delete(xsz)
                session.commit()
            else:
                return
    #     检查借贷方是否相等
    if not math.isclose(df['debit'].sum(), df['credit'].sum(), rel_tol=1e-5):
        raise Exception("{}序时账借方发生额和贷方发生额合计不一致".format(company_name))
    #     存储到数据库
    for i in range(len(df)):
        year = str(df.iat[i, 0])
        month = str(df.iat[i, 1])
        record_time = str(df.iat[i, 2])
        record_time = datetime.strptime(record_time, '%Y%m%d')
        vocher_num = str(df.iat[i, 3])
        vocher_type = df.iat[i, 4]
        subentry_num = str(df.iat[i, 5])
        description = df.iat[i, 6]
        subject_num = str(df.iat[i, 7])
        subject_name = df.iat[i, 8]
        debit = str_to_float(df.iat[i, 9])
        credit = str_to_float(df.iat[i, 10])
        debit_foreign_currency = str_to_float(df.iat[i, 11])
        debit_foreign_currency = debit_foreign_currency if debit_foreign_currency else 0.00
        credit_foreign_currency = str_to_float(df.iat[i, 12])
        credit_foreign_currency = credit_foreign_currency if credit_foreign_currency else 0.00
        debit_number = str_to_float(df.iat[i, 13])
        debit_number = debit_number if debit_number else 0.00
        credit_number = str_to_float(df.iat[i, 14])
        credit_number = credit_number if credit_number else 0.00
        debit_price = str_to_float(df.iat[i, 15])
        debit_price = debit_price if debit_price else 0.00
        credit_price = str_to_float(df.iat[i, 16])
        credit_price = credit_price if credit_price else 0.00
        currency_type = df.iat[i, 17]
        currency_type = currency_type if currency_type else "人民币"
        auxiliary = df.iat[i, 18]
        auxiliary = auxiliary if auxiliary else ""
        chronologicalaccount = ChronologicalAccount(company_code=company.code, company_name=company.name, year=year,
                                                    month=month, record_time=record_time, vocher_type=vocher_type,
                                                    vocher_num=vocher_num, subentry_num=subentry_num,
                                                    description=description, subject_num=subject_num,
                                                    subject_name=subject_name, currency_type=currency_type, debit=debit,
                                                    credit=credit, debit_foreign_currency=debit_foreign_currency,
                                                    credit_foreign_currency=credit_foreign_currency,
                                                    debit_number=debit_number,
                                                    credit_number=credit_number, debit_price=debit_price,
                                                    credit_price=credit_price,
                                                    auxiliary=auxiliary
                                                    )
        session.add(chronologicalaccount)
    session.commit()


def save_hs(company_name, start_time, end_time, hs_path):
    '''
    科目名称	科目编号	核算项目类型编号	核算项目类型名称	借贷方向	核算项目编号	核算项目名称
    账面期初数	账面借方发生额	账面贷方发生额 账面期末数
    期初数量	借方数量	贷方数量		期末数量

    :param company_name:
    :param hs_path:
    :return:
    '''
    company = session.query(Company).filter(Company.name == company_name).first()
    # 转换起止时间
    start_time = datetime.strptime(start_time, '%Y-%m-%d')
    end_time = datetime.strptime(end_time, '%Y-%m-%d')
    check_start_end_date(start_time, end_time)
    # 读取辅助核算表
    df = pd.read_excel(hs_path, index_col=0)
    df = df.rename(index=str, columns={
        "科目名称": "subject_name",
        "科目编号": "subject_num",
        "核算项目类型编号": "type_num",
        "核算项目类型名称": "type_name",
        "核算项目编号": "code",
        "核算项目名称": "name",
        "借贷方向": "direction",
        "账面期初数": "initial_amount",
        "账面借方发生额": "debit_amount",
        "账面贷方发生额": "credit_amount",
        "账面期末数": "terminal_amount",
        "期初数量": "initial_num",
        "借方数量": "debit_num",
        "贷方数量": "credit_num",
        "期末数量": "terminal_num",
    })
    if "initial_num" in df.columns:
        df = df[['subject_name', 'subject_num', 'type_num', 'type_name', 'code', 'name', 'direction', 'initial_amount',
                 'debit_amount', 'credit_amount', 'terminal_amount', 'initial_num', 'debit_num', 'credit_num',
                 'terminal_num'
                 ]]
        for i in range(len(df)):
            subject_name = df.iat[i, 0]
            subject_num = str(df.iat[i, 1])
            type_num = str(df.iat[i, 2])
            type_name = str(df.iat[i, 3])
            code = str(df.iat[i, 4])
            name = str(df.iat[i, 5])
            direction = str(df.iat[i, 6])
            initial_amount = str_to_float(df.iat[i, 7])
            debit_amount = str_to_float(df.iat[i, 8])
            credit_amount = str_to_float(df.iat[i, 9])
            terminal_amount = str_to_float(df.iat[i, 10])
            initial_num = str_to_float(df.iat[i, 11])
            debit_num = str_to_float(df.iat[i, 12])
            credit_num = str_to_float(df.iat[i, 13])
            terminal_num = str_to_float(df.iat[i, 14])

            if direction == "借":
                if not math.isclose(initial_amount + debit_amount - credit_amount, terminal_amount, rel_tol=1e-5):
                    raise Exception("{}辅助核算{}期初+本期借方-本期贷方不等于期末数".format(company_name, code))
            else:
                if not math.isclose(initial_amount - debit_amount + credit_amount, terminal_amount, rel_tol=1e-5):
                    raise Exception("{}辅助核算{}期初+本期借方-本期贷方不等于期末数".format(company_name, code))

            auxiliary = Auxiliary(company_code=company.code, company_name=company.name, subject_num=subject_num,
                                  start_time=start_time, end_time=end_time,
                                  subject_name=subject_name, type_name=type_name, type_num=type_num, code=code,
                                  name=name, direction=direction, initial_amount=initial_amount,
                                  debit_amount=debit_amount,
                                  credit_amount=credit_amount, terminal_amount=terminal_amount, initial_num=initial_num,
                                  debit_num=debit_num, credit_num=credit_num, terminal_num=terminal_num)
            session.add(auxiliary)
        session.commit()
    else:
        df = df[['subject_name', 'subject_num', 'type_num', 'type_name', 'code', 'name', 'direction',
                 'initial_amount','debit_amount', 'credit_amount', 'terminal_amount'
                 ]]
        for i in range(len(df)):
            subject_name = df.iat[i, 0]
            subject_num = str(df.iat[i, 1])
            type_num = str(df.iat[i, 2])
            type_name = str(df.iat[i, 3])
            code = str(df.iat[i, 4])
            name = str(df.iat[i, 5])
            direction = str(df.iat[i, 6])
            initial_amount = str_to_float(df.iat[i, 7])
            debit_amount = str_to_float(df.iat[i, 8])
            credit_amount = str_to_float(df.iat[i, 9])
            terminal_amount = str_to_float(df.iat[i, 10])
            initial_num = 0.00
            debit_num = 0.00
            credit_num = 0.00
            terminal_num = 0.00
            auxiliary = Auxiliary(company_code=company.code, company_name=company.name, subject_num=subject_num,
                                  subject_name=subject_name, type_name=type_name, type_num=type_num, code=code,
                                  name=name, direction=direction, initial_amount=initial_amount,
                                  debit_amount=debit_amount, start_time=start_time, end_time=end_time,
                                  credit_amount=credit_amount, terminal_amount=terminal_amount, initial_num=initial_num,
                                  debit_num=debit_num, credit_num=credit_num, terminal_num=terminal_num)
            session.add(auxiliary)
        session.commit()


def check_import_data(start_time, end_time):
    df_km = pd.read_sql_table('subjectbalance', engine)
    df_xsz = pd.read_sql_table('chronologicalaccount', engine)
    df_hs = pd.read_sql_table('auxiliary', engine)


if __name__ == '__main__':
    km_path = './data/zhsx_km.xlsx'
    xsz_path = './data/zhsx_xsz.xlsx'
    hs_path = './data/zhsx_hs.xlsx'
    # save_km("深圳市众恒世讯科技股份有限公司", start_time="2016-1-1", end_time="2016-12-31", km_path=km_path)
    # save_xsz("深圳市众恒世讯科技股份有限公司",start_time="2016-1-1", end_time="2016-12-31",xsz_path=xsz_path)
    save_hs("深圳市众恒世讯科技股份有限公司",start_time="2016-1-1", end_time="2016-12-31",hs_path=hs_path)
    # check_import_data()
