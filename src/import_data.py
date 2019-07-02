# -*- coding:utf-8 -*-

import pandas as pd
from datetime import datetime
from decimal import Decimal
import math
from src.database import Company, Auxiliary, SubjectBalance, ChronologicalAccount
from src.utils import str_to_float,check_start_end_date,get_session_and_engine



def save_km(company_name, start_time, end_time, km_path,session):
    '''
    科目编号	科目名称	科目类别	借贷方向	是否明细科目	科目级次
    账面期初数	账面借方发生额	账面贷方发生额	账面期末数
    :param company_name:
    :param start_time:科目余额表开始时间，如20019-1-1
    :param end_time:科目余额表结束时间，如20019-12-31
    :return:
    '''
    # 获取公司信息
    # 需要检查是否存在该公司，否则会报异常
    company = session.query(Company).filter(Company.name == company_name).first()
    # 读取文件
    df = pd.read_excel(km_path, index_col=0)
    columns = [column for column in df]
    names = ["科目编号","科目名称","科目类别","借贷方向","是否明细科目","科目级次","账面期初数","账面借方发生额","账面贷方发生额","账面期末数"]
    columns_right = [False for name in names if name not in columns]
    if False in columns_right:
        raise Exception("项目名称必须包含科目编号,科目名称,科目类别,借贷方向,是否明细科目,科目级次,账面期初数,账面借方发生额,账面贷方发生额,账面期末数等信息")

    # 转换起止时间
    start_time = datetime.strptime(start_time, '%Y-%m-%d')
    end_time = datetime.strptime(end_time, '%Y-%m-%d')
    check_start_end_date(start_time, end_time)
    # 检查是否已经存储科目余额表
    kms = session.query(SubjectBalance).filter(SubjectBalance.company_name == company_name,
                                               SubjectBalance.start_time == start_time,
                                               SubjectBalance.end_time == end_time).all()
    #如果已经存储了，则替代原来的
    if len(kms) > 0:
        choice = "yes"
        # choice = input('已经存在科目余额表，是否要替换')
        if choice == "yes":
            print('开始删除')
            for km in kms:
                session.delete(km)
            session.commit()
        else:
            return
    # 读取科目余额表
    print('开始存储')

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
        if direction == "借":
            value = abs(Decimal(initial_amount) +Decimal(debit_amount)  - Decimal(credit_amount) - Decimal(terminal_amount))
            if value > 0.000001:
                raise Exception("{}的期初{}+本期借方{}-本期贷方{}不等于期末数{}".format(subject_name,initial_amount,debit_amount,credit_amount,terminal_amount))
        else:
            value = abs(
                Decimal(initial_amount) - Decimal(debit_amount) + Decimal(credit_amount) - Decimal(terminal_amount))
            if  value > 0.000001:
                raise Exception("{}的期初{}-本期借方{}+本期贷方{}不等于期末数{}".format(subject_name,initial_amount,debit_amount,credit_amount,terminal_amount))

        km = SubjectBalance(company_code=company.code, company_name=company.name, start_time=start_time,
                            end_time=end_time,
                            subject_num=subject_num, subject_name=subject_name, subject_type=subject_type,
                            direction=direction, is_specific=is_specific, subject_gradation=subject_gradation,
                            initial_amount=initial_amount, debit_amount=debit_amount, credit_amount=credit_amount,
                            terminal_amount=terminal_amount
                            )
        session.add(km)
    session.commit()


def save_xsz(company_name, start_time, end_time, xsz_path,session):
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
    # 检查序时账是否符合规范
    columns = [column for column in df]
    names = ["会计年", "会计月", "记账时间", "凭证编号", "凭证种类", "编号", "业务说明", "科目编号", "科目名称",
             "借方发生额","贷方发生额","借方发生额_外币","贷方发生额_外币","借方数量","贷方数量","借方单价","贷方单价",
             "货币种类","核算项目名称"
             ]
    columns_right = [False for name in names if name not in columns]
    if False in columns_right:
        raise Exception("项目名称不符合序时账导入规则")
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

    xszs = session.query(ChronologicalAccount).filter(ChronologicalAccount.company_name == company_name,
                                                      ChronologicalAccount.year == start_time.year).all()
    if len(xszs) > 0:
        # choice = input('{}年度已经存在序时账，是否要替换原有数据'.format(year))
        choice = "yes"
        if choice == 'yes':
            for xsz in xszs:
                session.delete(xsz)
            session.commit()
        else:
            return
    #     检查借贷方是否相等
    if not math.isclose(df['debit'].map(lambda x: str_to_float(x)).sum(), df['credit'].map(lambda x: str_to_float(x)).sum(), rel_tol=1e-5):
        raise Exception("{}序时账借方发生额和贷方发生额合计不一致".format(company_name))
    #     存储到数据库
    for i in range(len(df)):
        year = str(df.iat[i, 0])
        month = str(df.iat[i, 1])
        record_time = str(df.iat[i, 2])
        record_time = datetime.strptime(record_time, '%Y%m%d')
        vocher_num = int(df.iat[i, 3])
        vocher_type = str(df.iat[i, 4])
        subentry_num = int(df.iat[i, 5])
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


def save_hs(company_name, start_time, end_time, hs_path,session):
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
    # 检查核算表是否符合规范
    columns = [column for column in df]
    names = ["科目名称", "科目编号", "核算项目类型编号", "核算项目类型名称", "核算项目编号", "核算项目名称", "借贷方向", "账面期初数", "账面借方发生额",
             "账面贷方发生额", "账面期末数"]
    columns_right = [False for name in names if name not in columns]
    if False in columns_right:
        raise Exception("项目名称不符合辅助核算导入规则")

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
    # 检查是否已经存核算
    hs = session.query(Auxiliary).filter(Auxiliary.company_name == company_name,
                                          Auxiliary.start_time == start_time,
                                          Auxiliary.end_time == end_time).all()
    # 如果已经存储了，则替代原来的
    if len(hs) > 0:
        choice = "yes"
        # choice = input('已经存在科目余额表，是否要替换')
        if choice == "yes":
            print('开始删除')
            for h in hs:
                session.delete(h)
            session.commit()
        else:
            return
    # 开始存储
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


def check_import_data(company_name,start_time, end_time,engine):
    '''
    检查科目余额表与序时账是否一致
    检查科目余额表与辅助核算明细表是否一致
    :param company_name:
    :param start_time:
    :param end_time:
    :return:
    '''
    start_time = datetime.strptime(start_time, '%Y-%m-%d')
    end_time = datetime.strptime(end_time, '%Y-%m-%d')
    check_start_end_date(start_time, end_time)
    year = start_time.year
    start_month = start_time.month
    end_month = end_time.month
    # 获取科目余额表
    df_km = pd.read_sql_table('subjectbalance', engine)
    df_km = df_km[(df_km['start_time']==start_time) & (df_km['end_time']==end_time) & (df_km['company_name']==company_name) ]
    # 获取序时账
    df_xsz = pd.read_sql_table('chronologicalaccount', engine)
    df_xsz = df_xsz[(df_xsz['year'] == year) & (df_xsz['month'] >= start_month) & (df_xsz['month'] <= end_month) & (
                df_xsz['company_name'] == company_name)]
    # 获取辅助核算明细表
    df_hs = pd.read_sql_table('auxiliary', engine)
    df_hs = df_hs[
        (df_hs['start_time'] == start_time) & (df_hs['end_time'] == end_time) & (df_hs['company_name'] == company_name)]
    # （1）检查序时账借贷方发生额与科目余额表是否一致
    # 获取科目余额表借贷方发生额
    df_km_slim = df_km[['subject_num','debit_amount','credit_amount']]
    df_km_subject = df_km_slim.set_index('subject_num')
    # 获取序时账数据透视表，合计为借贷方，索引为科目编码
    df_xsz_pivot = df_xsz.pivot_table(values=['debit', 'credit'], index='subject_num',  aggfunc='sum')
    # 合并两个表，并比较发生额是否一致
    df1 = pd.merge(df_xsz_pivot, df_km_subject, left_index=True, right_index=True, how='left')
    df1['credit_equal'] = df1['credit'] - df1['credit_amount'] < 0.001
    df1['debit_equal'] = df1['debit'] - df1['debit_amount'] < 0.001
    if not df1['credit_equal'].all():
        raise Exception('贷方合计错误')
    if not df1['debit_equal'].all():
        raise Exception('借方合计错误')
    #  （2）检查辅助核算明细表与科目余额表是否一致
    #     分别检查期初数/本期借方/本期贷方/期末数是否一致
    # 获取科目余额表期初数/本期借方/本期贷方/期末数
    if len(df_hs) > 0:
        df_km_slim2 = df_km[['subject_num','initial_amount', 'debit_amount', 'credit_amount','terminal_amount']]
        df_km_subject2 = df_km_slim2.set_index('subject_num')
        # 获取辅助核算数据透视表，合计为期初期末和借贷方，索引为科目编码
        df_hs_pivot = df_hs.pivot_table(values=['initial_amount', 'debit_amount','credit_amount','terminal_amount'],
                                        index=['subject_num','type_num'], aggfunc='sum')
        # 合并两个表，并比较发生额是否一致
        df2 = pd.merge(df_hs_pivot, df_km_subject2, left_index=True, right_index=True, how='left')
        df2['initial_equal'] = df2['initial_amount_x'] - df2['initial_amount_y'] < 0.001
        if not df2['initial_equal'].all():
            raise Exception('期初数不一致')
        df2['credit_equal'] = df2['credit_amount_x'] - df2['credit_amount_y'] < 0.001
        if not df2['credit_equal'].all():
            raise Exception('贷方发生额不一致')
        df2['debit_equal'] = df2['debit_amount_x'] - df2['debit_amount_y'] < 0.001
        if not df2['debit_equal'].all():
            raise Exception('借方发生额不一致')
        df2['terminal_equal'] = df2['terminal_amount_x'] - df2['terminal_amount_y'] < 0.001
        if not df2['terminal_equal'].all():
            raise Exception('期末数不一致')

    return True


if __name__ == '__main__':
    session,engine  = get_session_and_engine()
    km_path = '../data/zhsx_km.xlsx'
    xsz_path = '../data/zhsx_xsz.xlsx'
    hs_path = '../data/zhsx_hs.xlsx'
    # save_km("深圳市众恒世讯科技股份有限公司", start_time="2016-1-1", end_time="2016-12-31", km_path=km_path,session=session)
    # save_xsz("深圳市众恒世讯科技股份有限公司",start_time="2016-1-1", end_time="2016-12-31",xsz_path=xsz_path,session=session)
    save_hs("深圳市众恒世讯科技股份有限公司",start_time="2016-1-1", end_time="2016-12-31",hs_path=hs_path,session=session)
    # check_import_data("深圳市众恒世讯科技股份有限公司",start_time="2016-1-1", end_time="2016-12-31",engine=engine)

