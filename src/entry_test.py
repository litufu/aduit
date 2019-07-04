from collections import defaultdict
import json
import pandas as pd
from datetime import datetime
from src.database import EntryClassify,AuditRecord,OutputTax,TransactionEvent
from src.get_tb  import get_new_km_xsz_df
from src.utils import gen_df_line,  get_session_and_engine,parse_auxiliary
from settings.constant import inventory,long_term_assets,expense,recognition_income_debit,recognition_income_credit,\
    sale_rate,other_income_rent_desc,notes_receivable_subjects,monetary_funds,inventory_tax,long_term_assets_tax,expense_tax,\
    payments,receivables,interest_desc,bank_charges_desc,exchange_desc



def add_event(company_name,start_time,end_time,session,month,vocher_type,vocher_num,subentry_num,desc):
    event = TransactionEvent(
        company_name=company_name,
        start_time=start_time,
        end_time=end_time,
        month=month,
        vocher_type=vocher_type,
        vocher_num=vocher_num,
        subentry_num=subentry_num,
        desc=desc
    )
    session.add(event)
    session.commit()

def get_account_nature(df_xsz,name):
    '''
    从本年度序时账获取供应商款项性质
    :param df_xsz: 序时账
    :param name: 供应商名称
    :return: 供应商采购的款项性质
    '''
    df_tmp_xsz = df_xsz[
        (df_xsz["auxiliary"].str.contains(name)) & (df_xsz["credit"].abs() > 0)]
    if len(df_tmp_xsz) > 0:
        for obj in gen_df_line(df_tmp_xsz[:1]):
            df_supplier_xsz = df_xsz[
                (df_xsz["month"] == obj["month"]) &
                (df_xsz["vocher_type"] == obj["vocher_type"]) &
                (df_xsz["vocher_num"] == obj["vocher_num"]) &
                (df_xsz["debit"].abs() > 0)
                ]
            for i in long_term_assets:
                if df_supplier_xsz["subject_name_1"].str.contains(i).any():
                    return "长期资产"
            for i in inventory:
                if df_supplier_xsz["subject_name_1"].str.contains(i).any():
                    return "材料费"
            for i in expense:
                if df_supplier_xsz["subject_name_1"].str.contains(i).any():
                    return "费用"
    return "材料费"

def get_supplier_nature(str,df_xsz):
    '''
    根据字符串获取供应商款项性质
    :param str:辅助核算字符串
    :param df_xsz:序时账
    :return:款项性质
    '''
    if str and "供应商"  in str:
        auxiliary = parse_auxiliary(str)
        supplier = auxiliary.get("供应商")
        if supplier:
            nature = get_account_nature(df_xsz, supplier)
            return nature
    return ""




def check_subject_and_desc_contains(df_one_entry,strs,grades):
    '''
    检查凭证摘要或科目中是否包含特定的字符串
    :param df_one_entry:凭证
    :param strs:字符串或者列表
    :param grades:科目级次
    :return:包含或不包含
    '''
    if not isinstance(strs,list):
        raise Exception("需要传入字符串列表")
    for obj in gen_df_line(df_one_entry):
        containers = []
        desc = obj["description"]
        containers.append(desc)
        for grade in range(grades):
            subject_name = "subject_name_{}".format(grade+1)
            containers.append(obj[subject_name])
        for container in containers:
            for str in strs:
                if str in container:
                    return True
    return False

# 检查科目是否为标准科目
def check_event_subject_std(company_name,start_time,end_time,df_one_entry,record,session,std_debit_subjects,std_credit_subjects):
    '''

    :param company_name:
    :param start_time:
    :param end_time:
    :param df_one_entry: 凭证
    :param record: 凭证记录
    :param session:
    :param std_debit_subjects: 标准借方科目
    :param std_credit_subjects: 标准贷方科目
    :return:
    '''
    # 获取凭证科目
    subjects = get_entry_subjects(df_one_entry,"subject_name_1")
    # 分别检查借方科目和贷方科目

    for debit_subject in subjects["debit"]:
        if not debit_subject in std_debit_subjects:
            audit_record = AuditRecord(
                company_name=company_name,
                start_time=start_time,
                end_time=end_time,
                problem="记账凭证借方为非标准记账凭证,凭证包含科目{}，{}-{}-{}".format(debit_subject,record['month'],
                                                                  record['vocher_type'], record['vocher_num'])
            )
            session.add(audit_record)
            session.commit()
            break
    for credit_subject in subjects["credit"]:
        if not credit_subject in std_credit_subjects:
            audit_record = AuditRecord(
                company_name=company_name,
                start_time=start_time,
                end_time=end_time,
                problem="记账凭证贷方为非标准记账凭证,凭证包含科目{}，{}-{}-{}".format(credit_subject, record['month'], record['vocher_type'],
                                                                  record['vocher_num'])
            )
            session.add(audit_record)
            session.commit()
            break

def get_expected_rates(year,month):
    '''
    根据审计时期，获取法律规定的增值税率
    :param year: 审计年度
    :param month: 审计月份
    :return: 增值税率表
    '''
    transaction_date = datetime(year=year, month=month, day=2)
    sale_rate_dates = list(sale_rate.keys())
    sale_rate_dates.sort(reverse=True)
    for sale_rate_date in sale_rate_dates:
        sale_rate_date = datetime.strptime(sale_rate_date, '%Y-%m-%d')
        if transaction_date > sale_rate_date:
            return sale_rate[sale_rate_date]
    raise Exception("日期在1994-1-1之前，没有增值税暂行条例呢")


def aduit_recognition_income(company_name,start_time,end_time,df_one_entry,record,session):
    '''
    # 审计程序：
    # a/检查判断是否执行建造合同准则，
    # 1、检查收入确认是否与标准凭证一致：借：应收账款、预收账款、现金、银行存款：贷：主营业务收入，应交税费
    # 2、检查每一笔收入确认的税费对应的税率，检查税率是否存在异常的情况。
    :param company_name: 公司名称
    :param start_time: 开始时间
    :param end_time: 结束时间
    :param df_one_entry: 收入凭证
    :param record: 凭证记录
    :return: 审核记录
    '''
    # 程序1：检查是否执行建造合同准则
    if df_one_entry["tb_subject"].str.contains("工程施工"):
        # 未来添加--此处添加对执行建造合同的审计
        return

    # 程序2：检查凭证是否标准
    check_event_subject_std(company_name, start_time, end_time, df_one_entry, record, session, recognition_income_debit,
                            recognition_income_credit)

    # 程序3：检查增值税销项税率是否符合税法规定
    income_amount = df_one_entry[df_one_entry["tb_subject"]=="主营业务收入"]["credit"].sum()
    tax_amount = df_one_entry[df_one_entry["tb_subject"]=="应交税费"]["credit"].sum()
    tax_rate = round(tax_amount / income_amount,2)
    # 预期税率
    year = start_time.year
    month = record["month"]
    std_sale_rates = get_expected_rates(year,month)
    if tax_rate in std_sale_rates:
        expected_tax_rate = tax_rate
        difference = False
    else:
        expected_tax_rate = 1.0
        difference = True

    output_tax = OutputTax(company_name=company_name,
                           start_time=start_time,
                           end_time=end_time,
                           month=record["month"],
                           vocher_type=record["vocher_type"],
                           vocher_num=record["vocher_num"],
                           income = income_amount,
                           tax = tax_amount,
                           tax_rate=tax_rate,
                           expected_tax_rate=expected_tax_rate,
                           difference=difference)
    session.add(output_tax)
    session.commit()

def aduit_entry(company_name,start_time,end_time,session,engine,add_suggestion):
    # 获取科目余额表和序时账
    df_km, df_xsz = get_new_km_xsz_df(company_name, start_time, end_time, engine, add_suggestion, session)
    # 获取科目级次
    df_km_gradation = df_km.drop_duplicates('subject_gradation', keep='first')
    grades = len(df_km_gradation)
    # 合并序时账和标准科目对照表
    df_std = pd.read_sql_table('subjectcontrast', engine)
    df_std = df_std[["origin_subject","tb_subject"]]
    df_xsz = pd.merge(df_xsz,df_std,how="left",left_on="subject_name_1",right_on="origin_subject")
    df_xsz["tb_subject"].fillna(df_xsz["subject_name_1"],inplace=True)
    df_xsz["direction"] = df_xsz["debit"].apply(lambda x: "借" if abs(x)>0 else "贷" )
    df_xsz["nature"] = df_xsz["auxiliary"].apply(get_supplier_nature, args=(df_xsz,))
    df_xsz["subject_name_nature"] = df_xsz["subject_name_1"] + df_xsz["nature"]
    # 获取所有的凭证记录
    df_xsz_record = df_xsz[["month", "vocher_num", "vocher_type"]].drop_duplicates()
    records = df_xsz_record.to_dict('records')
    # 获取每一笔凭证
    start_time = datetime.strptime(start_time, '%Y-%m-%d')
    end_time = datetime.strptime(end_time, '%Y-%m-%d')

    for record in records:
        # 获取单笔凭证
        df_tmp = df_xsz[(df_xsz["month"] == record["month"])
                        & (df_xsz["vocher_num"] == record["vocher_num"])
                        & (df_xsz["vocher_type"] == record["vocher_type"])
                        ]
        # 获取凭证的借方和贷方一级科目名称
        subjects = get_entry_subjects(df_tmp, "subject_name_nature")
        debit_subjects_list = subjects["debit"]
        credit_subjects_list = subjects["credit"]
        # 合并科目名称
        debit_subject_desc = "%".join(debit_subjects_list)
        credit_subjects_desc = "%".join(credit_subjects_list)
        # 借方会计科目为：
        df_tmp_debit = df_tmp[df_tmp["direction"] == "借"]
        # 贷方会计科目
        df_tmp_credit = df_tmp[df_tmp["direction"] == "贷"]

        # 处理本年利润结转凭证
        if df_tmp["tb_subject"].str.contains("本年利润").any():
            for obj in gen_df_line(df_tmp):
                add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                          obj["vocher_num"], obj["subentry_num"],"结转本年利润")
            continue

        #     结转收入
        # 处理收入确认凭证
        if df_tmp["tb_subject"].str.contains("主营业务收入").any():
            if df_tmp[(df_tmp["tb_subject"]=="主营业务收入") &(df_tmp["direction"]=="借")]:
                raise Exception("主营业务收入在借方")
            # 审计程序：
            aduit_recognition_income(company_name,start_time,end_time,df_tmp,record,session)
            # 为收入确认添加交易描述
            for obj in gen_df_line(df_tmp):
                add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                          obj["vocher_num"], obj["subentry_num"],"确认主营业务收入")
            continue
        # 处理其他业务收入
        if df_tmp["tb_subject"].str.contains("其他业务收入").any():
            if df_tmp[(df_tmp["tb_subject"]=="其他业务收入") &(df_tmp["direction"]=="借")]:
                raise Exception("其他业务收入在借方")
            # 为收入确认添加交易描述
            for obj in gen_df_line(df_tmp):
                # 检查是否为出租收入，如果出租收入应该列示为确认其他业务收入-租赁收入，租赁收入现金流放入"收到其他经营活动"
                if check_subject_and_desc_contains(df_tmp,other_income_rent_desc,grades):
                    add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                              obj["vocher_num"], obj["subentry_num"], "其他业务收入-租赁收入")
                else:
                    add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                              obj["vocher_num"], obj["subentry_num"], "其他业务收入-非租赁收入")
            continue
        # 处理应收账款减少
        if df_tmp["tb_subject"].str.contains("应收账款").any():
            # 检查应收账款是否在借方，该应收账款标记为非收入确认
            if len(df_tmp[(df_tmp["tb_subject"]=="应收账款") &(df_tmp["direction"]=="借")])>0:
                for obj in gen_df_line(df_tmp):
                    add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                              obj["vocher_num"], obj["subentry_num"], "应收账款增加-非收入确认")
                audit_record = AuditRecord(
                    company_name=company_name,
                    start_time=start_time,
                    end_time=end_time,
                    problem="应收账款对应的贷方不是收入确认，凭证见{}-{}-{}".format(record['month'],
                                                                             record['vocher_type'],
                                                                             record['vocher_num'])
                )
                session.add(audit_record)
                session.commit()
            else:
                #判断贷方是否仅有应收账款一个科目
                if len(credit_subjects_list) > 1:
                    for obj in gen_df_line(df_tmp):
                        add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                                  obj["vocher_num"], obj["subentry_num"], "应收账款减少-{}".format(debit_subject_desc))
                        audit_record = AuditRecord(
                            company_name=company_name,
                            start_time=start_time,
                            end_time=end_time,
                            problem="记账凭证为非标准记账凭证,应收账款减少贷方含有其他科目，凭证见{}-{}-{}".format(record['month'],
                                                                              record['vocher_type'],
                                                                              record['vocher_num'])
                        )
                        session.add(audit_record)
                        session.commit()
                else:
                    # 应收账款减少的情形
                    # 1、收回货币资金
                    # 2、转为应收票据、其他应收款等其他应收款项
                    # 3、冲减应付款
                    # 4、核销应收账款
                    # 5、收回存货
                    # 6、收回长期资产
                    # 7、变为费用
                    #
                    # 借方会计科目为：
                    df_tmp_debit = df_tmp[df_tmp["direction"]=="借"]
                    if len([False for subject in df_tmp_debit["subject_name_1"] if subject not in monetary_funds]) == 0:
                        for obj in gen_df_line(df_tmp):
                            add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                                      obj["vocher_num"], obj["subentry_num"],
                                      "应收账款减少-收回货币资金")
                    elif len([False for subject in df_tmp_debit["subject_name_1"] if subject not in [*monetary_funds,"财务费用"]]) == 0:
                        for obj in gen_df_line(df_tmp):
                            add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                                      obj["vocher_num"], obj["subentry_num"],
                                      "应收账款减少-带折扣收回货币资金")
                    elif len([False for subject in df_tmp_debit["subject_name_1"] if subject not in inventory_tax]) == 0:
                        for obj in gen_df_line(df_tmp):
                            add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                                      obj["vocher_num"], obj["subentry_num"],
                                      "应收账款减少-交换存货")
                    elif len([False for subject in df_tmp_debit["subject_name_1"] if subject not in long_term_assets_tax]) == 0:
                        for obj in gen_df_line(df_tmp):
                            add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                                      obj["vocher_num"], obj["subentry_num"],
                                      "应收账款减少-交换长期资产")
                    elif len([False for subject in df_tmp_debit["subject_name_1"] if subject not in expense_tax]) == 0:
                        for obj in gen_df_line(df_tmp):
                            add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                                      obj["vocher_num"], obj["subentry_num"],
                                      "应收账款减少-转为费用")
                    elif len([False for subject in df_tmp_debit["subject_name_1"] if subject not in payments]) == 0:
                        for obj in gen_df_line(df_tmp):
                            add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                                      obj["vocher_num"], obj["subentry_num"],
                                      "应收账款减少-冲减应付款-{}".format(debit_subject_desc))
                    elif len([False for subject in df_tmp_debit["subject_name_1"] if subject not in receivables]) == 0:
                        for obj in gen_df_line(df_tmp):
                            add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                                      obj["vocher_num"], obj["subentry_num"],
                                      "应收账款减少-转为其他应收款项")
                    elif len([False for subject in df_tmp_debit["subject_name_1"] if subject not in receivables]) == 0:
                        for obj in gen_df_line(df_tmp):
                            add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                                      obj["vocher_num"], obj["subentry_num"],
                                      "应收账款减少-转为其他应收款项")
                    else:
                        for obj in gen_df_line(df_tmp):
                            add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                                      obj["vocher_num"], obj["subentry_num"],
                                      "应收账款减少-{}".format(debit_subject_desc))
                        audit_record = AuditRecord(
                            company_name=company_name,
                            start_time=start_time,
                            end_time=end_time,
                            problem="应收账款减少不为标准项目，凭证见{}-{}-{}".format(
                                                                                record['month'],
                                                                                record['vocher_type'],
                                                                                record['vocher_num'])
                        )
                        session.add(audit_record)
                        session.commit()
            continue
        # 处理应收票据减少
        if df_tmp["tb_subject"].str.contains("应收票据").any():
            # 检查应收票据是否在借方，贷方是否非应收账款
            if len(df_tmp[(df_tmp["tb_subject"]=="应收票据") &(df_tmp["direction"]=="借")])>0:
                for obj in gen_df_line(df_tmp):
                    add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                              obj["vocher_num"], obj["subentry_num"], "应收票据增加-非应收账款转入")
                audit_record = AuditRecord(
                    company_name=company_name,
                    start_time=start_time,
                    end_time=end_time,
                    problem="应收票据增加贷方非应收账款，凭证见{}-{}-{}".format(record['month'],
                                                                             record['vocher_type'],
                                                                             record['vocher_num'])
                )
                session.add(audit_record)
                session.commit()
            else:
                #判断贷方是否仅有应收票据一个科目
                if len(credit_subjects_list) > 1:
                    for obj in gen_df_line(df_tmp):
                        add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                                  obj["vocher_num"], obj["subentry_num"], "应收票据减少-非标准-{}".format(debit_subject_desc))
                        audit_record = AuditRecord(
                            company_name=company_name,
                            start_time=start_time,
                            end_time=end_time,
                            problem="记账凭证为非标准记账凭证,应收票据减少贷方含有其他科目，凭证见{}-{}-{}".format(record['month'],
                                                                              record['vocher_type'],
                                                                              record['vocher_num'])
                        )
                        session.add(audit_record)
                        session.commit()
                else:
                    # 应收票据减少标准：
                    # 1、收回货款
                    # 2、贴现
                    # 3、冲减应付款


                    if len([False for subject in df_tmp_debit["subject_name_1"] if subject not in monetary_funds]) == 0:
                        for obj in gen_df_line(df_tmp):
                            add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                                      obj["vocher_num"], obj["subentry_num"],
                                      "应收票据减少-收回货币资金")
                    elif len([False for subject in df_tmp_debit["subject_name_1"] if subject not in [*monetary_funds,"财务费用"]]) == 0:
                        for obj in gen_df_line(df_tmp):
                            add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                                      obj["vocher_num"], obj["subentry_num"],
                                      "应收票据减少-贴现")
                    elif len([False for subject in df_tmp_debit["subject_name_1"] if subject not in payments]) == 0:
                        for obj in gen_df_line(df_tmp):
                            add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                                      obj["vocher_num"], obj["subentry_num"],
                                      "应收票据减少-冲减应付款")
                    else:
                        audit_record = AuditRecord(
                            company_name=company_name,
                            start_time=start_time,
                            end_time=end_time,
                            problem="应收票据减少不是标准案例，凭证见{}-{}-{}".format(
                                                                                record['month'],
                                                                                record['vocher_type'],
                                                                                record['vocher_num'])
                        )
                        session.add(audit_record)
                        session.commit()
                        for obj in gen_df_line(df_tmp):
                            add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                                      obj["vocher_num"], obj["subentry_num"],
                                      "应收票据减少-{}".format(debit_subject_desc))
            continue
        # 处理预收账款增加
        if df_tmp["tb_subject"].str.contains("预收款项").any():
            # 检查预收款项是否在借方，该预收款项标记为非收入确认
            if len(df_tmp[(df_tmp["tb_subject"]=="预收款项") &(df_tmp["direction"]=="借")])>0:
                for obj in gen_df_line(df_tmp):
                    add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                              obj["vocher_num"], obj["subentry_num"], "预收款项减少-非收入确认")
                audit_record = AuditRecord(
                    company_name=company_name,
                    start_time=start_time,
                    end_time=end_time,
                    problem="预收款项对应的贷方不是收入确认，凭证见{}-{}-{}".format(record['month'],
                                                                             record['vocher_type'],
                                                                             record['vocher_num'])
                )
                session.add(audit_record)
                session.commit()
            else:
                # 根据对方科目判断预收账款增加的情形：
                # 获取凭证的借方和贷方一级科目名称
                subjects = get_entry_subjects(df_tmp,"subject_name_nature")
                debit_subjects_list = subjects["debit"]
                credit_subjects_list = subjects["credit"]
                # 合并科目名称
                debit_subject_desc = "%".join(debit_subjects_list)
                #判断贷方是否仅有预收款项一个科目
                if len(credit_subjects_list) > 1:
                    for obj in gen_df_line(df_tmp):
                        add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                                  obj["vocher_num"], obj["subentry_num"], "预收款项增加-{}".format(debit_subject_desc))
                        audit_record = AuditRecord(
                            company_name=company_name,
                            start_time=start_time,
                            end_time=end_time,
                            problem="记账凭证为非标准记账凭证,预收款项增加贷方含有其他科目，凭证见{}-{}-{}".format(record['month'],
                                                                              record['vocher_type'],
                                                                              record['vocher_num'])
                        )
                        session.add(audit_record)
                        session.commit()
                else:
                    # 预收款项增加的情形
                    # 1、收到货币资金
                    # 借方会计科目为：
                    df_tmp_debit = df_tmp[df_tmp["direction"]=="借"]
                    if len([False for subject in df_tmp_debit["subject_name_1"] if subject not in monetary_funds]) == 0:
                        for obj in gen_df_line(df_tmp):
                            add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                                      obj["vocher_num"], obj["subentry_num"],
                                      "预收款项增加-收到货币资金")
                    else:
                        for obj in gen_df_line(df_tmp):
                            add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                                      obj["vocher_num"], obj["subentry_num"],
                                      "预收款项增加-{}".format(debit_subject_desc))
                        audit_record = AuditRecord(
                            company_name=company_name,
                            start_time=start_time,
                            end_time=end_time,
                            problem="预收款项增加不为标准项目，凭证见{}-{}-{}".format(
                                                                                record['month'],
                                                                                record['vocher_type'],
                                                                                record['vocher_num'])
                        )
                        session.add(audit_record)
                        session.commit()
            continue

        # 结转成本
        #处理主营业务成本结转
        if df_tmp["tb_subject"].str.contains("主营业务成本").any():
            for obj in gen_df_line(df_tmp):
                add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                          obj["vocher_num"], obj["subentry_num"],"结转主营业务成本")
            continue
        # 处理其他业务成本结转
        if df_tmp["tb_subject"].str.contains("其他业务成本").any():
            for obj in gen_df_line(df_tmp):
                add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                          obj["vocher_num"], obj["subentry_num"],"结转其他业务成本")
            continue

        # 折旧与摊销
        # 计提累计折旧
        if df_tmp["tb_subject"].str.contains("累计折旧").any():
            # 1、累计折旧在借方
            if len(df_tmp[(df_tmp["tb_subject"]=="累计折旧") &(df_tmp["direction"]=="借")])>0:
                if "固定资产" in credit_subjects_list:
                    for obj in gen_df_line(df_tmp):
                        add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                                  obj["vocher_num"], obj["subentry_num"], "处置固定资产")
                else:
                    for obj in gen_df_line(df_tmp):
                        add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                                  obj["vocher_num"], obj["subentry_num"], "累计折旧减少-{}".format(credit_subjects_desc))
                    audit_record = AuditRecord(
                        company_name=company_name,
                        start_time=start_time,
                        end_time=end_time,
                        problem="累计折旧减少贷方非固定资产，凭证见{}-{}-{}".format(record['month'],
                                                                                 record['vocher_type'],
                                                                                 record['vocher_num'])
                    )
                    session.add(audit_record)
                    session.commit()
            else:
                for obj in gen_df_line(df_tmp):
                    add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                              obj["vocher_num"], obj["subentry_num"], "计提折旧")
            continue
        #   计提累计摊销
        if df_tmp["tb_subject"].str.contains("累计摊销").any():
            # 1、累计折旧在借方
            if len(df_tmp[(df_tmp["tb_subject"] == "累计摊销") & (df_tmp["direction"] == "借")]) > 0:
                if "无形资产" in credit_subjects_list:
                    for obj in gen_df_line(df_tmp):
                        add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                                  obj["vocher_num"], obj["subentry_num"], "处置无形资产")
                else:
                    for obj in gen_df_line(df_tmp):
                        add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                                  obj["vocher_num"], obj["subentry_num"], "累计摊销减少-{}".format(credit_subjects_desc))
                    audit_record = AuditRecord(
                        company_name=company_name,
                        start_time=start_time,
                        end_time=end_time,
                        problem="累计摊销减少贷方非无形资产，凭证见{}-{}-{}".format(record['month'],
                                                                   record['vocher_type'],
                                                                   record['vocher_num'])
                    )
                    session.add(audit_record)
                    session.commit()
            else:
                for obj in gen_df_line(df_tmp):
                    add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                              obj["vocher_num"], obj["subentry_num"], "无形资产摊销")
            continue
        # 长期待摊费用
        if df_tmp["tb_subject"].str.contains("长期待摊费用").any():
            # 1、长期待摊费用在借方
            if len(df_tmp[(df_tmp["tb_subject"] == "长期待摊费用") & (df_tmp["direction"] == "借")]) > 0:
                for obj in gen_df_line(df_tmp):
                    add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                              obj["vocher_num"], obj["subentry_num"], "长期待摊费用增加")

            else:
                for obj in gen_df_line(df_tmp):
                    add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                              obj["vocher_num"], obj["subentry_num"], "长期待摊费用摊销或减少")
            continue

        # 财务费用类
        # 应付利息
        if df_tmp["tb_subject"].str.contains("应付利息").any():
            # 1、应付利息在借方
            if len(df_tmp[(df_tmp["tb_subject"] == "应付利息") & (df_tmp["direction"] == "借")]) > 0:
                if len(credit_subjects_list)==1 and credit_subjects_list[0] == "银行存款":
                    for obj in gen_df_line(df_tmp):
                        add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                                  obj["vocher_num"], obj["subentry_num"], "支付应付利息")
                else:
                    for obj in gen_df_line(df_tmp):
                        add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                                  obj["vocher_num"], obj["subentry_num"], "应付利息减少-{}".format(credit_subjects_desc))
                    audit_record = AuditRecord(
                        company_name=company_name,
                        start_time=start_time,
                        end_time=end_time,
                        problem="应付利息减少贷方非银行存款，凭证见{}-{}-{}".format(record['month'],
                                                                   record['vocher_type'],
                                                                   record['vocher_num'])
                    )
                    session.add(audit_record)
                    session.commit()
            else:
                for obj in gen_df_line(df_tmp):
                    add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                              obj["vocher_num"], obj["subentry_num"], "计提利息")
            continue
        # 财务费用
        if df_tmp["tb_subject"].str.contains("财务费用").any():
            if df_tmp[(df_tmp["tb_subject"]=="财务费用") &(df_tmp["direction"]=="贷")]:
                raise Exception("财务费用在贷方")
            # 如果包含利息，负数则为利息收入，否则为利息支出
            df_tmp_financial_expense = df_tmp[df_tmp["tb_subject"]=="财务费用"]
            if check_subject_and_desc_contains(df_tmp, ["融资费用"], grades) :
                for obj in gen_df_line(df_tmp):
                    add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                              obj["vocher_num"], obj["subentry_num"], "财务费用-未确认融资费用")
            elif check_subject_and_desc_contains(df_tmp, ["融资收益"], grades) :
                for obj in gen_df_line(df_tmp):
                    add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                              obj["vocher_num"], obj["subentry_num"], "财务费用-未实现融资收益")
            elif check_subject_and_desc_contains(df_tmp, ["租赁负债"], grades) :
                for obj in gen_df_line(df_tmp):
                    add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                              obj["vocher_num"], obj["subentry_num"], "财务费用-租赁负债利息")
            elif check_subject_and_desc_contains(df_tmp, ["资金占用"], grades) :
                for obj in gen_df_line(df_tmp):
                    add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                              obj["vocher_num"], obj["subentry_num"], "财务费用-资金占用费")
                if len([False for debit in df_tmp_financial_expense["debit"] if debit > 0.0]) == 0:
                    audit_record = AuditRecord(
                        company_name=company_name,
                        start_time=start_time,
                        end_time=end_time,
                        problem="资金占用费收入建议计入其他业务收入或投资收益，凭证见{}-{}-{}".format(record['month'],
                                                                   record['vocher_type'],
                                                                   record['vocher_num'])
                    )
                    session.add(audit_record)
                    session.commit()
            elif check_subject_and_desc_contains(df_tmp, interest_desc, grades) :
                if len([False for debit in df_tmp_financial_expense["debit"] if debit >0.0])==0:
                    for obj in gen_df_line(df_tmp):
                        add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                                  obj["vocher_num"], obj["subentry_num"], "财务费用-利息收入")
                else:
                    for obj in gen_df_line(df_tmp):
                        add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                                  obj["vocher_num"], obj["subentry_num"], "财务费用-利息支出")
            elif check_subject_and_desc_contains(df_tmp,bank_charges_desc, grades):
                for obj in gen_df_line(df_tmp):
                    add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                              obj["vocher_num"], obj["subentry_num"], "财务费用-手续费")
            elif check_subject_and_desc_contains(df_tmp,exchange_desc, grades):
                for obj in gen_df_line(df_tmp):
                    add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                              obj["vocher_num"], obj["subentry_num"], "财务费用-汇兑损益")
            else:
                for obj in gen_df_line(df_tmp):
                    add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                              obj["vocher_num"], obj["subentry_num"], "财务费用-{}".format(credit_subjects_desc))
                audit_record = AuditRecord(
                    company_name=company_name,
                    start_time=start_time,
                    end_time=end_time,
                    problem="未识别财务费用类型，凭证见{}-{}-{}".format(record['month'],
                                                               record['vocher_type'],
                                                               record['vocher_num'])
                )
                session.add(audit_record)
                session.commit()

        # 税金及附加
        if df_tmp["tb_subject"].str.contains("税金及附加").any():
            if len(df_tmp[(df_tmp["tb_subject"]=="税金及附加") &(df_tmp["direction"]=="贷")])>0:
                raise Exception("税金及附加在贷方")
            if len(credit_subjects_list) == 1:
                if credit_subjects_list[0] == "应交税费":
                    for obj in gen_df_line(df_tmp):
                        add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                                  obj["vocher_num"], obj["subentry_num"], "计提税金及附加")
                elif credit_subjects_list[0] == "银行存款":
                    for obj in gen_df_line(df_tmp):
                        add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                                  obj["vocher_num"], obj["subentry_num"], "支付税金及附加")
                else:
                    pass
            for obj in gen_df_line(df_tmp):
                add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                          obj["vocher_num"], obj["subentry_num"], "税金及附加-{}".format(credit_subjects_desc))
            audit_record = AuditRecord(
                company_name=company_name,
                start_time=start_time,
                end_time=end_time,
                problem="未识别税金及附加类型，凭证见{}-{}-{}".format(record['month'],
                                                       record['vocher_type'],
                                                       record['vocher_num'])
            )
            session.add(audit_record)
            session.commit()

        # 职工薪酬


# #         处理应付账款
#         if df_tmp["tb_subject"].str.contains("应付账款").any():
#             if len(df_tmp[(df_tmp["tb_subject"]=="应付账款") &(df_tmp["direction"]=="贷")])>0:
#                 raise Exception("应付账款在贷方")
#             auxiliary_strs = df_tmp[df_tmp["tb_subject"]=="应付账款"]["auxiliary"]
#             if len(auxiliary_strs) == 1:
#                 auxiliary = parse_auxiliary(auxiliary_strs[0])
#                 supplier = auxiliary.get("供应商")
#                 if supplier:
#                     nature = get_account_nature(df_xsz, supplier)
#                     df_xsz_new.loc[
#                         (record['month'], record['vocher_type'], record['vocher_num']),
#                         'event'] = "应付账款减少-{}".format(nature)
#                 else:
#                     raise Exception("应付账款未进行辅助核算")
#             continue


def get_entry_subjects(df_one_entry,subject_name_grade):
    '''
    获取凭证的借贷方科目
    :param df_one_entry: 一笔凭证
    :return: 借方科目列表和贷方科目列表组成的字典{"debit":debit_subjects_list,"credit":credit_subjects_list}
    '''
    debit_subjects = set()
    credit_subjects = set()
    # 获取凭证的借贷方
    for obj in gen_df_line(df_one_entry):
        if abs(obj["debit"]) > 1e-5:
            debit_subjects.add(obj[subject_name_grade])
        else:
            credit_subjects.add((obj[subject_name_grade]))

    debit_subjects_list = list(debit_subjects)
    credit_subjects_list = list(credit_subjects)
    debit_subjects_list.sort()
    credit_subjects_list.sort()

    return {"debit":debit_subjects_list,"credit":credit_subjects_list}

def analyse_entry(company_name,start_time,end_time,session,engine,add_suggestion):
    # 获取科目余额表和序时账
    df_km, df_xsz = get_new_km_xsz_df(company_name, start_time, end_time, engine, add_suggestion, session)
    # 获取所有的凭证记录
    df_xsz_record = df_xsz[["month", "vocher_num", "vocher_type"]].drop_duplicates()
    records = df_xsz_record.to_dict('records')
    # 1/按照借方和贷方分类
    dict_tmp = defaultdict(list)
    for record in records:
        # 获取每一笔凭证
        df_tmp = df_xsz[(df_xsz["month"] == record["month"])
                        & (df_xsz["vocher_num"] == record["vocher_num"])
                        & (df_xsz["vocher_type"] == record["vocher_type"])
                        ]
        # 获取凭证的借方和贷方一级科目名称
        subjects = get_entry_subjects(df_tmp,"subject_name_1")
        debit_subjects_list = subjects["debit"]
        credit_subjects_list = subjects["credit"]
        # 合并科目名称
        debit_subject_desc = "%".join(debit_subjects_list)
        credit_subjects_desc = "%".join(credit_subjects_list)
        entry_desc = debit_subject_desc + "@" + credit_subjects_desc
        dict_tmp[entry_desc].append((record["month"], record["vocher_num"], record["vocher_type"]))

    start_time = datetime.strptime(start_time, '%Y-%m-%d')
    end_time = datetime.strptime(end_time, '%Y-%m-%d')

    for key in dict_tmp:
        entryclassify = EntryClassify(
            start_time=start_time,
            end_time=end_time,
            company_name=company_name,
            step=1,
            desc=key,
            number=len(dict_tmp[key]),
            records=json.dumps(dict_tmp[key],ensure_ascii=False)
        )
        session.add(entryclassify)
    session.commit()


def anylyse_entry_next(company_name,start_time,end_time,session,engine,add_suggestion):
    #     长期待摊费用特别处理
    # 1、长期待摊费用借方代表长期资产
    # 2、长期待摊费用贷方如果是摊销代表长期资产摊销
    # 获取科目余额表和序时账
    df_km, df_xsz = get_new_km_xsz_df(company_name, start_time, end_time, engine, add_suggestion, session)
    # 获取所有的凭证记录
    df_xsz_record = df_xsz[["month", "vocher_num", "vocher_type"]].drop_duplicates()
    records = df_xsz_record.to_dict('records')
    # 1/按照借方和贷方分类
    dict_tmp = defaultdict(list)
    df_cash_std = pd.read_excel('../data/subject_contrast.xlsx',sheet_name="cash")
    df_cash_std = df_cash_std.set_index("origin")

    for record in records:
        # 获取每一笔凭证
        df_tmp = df_xsz[(df_xsz["month"] == record["month"])
                        & (df_xsz["vocher_num"] == record["vocher_num"])
                        & (df_xsz["vocher_type"] == record["vocher_type"])
                        ]
        debit_subjects = set()
        credit_subjects = set()
        # 获取凭证的借贷方
        for obj in gen_df_line(df_tmp):
            if obj["subject_name_1"] == "长期待摊费用":
                if abs(obj["debit"]) > 1e-5:
                    subjct_name="长期资产"
                else:
                    subjct_name="长期资产摊销和折旧"
            else:
                subjct_name = df_cash_std.at[obj["subject_name_1"],"cash"]
            if abs(obj["debit"]) > 1e-5:
                debit_subjects.add(subjct_name)
            else:
                credit_subjects.add(subjct_name)

        debit_subjects_list = list(debit_subjects)
        credit_subjects_list = list(credit_subjects)
        debit_subjects_list.sort()
        credit_subjects_list.sort()

        debit_subject_desc = "%".join(debit_subjects_list)
        credit_subjects_desc = "%".join(credit_subjects_list)
        entry_desc = debit_subject_desc + "@" + credit_subjects_desc
        dict_tmp[entry_desc].append((obj["month"], obj["vocher_num"], obj["vocher_type"]))

    start_time = datetime.strptime(start_time, '%Y-%m-%d')
    end_time = datetime.strptime(end_time, '%Y-%m-%d')

    for key in dict_tmp:
        entryclassify = EntryClassify(
            start_time=start_time,
            end_time=end_time,
            company_name=company_name,
            step=2,
            desc=key,
            number=len(dict_tmp[key]),
            records=json.dumps(dict_tmp[key],ensure_ascii=False)
        )
        session.add(entryclassify)
    session.commit()


def entry_third_analyse(company_name,start_time,end_time,engine):
    df = pd.read_sql_table("entryclassify",con=engine)
    df = df[(df["company_name"]==company_name) & (df["start_time"]==start_time) & (df["end_time"]==end_time)& (df["step"]==2)]
    classify = {
        "长期资产":0,
        "货币资金":0,
        "存货":0,
        "借款":0,
        "收入":0,
        "职工薪酬":0,
        "税费":0,
        "资产减值":0,
        "费用":0,
        "往来款":0,
        "利润分配":0,
        "其他":0
    }
    for item in classify:
        if item == "资产减值":
            df_tmp = df[df["desc"].str.contains("减值") | df["desc"].str.contains("准备")]
            classify[item] = df_tmp["number"].sum()
        elif item == "税费":
            df_tmp = df[df["desc"].str.contains("税")]
            classify[item] = df_tmp["number"].sum()
        elif item == "收入":
            df_tmp = df[df["desc"].str.contains("业务收入")]
            classify[item] = df_tmp["number"].sum()
        elif item == "往来款":
            df_tmp = df[~(df["desc"].str.contains("业务收入")) &
                        ~(df["desc"].str.contains("税")) &
                        ~(df["desc"].str.contains("减值")) &
                        ~(df["desc"].str.contains("准备")) &
                        ~(df["desc"].str.contains("长期资产")) &
                        ~(df["desc"].str.contains("货币资金")) &
                        ~(df["desc"].str.contains("借款")) &
                        ~(df["desc"].str.contains("存货")) &
                        ~(df["desc"].str.contains("费用")) &
                        ~(df["desc"].str.contains("职工薪酬")) &
                        (df["desc"].str.contains("往来款"))
                        ]
            classify[item] = df_tmp["number"].sum()
        elif item == "其他":
            df_tmp = df[~(df["desc"].str.contains("业务收入")) &
                        ~(df["desc"].str.contains("税")) &
                        ~(df["desc"].str.contains("减值")) &
                        ~(df["desc"].str.contains("准备")) &
                        ~(df["desc"].str.contains("长期资产")) &
                        ~(df["desc"].str.contains("货币资金")) &
                        ~(df["desc"].str.contains("借款")) &
                        ~(df["desc"].str.contains("费用")) &
                        ~(df["desc"].str.contains("往来款")) &
                        ~(df["desc"].str.contains("存货")) &
                        ~(df["desc"].str.contains("职工薪酬")) &
                        ~(df["desc"].str.contains("利润分配"))
            ]
            classify[item] = df_tmp["number"].sum()
        else:
            df_tmp = df[df["desc"].str.contains(item)]
            classify[item] = df_tmp["number"].sum()
    print(classify)
    return classify


if __name__ == '__main__':
    from src.utils import add_suggestion
    session, engine = get_session_and_engine()
    company_name = "深圳市众恒世讯科技股份有限公司"
    start_time = "2016-1-1"
    end_time = "2016-12-31"
    aduit_entry(company_name, start_time, end_time, session, engine, add_suggestion)
    # entry_third_analyse(company_name, start_time, end_time, engine)
    # analyse_entry(company_name, start_time, end_time, session, engine, add_suggestion)
    # anylyse_entry_next(company_name, start_time, end_time, session, engine, add_suggestion)
