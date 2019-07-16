from collections import defaultdict
import json
import pandas as pd
from datetime import datetime
from src.database import EntryClassify,AuditRecord,OutputTax,TransactionEvent
from src.get_tb  import get_new_km_xsz_df
from src.utils import gen_df_line,  get_session_and_engine,parse_auxiliary
from settings.constant import inventory,long_term_assets,expense,recognition_income_debit,recognition_income_credit,\
    sale_rate,salary_desc,salary_collection_subjects,subject_descs




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

def get_not_through_salary_entry(df_xsz,grades,start_time,end_time):
    # 获取未通过职工薪酬核算的凭证
    # 获取职工薪酬类科目
    # 有些企业直接支付职工薪酬，未通过职工薪酬核算
    # 识别未通过职工薪酬核算的职工薪酬
    # 步骤1：识别应付职工薪酬计提所进入的科目{费用、在建工程、开发支出}
    # 步骤2：识别所有职工薪酬归集科目所对应的科目中没有应付职工薪酬的凭证
    # 步骤3：扣除上述步骤外的凭证中含有职工薪酬描述的凭证
    df_salary_xsz_credit = df_xsz[(df_xsz["tb_subject"] == "应付职工薪酬") & (df_xsz["direction"] == "贷") ]
    df_salary_xsz_credit_record = df_salary_xsz_credit[["month", "vocher_num", "vocher_type"]].drop_duplicates()
    # 获取完整借贷方凭证
    df_salary_xsz_credit_all = pd.merge(df_xsz, df_salary_xsz_credit_record, how="inner",
                              on=["month", "vocher_num", "vocher_type"])
    df_salary_xsz_credit_all_debit = df_salary_xsz_credit_all[(df_salary_xsz_credit_all["direction"]=="借")
                                                       & (df_salary_xsz_credit_all["tb_subject"].isin(salary_collection_subjects))]
    # 获取所有应付职工薪酬归集的会计科目
    subject_nums = set([subject_num for subject_num in df_salary_xsz_credit_all_debit["subject_num"]])
    # 获取所有不包含应付职工薪酬的归集凭证
    df_xsz_salary_collection = df_xsz[(df_xsz["subject_num"].isin(subject_nums))&(df_xsz["direction"]=="借")]
    df_xsz_salary_collection_record = df_xsz_salary_collection[["month", "vocher_num", "vocher_type"]].drop_duplicates()
    df_xsz_salary_collection_all = pd.merge(df_xsz, df_xsz_salary_collection_record, how="inner",
                                        on=["month", "vocher_num", "vocher_type"])
    # 去除掉包含应付职工薪酬的凭证
    df_xsz_salary_collection_all_new = df_xsz_salary_collection_all.append(df_salary_xsz_credit_all)
    df_xsz_salary_collection_all_new.drop_duplicates(keep=False,inplace=True)
    # 获取所有序时账归集的科目
    df_xsz_salary_collection_all_new_record = df_xsz_salary_collection_all_new[["month", "vocher_num", "vocher_type"]].drop_duplicates()
    records = df_xsz_salary_collection_all_new_record.to_dict('records')
    # 遍历所有不包含应付职工薪酬的可能归集凭证
    results = []
    for record in records:
        # 获取单笔凭证
        df_tmp = df_xsz[(df_xsz["month"] == record["month"])
                        & (df_xsz["vocher_num"] == record["vocher_num"])
                        & (df_xsz["vocher_type"] == record["vocher_type"])
                        ]
        if check_subject_and_desc_contains(df_tmp, salary_desc, grades):
            res = "{}-{}-{}".format(record["month"],record["vocher_type"],record["vocher_num"])
            results.append(res)
    if len(results)>0:
        add_suggestion(kind="会计处理",
                       content="存在未通过应付职工薪酬核算的职工薪酬，建议所有职工薪酬项目应经过职工薪酬。如{}".format(results),
                       start_time=start_time,
                       end_time=end_time,
                       company_name=company_name
                       )
    return results


def entry_contain_subject(df_entry,subject):
    '''
    凭证中的tb科目是否包含某个科目
    :param df_entry: 凭证df
    :param subject: 科目名称
    :return: True /False
    '''
    return df_entry["tb_subject"].str.contains(subject).any()

def entry_subject_direction(df_entry,subject):
    '''
    返回科目所在的方向
    :param df_entry:凭证
    :param subject: 科目
    :return: "借""贷""双向"

    '''

    df_subject = df_entry[df_entry["tb_subject"]==subject].drop_duplicates("direction")
    if len(df_subject) == 1:
        return df_subject["direction"].values[0]
    else:
        return  "双向"

def add_entry_desc(company_name, start_time, end_time, session,df_entry,desc):
    '''
    为凭证所有的分录添加相同的描述
    :param company_name: 公司名
    :param start_time: 开始时间
    :param end_time: 结束时间
    :param session: 数据库session
    :param df_entry: 记账凭证
    :param desc: 描述
    :return: 向数据库添加该凭证的描述
    '''
    for obj in gen_df_line(df_entry):
        add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                  obj["vocher_num"], obj["subentry_num"], desc)

def add_audit_record(company_name,start_time,end_time,session,problem):
    '''
    添加审核记录
    :param company_name: 公司名
    :param start_time: 开始时间
    :param end_time: 结束时间
    :param session: 数据库session
    :param problem: 审核问题
    :return: 向数据库添加审核记录
    '''
    audit_record = AuditRecord(
        company_name=company_name,
        start_time=start_time,
        end_time=end_time,
        problem=problem
    )
    session.add(audit_record)
    session.commit()

def opposite_subjects_contain(opposite_subjects_list,opposite_subjects):
    '''
    判断对方科目是否全部包含在期望的科目列表中
    :param opposite_subjects_list: 对方科目列表
    :param opposite_subjects: 期望科目列表
    :return: True or False
    '''
    if isinstance(opposite_subjects,list):
        return len([False for subject in opposite_subjects_list if subject not in opposite_subjects]) == 0
    elif isinstance(opposite_subjects,str):
        if  opposite_subjects=="all":
            return True
        else:
            return False
    else:
        raise Exception("参数错误")


def opposite_contains(opposite_subjects_list,direction_descs,df_entry,record,han_direction,subject_name, opposite_subject_desc):
    '''
    根据对方科目判断
    :param opposite_subjects_list:
    :param direction_descs:
    :param df_entry:
    :param record:
    :param han_direction:
    :param subject_name:
    :param opposite_subject_desc:
    :return:
    '''
    contains = [opposite_subjects_contain(opposite_subjects_list, direction_desc["opposite"]) for direction_desc in
                direction_descs]
    if True in contains:
        contain_index = contains.index(True)
        direction_desc = direction_descs[contain_index]
        event_desc = direction_desc["event"]
        problem_desc = direction_desc["problem"]
        add_entry_desc(company_name, start_time, end_time, session, df_entry, event_desc)
        if problem_desc:
            problem = "{}，凭证见{}-{}-{}".format(problem_desc, record['month'],
                                              record['vocher_type'],
                                              record['vocher_num'])
            add_audit_record(company_name, start_time, end_time, session, problem)
    else:
        add_entry_desc(company_name, start_time, end_time, session, df_entry,
                       "{}{}-非标准-{}".format(han_direction, subject_name, opposite_subject_desc))
        problem = "记账凭证为非标准记账凭证,{}未发现对方科目，凭证见{}-{}-{}".format(subject_name, record['month'],
                                                              record['vocher_type'],
                                                              record['vocher_num'])
        add_audit_record(company_name, start_time, end_time, session, problem)

def entry_contains(direction_descs,df_entry,record,han_direction,subject_name, opposite_subject_desc,grades):
    '''
    根据凭证摘要、明细科目判断
    :param opposite_subjects_list:
    :param direction_descs:
    :param df_entry:
    :param record:
    :param han_direction:
    :param subject_name:
    :param opposite_subject_desc:
    :return:
    '''
    contains = [check_subject_and_desc_contains(df_entry, direction_desc["keywords"], grades) for direction_desc in
                direction_descs]
    if True in contains:
        contain_index = contains.index(True)
        direction_desc = direction_descs[contain_index]
        event_desc = direction_desc["contain_event"]
        problem_desc = direction_desc["problem"]
        add_entry_desc(company_name, start_time, end_time, session, df_entry, event_desc)
        if problem_desc:
            problem = "{}，凭证见{}-{}-{}".format(problem_desc, record['month'],
                                              record['vocher_type'],
                                              record['vocher_num'])
            add_audit_record(company_name, start_time, end_time, session, problem)
    else:
        add_entry_desc(company_name, start_time, end_time, session, df_entry,
                       "{}{}-非标准-{}".format(han_direction, subject_name, opposite_subject_desc))
        problem = "记账凭证为非标准记账凭证,{}未发现对方科目，凭证见{}-{}-{}".format(subject_name, record['month'],
                                                              record['vocher_type'],
                                                              record['vocher_num'])
        add_audit_record(company_name, start_time, end_time, session, problem)

def handle_keywords_dict(direction_descs,df_entry,grades,opposite_subjects_list,record,han_direction,subject_name,opposite_subject_desc):
    #处理包含关键词的数据结构
    keywords = direction_descs["keywords"]
    contain_descs = direction_descs["contain_event"]
    not_contain_descs = direction_descs["not_contain_event"]
    if check_subject_and_desc_contains(df_entry, keywords, grades):
        if isinstance(contain_descs, str):
            add_entry_desc(company_name, start_time, end_time, session, df_entry, contain_descs)
        elif isinstance(contain_descs, list):
            opposite_contains(opposite_subjects_list, contain_descs, df_entry, record, han_direction, subject_name,
                         opposite_subject_desc)
    else:
        if isinstance(not_contain_descs, str):
            add_entry_desc(company_name, start_time, end_time, session, df_entry, not_contain_descs)
        elif isinstance(not_contain_descs, list):
            opposite_contains(opposite_subjects_list, not_contain_descs, df_entry, record, han_direction, subject_name,
                         opposite_subject_desc)



def get_direction_desc(diretciton_subject_desc,direction,df_entry,record,opposite_subjects_list,subject_name,opposite_subject_desc,grades):
    '''

    :param diretciton_subject_desc: 所有科目的描述
    :param direction: 借贷方向：debit credit
    :param df_entry: 凭证df
    :param record: 凭证记录
    :param opposite_subjects_list: 对方科目列表
    :param subject_name: 科目名称
    :param opposite_subject_desc: 对方科目描述
    :param grades: 科目级次
    :return: 添加审核记录和事项描述
    '''
    direction_descs = diretciton_subject_desc[direction]
    han_direction = "借方" if direction == "debit" else "贷方"
    if isinstance(direction_descs,dict):
        # 根据关键字判断后再根据对方科目判断，仅限一个关键词组
        handle_keywords_dict(direction_descs, df_entry, grades, opposite_subjects_list, record, han_direction,
                             subject_name, opposite_subject_desc)
    elif isinstance(direction_descs,list):
        # 判断是否为包含关键词的dict
        if "keywords" in direction_descs[0]:
            # 根据凭证中包含的关键词判断凭证性质
            entry_contains(direction_descs, df_entry, record, han_direction, subject_name, opposite_subject_desc,
                           grades)
        elif "opposite" in direction_descs[0]:
            # 根据对方会计科目判断凭证性质
            opposite_contains(opposite_subjects_list, direction_descs, df_entry, record, han_direction, subject_name,
                         opposite_subject_desc)


def handle_entry(df_entry,record,subject_descs):
    # 获取凭证科目名称
    subjects = get_entry_subjects(df_entry, "subject_name_nature")
    debit_subjects_list = subjects["debit"]
    credit_subjects_list = subjects["credit"]
    # 合并科目名称
    debit_subject_desc = "%".join(debit_subjects_list)
    credit_subjects_desc = "%".join(credit_subjects_list)

    for subject_desc in subject_descs:
        subject_name = subject_desc["subject"]
        if entry_contain_subject(df_entry,subject_name):
            # 检查科目是否在借方
            direction = entry_subject_direction(df_entry,subject_name)
            if direction == "借":
                debit_only_one = subject_desc["debit_only_one"]
                if debit_only_one:
                    if len(debit_subjects_list) > 1:
                        add_entry_desc(company_name, start_time, end_time, session, df_entry,
                                       "{}借方不唯一借方{}-贷方{}".format(subject_name,debit_subject_desc,credit_subjects_desc))
                        problem = "记账凭证为非标准记账凭证,{}借方含有其他科目，凭证见{}-{}-{}".format(subject_name,record['month'],
                                                                                   record['vocher_type'],
                                                                                   record['vocher_num'])
                        add_audit_record(company_name, start_time, end_time, session, problem)
                    else:
                        get_direction_desc(subject_desc, "debit", df_entry, record, credit_subjects_list,
                                           subject_name, credit_subjects_desc)
            elif direction == "贷":
                credit_only_one = subject_desc["credit_only_one"]
                if credit_only_one:
                    if len(credit_subjects_list) > 1:
                        add_entry_desc(company_name, start_time, end_time, session, df_entry,
                                       "{}贷方不唯一借方{}-贷方{}".format(subject_name,debit_subject_desc,credit_subjects_desc))
                        problem = "记账凭证为非标准记账凭证,{}贷方含有其他科目，凭证见{}-{}-{}".format(subject_name,record['month'],
                                                                                   record['vocher_type'],
                                                                                   record['vocher_num'])
                        add_audit_record(company_name, start_time, end_time, session, problem)
                    else:
                        get_direction_desc(subject_desc, "credit", df_entry, record, debit_subjects_list,
                                           subject_name, debit_subject_desc)
            else:
                pass


def aduit_entry(company_name,start_time,end_time,session,engine,add_suggestion,subject_descs):
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

    # 获取可能未通过应付职工薪酬核算的职工薪酬项目
    not_through_salary_entries = get_not_through_salary_entry(df_xsz,grades,start_time,end_time)

    for record in records:
        # 获取单笔凭证
        df_tmp = df_xsz[(df_xsz["month"] == record["month"])
                        & (df_xsz["vocher_num"] == record["vocher_num"])
                        & (df_xsz["vocher_type"] == record["vocher_type"])
                        ]

        # 处理没有通过应付职工薪酬核算的职工薪酬
        if len(not_through_salary_entries) > 0:
            res = "{}-{}-{}".format(record["month"], record["vocher_type"], record["vocher_num"])
            if res in not_through_salary_entries:
                for obj in gen_df_line(df_tmp):
                    add_event(company_name, start_time, end_time, session, obj["month"], obj["vocher_type"],
                              obj["vocher_num"], obj["subentry_num"], "职工薪酬-未通过应付职工薪酬核算")
                continue
        # 处理其他凭证
        handle_entry(df_tmp, record, subject_descs)


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
    aduit_entry(company_name, start_time, end_time, session, engine, add_suggestion,subject_descs)
    # entry_third_analyse(company_name, start_time, end_time, engine)
    # analyse_entry(company_name, start_time, end_time, session, engine, add_suggestion)
    # anylyse_entry_next(company_name, start_time, end_time, session, engine, add_suggestion)
