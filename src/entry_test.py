from collections import defaultdict
import json
import pandas as pd
from datetime import datetime
from src.database import EntryClassify
from src.get_tb  import get_new_km_xsz_df
from src.utils import gen_df_line,  get_session_and_engine

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
        debit_subjects = set()
        credit_subjects = set()
        # 获取凭证的借贷方
        for obj in gen_df_line(df_tmp):
            if abs(obj["debit"]) > 1e-5:
                debit_subjects.add(obj["subject_name_1"])
            else:
                credit_subjects.add((obj["subject_name_1"]))

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
    entry_third_analyse(company_name, start_time, end_time, engine)
    # analyse_entry(company_name, start_time, end_time, session, engine, add_suggestion)
    # anylyse_entry_next(company_name, start_time, end_time, session, engine, add_suggestion)
