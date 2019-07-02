from collections import defaultdict
import json
import pandas as pd
from datetime import datetime
from src.database import EntryClassify
from src.get_tb  import get_new_km_xsz_df
from src.utils import gen_df_line,  get_session_and_engine,parse_auxiliary
from settings.constant import inventory,long_term_assets,expense

def get_account_nature(df_xsz,name):
    '''
    从本年度序时账获取供应商款项性质
    :param df_xsz: 序时账
    :param name: 
    :return: 
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
                   

def add_event(company_name,start_time,end_time,session,engine,add_suggestion):
    # 获取科目余额表和序时账
    df_km, df_xsz = get_new_km_xsz_df(company_name, start_time, end_time, engine, add_suggestion, session)
    # 为序时账添加一列用来添加业务描述
    df_std = pd.read_sql_table('subjectcontrast', engine)
    df_std = df_std[["origin_subject","tb_subject"]]
    df_xsz = pd.merge(df_xsz,df_std,how="left",left_on="subject_name_1",right_on="origin_subject")
    df_xsz["tb_subject"].fillna(df_xsz["subject_name_1"],inplace=True)
    df_xsz["direction"] = df_xsz["debit"].apply(lambda x: "借" if abs(x)>0 else "贷" )
    df_xsz["event"] = ""
    # 获取所有的凭证记录
    df_xsz_record = df_xsz[["month", "vocher_num", "vocher_type"]].drop_duplicates()
    records = df_xsz_record.to_dict('records')
    df_xsz_new = df_xsz.copy().set_index(['month', 'vocher_type', 'vocher_num', 'subentry_num'])
    # 获取每一笔凭证
    for record in records:
        df_tmp = df_xsz[(df_xsz["month"] == record["month"])
                        & (df_xsz["vocher_num"] == record["vocher_num"])
                        & (df_xsz["vocher_type"] == record["vocher_type"])
                        ]
        # 处理本年利润结转凭证
        if df_tmp["tb_subject"].str.contains("本年利润").any():
            df_xsz_new.loc[
                (record['month'], record['vocher_type'], record['vocher_num']),
                'event'] = "结转本年利润"
            continue
        # 处理收入确认凭证
        if df_tmp["tb_subject"].str.contains("主营业务收入").any():
            if df_tmp[(df_tmp["tb_subject"]=="主营业务收入") &(df_tmp["direction"]=="借")]:
                raise Exception("主营业务收入在借方")
            df_xsz_new.loc[
                (record['month'], record['vocher_type'], record['vocher_num']),
                'event'] = "确认主营业务收入"
            continue

        if df_tmp["tb_subject"].str.contains("其他业务收入").any():
            if df_tmp[(df_tmp["tb_subject"]=="其他业务收入") &(df_tmp["direction"]=="借")]:
                raise Exception("其他业务收入在借方")
            df_xsz_new.loc[
                (record['month'], record['vocher_type'], record['vocher_num']),
                'event'] = "确认其他业务收入"
            continue

        # 处理应收账款减少
        if df_tmp["tb_subject"].str.contains("应收账款").any():
            # 检查应收账款是否在贷方，且贷方仅有应收账款科目
            if len(df_tmp[(df_tmp["tb_subject"]=="应收账款") & (df_tmp["direction"]=="贷")])==1:
            #     判断对方科目是否唯一
                if len(df_tmp[df_tmp["direction"]=="借"])==1:
                    opposite_subject = df_tmp[df_tmp["direction"]=="借"]["subject_name_1"].values()[0]
                    df_xsz_new.loc[
                        (record['month'], record['vocher_type'], record['vocher_num']),
                        'event'] = "应收账款减少-{}".format(opposite_subject)
                else:
                    for obj in gen_df_line(df_tmp[df_tmp["debit"].abs()>0]):
                        pass


        if df_tmp["tb_subject"].str.contains("其他业务收入").any():
            df_xsz_new.loc[
                (record['month'], record['vocher_type'], record['vocher_num']),
                'event'] = "确认其他业务收入"
            continue


        #处理成本结转
        if df_tmp["tb_subject"].str.contains("主营业务成本").any():
            df_xsz_new.loc[
                (record['month'], record['vocher_type'], record['vocher_num']),
                'event'] = "结转主营业务成本"
            continue

        if df_tmp["tb_subject"].str.contains("其他业务成本").any():
            df_xsz_new.loc[
                (record['month'], record['vocher_type'], record['vocher_num']),
                'event'] = "结转其他业务成本"
            continue
            
#         处理应付账款
        if df_tmp["tb_subject"].str.contains("应付账款").any():
            if len(df_tmp[(df_tmp["tb_subject"]=="应付账款") &(df_tmp["direction"]=="贷")])>0:
                raise Exception("应付账款在贷方")
            auxiliary_strs = df_tmp[df_tmp["tb_subject"]=="应付账款"]["auxiliary"]
            if len(auxiliary_strs) == 1:
                auxiliary = parse_auxiliary(auxiliary_strs[0])
                supplier = auxiliary.get("供应商")
                if supplier:
                    nature = get_account_nature(df_xsz, supplier)
                    df_xsz_new.loc[
                        (record['month'], record['vocher_type'], record['vocher_num']),
                        'event'] = "应付账款减少-{}".format(nature)
                else:
                    raise Exception("应付账款未进行辅助核算")
            continue

        











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
    add_event(company_name, start_time, end_time, session, engine, add_suggestion)
    # entry_third_analyse(company_name, start_time, end_time, engine)
    # analyse_entry(company_name, start_time, end_time, session, engine, add_suggestion)
    # anylyse_entry_next(company_name, start_time, end_time, session, engine, add_suggestion)
