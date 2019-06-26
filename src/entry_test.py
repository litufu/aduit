from collections import defaultdict
import json
from datetime import datetime
from src.database import EntryClassify
from src.get_tb  import get_new_km_xsz_df
from src.utils import gen_df_line,  get_session_and_engine

def analyse_entry(company_name,start_time,end_time,session,engine,add_suggestion):
    # 获取科目余额表和序时账
    df_km, df_xsz = get_new_km_xsz_df(company_name, start_time, end_time, engine, add_suggestion, session)
    # 获取所有的配置记录
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
            desc=key,
            number=len(dict_tmp[key]),
            records=json.dumps(dict_tmp[key])
        )
        session.add(entryclassify)
    session.commit()



if __name__ == '__main__':
    from src.utils import add_suggestion
    session, engine = get_session_and_engine()
    company_name = "深圳市众恒世讯科技股份有限公司"
    start_time = "2016-1-1"
    end_time = "2016-12-31"
    analyse_entry(company_name, start_time, end_time, session, engine, add_suggestion)