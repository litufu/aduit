from datetime import datetime
from src.database import Auxiliary
from src.get_tb  import get_new_km_xsz_df
from src.utils import gen_df_line,  get_session_and_engine,add_suggestion

inventory = ["原材料", "包装物", "低值易耗品", "库存商品", "委托加工物资", "周转材料", "材料采购", "物资采购", "包装物及低值易耗品", "产成品", "主营业务成本"]
long_term_assets = ["固定资产", "无形资产", "在建工程", "工程物资", "长期待摊费用", "开发支出"]
expense = ["管理费用", "销售费用", "营业费用", "研发费用", "制造费用"]

def add_nature(auxiliaries,df_xsz_last,session):
    for auxiliary in auxiliaries:
        df_tmp_xsz = df_xsz_last[(df_xsz_last["auxiliary"].str.contains(auxiliary.name)) & (df_xsz_last["credit"].abs() > 0)]
        if len(df_tmp_xsz) > 0:
            for obj in gen_df_line(df_tmp_xsz[:1]):
                df_supplier_xsz = df_xsz_last[
                    (df_xsz_last["month"] == obj["month"]) &
                    (df_xsz_last["vocher_type"] == obj["vocher_type"]) &
                    (df_xsz_last["vocher_num"] == obj["vocher_num"]) &
                    (df_xsz_last["debit"].abs() > 0)
                    ]
                for i in long_term_assets:
                    if df_supplier_xsz["subject_name_1"].str.contains(i).any():
                        auxiliary.nature = "长期资产"
                        break
                for i in inventory:
                    if df_supplier_xsz["subject_name_1"].str.contains(i).any():
                        auxiliary.nature = "材料费"
                        break
                for i in expense:
                    if df_supplier_xsz["subject_name_1"].str.contains(i).any():
                        auxiliary.nature = "费用"
                        break
                session.commit()



def get_account_nature(company_name,start_time,end_time,session,engine,last=False):
    '''
    根据序时账获取辅助核算项目供应商的款项性质
    :param company_name:
    :param start_time:
    :param end_time:
    :param session:
    :param engine:
    :param last:是否对本年度没有贷方发生额的项目检查前一年度的凭证
    :return:
    '''
    # 获取本年度序时账和科目余额表
    last_year = int(start_time[:4]) - 1
    last_start_time = "{}-1-1".format(last_year)
    last_end_time = "{}-12-31".format(last_year)
    # 获取本年度序时账
    df_km, df_xsz = get_new_km_xsz_df(company_name, start_time, end_time, engine, add_suggestion, session)

    # 获取辅助核算中的供应商列表，标明性质
    start_time = datetime.strptime(start_time, '%Y-%m-%d')
    end_time = datetime.strptime(end_time, '%Y-%m-%d')
    auxiliaries = session.query(Auxiliary).filter(
        Auxiliary.company_name == company_name,
        Auxiliary.start_time == start_time,
        Auxiliary.end_time == end_time,
        Auxiliary.type_name == "供应商"
    ).all()
    add_nature(auxiliaries, df_xsz, session)

    if last:
        # 获取辅助核算中的供应商列表，标明性质
        auxiliaries = session.query(Auxiliary).filter(
            Auxiliary.company_name == company_name,
            Auxiliary.start_time == start_time,
            Auxiliary.end_time == end_time,
            Auxiliary.type_name == "供应商",
            Auxiliary.nature == ""
        ).all()
        df_km_last, df_xsz_last = get_new_km_xsz_df(company_name, last_start_time, last_end_time, engine,
                                                    add_suggestion, session)
        add_nature(auxiliaries, df_xsz_last, session)



if __name__ == '__main__':
    session,engine = get_session_and_engine()
    company_name = "深圳市众恒世讯科技股份有限公司"
    start_time = "2016-1-1"
    end_time = "2016-12-31"
    get_account_nature(company_name, start_time, end_time, session, engine, last=True)