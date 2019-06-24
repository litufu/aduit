# -*- coding:utf-8 -*-

from src.company import download_company
from src.import_data import save_km,save_xsz,save_hs,check_import_data
from src.get_tb import recalculation
from src.utils import add_suggestion,get_session_and_engine

def main(company_name,start_time,end_time,session, engine,km_path,xsz_path):
    download_company(company_name, session, engine)
    save_km(company_name, start_time=start_time, end_time=end_time, km_path=km_path, session=session)
    save_xsz(company_name, start_time=start_time, end_time=end_time, xsz_path=xsz_path, session=session)
    check_import_data(company_name, start_time=start_time, end_time=end_time, engine=engine)
    recalculation(company_name, start_time=start_time, end_time=end_time, engine=engine,
                  add_suggestion=add_suggestion,session=session)


if __name__ == '__main__':
    company_name = "丽珠集团利民制药厂"
    start_time = "2016-1-1"
    end_time = "2016-12-31"
    km_path = r'D:\审计\case\lm\kemu_limin.XLS'
    xsz_path = 'D:\审计\case\lm\pz_limin.XLS'
    session, engine = get_session_and_engine()
    main(company_name,start_time,end_time,session, engine,km_path,xsz_path)
