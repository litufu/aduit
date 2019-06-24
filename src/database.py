# -*- coding:utf-8 -*-

from sqlalchemy import Column, DateTime, String, Integer, ForeignKey, func,Boolean,Float,Numeric
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine

Base = declarative_base()


# 财务报表科目
class FSSubject(Base):
    __tablename__ = 'fssubject'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    show = Column(String)
    subject = Column(String)
    direction = Column(String)

class TBSubject(Base):
    __tablename__ = 'tbsubject'
    id = Column(Integer, primary_key=True)
    show = Column(String,unique=True)
    subject = Column(String)
    direction = Column(String)
    order = Column(Integer)

# 一级会计科目
class FirstClassSubject(Base):
    __tablename__ = 'firstclasssubject'
    id = Column(Integer, primary_key=True)
    code = Column(String)
    name = Column(String)

# 被审计单位
class Company(Base):
    __tablename__ = 'company'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    legal_representative=Column(String)
    establish_date = Column(DateTime)
    address = Column(String)
    code = Column(String,unique=True)
    registered_capital = Column(String)
    business_scope = Column(String)
    holders = relationship('Holder', backref='company')

# 公司股东
class Holder(Base):
    __tablename__ = 'holder'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    ratio = Column(Float)
    company_code = Column(String, ForeignKey('company.code'))

# 会计师事务所
class AccountingFirm(Base):
    __tablename__ = 'accountingfirm'
    id = Column(Integer, primary_key=True)
    code = Column(String)
    name = Column(String)
    address = Column(String)
    contact =  Column(String)
    phone = Column(String)
    users = relationship('User', backref='user')

# 审计人员
class User(Base):
    __tablename__ = 'user'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    accountingfirm_code = Column(String, ForeignKey('accountingfirm.code'))

# 辅助核算
# 科目名称	科目编号	核算项目类型编号	借贷方向	核算项目类型名称	核算项目编号	核算项目名称
class Auxiliary(Base):
    __tablename__ = 'auxiliary'
    id = Column(Integer, primary_key=True)
    company_code = Column(String, ForeignKey('company.code'))
    company_name = Column(String, ForeignKey('company.name'))
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    subject_num = Column(String)
    subject_name = Column(String)
    type_num = Column(String)
    type_name = Column(String)
    code = Column(String)
    name = Column(String)
    direction = Column(String)
    initial_amount = Column(Numeric(20,4))
    debit_amount = Column(Numeric(20,4))
    credit_amount = Column(Numeric(20,4))
    terminal_amount = Column(Numeric(20,4))
    initial_num = Column(Numeric(20,4))
    debit_num = Column(Numeric(20,4))
    credit_num = Column(Numeric(20,4))
    terminal_num = Column(Numeric(20,4))

# 科目余额表
# 科目编号	科目名称	科目类别	借贷方向	是否明细科目	科目级次	账面期初数	账面借方发生额	账面贷方发生额	账面期末数
class SubjectBalance(Base):
    __tablename__ = 'subjectbalance'
    id = Column(Integer, primary_key=True)
    company_code = Column(String, ForeignKey('company.code'))
    company_name = Column(String, ForeignKey('company.name'))
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    subject_num = Column(String)
    subject_name = Column(String)
    subject_type = Column(String)
    direction = Column(String)
    is_specific = Column(Boolean)
    subject_gradation = Column(Integer)
    initial_amount = Column(Numeric(20,4))
    debit_amount =  Column(Numeric(20,4))
    credit_amount = Column(Numeric(20,4))
    terminal_amount = Column(Numeric(20,4))

# 序时账
# 会计年	会计月	记账时间	凭证编号	凭证种类	编号	业务说明	科目编号	科目名称	借方发生额	贷方发生额	借方发生额_外币	贷方发生额_外币
#  借方数量	贷方数量	借方单价	贷方单价 货币种类 核算项目名称
class ChronologicalAccount(Base):
    __tablename__ = 'chronologicalaccount'
    id = Column(Integer, primary_key=True)
    company_code = Column(String, ForeignKey('company.code'))
    company_name = Column(String, ForeignKey('company.name'))
    year = Column(Integer)
    month = Column(Integer)
    record_time = Column(DateTime)
    vocher_type = Column(String)
    vocher_num = Column(Integer)
    subentry_num = Column(Integer)
    description = Column(String)
    subject_num = Column(String)
    subject_name = Column(String)
    currency_type = Column(String)
    debit = Column(Numeric(20,4))
    credit = Column(Numeric(20,4))
    debit_foreign_currency = Column(Numeric(20,4))
    credit_foreign_currency = Column(Numeric(20,4))
    debit_number = Column(Numeric(20,4))
    credit_number = Column(Numeric(20,4))
    debit_price = Column(Numeric(20,4))
    credit_price = Column(Numeric(20,4))
    auxiliary = Column(String)

class SubjectContrast(Base):
    __tablename__ = 'subjectcontrast'
    id = Column(Integer, primary_key=True)
    origin_subject = Column(String,unique=True)
    tb_subject = Column(String)
    fs_subject = Column(String)
    coefficient = Column(Integer)
    direction = Column(String)
    first_class = Column(String)
    second_class =  Column(String)

class Suggestion(Base):
    __tablename__ = 'suggestion'
    id = Column(Integer, primary_key=True)
    company_name=Column(String, ForeignKey('company.name'))
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    kind = Column(String)
    content = Column(String)

class TB(Base):
    __tablename__ = 'tb'
    id = Column(Integer, primary_key=True)
    company_name = Column(String, ForeignKey('company.name'))
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    subject_name = Column(String)
    direction = Column(String)
    amount = Column(Numeric(20,4))


if __name__ == '__main__':
    engine = create_engine('sqlite:///audit.sqlite?check_same_thread=False')
    Base.metadata.create_all(engine)