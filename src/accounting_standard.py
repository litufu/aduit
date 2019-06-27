from enum import Enum, unique

@unique
class Measurement(Enum):
    History = 0 # 历史成本
    Reset = 1 #重置成本
    NetRealizable = 2  #可变现净值
    Present = 3 #现值
    Fair = 4 #公允价值

# 会计实体
class Entity(object):

    def __init__(self,name,start_date,end_date,assets,liabilities,owner_equities,incomes,costs,profits):
        self.name = name
        self.start_date = start_date
        self.end_date = end_date
        self.assets = assets
        self.liabilities = liabilities
        self.owner_equities = owner_equities
        self.incomes = incomes
        self.costs = costs
        self.profits = profits


# 交易或事项
class  TransactionEvent(object):

    def __init__(self,name,happen_date,desc):
        '''

        :param name: 交易或事项名称
        :param happen_date: 交易或事项发生时间
        :param desc: 交易或事项描述
        '''
        self.name = name
        self.happen_date = happen_date
        self.desc = desc



# 销售商品、提供劳务
class Sell(TransactionEvent):
    pass

# 购买商品和劳务
class Purchase(TransactionEvent):
    pass

# 存货生产
class Produce(TransactionEvent):
    pass

# 费用
class Expense(TransactionEvent):
    pass

# 职工薪酬
class Salary(TransactionEvent):
    pass

# 税费
class Tax(TransactionEvent):
    pass

# 资产减值
class AssetImpairment(TransactionEvent):
    pass

# 构建长期资产
class BuildLongTermAssert(TransactionEvent):
    pass

# 长期资产折旧和摊销
class DepreciationAmortization(TransactionEvent):
    pass

# 借款
class Loan(TransactionEvent):
    pass

# 偿还借款
class Repayment(TransactionEvent):
    pass

# 支付利息
class PayInterest(TransactionEvent):
    pass

# 利润分配
class ProfitDistribution(TransactionEvent):
    pass

# 投资者投入
class InvestorInput(TransactionEvent):
    pass


# 会计要素
class Elements(object):

    def __init__(self,name,entity,currency_type,record_time,measurement,right_and_duty,value):
        self.name = name
        self.entity = entity
        self.record_time = record_time
        self.currency_type = currency_type
        self.measurement = measurement
        self.value = value
        self.right_and_duty = right_and_duty


# 资产
class Assert(Elements):

    def __init__(self,name,entity, currency_type, record_time, measurement, right_and_duty, value,proof,impairment,source_event):
        '''

        :param name: 会计要素名称
        :param entity: 会计实体
        :param currency_type: 货币种类
        :param record_time: 记账时间
        :param measurement: 计价属性
        :param right_and_duty: 权责发生 true false
        :param value: 价值金额
        :param proof: 所有权证明
        :param impairment: 减值准备
        :param source_event: 来源的交易或事项
        '''
        Elements.__init__(self,name,entity, currency_type, record_time, measurement, right_and_duty, value)
        self.proof = proof
        self.impairment = impairment
        self.net_value = self.value - self.impairment
        self.source_event = source_event
        self.benefit_inflow = True



# 负债
class Liability(Elements):

    def __init__(self,name,entity, currency_type, record_time, measurement, right_and_duty, value,proof,source_event):
        '''

        :param name: 会计要素名称
        :param entity: 会计实体
        :param currency_type: 货币种类
        :param record_time: 记账时间
        :param measurement: 计价属性
        :param right_and_duty: 权责发生 true false
        :param value: 价值金额
        :param proof: 责任证明
        :param source_event: 来源的交易或事项
        '''
        Elements.__init__(self, name,entity, currency_type, record_time, measurement, right_and_duty, value)
        self.proof = proof
        self.source_event = source_event
        self.benefit_inflow = False


# 所有者权益
class OwnerEquity(Elements):

    def __init__(self,name,entity, currency_type, record_time, measurement, right_and_duty, value,proof,source_event):
        '''

        :param name: 会计要素名称
        :param entity: 会计实体
        :param currency_type: 货币种类
        :param record_time: 记账时间
        :param measurement: 计价属性
        :param right_and_duty: 权责发生 true false
        :param value: 价值金额
        '''
        Elements.__init__(self, name,entity, currency_type, record_time, measurement, right_and_duty, value)
        self.proof = proof
        self.source_event = source_event


# 收入
class Income(Elements):
    def __init__(self,name,entity, currency_type, record_time, measurement, right_and_duty, value,proof,source_event):
        '''

        :param name: 会计要素名称
        :param entity: 会计实体
        :param currency_type: 货币种类
        :param record_time: 记账时间
        :param measurement: 计价属性
        :param right_and_duty: 权责发生 true false
        :param value: 价值金额
        '''
        Elements.__init__(self, name,entity, currency_type, record_time, measurement, right_and_duty, value)
        self.proof = proof
        self.source_event = source_event
        self.benefit_inflow = True

# 利得
class Gain(Elements):
    def __init__(self,name,entity, currency_type, record_time, measurement, right_and_duty, value,proof,source_event):
        '''

        :param name: 会计要素名称
        :param entity: 会计实体
        :param currency_type: 货币种类
        :param record_time: 记账时间
        :param measurement: 计价属性
        :param right_and_duty: 权责发生 true false
        :param value: 价值金额
        '''
        Elements.__init__(self, name,entity, currency_type, record_time, measurement, right_and_duty, value)
        self.proof = proof
        self.source_event = source_event
        self.benefit_inflow = True

# 费用
class Cost(Elements):
    def __init__(self,name,entity, currency_type, record_time, measurement, right_and_duty, value,proof,source_event):
        '''

        :param name: 会计要素名称
        :param entity: 会计实体
        :param currency_type: 货币种类
        :param record_time: 记账时间
        :param measurement: 计价属性
        :param right_and_duty: 权责发生 true false
        :param value: 价值金额
        '''
        Elements.__init__(self, name,entity, currency_type, record_time, measurement, right_and_duty, value)
        self.proof = proof
        self.source_event = source_event
        self.benefit_inflow = False

# 损失
class Loss(Elements):
    def __init__(self,name,entity, currency_type, record_time, measurement, right_and_duty, value,proof,source_event):
        '''

        :param name: 会计要素名称
        :param entity: 会计实体
        :param currency_type: 货币种类
        :param record_time: 记账时间
        :param measurement: 计价属性
        :param right_and_duty: 权责发生 true false
        :param value: 价值金额
        '''
        Elements.__init__(self, name,entity, currency_type, record_time, measurement, right_and_duty, value)
        self.proof = proof
        self.source_event = source_event
        self.benefit_inflow = False

# 利润
class Profit(Elements):
    def __init__(self,name,entity, currency_type, record_time, measurement, right_and_duty, value):
        '''

        :param name: 会计要素名称
        :param entity: 会计实体
        :param currency_type: 货币种类
        :param record_time: 记账时间
        :param measurement: 计价属性
        :param right_and_duty: 权责发生 true false
        :param value: 价值金额
        '''
        Elements.__init__(self, name,entity, currency_type, record_time, measurement, right_and_duty, value)

# 财务报告
class FinancialReport(object):

    def __init__(self,start_date,end_date,entity,start_assets,start_liabilities,start_owner_equities):
        self.start_date = start_date
        self.end_date = end_date
        self.entity = entity
        self.start_assets = start_assets
        self.start_liabilities = start_liabilities
        self.start_owner_equities =start_owner_equities

    def balance_sheet(self):
        # 计算资产负债表
        pass

    def profit_statement(self):
        # 计算利润表
        pass

    def cash_flow_statement(self):
        # 计算现金流量表
        pass

    def note_appended(self):
        # 生成附注
        pass







