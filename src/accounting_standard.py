# -*- coding:utf-8 -*-

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

# 销售商品、提供劳务交易
class SellEvent(TransactionEvent):
    pass

# 购买商品和劳务交易
class PurchaseEvent(TransactionEvent):
    pass

# 存货生产事项
class ProduceEvent(TransactionEvent):
    pass

# 费用交易
class Expense(TransactionEvent):
    pass

# 职工薪酬交易
class SalaryEvent(TransactionEvent):
    pass

# 税费交易
class TaxEvent(TransactionEvent):
    pass

# 资产减值事项
class AssetImpairmentEvent(TransactionEvent):
    pass

# 构建长期资产交易
class BuildLongTermAssert(TransactionEvent):
    pass

# 长期资产折旧和摊销事项
class DepreciationAmortizationEvent(TransactionEvent):
    pass

# 借款交易
class LoanEvent(TransactionEvent):
    pass

# 偿还借款交易
class RepaymentEvent(TransactionEvent):
    pass

# 支付利息交易
class PayInterestEvent(TransactionEvent):
    pass

# 利润分配交易
class ProfitDistributionEvent(TransactionEvent):
    pass

# 投资者投入交易
class InvestorInputEvent(TransactionEvent):
    pass

# 会计分录
class Record(object):

    def __init__(self,direction,subentry_num,element):
        self.direction = direction
        self.subentry_num = subentry_num
        self.element = element

# 会计凭证
class Entry(object):

    def __init__(self,year,month,vocher_type,vocher_num,records):
        self.year = year
        self.month = month
        self.vocher_type = vocher_type
        self.vocher_num = vocher_num
        self.records = records

# 会计要素
class Element(object):

    def __init__(self,name,entity,currency_type,record_time,measurement,value,right_and_duty):
        self.name = name
        self.entity = entity
        self.record_time = record_time
        self.currency_type = currency_type
        self.measurement = measurement
        self.value = value
        self.right_and_duty = right_and_duty

# 往来款
class ReceivableAndPayable(Element):
    pass

# 资产
class Asset(Element):

    def __init__(self,name,entity, currency_type, record_time, measurement, right_and_duty, value,proof,transaction_event_desc):
        '''

        :param name: 会计要素名称
        :param entity: 会计实体
        :param currency_type: 货币种类
        :param record_time: 记账时间
        :param measurement: 计价属性
        :param right_and_duty: 权责发生 true false
        :param value: 价值金额
        :param proof: 所有权证明
        :param transaction_event_desc: 交易或事项描述，通常为凭证摘要
        '''
        super(Asset, self).__init__(name,entity, currency_type, record_time, measurement, right_and_duty, value)
        self.proof = proof
        self.transaction_event_desc = transaction_event_desc
        self.benefit_inflow = True

# 资产减值
class AssetImpairment(Asset):
    def __init__(self, name, entity, currency_type, record_time, measurement, right_and_duty, value, proof,
                 transaction_event_desc):
        super(AssetImpairment, self).__init__(name, entity, currency_type, record_time, measurement, right_and_duty, value,proof,transaction_event_desc)
        self.benefit_inflow = False

# 坏账准备
class BadDebtReserve(AssetImpairment):
    pass

# 应收账款坏账准备
class AccountsReceivableBadDebtReserve(BadDebtReserve):
    pass

# 其他应收款坏账准备
class OtherReceivableBadDebtReserve(BadDebtReserve):
    pass

# 存货跌价准备
class InventoryFallingPriceReserve(AssetImpairment):
    pass

# 持有待售资产减值准备
class HoldingAssetsForSaleDepreciationeserves(AssetImpairment):
    pass

# 长期股权投资减值
class LongTermEquityInvestmentDepreciationeserves(AssetImpairment):
    pass

# 固定资产减值准备
class FixedAssetsDepreciationeserves(AssetImpairment):
    pass

#投资性房地产减值准备
class InvestmentRealEstateAssetDepreciationeserves(AssetImpairment):
    pass

# 在建工程减值准备
class ConstructionInProgressDepreciationeserves(AssetImpairment):
    pass

# 无形资产减值准备
class IntangibleAssetsDepreciationeserves(AssetImpairment):
    pass

# 可供出售金融资产减值准备
class SellableFinancialAssetDepreciationeserves(AssetImpairment):
    pass

# 持有至到期投资减值准备
class HoldingUpToMaturityInvestmentDepreciationeserves(AssetImpairment):
    pass

# 流动资产
class CurrentAsset(Asset):
    pass

# 货币资金
class MonetaryFund(CurrentAsset):
    pass

# 库存现金
class Cash(MonetaryFund):
    pass

# 银行存款
class BankDeposit(MonetaryFund):
    pass

# 其他货币资金
class OtherCurrencyFund(MonetaryFund):
    pass

# 预付款项
class Prepayment(CurrentAsset,ReceivableAndPayable):
    pass

# 应收票据
class NotesReceivable(CurrentAsset,ReceivableAndPayable):
    pass

# 应收账款
class AccountsReceivable(CurrentAsset,ReceivableAndPayable):
    pass

# 其他应收款
class OtherReceivable(CurrentAsset,ReceivableAndPayable):
    pass

# 应收股利
class DividendsReceivable(CurrentAsset,ReceivableAndPayable):
    pass

# 应收利息
class InterestReceivable(CurrentAsset,ReceivableAndPayable):
    pass

# 持有待售资产
class HoldingAssetsForSale(CurrentAsset):
    pass

# 存货
class Inventory(CurrentAsset):

    def __init__(self, name, entity, currency_type, record_time, measurement, right_and_duty, value, proof,
                 transaction_event_desc):
        super(Inventory, self).__init__(name, entity, currency_type, record_time, measurement, right_and_duty, value,proof,transaction_event_desc)
        self.include = ["原材料",
                        "材料成本差异",
                        "库存商品",
                        "发出商品",
                        "自制半成品",
                        "委托加工物资",
                        "周转材料",
                        "分期收款发出商品",
                        "包装物及低值易耗品",
                        "包装物",
                        "低值易耗品",
                        "消耗性物物资产",
                        "生产成本",
                        "劳务成本",
                        "制造费用",
                        "开发成本",
                        "开发产品",
                        "材料成本差异",
                        "商品进销差价",
                        "委托代销商品",
                        "材料采购",
                        ]

    def get_envent_type(self,entry,record):
        '''
        根据凭证，到推出会计元素所代表的的交易或事项类型
        :param entry:凭证
        :return:会计要素所代表的意义
        '''
        if not isinstance(entry,Entry):
            raise TypeError("entry 必须是Entry类型")
        if not isinstance(record,Record):
            raise TypeError("record 必须是Record类型")

        if record.direction == "借":
            credit_records = [record for record in entry.records if record.direction=="贷"]
        else:
            debit_records = [record for record in entry.records if record.direction == "借"]

# 原材料
class RawMaterial(Inventory):
    pass


#库存商品
class InventoryGoods(Inventory):
    pass

# 产成品
class FinishedProducts(Inventory):
    pass

#发出商品
class DeliveryGoods(Inventory):
    pass

# 自制半成品
class SemiFinishedProduct(Inventory):
    pass


# 委托加工物资
class EntrustedProcessingMaterials(Inventory):
    pass

# 周转材料
class RevolvingMaterials(Inventory):
    pass

# 包装物
class Packages(Inventory):
    pass

# 低值易耗品
class LowValueConsumables(Inventory):
    pass

# 消耗性生物资产
class ConsumableBiologicalAssets(Inventory):
    pass

# 生产成本
class ProductionCost(Inventory):
    pass

# 制造费用
class ManufacturingCost(Inventory):
    pass

# 劳务成本
class LaborCost(Inventory):
    pass

# 开发成本
class DevelopmentCost(Inventory):
    pass

# 开发产品
class DevelopingProduct(Inventory):
    pass

# 合同资产
class ContractAssets(CurrentAsset):
    pass

# 其他流动资产
class OtherCurrentAssets(CurrentAsset):
    pass

# 非流动资产
class NoneCurrentAsset(Asset):
    pass

# 固定资产
class FixedAsset(NoneCurrentAsset):
    pass

# 无形资产
class IntangibleAsset(NoneCurrentAsset):
    pass

# 长期待摊费用
class LongTermPendingExpense(NoneCurrentAsset):
    pass

# 在建工程
class ConstructionInProgress(NoneCurrentAsset):
    pass

# 工程物资
class EngineeringMaterials(NoneCurrentAsset):
    pass
#
# 长期股权投资
class LongTermEquityInvestment(NoneCurrentAsset):
    pass

# 投资性房地产
class InvestmentRealEstate(NoneCurrentAsset):
    pass

# 生产性生物资产
class ProductiveBiologicalAssets(NoneCurrentAsset):
    pass

# 开发支出
class DevelopmentExpenditure(NoneCurrentAsset):
    pass

# 可供出售金融资产
class SellableFinancialAsset(NoneCurrentAsset):
    pass

# 持有至到期投资
class HoldingUpToMaturityInvestment(NoneCurrentAsset):
    pass


# 递延所得税资产
class DeferredTaxAssets(NoneCurrentAsset):
    pass

# 使用权资产
class RightOfUseAssets(NoneCurrentAsset):
    pass

# 其他非流动资产
class OtherNonCurrentAssets(NoneCurrentAsset):
    pass

# 负债
class Liability(Element):

    def __init__(self,name,entity, currency_type, record_time, measurement, right_and_duty, value,proof,transaction_event_desc):
        '''

        :param name: 会计要素名称
        :param entity: 会计实体
        :param currency_type: 货币种类
        :param record_time: 记账时间
        :param measurement: 计价属性
        :param right_and_duty: 权责发生 true false
        :param value: 价值金额
        :param proof: 责任证明
        :param transaction_event_desc: 交易或事项描述，通常为凭证摘要
        '''
        super(Liability, self).__init__(name,entity, currency_type, record_time, measurement, right_and_duty, value)
        self.proof = proof
        self.benefit_inflow = False
        self.transaction_event_desc = transaction_event_desc

#借款
class Loan(Liability):
    pass

# 流动负债
class CurrentLiability(Liability):
    pass

# 短期借款
class ShortTermLoan(Loan,CurrentLiability):
    pass

# 应付票据
class NotesPayable(CurrentLiability,ReceivableAndPayable):
    pass

# 应付账款
class AccountsPayable(CurrentLiability,ReceivableAndPayable):
    pass

# 预收账款
class AdvanceReceivable(CurrentLiability,ReceivableAndPayable):
    pass

# 其他应付款
class OtherAccountsPayable(CurrentLiability,ReceivableAndPayable):
    pass

# 应付股利
class DividendPayable(CurrentLiability,ReceivableAndPayable):
    pass

# 应付利息
class InterestPayable(CurrentLiability,ReceivableAndPayable):
    pass

# 合同负债
class ContractLiability(CurrentLiability,ReceivableAndPayable):
    pass

# 持有待售负债
class HoldingLiabilitiesForSale(CurrentLiability):
    pass

# 应付职工薪酬
class PayableRemuneration(CurrentLiability):
    pass

# 应交税费
class TaxesPayable(CurrentLiability):
    pass

# 持有待售负债
class HoldingLiabilityForSale(CurrentLiability):
    pass

# 其他流动负债
class OtherCurrentLiability(CurrentLiability):
    pass

# 非流动负债
class NoneCurrentLiability(Liability):
    pass

# 长期借款
class LongTermLoan(Loan,NoneCurrentLiability):
    pass

# 递延所得税负债
class DeferredTaxLiability(NoneCurrentLiability):
    pass

# 递延收益
class DeferredEarnings(NoneCurrentLiability):
    pass

# 租赁负债
class LeaseLiability(NoneCurrentLiability):
    pass

# 应付债券
class BondsPayable(NoneCurrentLiability):
    pass

# 预计负债
class ExpectedLiability(NoneCurrentLiability):
    pass

# 长期应付款
class LongTermAccountsPayable(NoneCurrentLiability):
    pass

# 其他非流动负债
class OtherNonCurrentLiability(NoneCurrentLiability):
    pass

# 所有者权益
class OwnerEquity(Element):

    def __init__(self,name,entity, currency_type, record_time, measurement, right_and_duty, value,proof,transaction_event_desc):
        '''

        :param name: 会计要素名称
        :param entity: 会计实体
        :param currency_type: 货币种类
        :param record_time: 记账时间
        :param measurement: 计价属性
        :param right_and_duty: 权责发生 true false
        :param value: 价值金额
        '''
        super(OwnerEquity, self).__init__(name,entity, currency_type, record_time, measurement, right_and_duty, value)
        self.proof = proof
        self.transaction_event_desc = transaction_event_desc

# 股本或实收资本
class Equity(OwnerEquity):
    pass


# 资本公积
class CapitalReserve(OwnerEquity):
    pass


# 盈余公积
class SurplusReserve(OwnerEquity):
    pass


# 本年利润
class ThisYearProfit(OwnerEquity):
    pass

# 利润分配
class ProfitDistribution(OwnerEquity):
    pass


 # 收入
class Income(Element):
    def __init__(self,name,entity, currency_type, record_time, measurement, right_and_duty, value,proof,transaction_event_desc):
        '''

        :param name: 会计要素名称
        :param entity: 会计实体
        :param currency_type: 货币种类
        :param record_time: 记账时间
        :param measurement: 计价属性
        :param right_and_duty: 权责发生 true false
        :param value: 价值金额
        '''
        super(Income, self).__init__(name,entity, currency_type, record_time, measurement, right_and_duty, value)
        self.proof = proof
        self.benefit_inflow = True
        self.transaction_event_desc = transaction_event_desc

# 营业收入
class OperatingIncome(Income):
    pass

# 主营业务收入
class PrimeOperatingIncome(OperatingIncome):
    pass

# 其他业务收入
class OtherOperatingIncome(OperatingIncome):
    pass

# 其他收益
class OtherBenefits(Income):
    pass

# 投资收益
class InvestmentIncome(Income):
    pass

# 公允价值变动收益
class FairValueChangeIncome(Income):
    pass

# 利得
class Gain(Element):
    def __init__(self,name,entity, currency_type, record_time, measurement, right_and_duty, value,proof,transaction_event_desc):
        '''

        :param name: 会计要素名称
        :param entity: 会计实体
        :param currency_type: 货币种类
        :param record_time: 记账时间
        :param measurement: 计价属性
        :param right_and_duty: 权责发生 true false
        :param value: 价值金额
        '''
        super(Gain, self).__init__( name,entity, currency_type, record_time, measurement, right_and_duty, value)
        self.proof = proof
        self.benefit_inflow = True
        self.transaction_event_desc = transaction_event_desc

# 资产处置收益
class ProceedsFromAssetDisposal(Gain):
    pass

# 营业外收入
class NoneBussinessIncome(Gain):
    pass

# 费用
class Cost(Element):
    def __init__(self,name,entity, currency_type, record_time, measurement, right_and_duty, value,proof,transaction_event_desc):
        '''

        :param name: 会计要素名称
        :param entity: 会计实体
        :param currency_type: 货币种类
        :param record_time: 记账时间
        :param measurement: 计价属性
        :param right_and_duty: 权责发生 true false
        :param value: 价值金额
        '''
        super(Cost, self).__init__(name,entity, currency_type, record_time, measurement, right_and_duty, value)
        self.proof = proof
        self.benefit_inflow = False
        self.transaction_event_desc = transaction_event_desc

# 营业成本
class OperatingCost(Cost):
    pass

# 主营业务成本
class PrimeOperatingCost(OperatingCost):
    pass

# 其他业务成本
class OtherOperatingCost(OperatingCost):
    pass

# 税金及附加
class TaxSurcharge(Cost):
    pass

# 经营费用
class OperatingExpense(Cost):
    pass

# 研发费用
class RDCost(OperatingExpense):
    pass

# 管理费用
class ManagementCost(OperatingExpense):
    pass

# 销售费用
class SellingExpense(OperatingExpense):
    pass

# 财务费用
class FinancialExpense(Cost):
    pass

# 信用减值损失
class LossOfCreditImpairment(Cost):
    pass

# 资产减值损失
class AssetsImpairmentLoss(Cost):
    pass

# 所得税费用
class IncomeTaxExpenses(Cost):
    pass

# 损失
class Loss(Element):
    def __init__(self,name,entity, currency_type, record_time, measurement, right_and_duty, value,proof,transaction_event_desc):
        '''

        :param name: 会计要素名称
        :param entity: 会计实体
        :param currency_type: 货币种类
        :param record_time: 记账时间
        :param measurement: 计价属性
        :param right_and_duty: 权责发生 true false
        :param value: 价值金额
        '''
        super(Loss, self).__init__(name,entity, currency_type, record_time, measurement, right_and_duty, value)
        self.proof = proof
        self.benefit_inflow = False
        self.transaction_event_desc = transaction_event_desc

# 营业外支出
class NoneBusinessExpenses(Loss):
    pass


# 利润
class Profit(Element):
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
        super(Profit, self).__init__(name,entity, currency_type, record_time, measurement, right_and_duty, value)

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




def entry_split(entry):
    '''
    根据凭证摘要拆分凭证
    :param entry: 多笔业务放在同一笔记账凭证中
    :return: entries
    '''
    if not isinstance(entry,Entry):
        raise TypeError("凭证分拆必须传入Entry类型")

    transaction_event_descs = set([record.element.transaction_event_desc for record in entry.records ])
    entries = []
    if len(transaction_event_descs)>1:
        for transaction_event_desc in transaction_event_descs:
            records = []
            for record in entry.records:
                if record.element.transaction_event_desc == transaction_event_desc:
                    records.append(record)
            entries.append(records)
        return entries
    else:
        return [entry]


def check_entry_equal(entry):
    '''
    检查凭证是否借贷方平衡
    :param entry:
    :return:True /False
    '''
    debit_records = []
    credit_records = []
    for record in entry.records:
        if record.direction == "借":
            debit_records.append(record)
        elif record.direction == "贷":
            credit_records.append(record)
        else:
            raise ValueError(
                "{}-{}-{}-{}分录方向必须为“借”或“贷”".format(record.year, record.month, record.vocher_type, record.vocher_num))

    # 会计分录借方和贷方必须相等
    debit_records_sum = sum([record.element.value for record in debit_records])
    credit_records_sum = sum([record.element.value for record in credit_records])
    if debit_records_sum - credit_records_sum > 1e-5:
        return False
    else:
        return True



