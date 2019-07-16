# 货币资金
monetary_funds = ["库存现金","银行存款","其他货币资金"]
# 存货类科目
inventory = ["原材料", "包装物", "低值易耗品", "库存商品", "委托加工物资", "周转材料", "材料采购", "物资采购", "包装物及低值易耗品", "产成品", "主营业务成本"]
# 存货类科目包含税费
inventory_tax = ["原材料", "包装物", "低值易耗品", "库存商品", "委托加工物资", "周转材料", "材料采购", "物资采购", "包装物及低值易耗品", "产成品", "主营业务成本","应交税费"]
# 长期资产类科目
long_term_assets = ["固定资产", "无形资产", "在建工程", "工程物资", "长期待摊费用", "开发支出"]
# 长期资产类科目
long_term_assets_tax = ["固定资产", "无形资产", "在建工程", "工程物资", "长期待摊费用", "开发支出","应交税费"]
# 费用类科目
expense = ["管理费用", "销售费用", "营业费用", "研发费用", "制造费用"]
# 费用类科目包含税费
expense_tax = ["管理费用", "销售费用", "营业费用", "研发费用", "制造费用","应交税费"]
# 收入确认借方标准科目
recognition_income_debit = ["应收账款","库存现金","银行存款","预收款项","合同负债","长期应收款","应收票据"]
# 收入确认贷方标准科目
recognition_income_credit = ["应交税费","主营业务收入"]
# 应收款项
receivables = ["应收账款","预付款项","其他应收款","应收票据"]
# 应付款项
payments = ["应付账款","预收款项","其他应付款","应付票据"]
# 往来款项
receivables_and_payments = ["应收账款","预付款项","其他应收款","应付账款","预收款项","其他应付款"]
# 资产减值准备
asset_impairment = ["坏账准备","存货跌价准备",'可供出售金融资产减值准备',"持有至到期投资减值准备",
                    "长期股权投资减值准备","固定资产减值准备","在建工程减值准备","无形资产减值准备",
                    "商誉减值准备"
                    ]
monetary_funds_and_financial_fee = ["库存现金","银行存款","其他货币资金","财务费用"]
# 增值税标准销项税率
sale_rate = {
    "1994-1-1":[0.17,0.13,0.06],
    "2017-7-1":[0.17,0.11,0.06],
    "2018-5-1":[0.16,0.10,0.06],
    "2019-4-1":[0.13,0.09,0.06]
}
# 其他业务收入-租赁收入描述
other_income_rent_desc = ["出租","租赁","租金"]
# 利息描述
interest_desc = ["利息","结息"]
# 手续费描述
bank_charges_desc =  ["手续费","服务费"]
# 汇兑损益描述
exchange_desc = ["汇率","汇兑","外币","结汇","兑换","折算"]
# 职工薪酬描述
salary_desc = ["工资","奖金","福利","津贴","社会保险","社保","养老保险","劳动保险","医疗保险",
               "失业保险","工伤保险","公积金","生育保险","意外伤害险","直接人工","人工费","职工教育经费",
               "退休金","工会经费","过节费","辞退福利","职工薪酬","补充养老保险","补充医疗保险"
               ]
# 职工薪酬归集科目
salary_collection_subjects= ["管理费用", "销售费用", "营业费用", "研发费用", "制造费用","在建工程", "长期待摊费用", "开发支出","生产成本"]
# 应收票据减少标准科目名称
notes_receivable_subjects = ["银行存款","财务费用","应收票据","应付账款"]
# 财政贴息描述
interest_on_financial_subsidy = ["财政贴息", "政府贴息","贴息"]
# 政府补助
government_grants = ["政府补助", "政府补贴"]
# 应付债券-应计利息
bonds_payable_interest = ["应计利息"]
# 利息归集科目
interest_collection_subjects=["在建工程","财务费用","制造费用"]
# 科目对应的描述



subject_descs = [
    {"no":1,"subject":"本年利润","debit_only_one":False,"debit":
        [
            {"opposite":["利润分配"],"event":"本年利润结转至利润分配","problem":None},
            {"opposite":"all","event":"损益结转至本年利润","problem":None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":["利润分配"],"event":"本年利润结转至利润分配","problem":None},
            {"opposite":"all","event":"损益结转至本年利润","problem":None},
     ]},
{"no":2,"subject":"主营业务收入","debit_only_one":False,"debit":
        [
            {"opposite":"all","event":"非本年利润结转,主营业务收入在借方","problem":"非本年利润结转,主营业务收入在借方"},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"确认主营业务收入","problem":None},
     ]},
{"no":3,"subject":"其他业务收入","debit_only_one":False,"debit":
[
            {"opposite":"all","event":"非本年利润结转,主营业务收入在借方","problem":"非本年利润结转,主营业务收入在借方"},
        ],

     "credit_only_one":False,
     "credit":{"keywords":other_income_rent_desc,"contain_event":"确认其他业务收入-租赁收入","not_contain_event":"确认其他业务收入-非租赁收入"}
 },
{"no":4,"subject":"应收账款","debit_only_one":False,"debit":
        [
            {"opposite":"all","event":"应收账款增加-非收入确认","problem":"应收账款增加-非收入确认"},
        ],
     "credit_only_one":True,
     "credit":[
            {"opposite":monetary_funds,"event":"应收账款减少-收回货币资金","problem":None},
            {"opposite":monetary_funds_and_financial_fee,"event":"应收账款减少-带折扣收回货币资金","problem":None},
            {"opposite":inventory_tax,"event":"应收账款减少-交换存货","problem":None},
            {"opposite":long_term_assets_tax,"event":"应收账款减少-交换长期资产","problem":None},
            {"opposite":expense_tax,"event":"应收账款减少-转为费用","problem":None},
            {"opposite":payments,"event":"应收账款减少-冲减应付款","problem":None},
            {"opposite":receivables,"event":"应收账款减少-转为其他应收款项","problem":None},
     ]},
{"no":5,"subject":"应收票据","debit_only_one":False,"debit":
        [
            {"opposite":"all","event":"应收票据增加-非应收账款转入","problem":"应收票据增加-非应收账款转入"},
        ],
     "credit_only_one":True,
     "credit":[
            {"opposite":monetary_funds,"event":"应收票据减少-收回货币资金","problem":None},
            {"opposite":monetary_funds_and_financial_fee,"event":"应收票据减少-贴现","problem":None},
            {"opposite":payments,"event":"应收票据减少-冲减应付款","problem":None},
     ]},
{"no":6,"subject":"预收款项","debit_only_one":False,"debit":
        [
            {"opposite":"all","event":"预收款项减少-非收入确认","problem":"预收款项减少-非收入确认"},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":monetary_funds,"event":"预收款项增加-收到货币资金","problem":None},
     ]},
{"no":7,"subject":"主营业务成本","debit_only_one":False,"debit":
        [
            {"opposite":"all","event":"结转主营业务成本","problem":None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"非结转利润-主营业务成本在贷方","problem":"非结转利润-主营业务成本在贷方"},
     ]},
{"no":8,"subject":"其他业务成本","debit_only_one":False,"debit":
        [
            {"opposite":"all","event":"结转其他业务成本","problem":None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"非结转利润-其他业务成本在贷方","problem":"非结转利润-其他业务成本在贷方"},
     ]},
{"no":9,"subject":"累计折旧","debit_only_one":False,
"debit":[
            {"opposite":["固定资产"],"event":"处置固定资产","problem":None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"计提折旧","problem":None},
     ]},
{"no":10,"subject":"累计摊销","debit_only_one":False,
"debit":[
            {"opposite":["无形资产"],"event":"处置无形资产","problem":None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"无形资产摊销","problem":None},
     ]},
{"no":11,"subject":"长期待摊费用","debit_only_one":False,
"debit":[
            {"opposite":"all","event":"长期待摊费用增加","problem":None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"长期待摊费用摊销或减少","problem":None},
     ]},
{"no":12,"subject":"其他收益","debit_only_one":False,
"debit":[
            {"opposite":"all","event":"其他收益在借方","problem":"其他收益在借方"},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":["财务费用"],"event":"政府补助-其他收益-冲减借款费用","problem":None},
            {"opposite":["银行存款"],"event":"政府补助-其他收益-收到货币资金","problem":None},
            {"opposite":["递延收益"],"event":"政府补助-其他收益-递延收益摊销","problem":None},
     ]},
{"no":13,"subject":"营业外收入","debit_only_one":False,
"debit":[
            {"opposite":"all","event":"营业外收入在借方","problem":"营业外收入在借方"},
        ],
     "credit_only_one":False,
     "credit":
     {"keywords": government_grants,
      "contain_event": [
            {"opposite":["财务费用"],"event":"政府补助-营业外收入-冲减借款费用","problem":None},
            {"opposite":["银行存款"],"event":"政府补助-营业外收入-收到货币资金","problem":None},
            {"opposite":["递延收益"],"event":"政府补助-营业外收入-递延收益摊销","problem":None},
     ],
      "not_contain_event": "确认营业外收入-非政府补助项目"}
     },
{"no":14,"subject":"递延收益","debit_only_one":False,
"debit":[
            {"opposite":"all","event":"递延收益摊销","problem":None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"递延收益-收到政府补助","problem":None},
     ]},
{"no":15,"subject":"应付利息","debit_only_one":False,
"debit":[
            {"opposite":monetary_funds,"event":"支付应付利息","problem":None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"计提利息","problem":None},
     ]},
{"no":16,"subject":"应付利息","debit_only_one":False,
"debit":[
            {"opposite":monetary_funds,"event":"支付应付利息","problem":None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":interest_collection_subjects,"event":"计提利息","problem":None},
     ]},
{"no":17,"subject":"应付债券","debit_only_one":False,
"debit":
    {"keywords": bonds_payable_interest,
      "contain_event": [
            {"opposite":monetary_funds,"event":"归还应付债券本金和利息","problem":None},
     ],
    "not_contain_event": "偿还应付债券",
     },
     "credit_only_one":False,
     "credit":
     {"keywords": bonds_payable_interest,
      "contain_event": [
            {"opposite":interest_collection_subjects,"event":"计提债券利息","problem":None},
     ],
      "not_contain_event": [
            {"opposite":monetary_funds,"event":"发行债券收到现金","problem":None},
        ],
      }
     },
{"no":18,"subject":"财务费用","debit_only_one":False,
"debit":
    [
        {"keywords": ["融资费用"],"contain_event":"财务费用-未确认融资费用","problem": None},
        {"keywords": ["融资收益"],"contain_event":"财务费用-未实现融资收益","problem": None},
        {"keywords": ["租赁负债"],"contain_event":"财务费用-租赁负债利息","problem": None},
        {"keywords": ["资金占用"],"contain_event":"财务费用-资金占用费","problem": "资金占用费收入建议计入其他业务收入或投资收益"},
        {"keywords": ["利息收入","收到利息"],"contain_event":"财务费用-利息收入","problem": None},
        {"keywords": ["利息支出","支付利息"],"contain_event":"财务费用-利息支出","problem": None},
        {"keywords": bank_charges_desc,"contain_event":"财务费用-手续费","problem": None},
        {"keywords": exchange_desc,"contain_event":"财务费用-汇兑损益","problem": None},
     ],
     "credit_only_one":False,
     "credit":
        {"opposite":"all","event":"财务费用在贷方","problem":"财务费用在贷方"},
     },
{"no":19,"subject":"税金及附加","debit_only_one":False,
"debit":[
            {"opposite":["应交税费"],"event":"计提税金及附加","problem":None},
            {"opposite":["银行存款"],"event":"支付税金及附加","problem":None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"税金及附加在贷方","problem":"税金及附加在贷方"},
     ]},
{"no":20,"subject":"应付职工薪酬","debit_only_one":True,
"debit":[
            {"opposite":monetary_funds,"event":"支付职工薪酬","problem":None},
        ],
     "credit_only_one":True,
     "credit":[
            {"opposite":"all","event":"计提职工薪酬","problem":None},
     ]},
{"no":21,"subject":"资产减值损失","debit_only_one":True,
"debit":[
            {"opposite":"all","event":"计提资产减值损失","problem":None},
        ],
     "credit_only_one":True,
     "credit":[
            {"opposite":"all","event":"资产减值损失在贷方","problem":"资产减值损失在贷方"},
     ]},
{"no":22,"subject":"信用减值损失","debit_only_one":True,
"debit":[
            {"opposite":"all","event":"计提信用减值损失","problem":None},
        ],
     "credit_only_one":True,
     "credit":[
            {"opposite":"all","event":"信用减值损失在贷方","problem":"信用减值损失在贷方"},
     ]},
{"no":23,"subject":"所得税费用","debit_only_one":False,
"debit":[
            {"opposite":["应交税费"],"event":"计提所得税","problem":None},
            {"opposite":["银行存款"],"event":"支付所得税","problem":None},
            {"opposite":["递延所得税资产","递延所得税负债"],"event":"确认递延所得税费用","problem":None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"所得税费用在贷方","problem":"所得税费用在贷方"},
     ]},
{"no":24,"subject":"资产处置收益","debit_only_one":False,
"debit":[
            {"opposite":"all","event":"资产处置收益在借方","problem":"资产处置收益在借方"},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"资产处置收益","problem":None},
     ]},
{"no":25,"subject":"营业外收入","debit_only_one":False,
"debit":[
            {"opposite":"all","event":"营业外收入在借方","problem":"营业外收入在借方"},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"营业外收入","problem":None},
     ]},
{"no":26,"subject":"营业外支出","debit_only_one":False,
"debit":[
            {"opposite":"all","event":"营业外支出","problem":None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"营业外支出在贷方","problem":"营业外支出在贷方"},
     ]},
{"no":27,"subject":"短期借款","debit_only_one":False,
"debit":[
            {"opposite":"all","event":"偿还短期借款","problem":None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"取得短期借款","problem":None},
     ]},
{"no":27,"subject":"长期借款","debit_only_one":False,
"debit":[
            {"opposite":"all","event":"偿还长期借款","problem":None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"取得长期借款","problem":None},
     ]},
{"no":28,"subject":"应付股利","debit_only_one":False,
"debit":[
            {"opposite":"all","event":"支付应付股利","problem":None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"计提应付股利","problem":None},
     ]},
{"no":29,"subject":"盈余公积","debit_only_one":False,
"debit":[
    {"opposite": ["股本(实收资本）", "资本公积"], "event": "盈余公积转增资本", "problem": None},
    {"opposite": ["利润分配"], "event": "盈余公积弥补亏损", "problem": None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"提取盈余公积","problem":None},
     ]},
{"no":30,"subject":"一般风险准备","debit_only_one":False,
"debit":[
    {"opposite": ["利润分配"], "event": "一般风险准备弥补亏损", "problem": None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"提取一般风险准备","problem":None},
     ]},
{"no":31,"subject":"专项储备","debit_only_one":False,
"debit":[
    {"opposite": "all", "event": "冲减专项储备", "problem": None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"提取专项储备","problem":None},
     ]},
{"no":32,"subject":"利润分配","debit_only_one":False,
"debit":[
    {"opposite": monetary_funds, "event": "使用货币资金进行利润分配", "problem": None},
    {"opposite": ["本年利润"], "event": "本年利润结转利润分配", "problem": None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":["本年利润"],"event":"本年利润结转利润分配","problem":None},
     ]},
{"no":33,"subject":"股本(实收资本）","debit_only_one":False,
"debit":[
    {"opposite": "all", "event": "股本减少", "problem": None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":monetary_funds,"event":"收到股东投资款","problem":None},
            {"opposite":["资本公积"],"event":"资本公积转增股本","problem":None},
     ]},
{"no":34,"subject":"资本公积","debit_only_one":False,
"debit":[
    {"opposite": "all", "event": "资本公积减少", "problem": None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"资本公积增加","problem":None},
     ]},
{"no":35,"subject":"应收股利","debit_only_one":False,
"debit":[
    {"opposite": "all", "event": "确认应收股利", "problem": None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":monetary_funds,"event":"收回应收股利","problem":None},
     ]},
]
