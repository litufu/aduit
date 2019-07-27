# 货币资金
monetary_funds = ["库存现金","银行存款","其他货币资金"]
# 存货类科目
inventory = ["原材料", "包装物", "低值易耗品", "库存商品", "委托加工物资", "周转材料", "材料采购", "物资采购","材料采购", "包装物及低值易耗品", "产成品", "生产成本","主营业务成本"]
# 存货类科目包含税费
inventory_tax = ["原材料", "包装物", "低值易耗品", "库存商品", "委托加工物资", "周转材料", "材料采购", "物资采购","材料采购", "包装物及低值易耗品", "产成品","应交税费","生产成本","主营业务成本"]
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
# 应交税费
tax_payable = {"应交个人所得税":"个人所得税","应交土地使用税":"土地使用税","应交堤围费":"堤围费","应交堤围防护费":"堤围费",
               "应交企业所得税":"企业所得税","应交所得税":"企业所得税","应交城市维护建设税":"城市维护建设税","应交城建税":"城市维护建设税",
               "应交房产税":"房产税","应交土地增值税":"土地增值税","应交车船使用税":"车船使用税","应交车船税":"车船使用税",
                "应交印花税":"印花税","应交城镇土地使用税":"土地使用税","应交地方教育费附加":"地方教育费附加",
                "应交消费税":"消费税","应交营业税":"营业税","应交资源税":"资源税","预交增值税":"增值税-预交增值税",
               "待抵扣进项税额":"增值税-待抵扣进项税额","已交税金":"已交税金","未交增值税":"增值税-未交增值税",
               "待转销项税额":"增值税-待转销项税额","增值税留抵税额":"增值税-增值税留抵税额","简易计税":"增值税-简易计税",
               "转让金融商品应交增值税":"增值税-转让金融商品应交增值税","代扣代交增值税":"增值税-代扣代缴增值税",
               "转出未交增值税":"增值税-转出未交增值税","减免税款":"增值税-减免税款","出口退税":"增值税-出口退税",
               "出口抵减内销产品应纳税额":"增值税-出口抵减内销产品应纳税额","销项税额":"增值税-销项税额",
               "进项税额转出":"增值税-进项税额转出","转出多交增值税":"增值税-转出多交增值税","销项税额抵减":"增值税-销项税额抵减"
               }
# 往来款项
receivables_and_payments = ["应收账款","预付款项","其他应收款","应付账款","预收款项","其他应付款"]
# 其他应收款款项性质
other_receivable_natures = [
        {"keywords": ["押金","保证金","质保金","履约金"],"contain_event":"押金及保证金","problem": None},
        {"keywords": ["罚款","赔款","保险赔款"],"contain_event":"罚款及赔款","problem": None},
        {"keywords": ["备用金","员工借款"],"contain_event":"员工备用金","problem": None},
        {"keywords": ["代缴","代垫","代付","水电费","医药费","房租费","社保","公积金",
                      "养老保险","医疗保险","失业保险","工伤保险","生育保险","社会保险"],"contain_event":"代缴代付款","problem": None},
        {"keywords": ["公司","厂","往来款","集团内部","内部往来"],"contain_event":"公司往来款","problem": None},
     ]
other_payable_natures = [
         {"keywords": ["押金", "保证金","质保金","履约金"], "contain_event": "押金及保证金", "problem": None},
         {"keywords": ["罚款", "赔款", "保险赔款"], "contain_event": "罚款及赔款", "problem": None},
         {"keywords": [ "代收", "水电费", "医药费", "房租费", "社保", "公积金",
                       "养老保险", "医疗保险", "失业保险", "工伤保险", "生育保险", "社会保险"], "contain_event": "暂收代付款", "problem": None},
         {"keywords": ["公司", "厂", "往来款","集团内部","内部往来"], "contain_event": "公司往来款", "problem": None},
      ]
# 资产减值准备
asset_impairment = ["坏账准备","存货跌价准备",'可供出售金融资产减值准备',"持有至到期投资减值准备","债权投资减值准备",
                    "长期股权投资减值准备","固定资产减值准备","在建工程减值准备","无形资产减值准备",
                    "商誉减值准备"
                    ]
# 流动资产
current_assets = [*monetary_funds,"结算备付金","拆出资金","交易性金融资产","以公允价值计量且其变动计入当期损益的金融资产",
          "衍生金融资产","应收票据","应收账款","待摊费用","预付款项","应收股利","应收利息","应收保费","应收分保账款","应收分保合同准备金",
          "内部存款","内部存款","应收补贴款","其他应收款","买入返售金融资产",*inventory,"合同资产","持有待售资产","一年内到期的非流动资产",
          "待处理财产损溢","其他流动资产"
          ]
# 非流动资产
non_current_assets = [
    "委托贷款","发放委托贷款及垫款","债权投资","可供出售金融资产","其他债权投资","持有至到期投资","长期应收款","长期股权投资",
    "其他权益工具投资","其他非流动金融资产","投资性房地产","固定资产","固定资产清理","在建工程","生产性生物资产","油气资产",
    "使用权资产","无形资产","工程物资","开发支出","商誉","长期待摊费用","递延所得税资产","其他非流动资产"
]
# 资产
assets = [*current_assets,*non_current_assets]
# 流动负债
current_liabilities = ["短期借款","向中央银行借款","吸收存款及同业存放","拆入资金","交易性金融负债","以公允价值计量且其变动计入当期损益的金融负债",
                       "衍生金融负债","应付票据","应付账款","预收款项","卖出回购金融资产款","应付手续费及佣金","应付职工薪酬","应付福利费",
                       "其他应交款","应付利息","应付股利","应交税费","预提费用","其他应付款","应付分保账款","合同负债","保险合同准备金",
                       "代理买卖证券款","代理承销证券款","持有待售负债","一年内到期的非流动负债","其他流动负债"
                       ]
# 非流动负债
non_current_liabilities = ["长期借款","应付债券","租赁负债","长期应付款","专项应付款","长期应付职工薪酬","预计负债","递延收益",
                           "递延所得税负债","附属企业往来","其他非流动负债"
                           ]
# 负债
liabilities = [*current_liabilities,*non_current_liabilities]
# 所有者权益
equity = ["股本","其他权益工具","资本公积","库存股","其他综合收益","专项储备","盈余公积","一般风险准备","少数股东权益"]
# 收入
income = ["主营业务收入","其他业务收入","租赁收入","利息收入","已赚保费","手续费及佣金收入","其他收益","投资收益","净敞口套期收益",
          "公允价值变动收益","资产处置收益","汇兑收益","营业外收入"]
# 成本费用
cost = ["主营业务成本","其他业务成本","利息支出","手续费及佣金支出","退保金","赔付支出净额","提取保险合同准备金净额","保单红利支出",
        "分保费用","税金及附加","销售费用","管理费用","研发费用","财务费用","信用减值损失","资产减值损失","营业外支出","所得税费用"]

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
# 营业成本
operating_cost = ["主营业务成本","其他业务成本"]
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
            {"opposite":"all","event":"主营业务收入在借方","problem":"主营业务收入在借方"},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"确认主营业务收入","problem":None},
     ]},
{"no":3,"subject":"其他业务收入","debit_only_one":False,"debit":
[
            {"opposite":"all","event":"其他业务收入在借方","problem":"其他业务收入在借方"},
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
            {"opposite":["应收票据"],"event":"应收账款减少-转为应收票据","problem":None},
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
{"no":7,"subject":"主营业务成本","debit_only_one":True,"debit":
        [
            {"opposite":"all","event":"结转主营业务成本","problem":None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"主营业务成本在贷方","problem":"主营业务成本在贷方"},
     ]},
{"no":8,"subject":"其他业务成本","debit_only_one":True,"debit":
        [
            {"opposite":"all","event":"结转其他业务成本","problem":None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"其他业务成本在贷方","problem":"其他业务成本在贷方"},
     ]},
{"no":8,"subject":"预计负债","debit_only_one":False,"debit":
        [
            {"opposite":monetary_funds,"event":"偿还预计负债","problem":None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":["固定资产"],"event":"预计负债-固定资产弃置义务","problem":None},
            {"opposite":["油气资产"],"event":"预计负债-油气资产弃置义务","problem":None},
            {"opposite":["财务费用"],"event":"预计负债-计提预计负债利息","problem":None},
     ]},
{"no":39,"subject":"固定资产清理","debit_only_one":False,
"debit":[
    {"opposite": monetary_funds, "event": "支付固定资产清理款", "problem": None},
    {"opposite": ["其他应付款"], "event": "应付固定资产清理款", "problem": None},
    {"opposite": ["固定资产"], "event": "固定资产转入清理", "problem": None},
    {"opposite": ["营业外收入"], "event": "营业外收入-处置固定资产", "problem": "固定资产处置不在营业外收入核算，应归集为资产处置收益"},
    {"opposite": ["资产处置收益"], "event": "资产处置收益-固定资产处置", "problem": None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":["营业外支出"],"event":"营业外支出-固定资产毁损报废","problem":"检查固定资产是否为毁损报废，无使用价值。如果不是毁损报废，应计入资产处置收益。"},
            {"opposite":["其他应收款"],"event":"处置固定资产应收款","problem":None},
            {"opposite":monetary_funds,"event":"收到固定资产清理款","problem":None},
     ]},
{"no":21,"subject":"资产减值损失","debit_only_one":True,
"debit":[
            {"opposite":"all","event":"计提资产减值损失","problem":None},
        ],
     "credit_only_one":True,
     "credit":[
            {"opposite":"all","event":"冲减资产减值损失","problem":None},
     ]},
{"no":21,"subject":"长期股权投资减值准备","debit_only_one":True,
"debit":[
            {"opposite":["长期股权投资","应收股利","投资收益"],"event":"处置长期股权投资","problem":None},
        ],
     "credit_only_one":True,
     "credit":[
            {"opposite":"all","event":"长期股权投资减值准备增加","problem":"长期股权投资减值准备增加"},
     ]},
{"no":21,"subject":"固定资产减值准备","debit_only_one":True,
"debit":[
            {"opposite":"all","event":"固定资产减值准备减少","problem":None},
        ],
     "credit_only_one":True,
     "credit":[
            {"opposite":"all","event":"固定资产减值准备增加","problem":"固定资产减值准备增加"},
     ]},
{"no":21,"subject":"无形资产减值准备","debit_only_one":True,
"debit":[
            {"opposite":["无形资产"],"event":"处置无形资产","problem":None},
        ],
     "credit_only_one":True,
     "credit":[
            {"opposite":"all","event":"无形资产减值准备增加","problem":"无形资产减值准备增加"},
     ]},
{"no":8,"subject":"长期股权投资","debit_only_one":False,"debit":
        [
            {"opposite":["银行存款"],"event":"股权投资-货币资金","problem":None},
            {"opposite":["银行存款","营业外收入"],"event":"股权投资-货币资金，权益法下初始投资成本小于应享有的被投资单位可辨认净资产公允价值份额部分","problem":None},
            {"opposite":["银行存款","资本公积"],"event":"股权投资-货币资金，同一控制下合并取得的股权投资","problem":None},
            {"opposite":["银行存款","固定资产","无形资产","营业外收入","资本公积","其他应收款","其他应付款"],"event":"股权投资-包含其他资产或负债","problem":None},
            {"opposite":["银行存款","交易性金融资产","其他权益工具投资","可供出售金融资产"],"event":"股权投资-金融资产转为股权投资","problem":None},
            {"opposite":["股本","资本公积"],"event":"股权投资-发行权益性债券","problem":None},
            {"opposite":["投资收益"],"event":"股权投资-权益法下确认投资收益","problem":None},
            {"opposite":["其他综合收益","资本公积-其他资本公积"],"event":"股权投资-其他权益变动","problem":None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":["应收股利"],"event":"确认联营企业或合营企业股利","problem":None},
            {"opposite":["银行存款"],"event":"处置长期股权投资","problem":None},
     ],
    "two_way":[
        {"keywords": ["增资","权益法转为成本法","增加投资"],"contain_event":"权益法转为成本法核算","problem": None},
        {"keywords": ["减资","成本法转为权益法","处置投资"],"contain_event":"成本法转为权益法核算","problem": None},
    ]
 },

{"no":40,"subject":"固定资产","debit_only_one":False,
"debit":[
    {"opposite": ["在建工程"], "event": "在建工程转入固定资产", "problem": None},
    {"opposite": ["库存现金","银行存款","应付账款","预付款项","其他应付款","其他应收款","长期应付款"], "event": "购入固定资产", "problem": None},
    {"opposite": ["股本","资本公积"], "event": "股东投入的固定资产", "problem": None},
    {"opposite": ["累计折旧", "投资性房地产"], "event": "投资性房地产转入固定资产-成本核算", "problem": None},
    {"opposite": ["公允价值变动损益", "投资性房地产"], "event": "投资性房地产转入固定资产-公允价值核算", "problem": None},
    {"opposite": "all", "event": "固定资产增加", "problem": "固定资产增加"},

],
     "credit_only_one":False,
     "credit":[
            {"opposite": ["库存现金","银行存款","应付账款","预付款项","其他应付款","其他应收款","长期应付款"], "event": "退回固定资产", "problem": None},
            {"opposite": ["应交税费"], "event": "固定资产进项税冲减固定资产原值", "problem":None},
            {"opposite":["累计折旧","投资性房地产"],"event":"固定资产转入投资性房地产","problem":None},
            {"opposite":["累计折旧","固定资产清理"],"event":"固定资产转入清理","problem":None},
            {"opposite":monetary_funds,"event":"处置固定资产收到货币资金","problem":"固定资产处置建议通过固定资产清理科目核算"},
            {"opposite":"all","event":"固定资产减少","problem":"固定资产减少"},

     ]},
{"no": 40, "subject": "租赁负债", "debit_only_one": False,
 "debit": [
     {"opposite": monetary_funds, "event": "支付租赁负债", "problem": None},

 ],
 "credit_only_one": False,
 "credit": [
     {"opposite": ["使用权资产"], "event": "租赁增加使用权资产", "problem": None},

 ]},
{"no":40,"subject":"使用权资产","debit_only_one":False,
"debit":[
    {"opposite": ["库存现金","银行存款","租赁负债"], "event": "租赁增加使用权资产", "problem": None},

],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"使用权资产减少","problem":"使用权资产减少"},

     ]},
{"no":40,"subject":"无形资产","debit_only_one":False,
"debit":[
    {"opposite": ["开发支出"], "event": "开发支出转入无形资产", "problem": None},
    {"opposite": ["库存现金","银行存款","应付账款","其他应付款","其他应收款","长期应付款"], "event": "购入无形资产", "problem": None},
    {"opposite": ["股本","资本公积"], "event": "股东投入的无形资产", "problem": None},
    {"opposite": ["累计摊销", "投资性房地产"], "event": "投资性房地产转入无形资产-成本核算", "problem": None},
    {"opposite": ["公允价值变动损益", "投资性房地产"], "event": "投资性房地产转入无形资产-公允价值核算", "problem": None},

],
     "credit_only_one":False,
     "credit":[
            {"opposite":["累计摊销","投资性房地产"],"event":"无形资产转入投资性房地产","problem":None},
            {"opposite": ["银行存款", "累计摊销", "无形资产减值准备", "其他应收款"], "event": "处置无形资产", "problem": None},
            {"opposite": ["管理费用"], "event": "无形资产摊销未经过累计摊销直接计入管理费用", "problem": "无形资产摊销未经过累计摊销直接计入管理费用"},
     ]},
{"no":41,"subject":"投资性房地产","debit_only_one":False,
"debit":[
    {"opposite": ["公允价值变动损益"], "event": "投资性房地产公允价值变动", "problem": None},
    {"opposite": ["开发产品","公允价值变动损益","其他综合收益"], "event": "存货转换为投资性房地产", "problem": None},
    {"opposite": ["在建工程"], "event": "在建工程车转换为投资性房地产", "problem": None},
    {"opposite": monetary_funds, "event": "现金购买投资性房地产", "problem": None},
    {"opposite": ["应付账款","其他应付款","长期应付款"], "event": "欠款购买投资性房地产", "problem": None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":["开发产品","投资性房地产累计折旧","投资性房地产累计摊销","公允价值变动损益"],"event":"投资性房地产转为存货","problem":None},
            {"opposite":["其他业务成本","主营业务成本"],"event":"处置投资性房地产","problem":None},
     ]},
{"no":11,"subject":"开发支出","debit_only_one":False,
"debit":[
            {"opposite":"all","event":"开发支出增加","problem":None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":["管理费用"],"event":"开发支出转为管理费用","problem":"开发支出-费用化支出应转入研发费用科目"},
     ]},
{"no":11,"subject":"长期待摊费用","debit_only_one":False,
"debit":[
            {"opposite":"all","event":"长期待摊费用增加","problem":None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"长期待摊费用摊销或减少","problem":None},
     ]},
{"no":9,"subject":"投资性房地产累计折旧","debit_only_one":False,
"debit":[
            {"opposite":"all","event":"投资性房地产累计折旧减少","problem":"投资性房地产累计折旧减少"},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":operating_cost,"event":"计提投资性房地产折旧","problem":None},
     ]},
{"no":9,"subject":"累计折旧","debit_only_one":False,
"debit":[
            {"opposite":"all","event":"累计折旧减少","problem":"累计折旧减少"},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"计提折旧","problem":None},
     ]},
{"no":9,"subject":"使用权资产累计折旧","debit_only_one":False,
"debit":[
            {"opposite":"all","event":"使用权资产累计折旧减少","problem":"使用权资产累计折旧减少"},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"计提使用权资产折旧","problem":None},
     ]},
{"no":10,"subject":"累计摊销","debit_only_one":False,
"debit":[
            {"opposite":"all","event":"累计摊销减少","problem":"累计摊销减少"},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"无形资产摊销","problem":None},
     ]},

{"no":12,"subject":"其他收益","debit_only_one":False,
"debit":[
            {"opposite":"all","event":"冲减其他收益","problem":None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":["财务费用"],"event":"政府补助-其他收益-冲减借款费用","problem":None},
            {"opposite":["银行存款"],"event":"政府补助-其他收益-收到货币资金","problem":None},
            {"opposite":["递延收益"],"event":"政府补助-其他收益-递延收益摊销","problem":None},
     ]},
{"no":13,"subject":"营业外收入","debit_only_one":False,
"debit":[
            {"opposite":"all","event":"冲减营业外收入","problem":None},
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
            {"opposite":monetary_funds,"event":"递延收益-收到政府补助-货币资金","problem":None},
            {"opposite":"all","event":"递延收益-收到政府补助-非货币资金","problem":"递延收益-收到政府补助-非货币资金"},
     ]},
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
        [
        {"keywords": ["融资费用"],"contain_event":"财务费用-冲减未确认融资费用","problem": None},
        {"keywords": ["融资收益"],"contain_event":"财务费用-冲减未实现融资收益","problem": None},
        {"keywords": ["租赁负债"],"contain_event":"财务费用-冲减租赁负债利息","problem": None},
        {"keywords": ["资金占用"],"contain_event":"财务费用-冲减资金占用费","problem": "资金占用费收入建议计入其他业务收入或投资收益"},
        {"keywords": ["利息收入","收到利息"],"contain_event":"财务费用-冲减利息收入","problem": None},
        {"keywords": ["利息支出","支付利息"],"contain_event":"财务费用-冲减利息支出","problem": None},
        {"keywords": bank_charges_desc,"contain_event":"财务费用-冲减手续费","problem": None},
        {"keywords": exchange_desc,"contain_event":"财务费用-冲减汇兑损益","problem": None}
         ],
     },
{"no":15,"subject":"应付利息","debit_only_one":False,
"debit":[
            {"opposite":monetary_funds,"event":"支付应付利息","problem":None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"确认应付利息","problem":None},
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
{"no":15,"subject":"其他权益工具","debit_only_one":False,
"debit":[
            {"opposite":monetary_funds,"event":"发行其他权益工具","problem":None},
        ],
     "credit_only_one":False,
     "credit":[

            {"opposite":monetary_funds,"event":"现金赎回其他权益工具","problem":None},
            {"opposite":["银行存款","库存股"],"event":"回购股票赎回其他权益工具","problem":None},
            {"opposite":["股本","资本公积","银行存款","库存股"],"event":"其他权益工具转为股本","problem":None},
            {"opposite":["应付债券"],"event":"其他权益工具转为应付债券","problem":None},
     ]},
{"no":15,"subject":"库存股","debit_only_one":False,
"debit":[
            {"opposite":monetary_funds,"event":"回购公司股份","problem":None},
            {"opposite":[*monetary_funds,"资本公积"],"event":"可能为以权益结算的股份支付","problem":None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":["股本","资本公积","盈余公积","利润分配"],"event":"注销库存股","problem":None},
            {"opposite":[*monetary_funds,"资本公积","盈余公积","利润分配"],"event":"转让库存股","problem":None},
     ]},
{"no":16,"subject":"应收利息","debit_only_one":False,
"debit":[
            {"opposite":"all","event":"确认应收利息","problem":None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":monetary_funds,"event":"收到利息收入","problem":None},
     ]},


{"no":16,"subject":"交易性金融资产","debit_only_one":False,
"debit":[
            {"opposite":["公允价值变动损益"],"event":"确认交易性金融资产公允价值变动","problem":None},
            {"opposite":monetary_funds,"event":"购买交易性金融资产","problem":None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":["公允价值变动损益"],"event":"确认交易性金融资产公允价值变动","problem":None},
            {"opposite":[*monetary_funds,"投资收益","公允价值变动损益"],"event":"处置交易性金融资产","problem":None},
     ],
 },
{"no":16,"subject":"交易性金融负债","debit_only_one":False,
"debit":[
            {"opposite":["公允价值变动损益"],"event":"确认交易性金融负债公允价值变动","problem":None},
            {"opposite":[*monetary_funds,"投资收益","公允价值变动损益"],"event":"处置交易性金融负债","problem":None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":[*monetary_funds,"投资收益"],"event":"确认交易性金融负债","problem":None},
            {"opposite":["公允价值变动损益"],"event":"确认交易性金融负债公允价值变动","problem":None},
     ],
 },
{"no":16,"subject":"以公允价值计量且其变动计入当期损益的金融资产","debit_only_one":False,
"debit":[
    {"opposite": ["公允价值变动损益"], "event": "确认以公允价值计量且其变动计入当期损益的金融资产公允价值变动", "problem": "新准则更名为交易性金融资产"},
    {"opposite": monetary_funds, "event": "购买以公允价值计量且其变动计入当期损益的金融资产", "problem": "新准则更名为交易性金融资产"},
        ],
     "credit_only_one":False,
     "credit":[
         {"opposite": [*monetary_funds, "投资收益"], "event": "处置以公允价值计量且其变动计入当期损益的金融资产", "problem": "新准则更名为交易性金融资产"},
         {"opposite": ["公允价值变动损益"], "event": "确认以公允价值计量且其变动计入当期损益的金融资产公允价值变动", "problem": "新准则更名为交易性金融资产"},
     ],
 },
{"no":16,"subject":"以公允价值计量且其变动计入当期损益的金融负债","debit_only_one":False,
"debit":[
            {"opposite":["公允价值变动损益"],"event":"确认以公允价值计量且其变动计入当期损益的金融负债公允价值变动","problem":"新准则更名为交易性金融负债"},
            {"opposite":[*monetary_funds,"投资收益","公允价值变动损益"],"event":"处置以公允价值计量且其变动计入当期损益的金融负债","problem":"新准则更名为交易性金融负债"},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":[*monetary_funds,"投资收益"],"event":"确认以公允价值计量且其变动计入当期损益的金融负债","problem":"新准则更名为交易性金融负债"},
            {"opposite":["公允价值变动损益"],"event":"确认以公允价值计量且其变动计入当期损益的金融负债公允价值变动","problem":"新准则更名为交易性金融负债"},
     ],
 },
{"no":16,"subject":"投资收益","debit_only_one":False,
"debit":[
            {"opposite":"all","event":"投资收益在借方","problem":"投资收益在借方"},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"确认投资收益","problem":None},
     ]},
{"no":16,"subject":"公允价值变动损益","debit_only_one":False,
"debit":[
            {"opposite":"all","event":"公允价值变动损益在借方","problem":"公允价值变动损益在借方"},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"确认公允价值变动损益","problem":None},
     ]},
{"no":16,"subject":"持有至到期投资","debit_only_one":False,
"debit":[
            {"opposite":"all","event":"购买持有至到期投资","problem":"新金融准则分类为债权投资"},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"持有至到期投资减少","problem":"持有至到期投资减少"},
     ],
"two_way":[
        {"keywords": ["购买","购入"],"contain_event":"购买持有至到期投资","problem": "新金融准则分类为债权投资"},
    ]
 },
{"no":16,"subject":"债权投资","debit_only_one":False,
"debit":[
            {"opposite":"all","event":"购买债权投资","problem":None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"债权投资减少","problem":"债权投资减少"},

     ],
"two_way":[
        {"keywords": ["购买","购入"],"contain_event":"购买债权投资","problem": None},
        {"keywords": ["处置","出售"],"contain_event":"处置债权投资","problem": None},
    ]
 },
{"no":16,"subject":"可供出售金融资产","debit_only_one":False,
"debit":[
            {"opposite":monetary_funds,"event":"购买可供出售金融资产","problem":"新金融工具准则中可供出售金融资产分类为其他债权工具、其他权益工具投资或交易性金融资产"},
            {"opposite":["其他综合收益"],"event":"可供出售金融资产公允价值变动调整","problem":"新金融工具准则中可供出售金融资产分类为其他债权工具、其他权益工具投资或交易性金融资产"},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":["其他综合收益"],"event":"可供出售金融资产公允价值变动调整","problem":"新金融工具准则中可供出售金融资产分类为其他债权工具、其他权益工具投资或交易性金融资产"},
     ],
"two_way":[
        {"keywords": ["购买","购入"],"contain_event":"购买可供出售金融资产","problem": "新金融工具准则中可供出售金融资产分类为其他债权工具、其他权益工具投资或交易性金融资产"},
        {"keywords": ["处置","出售"],"contain_event":"处置可供出售金融资产","problem": "新金融工具准则中可供出售金融资产分类为其他债权工具、其他权益工具投资或交易性金融资产"},
    ]
 },
{"no":16,"subject":"其他债权投资","debit_only_one":False,
"debit":[
            {"opposite":monetary_funds,"event":"购买其他债权投资","problem":None},
            {"opposite":["其他综合收益"],"event":"其他债权投资公允价值变动调整","problem":None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":["其他综合收益"],"event":"其他债权投资公允价值变动调整","problem":None},
     ],
"two_way":[
        {"keywords": ["购买","购入"],"contain_event":"购买其他债权投资","problem": None},
        {"keywords": ["处置","出售"],"contain_event":"处置其他债权投资","problem": None},
    ]
 },
{"no":16,"subject":"其他权益工具投资","debit_only_one":False,
"debit":[
            {"opposite":monetary_funds,"event":"其他权益工具投资","problem":None},
            {"opposite":["其他综合收益"],"event":"其他权益工具投资公允价值变动调整","problem":None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":["其他综合收益"],"event":"其他权益工具投资公允价值变动调整","problem":None},
     ],
"two_way":[
        {"keywords": ["购买","购入"],"contain_event":"购买其他权益工具投资","problem": None},
        {"keywords": ["处置","出售"],"contain_event":"处置其他权益工具投资","problem": None},
    ]
 },
{"no":16,"subject":"其他综合收益","debit_only_one":False,
"debit":[
            {"opposite":["盈余公积","利润分配"],"event":"其他综合收益转入留存收益","problem":None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"其他综合收益增加","problem":"其他综合收益增加"},
     ],
 },

{"no":19,"subject":"税金及附加","debit_only_one":False,
"debit":[
            {"opposite":["应交税费"],"event":"计提税金及附加","problem":None},
            {"opposite":monetary_funds,"event":"支付税金及附加","problem":None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"冲减税金及附加","problem":None},
     ]},
{"no":20,"subject":"应付职工薪酬","debit_only_one":False,
"debit":[
            {"opposite":"all","event":"支付职工薪酬","problem":None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"计提职工薪酬","problem":None},
     ]},

{"no":22,"subject":"信用减值损失","debit_only_one":True,
"debit":[
            {"opposite":"all","event":"计提信用减值损失","problem":None},
        ],
     "credit_only_one":True,
     "credit":[
            {"opposite":"all","event":"冲减信用减值损失","problem":"冲减信用减值损失"},
     ]},
{"no":23,"subject":"所得税费用","debit_only_one":False,
"debit":[
            {"opposite":["应交税费"],"event":"计提所得税","problem":None},
            {"opposite":["银行存款"],"event":"支付所得税","problem":None},
            {"opposite":["递延所得税资产","递延所得税负债"],"event":"确认递延所得税费用","problem":None},
        ],
     "credit_only_one":False,
     "credit":[
         {"opposite": ["应交税费"], "event": "冲减多计提所得税", "problem": None},
         {"opposite": ["银行存款"], "event": "收到所得税退款", "problem": None},
         {"opposite": ["递延所得税资产", "递延所得税负债"], "event": "冲减递延所得税费用", "problem": None},
     ]},
{"no":23,"subject":"递延所得税资产","debit_only_one":False,
"debit":[
            {"opposite":"all","event":"递延所得税资产增加","problem":"递延所得税资产增加"},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"递延所得税资产减少","problem":"递延所得税资产减少"},
     ]},
{"no":23,"subject":"递延所得税负债","debit_only_one":False,
"debit":[
            {"opposite":"all","event":"递延所得税负减少","problem":"递延所得税负减少"},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"递延所得税负债增加","problem":"递延所得税负债增加"},
     ]},
{"no":24,"subject":"资产处置收益","debit_only_one":False,
"debit":[
            {"opposite":"all","event":"资产处置损失","problem":None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"资产处置收益","problem":None},
     ]},
{"no":25,"subject":"营业外收入","debit_only_one":False,
"debit":[
            {"opposite":"all","event":"冲减营业外收入","problem":None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":monetary_funds,"event":"营业外收入-收到货币资金","problem":None},
            {"opposite":"all","event":"营业外收入-未收到货币资金","problem":None},
     ]},

{"no":26,"subject":"营业外支出","debit_only_one":False,
"debit":[
            {"opposite":monetary_funds,"event":"营业外支出-支付货币资金","problem":None},
            {"opposite":"all","event":"营业外支出-非支付货币资金","problem":None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"冲减营业外支出","problem":None},
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
    {"opposite": ["股本", "资本公积"], "event": "盈余公积转增资本", "problem": None},
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
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":["股本","资本公积"],"event":"未分配利润转增股本","problem":None},
     ]},
{"no":33,"subject":"股本","debit_only_one":False,
"debit":[
    {"opposite": "all", "event": "股本减少", "problem": None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":monetary_funds,"event":"收到股东投资款-货币出资","problem":None},
            {"opposite":["原材料","库存商品","产成品","低值易耗品","固定资产","应交税费"],"event":"收到股东投资款-非货币出资","problem":None},
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
{"no":35,"subject":"发出商品","debit_only_one":False,
"debit":[
    {"opposite":["原材料","库存商品","产成品"] , "event": "结转发出商品", "problem": None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":["主营业务成本","其他业务成本"],"event":"发出商品结转至成本","problem":None},
     ]},
{"no":35,"subject":"自制半成品","debit_only_one":False,
"debit":[
    {"opposite":["生产成本"] , "event": "自制半成品入库", "problem": None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":["生产成本"],"event":"领用自制半成品用于生产","problem":None},
     ]},
{"no":35,"subject":"原材料","debit_only_one":False,
"debit":[
    {"opposite":["应付账款","预付款项","应付票据","其他应收款","库存现金","银行存款","物资采购","材料采购"] , "event": "购入原材料", "problem": None},
    {"opposite":["其他应付款"] , "event": "购入原材料-贷方为其他应付款", "problem": "购入原材料-贷方为其他应付款"},
    {"opposite":["在途物资"] , "event": "在途物资结转原材料", "problem": None},
    {"opposite":["委托加工物资","应付账款","物资采购","材料采购"] , "event": "委托加工物资结转原材料", "problem": None},
    {"opposite":["生产成本"] , "event": "自制原材料", "problem": None},
        ],
     "credit_only_one":False,
     "credit":[
            # {"opposite":["主营业务成本","其他业务成本"],"event":"销售原材料结转至成本","problem":None},
            # {"opposite":["管理费用","销售费用","研发费用"],"event":"领用原材料用于费用","problem":None},
            # {"opposite":["制造费用"],"event":"领用原材料用于制造费用","problem":None},
            # {"opposite":["生产成本"],"event":"领料用于生产","problem":None},
            # {"opposite":["在建工程"],"event":"领料用于在建工程","problem":None},
            # {"opposite":["开发支出"],"event":"领料用于开发支出","problem":None},
            # {"opposite":["委托加工物资"],"event":"领料用于委外加工","problem":None},
            {"opposite":["主营业务成本","其他业务成本","管理费用","销售费用","研发费用",
                         "制造费用","生产成本","在建工程","委托加工物资","开发支出","营业外支出"],"event":"领用原材料","problem":None},
     ]},
{"no":35,"subject":"材料采购","debit_only_one":False,
"debit":[
    {"opposite":["应付账款"] , "event": "材料采购转入应付账款", "problem": None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"材料采购增加","problem":"材料采购增加"},
     ]},
{"no":35,"subject":"物资采购","debit_only_one":False,
"debit":[
    {"opposite":["应付账款"] , "event": "物资采购转入应付账款", "problem": None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"物资采购增加","problem":"物资采购增加"},
     ]},
{"no":35,"subject":"低值易耗品","debit_only_one":False,
"debit":[
    {"opposite":["应付账款","预付款项","应付票据","其他应收款","库存现金","银行存款","物资采购","材料采购"] , "event": "购入低值易耗品", "problem": None},
    {"opposite":["在途物资"] , "event": "在途物资结转低值易耗品", "problem": None},
        ],
     "credit_only_one":False,
     "credit":[
            # {"opposite":["主营业务成本","其他业务成本"],"event":"销售低值易耗品结转至成本","problem":None},
            # {"opposite":["管理费用","销售费用","研发费用"],"event":"领用值易耗品用于费用","problem":None},
            # {"opposite":["制造费用"],"event":"领用低值易耗品用于制造费用","problem":None},
            # {"opposite":["生产成本"],"event":"领低值易耗品用于生产","problem":None},
            # {"opposite":["在建工程"],"event":"领低值易耗品用于在建工程","problem":None},
            # {"opposite":["开发支出"],"event":"领低值易耗品用于开发支出","problem":None},
            # {"opposite":["委托加工物资"],"event":"领低值易耗品用于委外加工","problem":None},
            {"opposite":["主营业务成本","其他业务成本","管理费用","销售费用","研发费用",
                         "制造费用","生产成本","在建工程","委托加工物资","开发支出","营业外支出"],"event":"领用低值易耗品","problem":None},
     ]},
{"no":35,"subject":"周转材料","debit_only_one":False,
"debit":[
    {"opposite":["应付账款","预付款项","应付票据","其他应收款","库存现金","银行存款","物资采购","材料采购"] , "event": "购入周转材料", "problem": None},
    {"opposite":["在途物资"] , "event": "在途物资结转周转材料", "problem": None},
        ],
     "credit_only_one":False,
     "credit":[
            # {"opposite":["主营业务成本","其他业务成本"],"event":"销售周转材料结转至成本","problem":None},
            # {"opposite":["管理费用","销售费用","研发费用"],"event":"领用值易耗品用于费用","problem":None},
            # {"opposite":["制造费用"],"event":"领用周转材料用于制造费用","problem":None},
            # {"opposite":["生产成本"],"event":"领周转材料用于生产","problem":None},
            # {"opposite":["在建工程"],"event":"领周转材料用于在建工程","problem":None},
            # {"opposite":["开发支出"],"event":"领周转材料用于开发支出","problem":None},
            # {"opposite":["委托加工物资"],"event":"领周转材料用于委外加工","problem":None},
            {"opposite":["主营业务成本","其他业务成本","管理费用","销售费用","研发费用",
                         "制造费用","生产成本","在建工程","委托加工物资","开发支出","营业外支出"],"event":"领用周转材料","problem":None},
     ]},
{"no":35,"subject":"包装物","debit_only_one":False,
"debit":[
{"opposite":["应付账款","预付款项","应付票据","其他应收款","库存现金","银行存款","物资采购","材料采购"] , "event": "购入包装物", "problem": None},
    {"opposite":["在途物资"] , "event": "在途物资结转包装物", "problem": None},
        ],
     "credit_only_one":False,
     "credit":[
            # {"opposite":["主营业务成本","其他业务成本"],"event":"销售包装物结转至成本","problem":None},
            # {"opposite":["管理费用","销售费用","研发费用"],"event":"领用包装物用于费用","problem":None},
            # {"opposite":["制造费用"],"event":"领用包装物用于制造费用","problem":None},
            # {"opposite":["生产成本"],"event":"领包装物用于生产","problem":None},
            # {"opposite":["在建工程"],"event":"领包装物用于在建工程","problem":None},
            # {"opposite":["开发支出"],"event":"领包装物用于开发支出","problem":None},
            # {"opposite":["委托加工物资"],"event":"领包装物用于委外加工","problem":None},
            {"opposite":["主营业务成本","其他业务成本","管理费用","销售费用","研发费用",
                         "制造费用","生产成本","在建工程","委托加工物资","开发支出","营业外支出"],"event":"领用包装物","problem":None},
     ]},
{"no":35,"subject":"包装物及低值易耗品","debit_only_one":False,
"debit":[
{"opposite":["应付账款","预付款项","应付票据","其他应收款","库存现金","银行存款","物资采购","材料采购"] , "event": "购入包装物及低值易耗品", "problem": None},
    {"opposite":["在途物资"] , "event": "在途物资结转包装物及低值易耗品", "problem": None},
        ],
     "credit_only_one":False,
     "credit":[
            # {"opposite":["主营业务成本","其他业务成本"],"event":"销售包装物及低值易耗品结转至成本","problem":None},
            # {"opposite":["管理费用","销售费用","研发费用"],"event":"领用包装物及低值易耗品用于费用","problem":None},
            # {"opposite":["制造费用"],"event":"领用包装物及低值易耗品用于制造费用","problem":None},
            # {"opposite":["生产成本"],"event":"领包装物及低值易耗品用于生产","problem":None},
            # {"opposite":["在建工程"],"event":"领包装物及低值易耗品用于在建工程","problem":None},
            # {"opposite":["开发支出"],"event":"领包装物及低值易耗品用于开发支出","problem":None},
            # {"opposite":["委托加工物资"],"event":"领包装物及低值易耗品用于委外加工","problem":None},
            {"opposite":["主营业务成本","其他业务成本","管理费用","销售费用","研发费用",
                         "制造费用","生产成本","在建工程","委托加工物资","开发支出","营业外支出"],"event":"领用包装物及低值易耗品","problem":None},
     ]},
{"no":35,"subject":"库存商品","debit_only_one":False,
"debit":[
{"opposite":["应付账款","预付款项","应付票据","其他应收款","库存现金","银行存款","物资采购","材料采购"] , "event": "购入库存商品", "problem": None},
    {"opposite":["在途物资"] , "event": "在途物资结转库存商品", "problem": None},
    {"opposite":["委托加工物资","应付账款","预付账款","其他应收款","其他应付款"] , "event": "委托加工物资结转库存商品", "problem": None},
    {"opposite":["生产成本"] , "event": "自制库存商品", "problem": None},
        ],
     "credit_only_one":False,
     "credit":[
            # {"opposite":["主营业务成本","其他业务成本"],"event":"销售库存商品结转至成本","problem":None},
            # {"opposite":["管理费用","销售费用","研发费用"],"event":"领用库存商品用于费用","problem":None},
            # {"opposite":["制造费用"],"event":"领用库存商品用于制造费用","problem":None},
            # {"opposite":["生产成本"],"event":"领用库存商品用于生产","problem":None},
            # {"opposite":["在建工程"],"event":"领用库存商品用于在建工程","problem":None},
            # {"opposite":["开发支出"],"event":"领用库存商品用于开发支出","problem":None},
            # {"opposite":["委托加工物资"],"event":"领用库存商品用于委外加工","problem":None},
            {"opposite": ["主营业务成本", "其他业务成本", "管理费用", "销售费用", "研发费用",
                       "制造费用", "生产成本", "在建工程", "委托加工物资", "开发支出","营业外支出"], "event": "领用库存商品", "problem": None},
     ]},
{"no":35,"subject":"产成品","debit_only_one":False,
"debit":[
    {"opposite":["生产成本"] , "event": "生产产成品入库", "problem": None},
        ],
     "credit_only_one":False,
     "credit":[
            # {"opposite":["主营业务成本","其他业务成本"],"event":"产成品销售结转成本","problem":None},
            # {"opposite":["在建工程"],"event":"领用产成品于在建工程","problem":None},
            # {"opposite":["管理费用","销售费用","研发费用"],"event":"领用产成品用于费用","problem":None},
            # {"opposite":["制造费用"],"event":"领用产成品用于制造费用","problem":None},
            # {"opposite":["开发支出"],"event":"领用产成品用于开发支出","problem":None},
            # {"opposite":["委托加工物资"],"event":"领用产成品用于委外加工","problem":None},
            {"opposite": ["主营业务成本", "其他业务成本", "管理费用", "销售费用", "研发费用",
                       "制造费用", "生产成本", "在建工程", "委托加工物资", "开发支出","营业外支出"], "event": "领用产成品", "problem": None},
     ]},
{"no":35,"subject":"委托加工物资","debit_only_one":False,
"debit":[
    {"opposite": ["应付账款", "预付款项", "应付票据", "其他应收款", "其他应付款", "库存现金", "银行存款"], "event": "付现委外加工费用", "problem": None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"委托加工物资减少","problem":"委托加工物资减少",},
     ]},
{"no":35,"subject":"生产成本","debit_only_one":False,
"debit":[
    {"opposite":["应付账款","预付款项","应付票据","其他应收款","其他应付款","库存现金","银行存款"] , "event": "付现生产成本", "problem": None},
    {"opposite":["制造费用"] , "event": "制造费用结转生产成本", "problem": None},
        ],
     "credit_only_one":False,
     "credit":[
         {"opposite": "all", "event": "生产成本减少", "problem": "生产成本减少", },
     ]},
{"no":35,"subject":"制造费用","debit_only_one":False,
"debit":[
    {"opposite":["库存现金","银行存款","应付账款","预付款项","其他应收款","其他应付款","应付票据"] , "event": "付现制造费用", "problem": None},
        ],
     "credit_only_one":False,
     "credit":[
         {"opposite": "all", "event": "制造费用减少", "problem": "制造费用减少", },
     ]},
{"no":35,"subject":"在建工程","debit_only_one":False,
"debit":[
    {"opposite": "all", "event": "在建工程增加", "problem": "在建工程增加"},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":["应交税费"],"event":"在建工程进项税冲减在建工程原值","problem":"在建工程进项税冲减在建工程原值"},
            {"opposite":"all","event":"在建工程减少","problem":"在建工程减少"},
     ]},
{"no":35,"subject":"工程物资","debit_only_one":False,
"debit":[
    {"opposite": ["应付账款","其他应付款","长期应付款","库存现金","银行存款"], "event": "购买工程物资", "problem": None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"工程物资减少","problem":None},
     ]},
{"no":35,"subject":"销售费用","debit_only_one":False,
"debit":[
    {"opposite": "all", "event": "费用增加-销售费用增加", "problem": None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"冲减销售费用","problem":None},
     ]},
{"no":35,"subject":"管理费用","debit_only_one":False,
"debit":[
    {"opposite": "all", "event": "费用增加-管理费用增加", "problem": None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"冲减管理费用","problem":None},
     ]},
{"no":35,"subject":"研发费用","debit_only_one":False,
"debit":[
    {"opposite": "all", "event": "费用增加-研发费用增加", "problem": None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"冲减研发费用","problem":None},
     ]},
{"no":35,"subject":"应交税费","debit_only_one":False,
"debit":[
    {"opposite": monetary_funds, "event": "支付应交税费", "problem": None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"应交税费增加","problem":"应交税费增加"},
     ],
    "two_way":[
        {"keywords": ["未交增值税"], "contain_event": "未交增值税结转", "problem": None},
    ]
 },
{"no":36,"subject":"应付账款","debit_only_one":False,
"debit":[
    {"opposite": monetary_funds, "event": "支付应付账款", "problem": None},
    {"opposite": ["预付款项"], "event": "应付账款与预付账款对冲", "problem": None},
    {"opposite": ["其他应付款"], "event": "应付账款转入其他应付款", "problem": None},
    {"opposite": ["其他应收款"], "event": "应付账款与其他应收款对冲", "problem": None},
    {"opposite": ["应付票据"], "event": "应付账款转入应付票据", "problem": None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"应付账款增加","problem":"应付账款增加"},
     ]},
{"no":36,"subject":"预付款项","debit_only_one":False,
"debit":[
    {"opposite": monetary_funds, "event": "支付预付款", "problem": None},
    {"opposite": ["其他应收款"], "event": "预付账款转入其他应收款", "problem": None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"预付账款减少","problem":"预付账款减少"},
     ]},
{"no":36,"subject":"应付票据","debit_only_one":False,
"debit":[
    {"opposite": monetary_funds, "event": "支付应付票据", "problem": None},
    {"opposite": ["预付款项"], "event": "应付票据与预付账款对冲", "problem": None},
    {"opposite": ["其他应付款"], "event": "应付票据转入其他应付款", "problem": None},
    {"opposite": ["其他应收款"], "event": "应付票据与其他应收款对冲", "problem": None},
    {"opposite": ["应付账款"], "event": "应付票据转入应付账款", "problem": None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"应付票据增加","problem":"应付票据增加"},
     ]},

{"no":36,"subject":"预付款项","debit_only_one":False,
"debit":[
    {"opposite": monetary_funds, "event": "支付预付款", "problem": None},
    {"opposite": ["其他应收款"], "event": "预付账款转入其他应收款", "problem": None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"预付账款减少","problem":"预付账款减少"},
     ]},
{"no":18,"subject":"其他应收款","debit_only_one":False,
"debit":
    [
        {"keywords": ["押金","保证金","质保金","履约金"],"contain_event":"支付押金及保证金","problem": None},
        {"keywords": ["罚款","赔款","保险赔款"],"contain_event":"支付罚款及赔款","problem": None},
        {"keywords": ["备用金","员工借款"],"contain_event":"支付员工备用金","problem": None},
        {"keywords": ["代缴","代垫","代付","水电费","医药费","房租费","社保","公积金",
                      "养老保险","医疗保险","失业保险","工伤保险","生育保险","社会保险"],"contain_event":"代缴代付款","problem": None},
        {"keywords": ["公司","厂","往来款","集团内部","内部往来"],"contain_event":"公司往来款","problem": None},
     ],
     "credit_only_one":False,
     "credit":[
        {"opposite":monetary_funds,"event":"收回其他应收款","problem":None},
        {"opposite":"all","event":"其他应收款减少","problem":"其他应收款减少"},
      ]
     },
{"no":18,"subject":"其他应付款","debit_only_one":False,
"debit":
    [
        {"opposite": monetary_funds, "event": "支付其他应付款", "problem": None},
        {"opposite": "all", "event": "其他应付款减少", "problem": "其他应付款减少"},
     ],
     "credit_only_one":False,
     "credit":[
         {"keywords": ["押金", "保证金","质保金","履约金"], "contain_event": "收到押金及保证金", "problem": None},
         {"keywords": ["罚款", "赔款", "保险赔款"], "contain_event": "收到罚款及赔款", "problem": None},
         {"keywords": [ "代收", "水电费", "医药费", "房租费", "社保", "公积金",
                       "养老保险", "医疗保险", "失业保险", "工伤保险", "生育保险", "社会保险"], "contain_event": "暂收代付款", "problem": None},
         {"keywords": ["公司", "厂", "往来款","集团内部","内部往来"], "contain_event": "公司往来款", "problem": None},
      ]
     },
{"no":36,"subject":"库存现金","debit_only_one":False,
"debit":[
    {"opposite": ["银行存款"], "event": "银行提取现金", "problem": None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":["银行存款"],"event":"现金存银行","problem":None},
     ]},
{"no":36,"subject":"银行存款","debit_only_one":False,
"debit":[
    {"opposite": "all", "event": "银行存款增加", "problem": None},
        ],
     "credit_only_one":False,
     "credit":[
            {"opposite":"all","event":"银行存款减少","problem":None},
     ]},
]
