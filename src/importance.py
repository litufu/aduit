

def importance_level(company_type,pre_tax_profit,income,net_assets,total_assets,mini_level=50000):
    '''

    :param company_type: 公司类型：上市公司、拟上市公司、国有企业，其他公司
    :param pre_tax_profit: 税前利润金额
    :param income: 收入金额
    :param net_assets: 净资产金额
    :param total_assets: 总资产金额
    :param mini_level: 事务所报表整体最低重要性水平
    :return:
    metrological_basis：计量基础
    metrological_basis_value：计量基础金额
    overall_report_form_level_ratio：报表整体重要性水平比例
    actual_importance_level_ratio：实际执行重要性水平比例
    uncorrected_misstatement：未更正错报名义金额比例
    '''

    if company_type == "上市公司" or company_type == "拟上市公司" :
        if pre_tax_profit * 0.05 > mini_level:
            metrological_basis = "税前利润"
            metrological_basis_value = pre_tax_profit
            overall_report_form_level_ratio = 0.05
        elif income * 0.005 > mini_level:
            metrological_basis = "营业收入"
            metrological_basis_value = income
            overall_report_form_level_ratio = 0.005
        elif net_assets * 0.01 > mini_level:
            metrological_basis = "净资产"
            metrological_basis_value = net_assets
            overall_report_form_level_ratio = 0.01
        elif net_assets * 0.0025 > mini_level:
            metrological_basis = "总资产"
            metrological_basis_value = total_assets
            overall_report_form_level_ratio = 0.0025
        else:
            metrological_basis = "事务所最低财务报表整体重要性水平"
            metrological_basis_value = mini_level
            overall_report_form_level_ratio = 1
        actual_importance_level_ratio = 0.5
        uncorrected_misstatement = 0.05
    elif company_type == "国有企业":
        if pre_tax_profit * 0.06 > mini_level:
            metrological_basis = "税前利润"
            metrological_basis_value = pre_tax_profit
            overall_report_form_level_ratio = 0.06
        elif income * 0.0075 > mini_level:
            metrological_basis = "营业收入"
            metrological_basis_value = income
            overall_report_form_level_ratio =  0.0075
        elif net_assets * 0.03 > mini_level:
            metrological_basis = "净资产"
            metrological_basis_value = net_assets
            overall_report_form_level_ratio = 0.03
        elif net_assets * 0.0035 > mini_level:
            metrological_basis = "总资产"
            metrological_basis_value = total_assets
            overall_report_form_level_ratio = 0.0035
        else:
            metrological_basis = "事务所最低财务报表整体重要性水平"
            metrological_basis_value = mini_level
            overall_report_form_level_ratio = 1
        actual_importance_level_ratio = 0.75
        uncorrected_misstatement = 0.03
    else:
        if pre_tax_profit * 0.08 > mini_level:
            metrological_basis = "税前利润"
            metrological_basis_value = pre_tax_profit
            overall_report_form_level_ratio = 0.08
        elif income * 0.01 > mini_level:
            metrological_basis = "营业收入"
            metrological_basis_value = income
            overall_report_form_level_ratio =  0.01
        elif net_assets * 0.05 > mini_level:
            metrological_basis = "净资产"
            metrological_basis_value = net_assets
            overall_report_form_level_ratio = 0.05
        elif net_assets * 0.005 > mini_level:
            metrological_basis = "总资产"
            metrological_basis_value = total_assets
            overall_report_form_level_ratio = 0.005
        else:
            metrological_basis = "事务所最低财务报表整体重要性水平"
            metrological_basis_value = mini_level
            overall_report_form_level_ratio = 1
        actual_importance_level_ratio = 0.75
        uncorrected_misstatement = 0.03

    return metrological_basis,metrological_basis_value,overall_report_form_level_ratio,\
           actual_importance_level_ratio,uncorrected_misstatement
