import pandas as pd
import json

def gen_df_line(df):
    '''
    将一个dataframe转换未一个行对象生成器
    :param df: dataframe
    :return: 生成一个行对象
    '''
    for i in range(len(df)):
        line_json = df.iloc[i].to_json(orient='columns',force_ascii=False)
        yield json.loads(line_json)
