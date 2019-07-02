# -*- coding:utf-8 -*-
import re
import json


def get_translate():
    '''
    将会计准则中的汉语翻译统计到translate.json文件中
    :return:
    '''
    f = open('accounting_standard.py','r',encoding='utf-8')
    lines = f.readlines()
    my_dict = {}
    pattern = re.compile('\(.*?\):')
    for i in range(len(lines)-1):
        cur_line = lines[i].strip()
        next_line = lines[i+1].strip()
        if cur_line.startswith("#") and next_line.startswith("class"):
            key = cur_line.replace('#','').strip()
            value = next_line.replace('class','').strip()
            value = re.sub(pattern,'',value)
            my_dict[key] = value

    f.close()
    f_w = open('../settings/translate.json','w',encoding='utf-8')
    json.dump(my_dict,f_w,ensure_ascii=False)
    f_w.close()