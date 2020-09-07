#! usr/bin/env python3
# -*- coding:utf-8 -*-
import pandas as pd
import json

def chexi_transform_id():
    path='large_area/车系分档202004（除云南、深圳、江苏）.xlsx'
    datas=pd.read_excel(path,skiprows=1)
    df=pd.DataFrame(datas,columns=['车系名称','工时标准档位'])
    VEHSERINAME_ID2alpha={1:'A(180档)',2:'B(240档)',3:'C(300档)',4:'D(380档)',5:'E(450档)',6:'F(500档)',7:'G(600档)',8:'H(680档)',9:'J(800档)',10:'K(1000档)'}
    alpha2VEHSERINAME_ID={v:k for k,v in VEHSERINAME_ID2alpha.items()}
    def get_upper(x):
        try:
            con = x.upper()
        except:
            con = x
        return con

    df['工时标准档位'].replace('（', '(', regex=True, inplace=True)
    df['工时标准档位'].replace('）', ')', regex=True, inplace=True)
    df['NUM']=df['工时标准档位'].map(alpha2VEHSERINAME_ID)
    df['车系名称']=df['车系名称'].apply(get_upper)
    chexi2id = dict(zip(df['车系名称'], df['NUM']))
    group_dict,group2id=gaoduan_chexi_transform_id()
    for chexi in group_dict:
        if chexi in chexi2id:
            del chexi2id[chexi]
    with open('large_area/chexi2id.json','w',encoding='utf-8') as writer:
        json.dump(chexi2id,writer,ensure_ascii=False)
    return chexi2id

def yunnan_chexi_transform_id():
    path='yunnan/云南车系分档2020（上报）.xlsx'
    datas=pd.read_excel(path,skiprows=1)
    df=pd.DataFrame(datas,columns=['车系名称','工时标准档位'])
    VEHSERINAME_ID2alpha = {1: 'A(180档)', 2: 'B(240档)', 3: 'C(300档)', 4: 'D(380档)', 5: 'E(450档)', 6: 'F(500档)',7: 'G(600档)', 8: 'H(680档)', 9: 'J(800档)', 10: 'K(1000档)'}
    alpha2VEHSERINAME_ID={v:k for k,v in VEHSERINAME_ID2alpha.items()}
    def get_upper(x):
        try:
            con = x.upper()
        except:
            con = x
        return con

    df['工时标准档位'].replace('（','(', regex=True, inplace=True)
    df['工时标准档位'].replace('）',')', regex=True, inplace=True)
    df['NUM']=df['工时标准档位'].map(alpha2VEHSERINAME_ID)
    df['车系名称']=df['车系名称'].apply(get_upper)
    chexi2id = dict(zip(df['车系名称'], df['NUM']))
    group_dict,group2id=gaoduan_chexi_transform_id()
    for chexi in group_dict:
        if chexi in chexi2id:
            del chexi2id[chexi]
    with open('yunnan/chexi2id.json','w',encoding='utf-8') as writer:
        json.dump(chexi2id,writer,ensure_ascii=False)
    return chexi2id

def jiangsu_chexi_transform_id():
    path='jiangsu/江苏车系分档2020.xlsx'
    datas=pd.read_excel(path,skiprows=1)
    df=pd.DataFrame(datas,columns=['车系名称','工时标准档位'])
    VEHSERINAME_ID2alpha = {1: 'A(180档)', 2: 'B(240档)', 3: 'C(300档)', 4: 'D(380档)', 5: 'E(450档)', 6: 'F(500档)',7: 'G(600档)', 8: 'H(680档)', 9: 'J(800档)', 10: 'K(1000档)'}
    alpha2VEHSERINAME_ID={v:k for k,v in VEHSERINAME_ID2alpha.items()}
    def get_upper(x):
        try:
            con = x.upper()
        except:
            con = x
        return con

    df['工时标准档位'].replace('（', '(', regex=True, inplace=True)
    df['工时标准档位'].replace('）', ')', regex=True, inplace=True)
    df['NUM']=df['工时标准档位'].map(alpha2VEHSERINAME_ID)
    df['车系名称']=df['车系名称'].apply(get_upper)
    chexi2id = dict(zip(df['车系名称'], df['NUM']))
    group_dict,group2id=gaoduan_chexi_transform_id()
    for chexi in group_dict:
        if chexi in chexi2id:
            del chexi2id[chexi]
    with open('jiangsu/chexi2id.json','w',encoding='utf-8') as writer:
        json.dump(chexi2id,writer,ensure_ascii=False)
    return chexi2id

def shenzhen_chexi_transform_id():
    path='shenzhen/深圳车系分档2020.xlsx'
    datas=pd.read_excel(path,skiprows=1)
    df=pd.DataFrame(datas,columns=['车系名称','工时标准档位'])
    VEHSERINAME_ID2alpha = {1: 'A(180档)', 2: 'B(240档)', 3: 'C(300档)', 4: 'D(380档)', 5: 'E(450档)', 6: 'F(500档)',7: 'G(600档)', 8: 'H(680档)', 9: 'J(800档)', 10: 'K(1000档)'}
    alpha2VEHSERINAME_ID={v:k for k,v in VEHSERINAME_ID2alpha.items()}
    def get_upper(x):
        try:
            con = x.upper()
        except:
            con = x
        return con

    df['工时标准档位'].replace('（', '(', regex=True, inplace=True)
    df['工时标准档位'].replace('）', ')', regex=True, inplace=True)
    df['NUM']=df['工时标准档位'].map(alpha2VEHSERINAME_ID)
    df['车系名称']=df['车系名称'].apply(get_upper)
    chexi2id = dict(zip(df['车系名称'], df['NUM']))
    group_dict,group2id=gaoduan_chexi_transform_id()
    for chexi in group_dict:
        if chexi in chexi2id:
            del chexi2id[chexi]
    with open('shenzhen/chexi2id.json','w',encoding='utf-8') as writer:
        json.dump(chexi2id,writer,ensure_ascii=False)
    return chexi2id

def gaoduan_chexi_transform_id():
    path = 'data/工时价格标准采集模板(发联保).xlsx'
    datas = pd.read_excel(path, sep='\t', sheet_name='25个高端品牌服务站工时标准（范例）', skiprows=1)
    datas = datas.iloc[0:1, 2:]
    columns = datas.columns
    group_dict = {}
    for col in columns:
        value_list = str(datas[col][0]).split('、')
        for value in value_list:
            group_dict[value.upper()] = col
    # group2id = dict(zip(list(columns), range(1, len(list(columns)) + 1)))
    group2id = {'奥迪A组':1,'JeepA组':2,'JeepB组':3,'奥迪B组':4,'宝马A组':5,'之诺':6,'凯迪拉克':7,'讴歌':8,'捷豹':9,'特斯拉':10,'路虎':11,'英菲尼迪':12,'沃尔沃':13,\
                '悍马':14,'雷克萨斯':15,'奔驰A组':16,'奥迪C组':17,'宝马B组':18,'奔驰B组':19,'林肯':20,'宝马C组':21,'奥迪D组':22,'奔驰C组':23,'阿尔法·罗密欧':24,\
                '迈巴赫':25,'保时捷':26,'法拉利':30,'玛莎拉蒂':30,'阿斯顿马丁':30,'兰博基尼':30,'宾利':30,'劳斯莱斯':30,'迈凯轮':30}
    with open('high_brand/group_dict.json', 'w', encoding='utf-8') as f1:
        json.dump(group_dict, f1, ensure_ascii=False)

    with open('high_brand/group2id.json', 'w', encoding='utf-8') as f2:
        json.dump(group2id, f2, ensure_ascii=False)
    return group_dict,group2id


if __name__=='__main__':
    chexi_transform_id()