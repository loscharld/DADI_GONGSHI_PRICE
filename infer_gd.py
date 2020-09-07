#! usr/bin/env python3
# -*- coding:utf-8 -*-

#! usr/bin/env python3
# -*- coding:utf-8 -*
import numpy as np
import pandas as pd
from sklearn.externals import joblib
import json

ss=joblib.load("high_brand/youqi/youqi_ss.model") ## 加载模型
xgb_model=joblib.load("high_brand/youqi/youqi_xgb.model") ## 加载模型

with open('high_brand/youqi/component2id.json', encoding='utf-8') as reader2:
    component2id = json.load(reader2)

# with open('chaizhuang/id2component.json', encoding='utf-8') as reader3:
#     id2component = json.load(reader3)

with open('high_brand/youqi/region2id.json', encoding='utf-8') as reader4:
    region2id = json.load(reader4)

with open('high_brand/group2id.json',encoding='utf-8') as reader5:
    group2id=json.load(reader5)

# VEHSERINAME_ID2alpha={1:'A(180档)',2:'B(240档)',3:'C(300档)',4:'D(380档)',5:'E(450档)',6:'F(500档)',7:'G(600档)',8:'H(680档)',9:'J(800档)',10:'K(1000档)'}
# alpha2VEHSERINAME_ID={v:k for k,v in VEHSERINAME_ID2alpha.items()}
dict1={'compname':['引擎盖(全喷)'],'REGION':['上海'],'VEHSERINAME_GROUP':['兰博基尼'],'is4s':[1]}
test_data=pd.DataFrame(dict1)
test_data['VEHSERINAME_ID'] = test_data['VEHSERINAME_GROUP'].map(group2id)
test_data['compnent'] = test_data['compname'].map(component2id)
test_data['REGION_ID'] = test_data['REGION'].map(region2id)
df2 = pd.DataFrame(test_data, columns=['REGION_ID', 'compnent', 'VEHSERINAME_ID', 'is4s'])
x_data = ss.transform(df2)
datas_pre = list(np.exp(xgb_model.predict(x_data)))
print(datas_pre)