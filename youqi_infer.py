#!/usr/bin/env python 
# -*- coding:utf-8 -*-

import numpy as np
import pandas as pd
from sklearn.externals import joblib
import json
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from socketserver import ThreadingMixIn
class ThreadXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass


ss=joblib.load("youqi/youqi_data_ss.model") ## 加载模型
xgb_model=joblib.load("youqi/youqi_xgb.model") ## 加载模型

with open('youqi/component2id.json', encoding='utf-8') as f:
    component2id = json.load(f)

with open('yunnan/chexi2id.json', encoding='utf-8') as reader:
    chexi2id = json.load(reader)

with open('youqi/province2id.json', encoding='utf-8') as reader1:
    province2id = json.load(reader1)

def paint_trainsform(x):
    if '全喷' in x:
        con=1
    elif '半喷' in x:
        con=0
    else:
        con=1
    return con

def is4s_transform(x):
    if x=='是':
        con=1
    else:
        con=0
    return con

def main(province,compnent,vehseriname,is4s,dgree_type):
    data = []
    if province in province2id.keys():
        province_id=province2id[province]
        data.append(province_id)
    else:
        return '省份输入错误，请重新输入！'
    if compnent in component2id.keys():
        id=component2id[compnent]
        data.append(id)
    else:
        return '工时组件输入错误，请重新输入！'
    if vehseriname in chexi2id.keys():
        vehseriname2id=chexi2id[vehseriname]
        data.append(vehseriname2id)
    else:
        return '车系输入错误，请重新输入！'
    is4s=is4s_transform(is4s)
    data.append(is4s)
    dgree_type=paint_trainsform(dgree_type)
    data.append(dgree_type)
    return data

# data=main('内蒙古','前叶子板（右）','东风本田思域',1,'全喷')
# data=ss.transform([data])
# pre=np.exp(xgb_model.predict(data))
# print(round(int(pre),-1))

class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

# Create server
with ThreadXMLRPCServer(("127.0.0.1", 5002), requestHandler=RequestHandler) as server:
    server.register_introspection_functions()

    def youqi_inferance(province,compnent,vehseriname,is4s,dgree_type):
        data=main(province,compnent,vehseriname,is4s,dgree_type)
        data = ss.transform([data])
        pre = np.exp(xgb_model.predict(data))
        return round(int(pre),-1)

    server.register_function(youqi_inferance, 'youqi_inferance')
    # server.register_function(wb_sim_, 'wb_sim_')
    # server.register_function(hash_sim_, 'hash_sim_')
    # server.register_instance(vn)
    print("server is start...........")
    # Run the server's main loop
    server.serve_forever()
    print("server is end...........")
