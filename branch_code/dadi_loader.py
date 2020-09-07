#!/usr/bin/env python 
# -*- coding:utf-8 -*-
import numpy as np
import pandas as pd
import json
import re
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import GridSearchCV
import xgboost as xgb
import time
from datetime import timedelta
from sklearn.model_selection import train_test_split
from sklearn.linear_model.coordinate_descent import ConvergenceWarning
from sklearn.preprocessing import StandardScaler
import warnings
from datetime import datetime
from sql_oracle import args

def get_time_dif(start_time):
    """获取已使用时间"""
    end_time = time.time()
    time_dif = end_time - start_time
    return timedelta(seconds=int(round(time_dif)))

def province_transform(x):
    x = str(x)
    if x.startswith('6501'):
        con = '新疆'
    elif x.startswith('2102'):
        con = '大连'
    elif x.startswith('4501'):
        con = '广西'
    elif x.startswith('3502'):
        con = '厦门'
    elif x.startswith('3401'):
        con = '安徽'
    elif x.startswith('4601'):
        con = '海南'
    elif x.startswith('2201'):
        con = '吉林'
    elif x.startswith('3702'):
        con = '青岛'
    elif x.startswith('6101'):
        con = '陕西'
    elif x.startswith('3101') or x.startswith('3106') or x.startswith('3109') or x.startswith('3199'):
        con = '上海'
    elif x.startswith('5001'):
        con = '重庆'
    elif x.startswith('3501'):
        con = '福建'
    elif x.startswith('6201'):
        con = '甘肃'
    elif x.startswith('2101'):
        con = '辽宁'
    elif x.startswith('1501'):
        con = '内蒙古'
    elif x.startswith('3302'):
        con = '宁波'
    elif x.startswith('6401'):
        con = '宁夏'
    elif x.startswith('4403'):
        con = '深圳'
    elif x.startswith('5101'):
        con = '四川'
    elif x.startswith('1201'):
        con = '天津'
    elif x.startswith('3301'):
        con = '浙江'
    elif x.startswith('1301'):
        con = '河北'
    elif x.startswith('4101'):
        con = '河南'
    elif x.startswith('3201'):
        con = '江苏'
    elif x.startswith('3601'):
        con = '江西'
    elif x.startswith('3701'):
        con = '山东'
    elif x.startswith('5201'):
        con = '贵州'
    elif x.startswith('1401'):
        con = '山西'
    elif x.startswith('5401'):
        con = '西藏'
    elif x.startswith('5301'):
        con = '云南'
    elif x.startswith('4401'):
        con = '广东'
    elif x.startswith('2301'):
        con = '黑龙江'
    elif x.startswith('4201'):
        con = '湖北'
    elif x.startswith('1101'):
        con = '北京'
    elif x.startswith('4301'):
        con = '湖南'
    elif x.startswith('6301'):
        con = '青海'
    else:
        con = np.nan
    return con


# 箱线图异常值检测,返回异常值（统计数据）
def detectoutliers_stac(price):
    outlier_list_col = []
        # 1st quartile (25%)
    Q1 = np.percentile(price, 25)
    # 3rd quartile (75%)
    Q3 = np.percentile(price,75)
    IQR = Q3 - Q1
    outlier_step = 3 * IQR
    try:
        for i in price:
            if ((i < Q1 - outlier_step) | (i > Q3 + outlier_step )):
                outlier_list_col.append(i)
    except Exception as e:
        print(e)
    if len(outlier_list_col) >0:
        return outlier_list_col

# 箱线图异常值检测,返回异常值(训练模型)
def detectoutliers_train(price):
    outlier_list_col = []
        # 1st quartile (25%)
    Q1 = np.percentile(price, 25)
    # 3rd quartile (75%)
    Q3 = np.percentile(price,75)
    IQR = Q3 - Q1
    outlier_step = 3 * IQR
    up_limit=Q3+outlier_step
    low_limit=Q1-outlier_step
    try:
        for i in price:
            if ((i < low_limit) | (i > up_limit)):
                outlier_list_col.append(i)
    except Exception as e:
        print(e)
    if len(outlier_list_col) >0 :
        return outlier_list_col

def gd_detectoutliers_train(price):
    outlier_list_col = []
        # 1st quartile (25%)
    Q1 = np.percentile(price, 25)
    # 3rd quartile (75%)
    Q3 = np.percentile(price,75)
    IQR = Q3 - Q1
    outlier_step = 1.5 * IQR
    up_limit=Q3+outlier_step
    low_limit=Q1-outlier_step
    try:
        for i in price:
            if ((i < low_limit) | (i > up_limit)):
                outlier_list_col.append(i)
    except Exception as e:
        print(e)
    if len(outlier_list_col) >0 :
        return outlier_list_col

def get_upper(x):
    try:
        con = x.upper()
    except:
        con = x
    return con


def gaoduan_chexi_standard(x):
    x=str(x)
    try:
        compil='|'.join(group_dict.keys()).replace('(','\(').replace(')','\)')
        chexi=re.search(compil,x).group()
    except:
        chexi=np.nan
    return chexi

# def chexi_standard(x):
#     x=str(x)
#     chexi = np.nan
#     try:
#         for cx in chexi2id.keys():
#             if x in cx:
#                 chexi=cx
#     except:
#         chexi=np.nan
#     return chexi

def valuation(prediction, label):
    result = np.sqrt(mean_squared_error(prediction, label))
    print('RMSE误差是：{}'.format(result))

def train_model1(df):
    ## 拦截异常
    warnings.filterwarnings(action='ignore', category=ConvergenceWarning)
    y = np.log(df[args.SUMVERILOSS])
    x = df.drop(args.SUMVERILOSS, axis=1, inplace=False)
    # 数据的分割，
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=14)
    # 标准化
    ss = StandardScaler(with_mean=True, with_std=True)
    x_train = ss.fit_transform(x_train)
    x_test = ss.transform(x_test)

    class grid():
        def __init__(self, model):
            self.model = model

        def grid_get(self, X, y, param_grid):
            grid_search = GridSearchCV(self.model, param_grid, cv=5, scoring="neg_mean_squared_error")
            grid_search.fit(X, y)
            print(grid_search.best_params_, np.sqrt(-grid_search.best_score_))
            #         grid_search.cv_results_['mean_test_score'] = np.sqrt(-grid_search.cv_results_['mean_test_score'])
            #         print(pd.DataFrame(grid_search.cv_results_)[['params','mean_test_score','std_test_score']])
            return grid_search

    xgb_model = grid(xgb.XGBRegressor(objective="reg:squarederror", n_jobs=-1)).grid_get(x_train, y_train,{
                                                                                    'gpu_id': [1],
                                                                                   'tree_method': ['gpu_hist'],
                                                                                    'max_depth': [5,10,15],
                                                                                    'learning_rate': [0.1],
                                                                                    'n_estimators': [300,600,1200]})
                                                                                    # 'min_child_weight':[1,3,5]

    return ss, xgb_model, x_test, y_test


def train_model2(df):
    ## 拦截异常
    warnings.filterwarnings(action='ignore', category=ConvergenceWarning)
    y = np.log(df[args.SUMVERILOSS])
    x = df.drop(args.SUMVERILOSS, axis=1, inplace=False)
    # 数据的分割，
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=14)
    # 标准化
    ss = StandardScaler(with_mean=True, with_std=True)
    x_train = ss.fit_transform(x_train)
    x_test = ss.transform(x_test)

    class grid():
        def __init__(self, model):
            self.model = model

        def grid_get(self, X, y, param_grid):
            grid_search = GridSearchCV(self.model, param_grid, cv=5, scoring="neg_mean_squared_error")
            grid_search.fit(X, y)
            print(grid_search.best_params_, np.sqrt(-grid_search.best_score_))
            #         grid_search.cv_results_['mean_test_score'] = np.sqrt(-grid_search.cv_results_['mean_test_score'])
            #         print(pd.DataFrame(grid_search.cv_results_)[['params','mean_test_score','std_test_score']])
            return grid_search

    xgb_model = grid(xgb.XGBRegressor(objective="reg:squarederror", n_jobs=-1)).grid_get(x_train, y_train,{
                                                                                    'gpu_id': [1],
                                                                                   'tree_method': ['gpu_hist'],
                                                                                    'max_depth': [5,10,15],
                                                                                    'learning_rate': [0.1,0.05],
                                                                                    'n_estimators': [300,600,1200]})
                                                                                    # 'min_child_weight': [1, 3, 5]
    return ss, xgb_model, x_test, y_test

def train_model3(df):
    ## 拦截异常
    warnings.filterwarnings(action='ignore', category=ConvergenceWarning)
    y = np.log(df[args.SUMVERILOSS])
    x = df.drop(args.SUMVERILOSS, axis=1, inplace=False)
    # 数据的分割，
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=14)
    # 标准化
    ss = StandardScaler(with_mean=True, with_std=True)
    x_train = ss.fit_transform(x_train)
    x_test = ss.transform(x_test)

    class grid():
        def __init__(self, model):
            self.model = model

        def grid_get(self, X, y, param_grid):
            grid_search = GridSearchCV(self.model, param_grid, cv=5, scoring="neg_mean_squared_error")
            grid_search.fit(X, y)
            print(grid_search.best_params_, np.sqrt(-grid_search.best_score_))
            #         grid_search.cv_results_['mean_test_score'] = np.sqrt(-grid_search.cv_results_['mean_test_score'])
            #         print(pd.DataFrame(grid_search.cv_results_)[['params','mean_test_score','std_test_score']])
            return grid_search

    xgb_model = grid(xgb.XGBRegressor(objective="reg:squarederror", n_jobs=-1)).grid_get(x_train, y_train,{
                                                                                    'gpu_id': [1],
                                                                                   'tree_method': ['gpu_hist'],
                                                                                    'max_depth': [5,10,15],
                                                                                    'learning_rate': [0.1,0.05],
                                                                                    'n_estimators': [300,600,1000]})
                                                                                    # 'min_child_weight': [1, 3, 5]

    return ss, xgb_model, x_test, y_test


def train_model4(df):
    ## 拦截异常
    warnings.filterwarnings(action='ignore', category=ConvergenceWarning)
    y = np.log(df[args.SUMVERILOSS])
    x = df.drop(args.SUMVERILOSS, axis=1, inplace=False)
    # 数据的分割，
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=14)
    # 标准化
    ss = StandardScaler(with_mean=True, with_std=True)
    x_train = ss.fit_transform(x_train)
    x_test = ss.transform(x_test)

    class grid():
        def __init__(self, model):
            self.model = model

        def grid_get(self, X, y, param_grid):
            grid_search = GridSearchCV(self.model, param_grid, cv=5, scoring="neg_mean_squared_error")
            grid_search.fit(X, y)
            print(grid_search.best_params_, np.sqrt(-grid_search.best_score_))
            #         grid_search.cv_results_['mean_test_score'] = np.sqrt(-grid_search.cv_results_['mean_test_score'])
            #         print(pd.DataFrame(grid_search.cv_results_)[['params','mean_test_score','std_test_score']])
            return grid_search

    xgb_model = grid(xgb.XGBRegressor(objective="reg:squarederror", n_jobs=-1)).grid_get(x_train, y_train,{
                                                                                    'gpu_id':[1],
                                                                                   'tree_method': ['gpu_hist'],
                                                                                    'max_depth': [6,7],
                                                                                    'learning_rate': [0.05],
                                                                                    'n_estimators': [1000,2000,3000,4000]})

    return ss, xgb_model, x_test, y_test


VEHSERINAME_ID2alpha = {1: 'A(180档)', 2: 'B(240档)', 3: 'C(300档)', 4: 'D(380档)', 5: 'E(450档)', 6: 'F(500档)',7: 'G(600档)', 8: 'H(680档)', 9: 'J(800档)', 10: 'K(1000档)'}
alpha2VEHSERINAME_ID={v:k for k,v in VEHSERINAME_ID2alpha.items()}

with open('high_brand/group_dict.json',encoding='utf-8') as f1:
    group_dict=json.load(f1)

with open('high_brand/group2id.json', encoding='utf-8') as f2:
    group2id=json.load(f2)

#提取当前时间往前推一年的数据
def get_time_data(datas):
    datas['date'] = pd.to_datetime(datas['核损通过时间'])
    # datas['date'] = datas['date'].map(lambda x:x.strftime('%Y-%m-%d'))
    datas['date'] = datas['date'].map(lambda x: pd.to_datetime(x).date())
    num = len(datas) - 1
    end_time = datas['date'][num]
    print(type(end_time))
    # end_time=pd.to_datetime(end_time)
    # 得到今年的的时间 （年份） 得到的today_year等于2016年
    today_year = end_time.year
    # 今年的时间减去1，得到去年的时间。last_year等于2015
    last_year = int(end_time.year) - 1
    # 得到今年的每个月的时间。today_year_months等于1 2 3 4 5 6 7 8 9，
    today_year_months = range(1, end_time.month + 2)
    # 得到去年的每个月的时间  last_year_months 等于10 11 12
    last_year_months = range(end_time.month + 1, 13)
    # 定义列表去年的数据
    data_list_lasts = []
    # 通过for循环，得到去年的时间夹月份的列表
    # 先遍历去年每个月的列表
    for last_year_month in last_year_months:
        # 定义date_list 去年加上去年的每个月
        date_list = '%s-%s' % (last_year, last_year_month)
        # 通过函数append，得到去年的列表
        data_list_lasts.append(date_list)

    data_list_todays = []
    # 通过for循环，得到今年的时间夹月份的列表
    # 先遍历今年每个月的列表
    for today_year_month in today_year_months:
        # 定义date_list 去年加上今年的每个月
        data_list = '%s-%s' % (today_year, today_year_month)
        # 通过函数append，得到今年的列表
        data_list_todays.append(data_list)
    # 去年的时间数据加上今年的时间数据得到年月时间列表
    data_year_month = data_list_lasts + data_list_todays
    data_year_month.reverse()
    start_time = pd.to_datetime(data_year_month[-1]).date()
    end_time = pd.to_datetime(data_year_month[0]).date()
    # datas = datas.loc[(datas['date'] <= end_time) & (datas['date'] >= start_time)]
    return start_time,end_time

def get_time_from_table(end_time):
    today_year = end_time.year
    # 今年的时间减去1，得到去年的时间。last_year等于2015
    last_year = int(end_time.year) - 1
    # 得到今年的每个月的时间。today_year_months等于1 2 3 4 5 6 7 8 9，
    today_year_months = range(1, end_time.month + 2)
    # 得到去年的每个月的时间  last_year_months 等于10 11 12
    last_year_months = range(end_time.month + 1, 13)
    # 定义列表去年的数据
    data_list_lasts = []
    # 通过for循环，得到去年的时间夹月份的列表
    # 先遍历去年每个月的列表
    for last_year_month in last_year_months:
        # 定义date_list 去年加上去年的每个月
        date_list = '%s-%s' % (last_year, last_year_month)
        # 通过函数append，得到去年的列表
        data_list_lasts.append(date_list)

    data_list_todays = []
    # 通过for循环，得到今年的时间夹月份的列表
    # 先遍历今年每个月的列表
    for today_year_month in today_year_months:
        # 定义date_list 去年加上今年的每个月
        data_list = '%s-%s' % (today_year, today_year_month)
        # 通过函数append，得到今年的列表
        data_list_todays.append(data_list)
    # 去年的时间数据加上今年的时间数据得到年月时间列表
    data_year_month = data_list_lasts + data_list_todays
    data_year_month.reverse()
    start_time = pd.to_datetime(data_year_month[-1])
    end_time = pd.to_datetime(data_year_month[0])
    # datas = datas.loc[(datas['date'] <= end_time) & (datas['date'] >= start_time)]
    return start_time,end_time

#取一年的数据
def get_time_from_table_1year(end_time):
    today_year = end_time.year
    # 今年的时间减去1，得到去年的时间。last_year等于2015
    last_year = int(end_time.year) - 1
    today_day = end_time.day - 1
    # 得到今年的每个月的时间。today_year_months等于1 2 3 4 5 6 7 8 9，
    today_year_months = range(1, end_time.month + 1)
    # 得到去年的每个月的时间  last_year_months 等于10 11 12
    last_year_months = range(end_time.month, 13)
    # 定义列表去年的数据
    data_list_lasts = []
    # 通过for循环，得到去年的时间夹月份的列表
    # 先遍历去年每个月的列表
    for last_year_month in last_year_months:
        # 定义date_list 去年加上去年的每个月
        date_list = '%s-%s' % (last_year, last_year_month)
        # 通过函数append，得到去年的列表
        data_list_lasts.append(date_list)

    data_list_todays = []
    # 通过for循环，得到今年的时间夹月份的列表
    # 先遍历今年每个月的列表
    for today_year_month in today_year_months:
        # 定义date_list 去年加上今年的每个月
        data_list = '%s-%s' % (today_year, today_year_month)
        # 通过函数append，得到今年的列表
        data_list_todays.append(data_list)
    # 去年的时间数据加上今年的时间数据得到年月时间列表
    data_year_month = data_list_lasts + data_list_todays
    data_year_month.reverse()
    start_time = pd.to_datetime(data_year_month[-1])
    delta = timedelta(days=today_day)
    start_time = start_time + delta
    end_time = end_time
    # datas = datas.loc[(datas['date'] <= end_time) & (datas['date'] >= start_time)]
    return start_time, end_time

def compname_process(x):
    x=str(x)
    if '喷漆' in x:
        con=x.replace('喷漆','').replace('/','')
    elif '(含调整)' in x:
        con=x.replace('(含调整)','').replace('/','')
    elif '拆装' in x:
        con=x.replace('拆装','').replace('/B柱','')
    elif '整形修复(中)' in x:
        con=x.replace('整形修复(中)','').replace('/B柱','')
    elif '整形修复(小)' in x:
        con=x.replace('整形修复(小)','').replace('/B柱','')
    elif '整形修复(大)' in x:
        con=x.replace('整形修复(大)','').replace('/B柱','')
    elif '修复' in x:
        con=x.replace('修复','').replace('/','')
    elif '/B柱' in x:
        con=x.replace('/B柱','')
    elif '/' in x:
        con = x.replace('/', '')
    else:
        con=x
    return con

def pinhua(x1,x2):
    x1=float(x1)
    x2=float(x2)
    if x1>=x2:
        x2=x1*1.05
    else:
        x2=x2
    return round(x2,-1)

#A档和B档比较，谁小取谁，作为第一个档次的基准
def pinhua1(x1,x2):
    x1=float(x1)
    x2=float(x2)
    if x1<=x2:
        con=x1
    else:
        con=x2
    return con

region2code={'大连':2102,'广西':4501,'厦门':3502,'安徽':3401,'海南':4601,'吉林':2201,'青岛':3702,'陕西':6101,
           '上海':3101,'重庆':5001,'福建':3501,'甘肃':6201,'辽宁':2101,'内蒙古':1501,'宁波':3302,'宁夏':6401,
           '深圳':4403,'四川':5101,'天津':1201,'浙江':3301,'河北':1301,'河南':4101,'江苏':3201,'江西':3601,
           '山东':3701,'贵州':5201,'山西':1401,'西藏':5401,'云南':5301,'广东':4401,'黑龙江':2301,'湖北':4201,
            '北京': 1101,'新疆': 6501,'青海': 6301,'湖南': 4301}


