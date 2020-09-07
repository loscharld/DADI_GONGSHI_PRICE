#! usr/bin/env python3
# -*- coding:utf-8 -*-
from ForCall01 import *
from sklearn.externals import joblib
import argparse
from datetime import timedelta
import logging
import shutil

if not os.path.exists('results'):
    os.makedirs('results')
if len(os.listdir('results')) > 0:
    shutil.rmtree('results')
    os.makedirs('results')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
logger.removeHandler(logger.handlers[0])
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

if len(logger.handlers) > 1:
    logger.removeHandler(logger.handlers[-1])
fh = logging.FileHandler('results/results.log', mode='w', encoding='utf-8')
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)

parser = argparse.ArgumentParser()
parser.add_argument("-LI", "--LOSS_ID", default="LOSSAPPROVALID", help="The input loss id")
parser.add_argument("-RC", "--REGION_CODE", default="LOSSAPPROVALCOMCODE", help="The input region code.")
parser.add_argument("-RN", "--REPORT_NUMBER", default="REGISTNO", help="The input report number.")
parser.add_argument("-CA", "--CAR", default="VEHSERINAME", help="The input Car series.")
parser.add_argument("-I4", "--IS4S", default="IS4S", help="The input is4s.")
parser.add_argument("-WT", "--WORK_TYPE", default="REPAIRTYPE", help="The input work type.")
parser.add_argument("-CN", "--COMPNAME", default="COMPNAME", help="The input Accessory description.")
parser.add_argument("-SL", "--SUMVERILOSS", default="SUMVERILOSS", help="The output Nuclear loss price.")
parser.add_argument("-DN", "--DATA_TABLE_NAME", default="LB_GS_ORIGINAL_HANDLE", help="The input Database table name.")
args = parser.parse_args()

from branch_code.dadi_loader import *

class Sql_oracle():
    def __init__(self,user,password,database,ip):
        self.user=user
        self.password=password
        self.database=database
        self.ip=ip
        self.oracle=useOracle(self.user, self.password, self.database)
        self.account = "{}/{}@{}/{}".format(self.user,self.password,self.ip,self.database)
        self.logger=logger

class Extract(Sql_oracle):
    def extract_data(self):
        # 从数据库读取数据
        commit = "select t.{},t.{},t.{},t.{}, t.{} ,t.{},t.{},t.{} from LB_GS_ORIGINAL_HANDLE t "\
        .format(args.LOSS_ID,args.REGION_CODE,args.CAR,args.IS4S,args.WORK_TYPE,args.COMPNAME,args.REPORT_NUMBER,args.SUMVERILOSS)
        datas = self.oracle.getData(commit, self.account)
        datas.dropna(subset=[args.WORK_TYPE, args.IS4S, args.SUMVERILOSS], axis=0, how='any', inplace=True)
        datas[args.WORK_TYPE] = datas[args.WORK_TYPE].astype(int)
        datas[args.IS4S] = datas[args.IS4S].astype(int)
        datas[args.SUMVERILOSS] = datas[args.SUMVERILOSS].astype(float)
        return datas

    def get_months_from_table(self,end_time,n):
        '''
        通过最近的时间，取到往前第几个月的时间
        :param end_time: 最近的时间
        :param n: 取几个月
        :return: 开始时间和结束时间
        '''
        today_year = end_time.year
        # 今年的时间减去1，得到去年的时间。last_year等于2015
        last_year = int(end_time.year) - 1
        today_day = end_time.day-1
        # 得到今年的每个月的时间。today_year_months等于1 2 3 4 5 6 7 8 9，
        today_year_months = range(1, end_time.month + 1)
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
        start_time = pd.to_datetime(data_year_month[n])
        delta = timedelta(days=today_day)
        start_time = start_time + delta
        end_time = end_time
        # datas = datas.loc[(datas['date'] <= end_time) & (datas['date'] >= start_time)]
        return start_time, end_time

    def extract_months_data(self):
        '''
        从数据库里取6个月的数据
        '''
        #从数据库读取最大月份
        comm1 = '''select  max(VERIFYFINALDATE) from  LB_GS_ORIGINAL_HANDLE t'''
        date = self.oracle.getData(comm1, self.account)
        date = date['MAX(VERIFYFINALDATE)'][0]
        #取6个月数据
        start_time, endtime = self.get_months_from_table(date,6)
        commit = "select t.{},t.{},t.{},t.{}, t.{} ,t.{},t.{},t.{} from LB_GS_ORIGINAL_HANDLE t where t.VERIFYFINALDATE between to_date('{}','yyyy/mm/dd hh24:mi:ss') and to_date('{}','yyyy/mm/dd hh24:mi:ss')" \
            .format(args.LOSS_ID, args.REGION_CODE, args.CAR, args.IS4S, args.WORK_TYPE, args.COMPNAME,
                    args.REPORT_NUMBER, args.SUMVERILOSS,start_time,endtime)
        datas=self.oracle.getData(commit,self.account)
        datas.dropna(subset=[args.WORK_TYPE, args.IS4S, args.SUMVERILOSS], axis=0, how='any', inplace=True)
        datas[args.WORK_TYPE] = datas[args.WORK_TYPE].astype(int)
        datas[args.IS4S] = datas[args.IS4S].astype(int)
        datas[args.SUMVERILOSS] = datas[args.SUMVERILOSS].astype(float)
        return datas

    def chexi_standard(self, x,chexi2id):
        '''
        车系标准化
        :param x: 车系
        :type x:str
        :param chexi2id:车系对id
        :type chexi2id:dict
        :return:标准车系
        :rtype:str
        '''
        x = str(x)
        try:
            compil='|'.join([str(i) for i in chexi2id.keys()]).replace('(','\(').replace(')','\)')
            chexi = re.search(compil, x).group()
        except Exception as e:
            chexi = np.nan
        return chexi

    def get_youqi_data(self,chexi2id):
        '''
        获取油漆数据
        :param chexi2id: 车系对id
        :type chexi2id: dict
        :return: 油漆数据
        :rtype: dataframe
        '''
        # 筛选油漆数据
        datas = self.extract_months_data()

        def read_compname():
            df = pd.read_excel('data/工时价格标准采集模板(发联保).xlsx', sheet_name='服务站工时标准（范例）', skiprows=1)
            df = df[df['工时组'] == '油漆'].reset_index(drop=True)
            compname_ls = list(set(df['项目名称'].values))
            return compname_ls

        compname_ls = read_compname()

        def compname_num(x):
            x = str(x)
            if x in compname_ls:
                con = x
            else:
                con = np.nan
            return con

        datas[args.COMPNAME] = datas[args.COMPNAME].apply(compname_num)
        datas.dropna(subset=[args.COMPNAME], how='any', axis=0, inplace=True)
        df = datas.loc[datas[args.WORK_TYPE] == 1].reset_index(drop=True)
        # 设定一个范围，过滤掉一些异常值
        df = df[(df[args.SUMVERILOSS] > 1) & (df[args.SUMVERILOSS] < 100000)]
        df = df.reset_index(drop=True)
        df[args.CAR] = df[args.CAR].apply(get_upper)
        df[args.CAR] = df.apply(lambda x:self.chexi_standard(x[args.CAR],chexi2id),axis=1)
        # 数据转换
        df['VEHSERINAME_ID'] = df[args.CAR].map(chexi2id)
        df['VEHSERINAME_GRADE'] = df['VEHSERINAME_ID'].map(VEHSERINAME_ID2alpha)

        # df.dropna(subset=['VEHSERINAME_GRADE'], how='any', axis=0, inplace=True)
        # 通过定损员归属机构字段生成区域字段
        df['REGION'] = df[args.REGION_CODE].apply(province_transform)
        # 去除云南，江苏，深圳的数据
        region_list = list(set(df['REGION'].tolist()))
        region_list.remove('云南')
        region_list.remove('江苏')
        region_list.remove('深圳')
        df = df.loc[df['REGION'].isin(region_list)]
        df.dropna(subset=['VEHSERINAME_GRADE', 'REGION'], how='any', axis=0, inplace=True)
        df = df.reset_index(drop=True)
        print(df.shape[0])

        return df

    def get_single_area_youqi_data(self,area,chexi2id):
        '''
        获取单个区域的油漆数据
        :param area: 江苏或云南或深圳
        :type area: str
        :param chexi2id: 车系对id
        :type chexi2id: dict
        :return: 单个区域油漆数据
        :rtype: dataframe
        '''
        if area=='jiangsu':
            area='江苏'
        elif area=='yunnan':
            area='云南'
        elif area=='shenzhen':
            area='深圳'
        else:
            print('区域输入错误！')
        # 筛选油漆数据
        datas = self.extract_months_data()

        def read_compname():
            df = pd.read_excel('data/工时价格标准采集模板(发联保).xlsx', sheet_name='服务站工时标准（范例）', skiprows=1)
            df = df[df['工时组'] == '油漆'].reset_index(drop=True)
            compname_ls = list(set(df['项目名称'].values))
            return compname_ls

        compname_ls = read_compname()

        def compname_num(x):
            x = str(x)
            if x in compname_ls:
                con = x
            else:
                con = np.nan
            return con

        datas[args.COMPNAME] = datas[args.COMPNAME].apply(compname_num)
        datas.dropna(subset=[args.COMPNAME], how='any', axis=0, inplace=True)
        df = datas.loc[datas[args.WORK_TYPE] == 1].reset_index(drop=True)
        # 设定一个范围，过滤掉一些异常值
        df = df[(df[args.SUMVERILOSS] > 1) & (df[args.SUMVERILOSS] < 100000)]
        df = df.reset_index(drop=True)
        # 通过定损员归属机构字段生成区域字段
        df['REGION'] = df[args.REGION_CODE].apply(province_transform)
        df = df.loc[df['REGION']==area]
        #车系标准化
        df[args.CAR] = df[args.CAR].apply(get_upper)
        df[args.CAR] = df.apply(lambda x:self.chexi_standard(x[args.CAR],chexi2id),axis=1)
        # 数据转换
        df['VEHSERINAME_ID'] = df[args.CAR].map(chexi2id)
        df['VEHSERINAME_GRADE'] = df['VEHSERINAME_ID'].map(VEHSERINAME_ID2alpha)
        df.dropna(subset=['VEHSERINAME_GRADE', 'REGION'], how='any', axis=0, inplace=True)
        df = df.reset_index(drop=True)
        print(df.shape[0])

        return df

    def get_banjin_data(self,chexi2id):
        '''
        获取钣金数据
        '''
        # 筛选钣金数据
        datas = self.extract_months_data()

        def read_compname():
            df = pd.read_excel('data/工时价格标准采集模板(发联保).xlsx', sheet_name='服务站工时标准（范例）', skiprows=1)

            def youqi_compname(x1, x2):
                x1 = str(x1)
                x2 = str(x2)
                if x1 == '钣金':
                    con = x2
                else:
                    con = np.nan
                return con

            df['项目名称'] = df.apply(lambda x: youqi_compname(x['工时组'], x['项目名称']), axis=1)
            df.dropna(subset=['项目名称'], how='any', axis=0, inplace=True)
            compname_ls = list(set(df['项目名称'].values))
            return compname_ls

        compname_ls = read_compname()

        def compname_num(x):
            x = str(x)
            if x in compname_ls:
                con = x
            else:
                con = np.nan
            return con

        datas[args.COMPNAME] = datas[args.COMPNAME].apply(compname_num)
        datas.dropna(subset=[args.COMPNAME], how='any', axis=0, inplace=True)
        df = datas.loc[datas[args.WORK_TYPE] == 2].reset_index(drop=True)
        # 设定一个范围，过滤掉一些异常值
        df = df[(df[args.SUMVERILOSS] > 5) & (df[args.SUMVERILOSS] < 10001)]
        df = df.reset_index(drop=True)
        df[args.CAR] = df[args.CAR].apply(get_upper)
        df[args.CAR] = df.apply(lambda x:self.chexi_standard(x[args.CAR],chexi2id),axis=1)
        # 数据转换
        df['VEHSERINAME_ID'] = df[args.CAR].map(chexi2id)
        df['VEHSERINAME_GRADE'] = df['VEHSERINAME_ID'].map(VEHSERINAME_ID2alpha)
        # 通过定损员归属机构字段生成区域字段
        df['REGION'] = df[args.REGION_CODE].apply(province_transform)
        # 去除云南，江苏，深圳的数据
        region_list = list(set(df['REGION'].tolist()))
        region_list.remove('云南')
        region_list.remove('江苏')
        region_list.remove('深圳')
        df = df.loc[df['REGION'].isin(region_list)].reset_index(drop=True)
        df.dropna(subset=[args.CAR, 'REGION'], how='any', axis=0, inplace=True)
        print(df.shape[0])
        return df

    def get_single_area_banjin_data(self,area,chexi2id):
        '''
        获取单个区域钣金数据
        '''
        if area=='jiangsu':
            area='江苏'
        elif area=='yunnan':
            area='云南'
        elif area=='shenzhen':
            area='深圳'
        else:
            print('区域输入错误！')
        # 筛选钣金数据
        datas = self.extract_months_data()

        def read_compname():
            df = pd.read_excel('data/工时价格标准采集模板(发联保).xlsx', sheet_name='服务站工时标准（范例）', skiprows=1)

            def youqi_compname(x1, x2):
                x1 = str(x1)
                x2 = str(x2)
                if x1 == '钣金':
                    con = x2
                else:
                    con = np.nan
                return con

            df['项目名称'] = df.apply(lambda x: youqi_compname(x['工时组'], x['项目名称']), axis=1)
            df.dropna(subset=['项目名称'], how='any', axis=0, inplace=True)
            compname_ls = list(set(df['项目名称'].values))
            return compname_ls

        compname_ls = read_compname()

        def compname_num(x):
            x = str(x)
            if x in compname_ls:
                con = x
            else:
                con = np.nan
            return con

        datas[args.COMPNAME] = datas[args.COMPNAME].apply(compname_num)
        datas.dropna(subset=[args.COMPNAME], how='any', axis=0, inplace=True)
        df = datas.loc[datas[args.WORK_TYPE] == 2].reset_index(drop=True)
        # 设定一个范围，过滤掉一些异常值
        df = df[(df[args.SUMVERILOSS] > 5) & (df[args.SUMVERILOSS] < 10001)]
        df = df.reset_index(drop=True)
        # 通过定损员归属机构字段生成区域字段
        df['REGION'] = df[args.REGION_CODE].apply(province_transform)
        df = df.loc[df['REGION']==area].reset_index(drop=True)
        #车系标准化
        df[args.CAR] = df[args.CAR].apply(get_upper)
        df[args.CAR] = df.apply(lambda x:self.chexi_standard(x[args.CAR],chexi2id),axis=1)
        # 数据转换
        df['VEHSERINAME_ID'] = df[args.CAR].map(chexi2id)
        df['VEHSERINAME_GRADE'] = df['VEHSERINAME_ID'].map(VEHSERINAME_ID2alpha)
        df.dropna(subset=[args.CAR, 'REGION'], how='any', axis=0, inplace=True)
        print(df.shape[0])
        return df

    def get_chaizhuang_data(self,chexi2id):
        '''
        获取拆装数据
        '''
        datas = self.extract_months_data()

        def read_compname():
            df = pd.read_excel('data/工时价格标准采集模板(发联保).xlsx', sheet_name='服务站工时标准（范例）', skiprows=1)

            def youqi_compname(x1, x2):
                x1 = str(x1)
                x2 = str(x2)
                if x1 == '拆装':
                    con = x2
                else:
                    con = np.nan
                return con

            df['项目名称'] = df.apply(lambda x: youqi_compname(x['工时组'], x['项目名称']), axis=1)
            df.dropna(subset=['项目名称'], how='any', axis=0, inplace=True)
            compname_ls = list(set(df['项目名称'].values))
            return compname_ls

        compname_ls = read_compname()

        def compname_num(x):
            x = str(x)
            if x in compname_ls:
                con = x
            else:
                con = np.nan
            return con

        datas[args.COMPNAME] = datas[args.COMPNAME].apply(compname_num)
        datas.dropna(subset=[args.COMPNAME], how='any', axis=0, inplace=True)
        df = datas.loc[datas[args.WORK_TYPE] == 3].reset_index(drop=True)
        # 设定一个范围，过滤掉一些异常值
        df = df[(df[args.SUMVERILOSS] > 5) & (df[args.SUMVERILOSS] < 5001)]
        df = df.reset_index(drop=True)
        df[args.CAR] = df[args.CAR].apply(get_upper)
        df[args.CAR] = df.apply(lambda x:self.chexi_standard(x[args.CAR],chexi2id),axis=1)
        # 数据转换
        df['VEHSERINAME_ID'] = df[args.CAR].map(chexi2id)
        df['VEHSERINAME_GRADE'] = df['VEHSERINAME_ID'].map(VEHSERINAME_ID2alpha)
        # 通过定损员归属机构字段生成区域字段
        df['REGION'] = df[args.REGION_CODE].apply(province_transform)
        # 去除云南，江苏，深圳的数据
        region_list = list(set(df['REGION'].tolist()))
        region_list.remove('云南')
        region_list.remove('江苏')
        region_list.remove('深圳')
        df = df.loc[df['REGION'].isin(region_list)].reset_index(drop=True)
        df.dropna(subset=[args.CAR, 'REGION'], how='any', axis=0, inplace=True)
        print(df.shape[0])
        return df

    def get_single_area_chaizhuang_data(self,area,chexi2id):
        '''
        获取单个区域拆装数据
        '''
        if area=='jiangsu':
            area='江苏'
        elif area=='yunnan':
            area='云南'
        elif area=='shenzhen':
            area='深圳'
        else:
            print('区域输入错误！')

        datas = self.extract_months_data()

        def read_compname():
            df = pd.read_excel('data/工时价格标准采集模板(发联保).xlsx', sheet_name='服务站工时标准（范例）', skiprows=1)

            def youqi_compname(x1, x2):
                x1 = str(x1)
                x2 = str(x2)
                if x1 == '拆装':
                    con = x2
                else:
                    con = np.nan
                return con

            df['项目名称'] = df.apply(lambda x: youqi_compname(x['工时组'], x['项目名称']), axis=1)
            df.dropna(subset=['项目名称'], how='any', axis=0, inplace=True)
            compname_ls = list(set(df['项目名称'].values))
            return compname_ls

        compname_ls = read_compname()

        def compname_num(x):
            x = str(x)
            if x in compname_ls:
                con = x
            else:
                con = np.nan
            return con

        datas[args.COMPNAME] = datas[args.COMPNAME].apply(compname_num)
        datas.dropna(subset=[args.COMPNAME], how='any', axis=0, inplace=True)
        df = datas.loc[datas[args.WORK_TYPE] == 3].reset_index(drop=True)
        # 设定一个范围，过滤掉一些异常值
        df = df[(df[args.SUMVERILOSS] > 5) & (df[args.SUMVERILOSS] < 5001)]
        df = df.reset_index(drop=True)
        # 通过定损员归属机构字段生成区域字段
        df['REGION'] = df[args.REGION_CODE].apply(province_transform)
        df = df.loc[df['REGION'] == area].reset_index(drop=True)
        #车系标准化
        df[args.CAR] = df[args.CAR].apply(get_upper)
        df[args.CAR] = df.apply(lambda x:self.chexi_standard(x[args.CAR],chexi2id),axis=1)
        # 数据转换
        df['VEHSERINAME_ID'] = df[args.CAR].map(chexi2id)
        df['VEHSERINAME_GRADE'] = df['VEHSERINAME_ID'].map(VEHSERINAME_ID2alpha)
        df.dropna(subset=[args.CAR, 'REGION'], how='any', axis=0, inplace=True)
        print(df.shape[0])
        return df

    def get_jixiu_data(self,chexi2id):
        '''
        获取机修数据
        '''
        datas = self.extract_data()

        def read_compname():
            df = pd.read_excel('data/工时价格标准采集模板(发联保).xlsx', sheet_name='服务站工时标准（范例）', skiprows=1)
            def youqi_compname(x1, x2):
                x1 = str(x1)
                x2 = str(x2)
                if x1 == '机修':
                    con = x2
                else:
                    con = np.nan
                return con

            df['项目名称'] = df.apply(lambda x: youqi_compname(x['工时组'], x['项目名称']), axis=1)
            df.dropna(subset=['项目名称'], how='any', axis=0, inplace=True)
            compname_ls = list(set(df['项目名称'].values))
            return compname_ls

        compname_ls = read_compname()

        def compname_num(x):
            x = str(x)
            if x in compname_ls:
                con = x
            else:
                con = np.nan
            return con

        datas[args.COMPNAME] = datas[args.COMPNAME].apply(compname_num)
        datas.dropna(subset=[args.COMPNAME], how='any', axis=0, inplace=True)
        df = datas.reset_index(drop=True)
        df = datas.loc[datas[args.WORK_TYPE] == 31].reset_index(drop=True)
        # 设定一个范围，过滤掉一些异常值
        df = df[(df[args.SUMVERILOSS] > 5) & (df[args.SUMVERILOSS] < 10000)]
        df = df.reset_index(drop=True)
        df[args.CAR] = df[args.CAR].apply(get_upper)
        df[args.CAR] = df.apply(lambda x:self.chexi_standard(x[args.CAR],chexi2id),axis=1)
        # 数据转换
        df['VEHSERINAME_ID'] = df[args.CAR].map(chexi2id)
        df['VEHSERINAME_GRADE'] = df['VEHSERINAME_ID'].map(VEHSERINAME_ID2alpha)
        # 通过定损员归属机构字段生成区域字段
        df['REGION'] = df[args.REGION_CODE].apply(province_transform)
        # 去除云南，江苏，深圳的数据
        region_list = list(set(df['REGION'].tolist()))
        region_list.remove('云南')
        region_list.remove('江苏')
        region_list.remove('深圳')
        df = df.loc[df['REGION'].isin(region_list)]
        df.dropna(subset=[args.CAR, 'REGION'], how='any', axis=0, inplace=True)
        print(df.shape[0])
        return df

    def get_single_area_jixiu_data(self,area,chexi2id):
        '''获取单个区域机修数据'''
        if area=='jiangsu':
            area='江苏'
        elif area=='yunnan':
            area='云南'
        elif area=='shenzhen':
            area='深圳'
        else:
            print('区域输入错误！')

        datas = self.extract_data()

        def read_compname():
            df = pd.read_excel('data/工时价格标准采集模板(发联保).xlsx', sheet_name='服务站工时标准（范例）', skiprows=1)
            def youqi_compname(x1, x2):
                x1 = str(x1)
                x2 = str(x2)
                if x1 == '机修':
                    con = x2
                else:
                    con = np.nan
                return con

            df['项目名称'] = df.apply(lambda x: youqi_compname(x['工时组'], x['项目名称']), axis=1)
            df.dropna(subset=['项目名称'], how='any', axis=0, inplace=True)
            compname_ls = list(set(df['项目名称'].values))
            return compname_ls

        compname_ls = read_compname()

        def compname_num(x):
            x = str(x)
            if x in compname_ls:
                con = x
            else:
                con = np.nan
            return con

        datas[args.COMPNAME] = datas[args.COMPNAME].apply(compname_num)
        datas.dropna(subset=[args.COMPNAME], how='any', axis=0, inplace=True)
        df = datas.reset_index(drop=True)
        df = datas.loc[datas[args.WORK_TYPE] == 31].reset_index(drop=True)
        # 设定一个范围，过滤掉一些异常值
        df = df[(df[args.SUMVERILOSS] > 5) & (df[args.SUMVERILOSS] < 10000)]
        df = df.reset_index(drop=True)
        # 通过定损员归属机构字段生成区域字段
        df['REGION'] = df[args.REGION_CODE].apply(province_transform)
        df = df.loc[df['REGION'] == area].reset_index(drop=True)
        df[args.CAR] = df[args.CAR].apply(get_upper)
        df[args.CAR] = df.apply(lambda x:self.chexi_standard(x[args.CAR],chexi2id),axis=1)
        # 数据转换
        df['VEHSERINAME_ID'] = df[args.CAR].map(chexi2id)
        df['VEHSERINAME_GRADE'] = df['VEHSERINAME_ID'].map(VEHSERINAME_ID2alpha)
        df.dropna(subset=[args.CAR, 'REGION'], how='any', axis=0, inplace=True)
        print(df.shape[0])
        return df

    def get_diangong_data(self,chexi2id):
        '''
        获取电工数据
        '''
        datas = self.extract_data()

        def read_compname():
            df = pd.read_excel('data/工时价格标准采集模板(发联保).xlsx', sheet_name='服务站工时标准（范例）', skiprows=1)

            def youqi_compname(x1, x2):
                x1 = str(x1)
                x2 = str(x2)
                if x1 == '电工':
                    con = x2
                else:
                    con = np.nan
                return con

            df['项目名称'] = df.apply(lambda x: youqi_compname(x['工时组'], x['项目名称']), axis=1)
            df.dropna(subset=['项目名称'], how='any', axis=0, inplace=True)
            compname_ls = list(set(df['项目名称'].values))
            return compname_ls

        compname_ls = read_compname()

        def compname_num(x):
            x = str(x)
            if x in compname_ls:
                con = x
            else:
                con = np.nan
            return con

        datas[args.COMPNAME] = datas[args.COMPNAME].apply(compname_num)
        datas.dropna(subset=[args.COMPNAME], how='any', axis=0, inplace=True)
        df = datas.loc[datas[args.WORK_TYPE] == 41].reset_index(drop=True)
        # 设定一个范围，过滤掉一些异常值
        df = df[(df[args.SUMVERILOSS] > 5) & (df[args.SUMVERILOSS] < 5000)]
        df = df.reset_index(drop=True)
        df[args.CAR] = df[args.CAR].apply(get_upper)
        df[args.CAR] = df.apply(lambda x:self.chexi_standard(x[args.CAR],chexi2id),axis=1)
        # 数据转换
        df['VEHSERINAME_ID'] = df[args.CAR].map(chexi2id)
        df['VEHSERINAME_GRADE'] = df['VEHSERINAME_ID'].map(VEHSERINAME_ID2alpha)
        # 通过定损员归属机构字段生成区域字段
        df['REGION'] = df[args.REGION_CODE].apply(province_transform)
        # 去除云南，江苏，深圳的数据
        region_list = list(set(df['REGION'].tolist()))
        region_list.remove('云南')
        region_list.remove('江苏')
        region_list.remove('深圳')
        df = df.loc[df['REGION'].isin(region_list)]
        df.dropna(subset=[args.CAR, 'REGION'], how='any', axis=0, inplace=True)
        print(df.shape[0])
        return df

    def get_single_area_diangong_data(self,area,chexi2id):
        '''
        获取单个区域电工数据
        '''
        if area=='jiangsu':
            area='江苏'
        elif area=='yunnan':
            area='云南'
        elif area=='shenzhen':
            area='深圳'
        else:
            print('区域输入错误！')

        datas = self.extract_data()

        def read_compname():
            df = pd.read_excel('data/工时价格标准采集模板(发联保).xlsx', sheet_name='服务站工时标准（范例）', skiprows=1)

            def youqi_compname(x1, x2):
                x1 = str(x1)
                x2 = str(x2)
                if x1 == '电工':
                    con = x2
                else:
                    con = np.nan
                return con

            df['项目名称'] = df.apply(lambda x: youqi_compname(x['工时组'], x['项目名称']), axis=1)
            df.dropna(subset=['项目名称'], how='any', axis=0, inplace=True)
            compname_ls = list(set(df['项目名称'].values))
            return compname_ls

        compname_ls = read_compname()

        def compname_num(x):
            x = str(x)
            if x in compname_ls:
                con = x
            else:
                con = np.nan
            return con

        datas[args.COMPNAME] = datas[args.COMPNAME].apply(compname_num)
        datas.dropna(subset=[args.COMPNAME], how='any', axis=0, inplace=True)
        df = datas.loc[datas[args.WORK_TYPE] == 41].reset_index(drop=True)
        # 设定一个范围，过滤掉一些异常值
        df = df[(df[args.SUMVERILOSS] > 5) & (df[args.SUMVERILOSS] < 5000)]
        df = df.reset_index(drop=True)
        # 通过定损员归属机构字段生成区域字段
        df['REGION'] = df[args.REGION_CODE].apply(province_transform)
        df = df.loc[df['REGION'] == area].reset_index(drop=True)
        df[args.CAR] = df[args.CAR].apply(get_upper)
        df[args.CAR] = df.apply(lambda x:self.chexi_standard(x[args.CAR],chexi2id),axis=1)
        # 数据转换
        df['VEHSERINAME_ID'] = df[args.CAR].map(chexi2id)
        df['VEHSERINAME_GRADE'] = df['VEHSERINAME_ID'].map(VEHSERINAME_ID2alpha)
        df.dropna(subset=[args.CAR, 'REGION'], how='any', axis=0, inplace=True)
        print(df.shape[0])
        return df

    def get_25_youqi_data(self,group_dict,group2id):
        '''
        获取25个高端品牌的油漆数据
        '''
        # 筛选油漆数据
        datas = self.extract_data()

        def read_compname():
            df = pd.read_excel('data/工时价格标准采集模板(发联保).xlsx', sheet_name='服务站工时标准（范例）', skiprows=1)

            def youqi_compname(x1, x2):
                x1 = str(x1)
                x2 = str(x2)
                if x1 == '油漆':
                    con = x2
                else:
                    con = np.nan
                return con

            df['项目名称'] = df.apply(lambda x: youqi_compname(x['工时组'], x['项目名称']), axis=1)
            df.dropna(subset=['项目名称'], how='any', axis=0, inplace=True)
            compname_ls = list(set(df['项目名称'].values))
            return compname_ls

        compname_ls = read_compname()

        def compname_num(x):
            x = str(x)
            if x in compname_ls:
                con = x
            else:
                con = np.nan
            return con

        datas[args.COMPNAME] = datas[args.COMPNAME].apply(compname_num)
        datas.dropna(subset=[args.COMPNAME], how='any', axis=0, inplace=True)
        datas=datas.loc[datas[args.COMPNAME]!='全车喷漆']
        df = datas.loc[datas[args.WORK_TYPE] == 1].reset_index(drop=True)
        # 设定一个范围，过滤掉一些异常值
        df = df[(df[args.SUMVERILOSS] > 4) & (df[args.SUMVERILOSS] < 300000)]
        df = df.reset_index(drop=True)
        # 高端车系标准化
        df[args.CAR] = df[args.CAR].apply(get_upper)
        df[args.CAR] = df.apply(lambda x:self.chexi_standard(x[args.CAR],group_dict),axis=1)
        # # 数据转换
        # df['VEHSERINAME_ID'] = df[args.CAR].map(chexi2id)
        # df['VEHSERINAME_GRADE'] = df['VEHSERINAME_ID'].map(VEHSERINAME_ID2alpha)
        # 数据转换
        df['VEHSERINAME_GRADE'] = df[args.CAR].map(group_dict)
        df['VEHSERINAME_ID'] = df['VEHSERINAME_GRADE'].map(group2id)
        # 通过定损员归属机构字段生成区域字段
        df['REGION'] = df[args.REGION_CODE].apply(province_transform)
        df.dropna(subset=[args.CAR, 'REGION'], how='any', axis=0, inplace=True)
        print(df.shape[0])
        return df

    def get_25_banjin_data(self,group_dict,group2id):
        '''获取25个高端品牌钣金数据'''
        # 筛选钣金数据
        datas = self.extract_data()

        def read_compname():
            df = pd.read_excel('data/工时价格标准采集模板(发联保).xlsx', sheet_name='服务站工时标准（范例）', skiprows=1)

            def youqi_compname(x1, x2):
                x1 = str(x1)
                x2 = str(x2)
                if x1 == '钣金':
                    con = x2
                else:
                    con = np.nan
                return con

            df['项目名称'] = df.apply(lambda x: youqi_compname(x['工时组'], x['项目名称']), axis=1)
            df.dropna(subset=['项目名称'], how='any', axis=0, inplace=True)
            compname_ls = list(set(df['项目名称'].values))
            return compname_ls

        compname_ls = read_compname()

        def compname_num(x):
            x = str(x)
            if x in compname_ls:
                con = x
            else:
                con = np.nan
            return con

        datas[args.COMPNAME] = datas[args.COMPNAME].apply(compname_num)
        datas.dropna(subset=[args.COMPNAME], how='any', axis=0, inplace=True)
        df = datas.loc[datas[args.WORK_TYPE] == 2].reset_index(drop=True)
        # 设定一个范围，过滤掉一些异常值
        df = df[(df[args.SUMVERILOSS] > 29) & (df[args.SUMVERILOSS] < 50000)]
        df = df.reset_index(drop=True)
        # 高端车系标准化
        df[args.CAR] = df[args.CAR].apply(get_upper)
        df[args.CAR] = df.apply(lambda x:self.chexi_standard(x[args.CAR],group_dict),axis=1)
        # # 数据转换
        # df['VEHSERINAME_ID'] = df[args.CAR].map(chexi2id)
        # df['VEHSERINAME_GRADE'] = df['VEHSERINAME_ID'].map(VEHSERINAME_ID2alpha)
        # 数据转换
        df['VEHSERINAME_GRADE'] = df[args.CAR].map(group_dict)
        df['VEHSERINAME_ID'] = df['VEHSERINAME_GRADE'].map(group2id)
        # 通过定损员归属机构字段生成区域字段
        df['REGION'] = df[args.REGION_CODE].apply(province_transform)
        df.dropna(subset=[args.CAR, 'REGION'], how='any', axis=0, inplace=True)
        print(df.shape[0])
        return df

    def get_25_chaizhuang_data(self,group_dict,group2id):
        '''
        获取25个高端品牌拆装数据
        '''
        # 筛选拆装数据
        datas = self.extract_data()

        def read_compname():
            df = pd.read_excel('data/工时价格标准采集模板(发联保).xlsx', sheet_name='服务站工时标准（范例）', skiprows=1)

            def youqi_compname(x1, x2):
                x1 = str(x1)
                x2 = str(x2)
                if x1 == '拆装':
                    con = x2
                else:
                    con = np.nan
                return con

            df['项目名称'] = df.apply(lambda x: youqi_compname(x['工时组'], x['项目名称']), axis=1)
            df.dropna(subset=['项目名称'], how='any', axis=0, inplace=True)
            compname_ls = list(set(df['项目名称'].values))
            return compname_ls

        compname_ls = read_compname()

        def compname_num(x):
            x = str(x)
            if x in compname_ls:
                con = x
            else:
                con = np.nan
            return con

        datas[args.COMPNAME] = datas[args.COMPNAME].apply(compname_num)
        datas.dropna(subset=[args.COMPNAME], how='any', axis=0, inplace=True)
        df = datas.loc[datas[args.WORK_TYPE] == 3].reset_index(drop=True)
        # 设定一个范围，过滤掉一些异常值
        df = df[(df[args.SUMVERILOSS] > 29) & (df[args.SUMVERILOSS] < 20000)]
        df = df.reset_index(drop=True)
        # 高端车系标准化
        df[args.CAR] = df[args.CAR].apply(get_upper)
        df[args.CAR] = df.apply(lambda x:self.chexi_standard(x[args.CAR],group_dict),axis=1)
        # # 数据转换
        # df['VEHSERINAME_ID'] = df[args.CAR].map(chexi2id)
        # df['VEHSERINAME_GRADE'] = df['VEHSERINAME_ID'].map(VEHSERINAME_ID2alpha)
        # 数据转换
        df['VEHSERINAME_GRADE'] = df[args.CAR].map(group_dict)
        df['VEHSERINAME_ID'] = df['VEHSERINAME_GRADE'].map(group2id)
        # 通过定损员归属机构字段生成区域字段
        df['REGION'] = df[args.REGION_CODE].apply(province_transform)
        df.dropna(subset=[args.CAR, 'REGION'], how='any', axis=0, inplace=True)
        print(df.shape[0])
        return df

    def get_25_jixiu_data(self,group_dict,group2id):
        '''
        获取25个高端品牌机修数据
        '''
        datas = self.extract_data()

        def read_compname():
            df = pd.read_excel('data/工时价格标准采集模板(发联保).xlsx', sheet_name='服务站工时标准（范例）', skiprows=1)

            def youqi_compname(x1, x2):
                x1 = str(x1)
                x2 = str(x2)
                if x1 == '机修':
                    con = x2
                else:
                    con = np.nan
                return con

            df['项目名称'] = df.apply(lambda x: youqi_compname(x['工时组'], x['项目名称']), axis=1)
            df.dropna(subset=['项目名称'], how='any', axis=0, inplace=True)
            compname_ls = list(set(df['项目名称'].values))
            return compname_ls

        compname_ls = read_compname()

        def compname_num(x):
            x = str(x)
            if x in compname_ls:
                con = x
            else:
                con = np.nan
            return con

        datas[args.COMPNAME] = datas[args.COMPNAME].apply(compname_num)
        datas.dropna(subset=[args.COMPNAME], how='any', axis=0, inplace=True)
        df = datas.loc[datas[args.WORK_TYPE] == 31].reset_index(drop=True)
        # 设定一个范围，过滤掉一些异常值
        df = df[(df[args.SUMVERILOSS] > 5) & (df[args.SUMVERILOSS] < 5000)]
        df = df.reset_index(drop=True)
        # 高端车系标准化
        df[args.CAR] = df[args.CAR].apply(get_upper)
        df[args.CAR] = df.apply(lambda x:self.chexi_standard(x[args.CAR],group_dict),axis=1)
        # # 数据转换
        # df['VEHSERINAME_ID'] = df[args.CAR].map(chexi2id)
        # df['VEHSERINAME_GRADE'] = df['VEHSERINAME_ID'].map(VEHSERINAME_ID2alpha)
        # 数据转换
        df['VEHSERINAME_GRADE'] = df[args.CAR].map(group_dict)
        df['VEHSERINAME_ID'] = df['VEHSERINAME_GRADE'].map(group2id)
        # 通过定损员归属机构字段生成区域字段
        df['REGION'] = df[args.REGION_CODE].apply(province_transform)
        df.dropna(subset=[args.CAR, 'REGION'], how='any', axis=0, inplace=True)
        print(df.shape[0])
        return df

    def get_25_diangong_data(self,group_dict,group2id):
        '''
        获取25个高端品牌电工数据
        '''
        datas = self.extract_data()

        def read_compname():
            df = pd.read_excel('data/工时价格标准采集模板(发联保).xlsx', sheet_name='服务站工时标准（范例）', skiprows=1)

            def youqi_compname(x1, x2):
                x1 = str(x1)
                x2 = str(x2)
                if x1 == '电工':
                    con = x2
                else:
                    con = np.nan
                return con

            df['项目名称'] = df.apply(lambda x: youqi_compname(x['工时组'], x['项目名称']), axis=1)
            df.dropna(subset=['项目名称'], how='any', axis=0, inplace=True)
            compname_ls = list(set(df['项目名称'].values))
            return compname_ls

        compname_ls = read_compname()

        def compname_num(x):
            x = str(x)
            if x in compname_ls:
                con = x
            else:
                con = np.nan
            return con

        datas[args.COMPNAME] = datas[args.COMPNAME].apply(compname_num)
        datas.dropna(subset=[args.COMPNAME], how='any', axis=0, inplace=True)
        df = datas.loc[datas[args.WORK_TYPE] == 41].reset_index(drop=True)
        # 设定一个范围，过滤掉一些异常值
        df = df[(df[args.SUMVERILOSS] > 5) & (df[args.SUMVERILOSS] < 5000)]
        df = df.reset_index(drop=True)
        # 高端车系标准化
        df[args.CAR] = df[args.CAR].apply(get_upper)
        df[args.CAR] = df.apply(lambda x:self.chexi_standard(x[args.CAR],group_dict),axis=1)
        # # 数据转换
        # df['VEHSERINAME_ID'] = df[args.CAR].map(chexi2id)
        # df['VEHSERINAME_GRADE'] = df['VEHSERINAME_ID'].map(VEHSERINAME_ID2alpha)
        # 数据转换
        df['VEHSERINAME_GRADE'] = df[args.CAR].map(group_dict)
        df['VEHSERINAME_ID'] = df['VEHSERINAME_GRADE'].map(group2id)
        # 通过定损员归属机构字段生成区域字段
        df['REGION'] = df[args.REGION_CODE].apply(province_transform)
        df.dropna(subset=[args.CAR, 'REGION'], how='any', axis=0, inplace=True)
        print(df.shape[0])
        return df

    def outlier_process_stac(self,df):
        '''
        统计异常值处理
        '''
        outliers = df.groupby([args.IS4S, 'VEHSERINAME_GRADE', args.COMPNAME])[args.SUMVERILOSS].apply(
            detectoutliers_stac).reset_index()
        # 有异常值的取出来
        outliers = outliers[outliers[args.SUMVERILOSS].notnull()]
        # 将核损总金额列拆开
        if len(outliers):
            outliers = outliers.set_index([args.IS4S, 'VEHSERINAME_GRADE', args.COMPNAME])[args.SUMVERILOSS].apply(
                pd.Series).stack().reset_index()
            outliers.columns = [args.IS4S, 'VEHSERINAME_GRADE', args.COMPNAME, 'INDEX', args.SUMVERILOSS]
            # 去重
            outliers = outliers[[args.IS4S, 'VEHSERINAME_GRADE', args.COMPNAME, args.SUMVERILOSS]].drop_duplicates()
            outliers['FLAG'] = 1
            df = pd.merge(df, outliers, how='left',
                          on=[args.IS4S, 'VEHSERINAME_GRADE', args.COMPNAME, args.SUMVERILOSS])
            # 去除异常值
            df.drop(df[df['FLAG'] == 1].index, inplace=True)
        else:
            df = df
        return df

    def gd_outlier_process_stac(self,df):
        '''
        高端品牌统计异常值处理
        '''
        outliers = df.groupby([args.IS4S, 'VEHSERINAME_GRADE', args.COMPNAME])[args.SUMVERILOSS].apply(
            gd_detectoutliers_train).reset_index()
        # 有异常值的取出来
        outliers = outliers[outliers[args.SUMVERILOSS].notnull()]
        # 将核损总金额列拆开
        if len(outliers):
            outliers = outliers.set_index([args.IS4S, 'VEHSERINAME_GRADE', args.COMPNAME])[args.SUMVERILOSS].apply(
                pd.Series).stack().reset_index()
            outliers.columns = [args.IS4S, 'VEHSERINAME_GRADE', args.COMPNAME, 'INDEX', args.SUMVERILOSS]
            # 去重
            outliers = outliers[[args.IS4S, 'VEHSERINAME_GRADE', args.COMPNAME, args.SUMVERILOSS]].drop_duplicates()
            outliers['FLAG'] = 1
            df = pd.merge(df, outliers, how='left',
                          on=[args.IS4S, 'VEHSERINAME_GRADE', args.COMPNAME, args.SUMVERILOSS])
            # 去除异常值
            df.drop(df[df['FLAG'] == 1].index, inplace=True)
        else:
            df = df
        return df

    def outlier_process_train(self,df,gongzhong,area):
        '''
        模型训练异常值处理
        :param df: 原始数据
        :type df: dataframe
        :param gongzhong: 工种，包括油漆，钣金，拆装，机修，电工
        :type gongzhong: str
        :param area: 区域，33个大区域，3个小区域
        :type area: str
        :return: 去除异常值后的数据
        :rtype: dataframe
        '''
        outliers = df.groupby([args.IS4S, 'VEHSERINAME_GRADE', args.COMPNAME])[args.SUMVERILOSS].apply(
            detectoutliers_train).reset_index()
        # 有异常值的取出来
        outliers = outliers[outliers[args.SUMVERILOSS].notnull()]
        # 将核损总金额列拆开
        if len(outliers):
            outliers = outliers.set_index([args.IS4S, 'VEHSERINAME_GRADE', args.COMPNAME])[args.SUMVERILOSS].apply(
                pd.Series).stack().reset_index()
            outliers.columns = [args.IS4S, 'VEHSERINAME_GRADE', args.COMPNAME, 'INDEX', args.SUMVERILOSS]
            # 去重
            outliers = outliers[[args.IS4S, 'VEHSERINAME_GRADE', args.COMPNAME, args.SUMVERILOSS]].drop_duplicates()
            outliers['FLAG'] = 1
            df = pd.merge(df, outliers, how='left',
                          on=[args.IS4S, 'VEHSERINAME_GRADE', args.COMPNAME, args.SUMVERILOSS])
            outliers_table = df.copy()
            outliers_table['REPAIRTYPE'] = gongzhong
            outliers_table = outliers_table[outliers_table['FLAG'] == 1]
            outliers_table = pd.DataFrame(outliers_table,
                                          columns=[args.LOSS_ID, 'REPAIRTYPE', 'REGION', args.IS4S, args.CAR,
                                                   'VEHSERINAME_GRADE', args.COMPNAME, args.SUMVERILOSS])
            outliers_table.to_csv('outliers/{}_{}.csv'.format(area,gongzhong), index=None, encoding='utf-8')
            # 去除异常值
            df.drop(df[df['FLAG'] == 1].index, inplace=True)
            df.drop('FLAG', axis=1, inplace=True)
        else:
            df = df
        return df

    def gd_outlier_process_train(self,df,gongzhong,area):
        '''
        高端品牌模型训练异常值处理
        :param df: 原始数据
        :type df: dataframe
        :param gongzhong: 工种，包括油漆，钣金，拆装，机修，电工
        :type gongzhong: str
        :param area: 区域，所有区域
        :type area: str
        :return: 去除异常值后的数据
        :rtype: dataframe
        '''
        outliers = df.groupby([args.IS4S, 'VEHSERINAME_ID', args.COMPNAME])[args.SUMVERILOSS].apply(
            gd_detectoutliers_train).reset_index()
        # 有异常值的取出来
        outliers = outliers[outliers[args.SUMVERILOSS].notnull()]
        # 将核损总金额列拆开
        if len(outliers):
            outliers = outliers.set_index([args.IS4S, 'VEHSERINAME_ID', args.COMPNAME])[args.SUMVERILOSS].apply(
                pd.Series).stack().reset_index()
            outliers.columns = [args.IS4S, 'VEHSERINAME_ID', args.COMPNAME, 'INDEX', args.SUMVERILOSS]
            # 去重
            outliers = outliers[[args.IS4S, 'VEHSERINAME_ID', args.COMPNAME, args.SUMVERILOSS]].drop_duplicates()
            outliers['FLAG'] = 1
            df = pd.merge(df, outliers, how='left',
                          on=[args.IS4S, 'VEHSERINAME_ID', args.COMPNAME, args.SUMVERILOSS])
            outliers_table = df.copy()
            outliers_table['REPAIRTYPE'] = gongzhong
            outliers_table = outliers_table[outliers_table['FLAG'] == 1]
            outliers_table = pd.DataFrame(outliers_table,
                                          columns=[args.LOSS_ID, 'REPAIRTYPE', 'REGION', args.IS4S, args.CAR,
                                                   'VEHSERINAME_GRADE', args.COMPNAME, args.SUMVERILOSS])
            outliers_table.to_csv('outliers/{}_{}.csv'.format(area,gongzhong), index=None, encoding='utf-8')
            # 去除异常值
            df.drop(df[df['FLAG'] == 1].index, inplace=True)
            df.drop('FLAG', axis=1, inplace=True)
        else:
            df = df
        return df

    def transform_process(self,df):
        df = df[['REGION', args.CAR, 'VEHSERINAME_ID', args.IS4S, args.COMPNAME, args.SUMVERILOSS]]
        return df

    def youqi_process(self,df,gongzhong,area):
        '''
        油漆数据处理
        '''
        df = self.transform_process(df)
        # 喷漆类型转ID
        # df['PAINTING_TYPE']=df[args.COMPNAME].apply(paint_trainsform)
        # print(df.loc[df['PAINTING_TYPE']==1])
        # 工时项目去重后转ID
        df.dropna(subset=[args.COMPNAME], how='any', axis=0, inplace=True)
        x = list(set(df[args.COMPNAME].tolist()))
        component2id = dict(zip(x, range(1, len(x) + 1)))
        id2component = {value: key for key, value in component2id.items()}

        with open('{}/{}_component2id.json'.format(area,gongzhong), 'w', encoding='utf-8') as f:
            json.dump(component2id, f, ensure_ascii=False)
        with open('{}/{}_id2component.json'.format(area,gongzhong), 'w', encoding='utf-8') as f:
            json.dump(id2component, f, ensure_ascii=False)

        def transform(x):
            if x in component2id:
                con = component2id[x]
            else:
                con = np.nan
            return con

        df['COMPONENT'] = df.apply(lambda x: transform(x[args.COMPNAME]), axis=1)
        # df.dropna(subset=['COMPONENT',args.IS4S,'COMPNAME',args.SUMVERILOSS],how='any',axis=0,inplace=True)
        df.dropna(subset=['REGION', 'COMPONENT', 'VEHSERINAME_ID', args.IS4S, args.COMPNAME, args.SUMVERILOSS],
                  how='any', axis=0, inplace=True)
        # 区域去重后转ID
        regions = list(set(df['REGION'].tolist()))
        REGION2ID = dict(zip(regions, range(1, len(regions) + 1)))
        df['REGION_ID'] = df['REGION'].map(REGION2ID)
        with open('{}/{}_region2id.json'.format(area,gongzhong), 'w', encoding='utf-8') as writer:
            json.dump(REGION2ID, writer, ensure_ascii=False)
        df.dropna(subset=['REGION_ID', 'COMPONENT', 'VEHSERINAME_ID', args.IS4S, args.SUMVERILOSS], how='any', axis=0,
                  inplace=True)
        df = df[['REGION_ID', 'COMPONENT', 'VEHSERINAME_ID', args.IS4S, args.SUMVERILOSS]].astype(float)

        if area=='large_area':
            return df
        else:
            df_all = pd.DataFrame()
            for i in range(10):
                df_all = pd.concat([df_all, df], axis=0)
            print(len(df_all))
            return df_all


    def banjin_process(self,df,gongzhong,area):
        '''
        钣金数据处理
        '''
        df = self.transform_process(df)
        # 喷漆类型转ID
        # df['DEGREE_TYPE']=df[args.COMPNAME].apply(banjin_transform)
        # 工时项目去重后转ID
        df.dropna(subset=[args.COMPNAME], how='any', axis=0, inplace=True)
        x = list(set(df[args.COMPNAME].values))
        component2id = dict(zip(x, range(1, len(x) + 1)))
        id2component = {value: key for key, value in component2id.items()}

        with open('{}/{}_component2id.json'.format(area,gongzhong), 'w', encoding='utf-8') as f:
            json.dump(component2id, f, ensure_ascii=False)
        with open('{}/{}_id2component.json'.format(area,gongzhong), 'w', encoding='utf-8') as f:
            json.dump(id2component, f, ensure_ascii=False)

        def transform(x):
            if x in component2id:
                con = component2id[x]
            else:
                con = np.nan
            return con

        df['COMPONENT'] = df.apply(lambda x: transform(x[args.COMPNAME]), axis=1)
        # df.dropna(subset=['COMPONENT',args.IS4S,'COMPNAME',args.SUMVERILOSS],how='any',axis=0,inplace=True)
        df.dropna(subset=['REGION', 'COMPONENT', 'VEHSERINAME_ID', args.IS4S, args.COMPNAME, args.SUMVERILOSS],
                  how='any', axis=0, inplace=True)
        # 区域去重后转ID
        regions = list(set(list(df['REGION'].values)))
        REGION2ID = dict(zip(regions, range(1, len(regions) + 1)))
        df['REGION_ID'] = df['REGION'].map(REGION2ID)
        with open('{}/{}_region2id.json'.format(area,gongzhong), 'w', encoding='utf-8') as writer:
            json.dump(REGION2ID, writer, ensure_ascii=False)
        df = df[['REGION_ID', 'COMPONENT', 'VEHSERINAME_ID', args.IS4S, args.SUMVERILOSS]].astype(float)

        if area == 'large_area':
            return df
        else:
            df_all = pd.DataFrame()
            for i in range(10):
                df_all = pd.concat([df_all, df], axis=0)
            print(len(df_all))
            return df_all

    def chaizhuang_process(self,df,gongzhong,area):
        '''拆装数据处理'''
        df = self.transform_process(df)
        # df['ANNEX_TYPE']=df[args.COMPNAME].apply(chaizhuang_transform)
        # 工时项目去重后转ID
        df.dropna(subset=[args.COMPNAME], how='any', axis=0, inplace=True)
        x = list(set(df[args.COMPNAME].values))
        component2id = dict(zip(x, range(1, len(x) + 1)))
        id2component = {value: key for key, value in component2id.items()}

        with open('{}/{}_component2id.json'.format(area,gongzhong), 'w', encoding='utf-8') as f:
            json.dump(component2id, f, ensure_ascii=False)
        with open('{}/{}_id2component.json'.format(area,gongzhong), 'w', encoding='utf-8') as f:
            json.dump(id2component, f, ensure_ascii=False)

        def transform(x):
            if x in component2id:
                con = component2id[x]
            else:
                con = np.nan
            return con

        df['COMPONENT'] = df.apply(lambda x: transform(x[args.COMPNAME]), axis=1)
        # df.dropna(subset=['COMPONENT',args.IS4S,'COMPNAME',args.SUMVERILOSS],how='any',axis=0,inplace=True)
        df.dropna(subset=['REGION', 'COMPONENT', 'VEHSERINAME_ID', args.IS4S, args.COMPNAME, args.SUMVERILOSS],
                  how='any', axis=0, inplace=True)
        # 区域去重后转ID
        regions = list(set(list(df['REGION'].values)))
        REGION2ID = dict(zip(regions, range(1, len(regions) + 1)))
        df['REGION_ID'] = df['REGION'].map(REGION2ID)
        with open('{}/{}_region2id.json'.format(area,gongzhong), 'w', encoding='utf-8') as writer:
            json.dump(REGION2ID, writer, ensure_ascii=False)
        df = df[['REGION_ID', 'COMPONENT', 'VEHSERINAME_ID', args.IS4S, args.SUMVERILOSS]].astype(float)

        if area=='large_area':
            return df
        else:
            df_all = pd.DataFrame()
            for i in range(10):
                df_all = pd.concat([df_all, df], axis=0)
            print(len(df_all))
            return df_all
    def compname_asc(self,df):
        # 工时项目排序
        df_com = df.loc[df['IS4S'] == 1]
        df_com = df_com[['VEHSERINAME_ID', args.COMPNAME, args.SUMVERILOSS]]
        df_com = df_com.groupby(['VEHSERINAME_ID', args.COMPNAME])[args.SUMVERILOSS].agg('mean').reset_index()
        counts = df_com['VEHSERINAME_ID'].value_counts().to_dict()
        keys = list(counts.keys())
        df_com = df_com.loc[df_com['VEHSERINAME_ID'] == keys[0]]
        df_com = pd.DataFrame(df_com, columns=[args.COMPNAME, args.SUMVERILOSS]).reset_index(drop=True)
        df_com = dict(zip(df_com[args.COMPNAME], df_com[args.SUMVERILOSS]))
        df_com = dict(sorted(df_com.items(), key=lambda x: x[1], reverse=False))
        component2id = dict(zip(list(df_com.keys()), range(1, len(df_com) + 1)))
        return component2id

    def youqi_25_process(self,df):
        '''25个高端品牌油漆数据处理'''
        df = self.transform_process(df)
        df.dropna(subset=[args.COMPNAME], how='any', axis=0, inplace=True)

        #工时项目排序
        # component2id=self.compname_asc(df)
        # with open('high_brand/youqi/component2id.json', 'w', encoding='utf-8') as f:
        #     json.dump(component2id, f, ensure_ascii=False)

        with open('high_brand/youqi/component2id.json', encoding='utf-8') as f:
            component2id=json.load(f)

        def transform(x):
            if x in component2id:
                con = component2id[x]
            else:
                con = np.nan
            return con

        df['COMPONENT'] = df.apply(lambda x: transform(x[args.COMPNAME]), axis=1)
        # df.dropna(subset=['COMPONENT',args.IS4S,'COMPNAME',args.SUMVERILOSS],how='any',axis=0,inplace=True)
        df.dropna(subset=['REGION', 'COMPONENT', 'VEHSERINAME_ID', args.IS4S, args.COMPNAME, args.SUMVERILOSS],
                  how='any', axis=0, inplace=True)
        # 区域去重后转ID
        regions = list(set(list(df['REGION'].values)))
        REGION2ID = dict(zip(regions, range(1, len(regions) + 1)))
        df['REGION_ID'] = df['REGION'].map(REGION2ID)
        with open('high_brand/youqi/region2id.json', 'w', encoding='utf-8') as writer:
            json.dump(REGION2ID, writer, ensure_ascii=False)
        df = df[['REGION_ID', 'COMPONENT', 'VEHSERINAME_ID', args.IS4S, args.SUMVERILOSS]].astype(float)
        print(df.shape[0])
        df_all = pd.DataFrame()
        for i in range(10):
            df_all = pd.concat([df_all, df], axis=0)
        print(len(df_all))

        return df

    def banjin_25_process(self,df):
        '''25个高端品牌钣金数据处理'''
        df = self.transform_process(df)
        df.dropna(subset=[args.COMPNAME], how='any', axis=0, inplace=True)

        # 工时项目排序
        # component2id=self.compname_asc(df)
        # with open('high_brand/banjin/component2id.json', 'w', encoding='utf-8') as f:
        #     json.dump(component2id, f, ensure_ascii=False)

        with open('high_brand/banjin/component2id.json', encoding='utf-8') as f:
            component2id = json.load(f)

        def transform(x):
            if x in component2id:
                con = component2id[x]
            else:
                con = np.nan
            return con

        df['COMPONENT'] = df.apply(lambda x: transform(x[args.COMPNAME]), axis=1)
        # df.dropna(subset=['COMPONENT',args.IS4S,'COMPNAME',args.SUMVERILOSS],how='any',axis=0,inplace=True)
        df.dropna(subset=['REGION', 'COMPONENT', 'VEHSERINAME_ID', args.IS4S, args.COMPNAME, args.SUMVERILOSS],
                  how='any', axis=0, inplace=True)
        # 区域去重后转ID
        regions = list(set(list(df['REGION'].values)))
        REGION2ID = dict(zip(regions, range(1, len(regions) + 1)))
        df['REGION_ID'] = df['REGION'].map(REGION2ID)
        with open('high_brand/banjin/region2id.json', 'w', encoding='utf-8') as writer:
            json.dump(REGION2ID, writer, ensure_ascii=False)
        df = df[['REGION_ID', 'COMPONENT', 'VEHSERINAME_ID', args.IS4S, args.SUMVERILOSS]].astype(float)
        print(df.shape[0])
        df_all = pd.DataFrame()
        for i in range(10):
            df_all = pd.concat([df_all, df], axis=0)
        print(len(df_all))

        return df

    def chaizhuang_25_process(self,df):
        '''25个高端品牌拆装数据处理'''
        df = self.transform_process(df)
        df.dropna(subset=[args.COMPNAME], how='any', axis=0, inplace=True)

        # 工时项目排序
        # component2id=self.compname_asc(df)
        # with open('high_brand/banjin/component2id.json', 'w', encoding='utf-8') as f:
        #     json.dump(component2id, f, ensure_ascii=False)

        with open('high_brand/chaizhuang/component2id.json', encoding='utf-8') as f:
            component2id = json.load(f)

        def transform(x):
            if x in component2id:
                con = component2id[x]
            else:
                con = np.nan
            return con

        df['COMPONENT'] = df.apply(lambda x: transform(x[args.COMPNAME]), axis=1)
        # df.dropna(subset=['COMPONENT',args.IS4S,'COMPNAME',args.SUMVERILOSS],how='any',axis=0,inplace=True)
        df.dropna(subset=['REGION', 'COMPONENT', 'VEHSERINAME_ID', args.IS4S, args.COMPNAME, args.SUMVERILOSS],
                  how='any', axis=0, inplace=True)
        # 区域去重后转ID
        regions = list(set(list(df['REGION'].values)))
        REGION2ID = dict(zip(regions, range(1, len(regions) + 1)))
        df['REGION_ID'] = df['REGION'].map(REGION2ID)
        with open('high_brand/chaizhuang/region2id.json', 'w', encoding='utf-8') as writer:
            json.dump(REGION2ID, writer, ensure_ascii=False)
        df = df[['REGION_ID', 'COMPONENT', 'VEHSERINAME_ID', args.IS4S, args.SUMVERILOSS]].astype(float)
        print(df.shape[0])

        df_all = pd.DataFrame()
        for i in range(10):
            df_all = pd.concat([df_all, df], axis=0)
        print(len(df_all))

        return df

    def youqi_model(self, youqi,area,chexi2id):
        '''33个大区域油漆模型'''
        start_time_model = time.time()
        df = self.get_youqi_data(chexi2id)
        df = self.outlier_process_train(df, youqi,area)
        # df.to_csv('data1/gongshi.csv',index=None,encoding='utf-8')
        df = self.youqi_process(df,youqi,area)
        ss, xgb_model, x_test, y_test = train_model1(df)
        print('模型训练耗时为{}'.format(get_time_dif(start_time_model)))
        joblib.dump(ss, "{}/{}_data_ss.model".format(area,youqi))  ## 将标准化模型保存
        joblib.dump(xgb_model, "{}/{}_xgb.model".format(area,youqi))  ## 将模型保存
        y_pred = xgb_model.predict(x_test)
        valuation(y_pred, y_test)

    def single_area_youqi_model(self, youqi,area,chexi2id):
        '''单独区域的油漆模型'''
        start_time_model = time.time()
        df = self.get_single_area_youqi_data(area,chexi2id)
        df = self.outlier_process_train(df, youqi,area)
        df = self.youqi_process(df,youqi,area)
        ss, xgb_model, x_test, y_test = train_model1(df)
        print('模型训练耗时为{}'.format(get_time_dif(start_time_model)))
        joblib.dump(ss, "{}/{}_data_ss.model".format(area,youqi))  ## 将标准化模型保存
        joblib.dump(xgb_model, "{}/{}_xgb.model".format(area,youqi))  ## 将模型保存
        y_pred = xgb_model.predict(x_test)
        valuation(y_pred, y_test)

    def outlier_initial_process(self,data):
        '''异常值初步处理，处理高端品牌中全国只出现2次的样本，一个样本数据很大，另一个样本数据很小,删除小的'''
        sumvers = list(data)
        results = []
        if len(sumvers) == 2:
            value = min(sumvers)
            index = sumvers.index(min(sumvers))
            if index == 0:
                ceof = (sumvers[1] - sumvers[0]) / sumvers[0]
            else:
                ceof = (sumvers[0] - sumvers[1]) / sumvers[1]
            if ceof > 5:
                results.append(value)
                return results

    def delete_outlier_initial(self,df):
        outlier_initial = df.groupby([args.IS4S, 'VEHSERINAME_GRADE', args.COMPNAME])[args.SUMVERILOSS].apply(
            self.outlier_initial_process).reset_index()
        # 有异常值的取出来
        outlier_initial = outlier_initial[outlier_initial[args.SUMVERILOSS].notnull()]
        if len(outlier_initial):
            outlier_initial = outlier_initial.set_index([args.IS4S, 'VEHSERINAME_GRADE', args.COMPNAME])[
                args.SUMVERILOSS].apply(
                pd.Series).stack().reset_index()
            outlier_initial.columns = [args.IS4S, 'VEHSERINAME_GRADE', args.COMPNAME, 'INDEX', args.SUMVERILOSS]
            # 去重
            outlier_initial = outlier_initial[
                [args.IS4S, 'VEHSERINAME_GRADE', args.COMPNAME, args.SUMVERILOSS]].drop_duplicates()
            outlier_initial['FLAG'] = 1
            df = pd.merge(df, outlier_initial, how='left',
                          on=[args.IS4S, 'VEHSERINAME_GRADE', args.COMPNAME, args.SUMVERILOSS])
            df.drop(df[df['FLAG'] == 1].index, inplace=True)
            df.drop('FLAG', axis=1, inplace=True)
        return df

    def youqi_25_model(self,group_dict,group2id):
        '''25个高端品牌油漆模型'''
        start_time_model = time.time()
        df = self.get_25_youqi_data(group_dict,group2id)
        df=self.delete_outlier_initial(df)
        df = self.gd_outlier_process_train(df,'油漆','全国')
        df = self.youqi_25_process(df)
        ss, xgb_model, x_test, y_test = train_model4(df)
        print('模型训练耗时为{}'.format(get_time_dif(start_time_model)))
        joblib.dump(ss, "high_brand/youqi/youqi_ss.model")  ## 将标准化模型保存
        joblib.dump(xgb_model, "high_brand/youqi/youqi_xgb.model")  ## 将模型保存
        y_pred = xgb_model.predict(x_test)
        valuation(y_pred, y_test)

    def banjin_model(self, banjin,area,chexi2id):
        '''33个区域钣金模型'''
        start_time_model = time.time()
        df = self.get_banjin_data(chexi2id)
        df = self.outlier_process_train(df, banjin,area)
        df = self.banjin_process(df,banjin,area)
        ss, xgb_model, x_test, y_test = train_model2(df)
        print('模型训练耗时为{}'.format(get_time_dif(start_time_model)))
        joblib.dump(ss, "{}/{}_data_ss.model".format(area,banjin))  ## 将标准化模型保存
        joblib.dump(xgb_model, "{}/{}_xgb.model".format(area,banjin))  ## 将模型保存
        y_pred = xgb_model.predict(x_test)
        valuation(y_pred, y_test)

    def single_area_banjin_model(self,banjin, area,chexi2id):
        '''单个区域钣金模型'''
        start_time_model = time.time()
        df = self.get_single_area_banjin_data(area,chexi2id)
        df = self.outlier_process_train(df, banjin, area)
        df = self.banjin_process(df, banjin, area)
        ss, xgb_model, x_test, y_test = train_model2(df)
        print('模型训练耗时为{}'.format(get_time_dif(start_time_model)))
        joblib.dump(ss, "{}/{}_data_ss.model".format(area,banjin))  ## 将标准化模型保存
        joblib.dump(xgb_model, "{}/{}_xgb.model".format(area,banjin))  ## 将模型保存
        y_pred = xgb_model.predict(x_test)
        valuation(y_pred, y_test)


    def banjin_25_model(self,group_dict,group2id):
        '''25个高端品牌钣金模型'''
        start_time_model = time.time()
        df = self.get_25_banjin_data(group_dict,group2id)
        df = self.delete_outlier_initial(df)
        df = self.gd_outlier_process_train(df,'钣金','全国')
        df = self.banjin_25_process(df)
        ss, xgb_model, x_test, y_test = train_model4(df)
        print('模型训练耗时为{}'.format(get_time_dif(start_time_model)))
        joblib.dump(ss, "high_brand/banjin/banjin_ss.model")  ## 将标准化模型保存
        joblib.dump(xgb_model, "high_brand/banjin/banjin_xgb.model")  ## 将模型保存
        y_pred = xgb_model.predict(x_test)
        valuation(y_pred, y_test)

    def chaizhaung_model(self, chaizhuang,area,chexi2id):
        '''33个区域拆装模型'''
        start_time_model = time.time()
        df = self.get_chaizhuang_data(chexi2id)
        df = self.outlier_process_train(df,chaizhuang,area)
        df = self.chaizhuang_process(df,chaizhuang,area)
        ss, xgb_model, x_test, y_test = train_model3(df)
        print('模型训练耗时为{}'.format(get_time_dif(start_time_model)))
        joblib.dump(ss, "{}/{}_data_ss.model".format(area,chaizhuang))  ## 将标准化模型保存
        joblib.dump(xgb_model, "{}/{}_xgb.model".format(area,chaizhuang))  ## 将模型保存
        y_pred = xgb_model.predict(x_test)
        valuation(y_pred, y_test)

    def single_area_chaizhaung_model(self,chaizhuang, area,chexi2id):
        '''单个区域拆装模型'''
        start_time_model = time.time()
        df = self.get_single_area_chaizhuang_data(area,chexi2id)
        df = self.outlier_process_train(df, chaizhuang, area)
        df = self.chaizhuang_process(df, chaizhuang, area)
        ss, xgb_model, x_test, y_test = train_model3(df)
        print('模型训练耗时为{}'.format(get_time_dif(start_time_model)))
        joblib.dump(ss, "{}/{}_data_ss.model".format(area,chaizhuang))  ## 将标准化模型保存
        joblib.dump(xgb_model, "{}/{}_xgb.model".format(area,chaizhuang))  ## 将模型保存
        y_pred = xgb_model.predict(x_test)
        valuation(y_pred, y_test)

    def chaizhaung_25_model(self,group_dict,group2id):
        '''25个高端品牌拆装模型'''
        start_time_model = time.time()
        df = self.get_25_chaizhuang_data(group_dict,group2id)
        df = self.delete_outlier_initial(df)
        df = self.gd_outlier_process_train(df,'拆装','全国')
        df = self.chaizhuang_25_process(df)
        ss, xgb_model, x_test, y_test = train_model4(df)
        print('模型训练耗时为{}'.format(get_time_dif(start_time_model)))
        joblib.dump(ss, "high_brand/chaizhuang/chaizhuang_ss.model")  ## 将标准化模型保存
        joblib.dump(xgb_model, "high_brand/chaizhuang/chaizhuang_xgb.model")  ## 将模型保存
        y_pred = xgb_model.predict(x_test)
        valuation(y_pred, y_test)

class Process(Extract):
    def trunct_table(self):
        '''清空数据表'''
        comm1 = '''truncate table LB_GS_SYSTEM'''
        comm2 = '''truncate table LB_GS_ORIGINAL_HANDLE'''
        comm3 = '''truncate table LB_GS_ORIGINAL_ABNORMAL'''
        comm4 = '''truncate table LB_GS_NATIONAL_GS_DATA'''
        list1 = [comm1, comm2, comm3,comm4]
        for comm in list1:
            self.oracle.executeCommit(comm, self.account)
        self.logger.info(('trunct table done!'))
        # print('trunct table done!')

    def handle2oracle(self):
        '''取载入表1年的数据到处理表中'''
        comm1 = '''select  max(VERIFYFINALDATE) from  LB_GS_ORIGINAL_DATA_LOAD t'''
        date = self.oracle.getData(comm1, self.account)
        date = date['MAX(VERIFYFINALDATE)'][0]
        start_time, endtime = get_time_from_table_1year(date)
        comm2 = '''insert into LB_GS_ORIGINAL_HANDLE select * from LB_GS_ORIGINAL_DATA_LOAD where VERIFYFINALDATE between to_date('{}','yyyy/mm/dd hh24:mi:ss') and to_date('{}','yyyy/mm/dd hh24:mi:ss') '''.format(
            start_time, endtime)
        self.oracle.executeCommitSubmit(comm2, self.account)
        self.logger.info(('handle2oracle done!'))
        # print('handle2oracle done!')

    def outlier(self):
        '''工时模块模型异常数据表'''
        stockpage_dir='outliers'
        pages = map(lambda _: os.path.join(stockpage_dir, _), os.listdir(stockpage_dir))
        pages = filter(lambda _: _.endswith('csv'), pages)
        datas=pd.DataFrame()
        for path in pages:
            df=pd.read_csv(path)
            datas=pd.concat([datas,df],axis=0)
        datas['REPAIRTYPE']=datas['REPAIRTYPE'].replace('chaizhuang','拆装').replace('youqi','油漆').replace('banjin','钣金')
        tableName = 'LB_GS_original_abnormal'
        self.oracle.BatchinsertDataToTable(datas, tableName,self.account)
        self.logger.info(('outlier done!'))
        # print('outlier done!')

    def youqi_statistics_pre(self,area,chexi2id):
        '''油漆数据统计预测'''
        if area=='large_area':
            df = self.get_youqi_data(chexi2id)
        else:
            df = self.get_single_area_youqi_data(area,chexi2id)
        df = self.outlier_process_stac(df)
        # df.to_csv('youqi/统计去异常值.csv',index=None,encoding='utf-8')
        # df[args.CAR] = df[args.CAR].apply(get_upper)
        # df[args.CAR]=df[args.CAR].apply(chexi_standard)
        df1 = df[['REGION', 'VEHSERINAME_ID', args.IS4S, args.COMPNAME, args.SUMVERILOSS]]
        # 数据转换
        df1['WORK_TYPE'] = '油漆'
        statistic_data = df1.groupby(['REGION', args.COMPNAME, 'VEHSERINAME_ID', args.IS4S, 'WORK_TYPE'])[
            args.SUMVERILOSS].describe().reset_index()
        mode = df1.groupby(['REGION', args.COMPNAME, 'VEHSERINAME_ID', args.IS4S, 'WORK_TYPE'])[
            args.SUMVERILOSS].agg(lambda x: x.value_counts().index[0]).reset_index()
        mode.rename(columns={args.SUMVERILOSS: 'mode'}, inplace=True)
        datas = pd.merge(statistic_data, mode, on=['REGION', args.COMPNAME, 'VEHSERINAME_ID', args.IS4S, 'WORK_TYPE'],
                         how='inner')
        datas['VEHSERINAME_GROUP'] = datas['VEHSERINAME_ID'].map(VEHSERINAME_ID2alpha)

        # XGboost测试
        test_data = datas.copy()
        ss = joblib.load("{}/youqi_data_ss.model".format(area))  ## 加载模型
        xgb_model = joblib.load("{}/youqi_xgb.model".format(area))  ## 加载模型

        with open('{}/youqi_component2id.json'.format(area), encoding='utf-8') as reader2:
            component2id = json.load(reader2)

        with open('{}/youqi_id2component.json'.format(area), encoding='utf-8') as reader3:
            id2component = json.load(reader3)

        with open('{}/youqi_region2id.json'.format(area), encoding='utf-8') as reader4:
            region2id = json.load(reader4)
        # test_data1=pd.DataFrame()
        test_data['compnent'] = test_data[args.COMPNAME].map(component2id)
        # test_data['painting_type'] = test_data['PAINTING_TYPE'].apply(paint_trainsform)
        # test_data['VEHSERINAME_ID'] = test_data['VEHSERINAME_GROUP'].map(alpha2VEHSERINAME_ID)
        test_data['REGION_ID'] = test_data['REGION'].map(region2id)
        test_data.dropna(subset=['REGION_ID', 'compnent', 'VEHSERINAME_ID'], how='any', axis=0, inplace=True)
        test_data = test_data.reset_index(drop=True)
        df2 = pd.DataFrame(test_data, columns=['REGION_ID', 'compnent', 'VEHSERINAME_ID', args.IS4S])

        x_data = ss.transform(df2)
        datas_pre = list(np.exp(xgb_model.predict(x_data)))
        df3 = pd.DataFrame()
        df3['XGBOOST_VALUE'] = datas_pre
        df3['参考值'] = df3['XGBOOST_VALUE'].map(lambda x: round(x, -1))
        df3['上限值'] = df3['参考值'].map(lambda x: round(1.1 * x, -1))
        df4 = pd.concat([test_data, df3], axis=1)
        df4 = pd.DataFrame(df4, columns=['REGION', args.COMPNAME, 'VEHSERINAME_GROUP', args.IS4S,
                                         'WORK_TYPE', 'count', 'mean', 'std', 'min', '50%', 'max', 'mode', '上限值',
                                         '参考值'])
        df4.rename(columns={'50%': 'median', 'mode': 'mode1'}, inplace=True)
        df4.to_csv('{}/油漆统计数据及模型预测.csv'.format(area), index=None, encoding='utf-8')
        # oracle = useOracle("VDATA", "xdf123", "LBORA170")
        # oracle.creatDataFrame(df4,'youqi_statis')
        # oracle.insertDataToTable(df4,'youqi_statis')

    def banjin_statistics_pre(self,area,chexi2id):
        '''钣金数据统计预测'''
        if area=='large_area':
            df = self.get_banjin_data(chexi2id)
        else:
            df = self.get_single_area_banjin_data(area,chexi2id)
        df = self.outlier_process_stac(df)
        # df[args.CAR] = df[args.CAR].apply(get_upper)
        # df[args.CAR]=df[args.CAR].apply(chexi_standard)
        df1 = df[['REGION', 'VEHSERINAME_ID', args.IS4S, args.COMPNAME, args.SUMVERILOSS]]
        # 数据转换
        df1['WORK_TYPE'] = '钣金'
        statistic_data = df1.groupby(['REGION', args.COMPNAME, 'VEHSERINAME_ID', args.IS4S, 'WORK_TYPE'])[
            args.SUMVERILOSS].describe().reset_index()
        mode = df1.groupby(['REGION', args.COMPNAME, 'VEHSERINAME_ID', args.IS4S, 'WORK_TYPE'])[
            args.SUMVERILOSS].agg(
            lambda x: x.value_counts().index[0]).reset_index()
        mode.rename(columns={args.SUMVERILOSS: 'mode'}, inplace=True)
        datas = pd.merge(statistic_data, mode, on=['REGION', args.COMPNAME, 'VEHSERINAME_ID', args.IS4S, 'WORK_TYPE'],
                         how='inner')
        # datas.rename(columns={'count':'计数(条)','mean':'均值','std':'标准差','min':'最小值','25%':'四分之一位','50%':'中位数','75%':'四分之三位','max':'最大值','mode':'众数'},inplace=True)
        # VEHSERINAME_ID2alpha={1:'A(180档)',2:'B(240档)',3:'C(300档)',4:'D(380档)',5:'E(450档)',6:'F(500档)',7:'G(600档)',8:'H(680档)',9:'J(800档)',10:'K(1000档)'}
        datas['VEHSERINAME_GROUP'] = datas['VEHSERINAME_ID'].map(VEHSERINAME_ID2alpha)
        # alpha2VEHSERINAME_ID={v:k for k,v in VEHSERINAME_ID2alpha.items()}
        # datas=datas[datas['喷漆类型']=='全喷']
        # datas['statistic1']=datas.apply(lambda x:round(np.median([x['mean'],x['50%'],x['mode']])),axis=1)
        # datas['statistic2']=datas.apply(lambda x:round(np.mean([x['mean'],x['50%'],x['mode']])),axis=1)

        # XGboost测试
        test_data = datas.copy()
        ss = joblib.load("{}/banjin_data_ss.model".format(area))  ## 加载模型
        xgb_model = joblib.load("{}/banjin_xgb.model".format(area))  ## 加载模型

        with open('{}/banjin_component2id.json'.format(area), encoding='utf-8') as reader2:
            component2id = json.load(reader2)

        with open('{}/banjin_id2component.json'.format(area), encoding='utf-8') as reader3:
            id2component = json.load(reader3)

        with open('{}/banjin_region2id.json'.format(area), encoding='utf-8') as reader4:
            region2id = json.load(reader4)
        # test_data1=pd.DataFrame()
        test_data['compnent'] = test_data[args.COMPNAME].map(component2id)
        # test_data['painting_type'] = test_data['PAINTING_TYPE'].apply(paint_trainsform)
        # test_data['VEHSERINAME_ID'] = test_data['VEHSERINAME_GROUP'].map(alpha2VEHSERINAME_ID)
        test_data['REGION_ID'] = test_data['REGION'].map(region2id)
        test_data.dropna(subset=['REGION_ID', 'compnent', 'VEHSERINAME_ID'], how='any', axis=0, inplace=True)
        test_data = test_data.reset_index(drop=True)
        df2 = pd.DataFrame(test_data, columns=['REGION_ID', 'compnent', 'VEHSERINAME_ID', args.IS4S])

        x_data = ss.transform(df2)
        datas_pre = list(np.exp(xgb_model.predict(x_data)))
        df3 = pd.DataFrame()
        df3['XGBOOST_VALUE'] = datas_pre
        df3['参考值'] = df3['XGBOOST_VALUE'].map(lambda x: round(x, -1))
        df3['上限值'] = df3['参考值'].map(lambda x: round(1.1 * x, -1))
        df4 = pd.concat([test_data, df3], axis=1)
        df4 = pd.DataFrame(df4, columns=['REGION', args.COMPNAME, 'VEHSERINAME_GROUP', args.IS4S,
                                         'WORK_TYPE', 'count', 'mean', 'std', 'min', '50%', 'max', 'mode', '上限值',
                                         '参考值'])
        df4.rename(columns={'50%': 'median', 'mode': 'mode1'}, inplace=True)
        df4.to_csv('{}/钣金统计数据及模型预测.csv'.format(area), index=None, encoding='utf-8')
        # oracle = useOracle("VDATA", "xdf123", "LBORA170")
        # oracle.creatDataFrame(df4,'banjin_statis')
        # oracle.insertDataToTable(df4,'banjin_statis')

    def chaizhuang_statistics_pre(self,area,chexi2id):
        '''拆装数据统计预测'''
        if area=='large_area':
            df = self.get_chaizhuang_data(chexi2id)
        else:
            df = self.get_single_area_chaizhuang_data(area,chexi2id)
        df = self.outlier_process_stac(df)
        # df[args.CAR] = df[args.CAR].apply(get_upper)
        # df[args.CAR]=df[args.CAR].apply(chexi_standard)
        df1 = df[['REGION', 'VEHSERINAME_ID', args.IS4S, args.COMPNAME, args.SUMVERILOSS]]
        # 数据转换
        df1['WORK_TYPE'] = '拆装'
        statistic_data = df1.groupby(['REGION', args.COMPNAME, 'VEHSERINAME_ID', args.IS4S, 'WORK_TYPE'])[
            args.SUMVERILOSS].describe().reset_index()
        mode = df1.groupby(['REGION', args.COMPNAME, 'VEHSERINAME_ID', args.IS4S, 'WORK_TYPE'])[
            args.SUMVERILOSS].agg(lambda x: x.value_counts().index[0]).reset_index()
        mode.rename(columns={args.SUMVERILOSS: 'mode'}, inplace=True)
        datas = pd.merge(statistic_data, mode, on=['REGION', args.COMPNAME, 'VEHSERINAME_ID', args.IS4S, 'WORK_TYPE'],
                         how='inner')
        # datas.rename(columns={'count':'计数(条)','mean':'均值','std':'标准差','min':'最小值','25%':'四分之一位','50%':'中位数','75%':'四分之三位','max':'最大值','mode':'众数'},inplace=True)
        # VEHSERINAME_ID2alpha={1:'A(180档)',2:'B(240档)',3:'C(300档)',4:'D(380档)',5:'E(450档)',6:'F(500档)',7:'G(600档)',8:'H(680档)',9:'J(800档)',10:'K(1000档)'}
        datas['VEHSERINAME_GROUP'] = datas['VEHSERINAME_ID'].map(VEHSERINAME_ID2alpha)
        # alpha2VEHSERINAME_ID={v:k for k,v in VEHSERINAME_ID2alpha.items()}
        # datas['statistic1']=datas.apply(lambda x:round(np.median([x['mean'],x['50%'],x['mode']])),axis=1)
        # datas['statistic2']=datas.apply(lambda x:round(np.mean([x['mean'],x['50%'],x['mode']])),axis=1)

        # XGboost测试
        test_data = datas.copy()
        ss = joblib.load("{}/chaizhuang_data_ss.model".format(area))  ## 加载模型
        xgb_model = joblib.load("{}/chaizhuang_xgb.model".format(area))  ## 加载模型

        with open('{}/chaizhuang_component2id.json'.format(area), encoding='utf-8') as reader2:
            component2id = json.load(reader2)

        with open('{}/chaizhuang_id2component.json'.format(area), encoding='utf-8') as reader3:
            id2component = json.load(reader3)

        with open('{}/chaizhuang_region2id.json'.format(area), encoding='utf-8') as reader4:
            region2id = json.load(reader4)
        # test_data1=pd.DataFrame()
        test_data['compnent'] = test_data[args.COMPNAME].map(component2id)
        # test_data['painting_type'] = test_data['PAINTING_TYPE'].apply(paint_trainsform)
        # test_data['VEHSERINAME_ID'] = test_data['VEHSERINAME_GROUP'].map(alpha2VEHSERINAME_ID)
        test_data['REGION_ID'] = test_data['REGION'].map(region2id)
        test_data.dropna(subset=['REGION_ID', 'compnent', 'VEHSERINAME_ID'], how='any', axis=0, inplace=True)
        test_data = test_data.reset_index(drop=True)
        df2 = pd.DataFrame(test_data, columns=['REGION_ID', 'compnent', 'VEHSERINAME_ID', args.IS4S])

        x_data = ss.transform(df2)
        datas_pre = list(np.exp(xgb_model.predict(x_data)))
        df3 = pd.DataFrame()
        df3['XGBOOST_VALUE'] = datas_pre
        df3['参考值'] = df3['XGBOOST_VALUE'].map(lambda x: round(x, -1))
        df3['上限值'] = df3['参考值'].map(lambda x: round(1.1 * x, -1))
        df4 = pd.concat([test_data, df3], axis=1)
        df4 = pd.DataFrame(df4, columns=['REGION', args.COMPNAME, 'VEHSERINAME_GROUP', args.IS4S,
                                         'WORK_TYPE', 'count', 'mean', 'std', 'min', '50%', 'max', 'mode', '上限值',
                                         '参考值'])
        df4.rename(columns={'50%': 'median', 'mode': 'mode1'}, inplace=True)
        df4.to_csv('{}/拆装统计数据及模型预测.csv'.format(area), index=None, encoding='utf-8')
        # oracle = useOracle("VDATA", "xdf123", "LBORA170")
        # oracle.creatDataFrame(df4,'chaizhuang_statis')
        # oracle.insertDataToTable(df4,'chaizhuang_statis')

    def jixiu_statistics_pre(self,area,chexi2id):
        '''机修数据统计'''
        if area=='large_area':
            df = self.get_jixiu_data(chexi2id)
        else:
            df = self.get_single_area_jixiu_data(area,chexi2id)

        df = self.outlier_process_stac(df)
        df1 = df[['REGION', 'VEHSERINAME_ID', args.IS4S, args.COMPNAME, args.SUMVERILOSS]]
        df1['WORK_TYPE'] = '机修'
        statistic_data = df1.groupby(['REGION', args.COMPNAME, 'VEHSERINAME_ID', args.IS4S, 'WORK_TYPE'])[
            args.SUMVERILOSS].describe().reset_index()
        mode = df1.groupby(['REGION', args.COMPNAME, 'VEHSERINAME_ID', args.IS4S, 'WORK_TYPE'])[
            args.SUMVERILOSS].agg(lambda x: x.value_counts().index[0]).reset_index()
        mode.rename(columns={args.SUMVERILOSS: 'mode'}, inplace=True)
        datas = pd.merge(statistic_data, mode, on=['REGION', args.COMPNAME, 'VEHSERINAME_ID', args.IS4S, 'WORK_TYPE'],
                         how='inner')
        datas['VEHSERINAME_GROUP'] = datas['VEHSERINAME_ID'].map(VEHSERINAME_ID2alpha)
        df4 = pd.DataFrame(datas, columns=['REGION', args.COMPNAME, 'VEHSERINAME_GROUP', args.IS4S,
                                           'WORK_TYPE', 'count', 'mean', 'std', 'min', '50%', 'max', 'mode'])
        df4.rename(columns={'50%': 'median', 'mode': 'mode1'}, inplace=True)
        df4.to_csv('{}/机修统计数据.csv'.format(area), index=None, encoding='utf-8')
        # alpha2VEHSERINAME_ID={v:k for k,v in VEHSERINAME_ID2alpha.items()}
        # datas['statistic1']=datas.apply(lambda x:round(np.median([x['mean'],x['50%'],x['mode']])),axis=1)
        # datas['statistic2']=datas.apply(lambda x:round(np.mean([x['mean'],x['50%'],x['mode']])),axis=1)

    def diangong_statistics_pre(self,area,chexi2id):
        '''电工数据统计'''
        if area=='large_area':
            df = self.get_diangong_data(chexi2id)
        else:
            df = self.get_single_area_diangong_data(area,chexi2id)
        df = self.outlier_process_stac(df)
        df1 = df[['REGION', 'VEHSERINAME_ID', args.IS4S, args.COMPNAME, args.SUMVERILOSS]]
        df1['WORK_TYPE'] = '电工'
        statistic_data = df1.groupby(['REGION', args.COMPNAME, 'VEHSERINAME_ID', args.IS4S, 'WORK_TYPE'])[
            args.SUMVERILOSS].describe().reset_index()
        mode = df1.groupby(['REGION', args.COMPNAME, 'VEHSERINAME_ID', args.IS4S, 'WORK_TYPE'])[
            args.SUMVERILOSS].agg(
            lambda x: x.value_counts().index[0]).reset_index()
        mode.rename(columns={args.SUMVERILOSS: 'mode'}, inplace=True)
        datas = pd.merge(statistic_data, mode, on=['REGION', args.COMPNAME, 'VEHSERINAME_ID', args.IS4S, 'WORK_TYPE'],
                         how='inner')
        datas['VEHSERINAME_GROUP'] = datas['VEHSERINAME_ID'].map(VEHSERINAME_ID2alpha)
        df4 = pd.DataFrame(datas, columns=['REGION', args.COMPNAME, 'VEHSERINAME_GROUP', args.IS4S,
                                           'WORK_TYPE', 'count', 'mean', 'std', 'min', '50%', 'max', 'mode'])
        df4.rename(columns={'50%': 'median', 'mode': 'mode1'}, inplace=True)
        df4.to_csv('{}/电工统计数据.csv'.format(area), index=None, encoding='utf-8')
        # alpha2VEHSERINAME_ID={v:k for k,v in VEHSERINAME_ID2alpha.items()}
        # datas['statistic1']=datas.apply(lambda x:round(np.median([x['mean'],x['50%'],x['mode']])),axis=1)
        # datas['statistic2']=datas.apply(lambda x:round(np.mean([x['mean'],x['50%'],x['mode']])),axis=1)

    def youqi_statistics_25_pre(self,group_dict,group2id):
        '''25个高端品牌油漆统计及预测'''
        df = self.get_25_youqi_data(group_dict,group2id)
        df = self.delete_outlier_initial(df)
        df = self.gd_outlier_process_stac(df)
        # df[args.CAR] = df[args.CAR].apply(get_upper)
        # df[args.CAR]=df[args.CAR].apply(chexi_standard)
        df1 = df[['REGION', 'VEHSERINAME_ID', args.IS4S, args.COMPNAME, args.SUMVERILOSS]]
        # 数据转换
        df1['WORK_TYPE'] = '油漆'
        statistic_data = df1.groupby(['REGION', args.COMPNAME, 'VEHSERINAME_ID', args.IS4S, 'WORK_TYPE'])[
            args.SUMVERILOSS].describe().reset_index()
        mode = df1.groupby(['REGION', args.COMPNAME, 'VEHSERINAME_ID', args.IS4S, 'WORK_TYPE'])[
            args.SUMVERILOSS].agg(lambda x: x.value_counts().index[0]).reset_index()
        mode.rename(columns={args.SUMVERILOSS: 'mode'}, inplace=True)
        datas = pd.merge(statistic_data, mode, on=['REGION', args.COMPNAME, 'VEHSERINAME_ID', args.IS4S, 'WORK_TYPE'],
                         how='inner')
        # datas.rename(columns={'count':'计数(条)','mean':'均值','std':'标准差','min':'最小值','25%':'四分之一位','50%':'中位数','75%':'四分之三位','max':'最大值','mode':'众数'},inplace=True)
        # VEHSERINAME_ID2alpha={1:'A(180档)',2:'B(240档)',3:'C(300档)',4:'D(380档)',5:'E(450档)',6:'F(500档)',7:'G(600档)',8:'H(680档)',9:'J(800档)',10:'K(1000档)'}
        # datas['VEHSERINAME_GROUP']=datas['VEHSERINAME_ID'].map(VEHSERINAME_ID2alpha)
        with open('high_brand/group2id.json', encoding='utf-8') as f1:
            group2id = json.load(f1)
        id2group = {v: k for k, v in group2id.items()}
        datas['VEHSERINAME_GROUP'] = datas['VEHSERINAME_ID'].map(id2group)
        # alpha2VEHSERINAME_ID={v:k for k,v in VEHSERINAME_ID2alpha.items()}
        # datas=datas[datas['喷漆类型']=='全喷']
        # datas['statistic1']=datas.apply(lambda x:round(np.median([x['mean'],x['50%'],x['mode']])),axis=1)
        # datas['statistic2']=datas.apply(lambda x:round(np.mean([x['mean'],x['50%'],x['mode']])),axis=1)

        # XGboost测试
        test_data = datas.copy()
        ss = joblib.load("high_brand/youqi/youqi_ss.model")  ## 加载模型
        xgb_model = joblib.load("high_brand/youqi/youqi_xgb.model")  ## 加载模型

        with open('high_brand/youqi/component2id.json', encoding='utf-8') as reader2:
            component2id = json.load(reader2)

        # with open('high_brand/youqi/id2component.json', encoding='utf-8') as reader3:
        #     id2component = json.load(reader3)

        with open('high_brand/youqi/region2id.json', encoding='utf-8') as reader4:
            region2id = json.load(reader4)
        # test_data1=pd.DataFrame()
        test_data['compnent'] = test_data[args.COMPNAME].map(component2id)
        # test_data['painting_type'] = test_data['PAINTING_TYPE'].apply(paint_trainsform)
        # test_data['VEHSERINAME_ID'] = test_data['VEHSERINAME_GROUP'].map(alpha2VEHSERINAME_ID)
        test_data['REGION_ID'] = test_data['REGION'].map(region2id)
        test_data.dropna(subset=['REGION_ID', 'compnent', 'VEHSERINAME_ID'], how='any', axis=0, inplace=True)
        test_data = test_data.reset_index(drop=True)
        df2 = pd.DataFrame(test_data, columns=['REGION_ID', 'compnent', 'VEHSERINAME_ID', args.IS4S])

        x_data = ss.transform(df2)
        datas_pre = list(np.exp(xgb_model.predict(x_data)))
        df3 = pd.DataFrame()
        df3['XGBOOST_VALUE'] = datas_pre
        df3['参考值'] = df3['XGBOOST_VALUE'].map(lambda x: round(x, -1))
        df3['上限值'] = df3['参考值'].map(lambda x: round(1.1 * x, -1))
        df4 = pd.concat([test_data, df3], axis=1)
        df4 = pd.DataFrame(df4, columns=['REGION', args.COMPNAME, 'VEHSERINAME_GROUP', args.IS4S,
                                         'WORK_TYPE', 'count', 'mean', 'std', 'min', '50%', 'max', 'mode', '上限值',
                                         '参考值'])
        df4.rename(columns={'50%': 'median', 'mode': 'mode1'}, inplace=True)
        df4.to_csv('high_brand/youqi/25个高端品牌油漆统计数据及模型预测.csv', index=None, encoding='utf-8')
        # oracle = useOracle("VDATA", "xdf123", "LBORA170")
        # oracle.creatDataFrame(df4,'youqi_statis')
        # oracle.insertDataToTable(df4,'youqi_statis')

    def banjin_statistics_25_pre(self,group_dict,group2id):
        '''25个高端品牌钣金统计及预测'''
        df = self.get_25_banjin_data(group_dict,group2id)
        df = self.delete_outlier_initial(df)
        df = self.gd_outlier_process_stac(df)
        # df[args.CAR] = df[args.CAR].apply(get_upper)
        # df[args.CAR]=df[args.CAR].apply(chexi_standard)
        df1 = df[['REGION', 'VEHSERINAME_ID', args.IS4S, args.COMPNAME, args.SUMVERILOSS]]
        # 数据转换
        df1['WORK_TYPE'] = '钣金'
        statistic_data = df1.groupby(['REGION', args.COMPNAME, 'VEHSERINAME_ID', args.IS4S, 'WORK_TYPE'])[
            args.SUMVERILOSS].describe().reset_index()
        mode = df1.groupby(['REGION', args.COMPNAME, 'VEHSERINAME_ID', args.IS4S, 'WORK_TYPE'])[
            args.SUMVERILOSS].agg(lambda x: x.value_counts().index[0]).reset_index()
        mode.rename(columns={args.SUMVERILOSS: 'mode'}, inplace=True)
        datas = pd.merge(statistic_data, mode, on=['REGION', args.COMPNAME, 'VEHSERINAME_ID', args.IS4S, 'WORK_TYPE'],
                         how='inner')
        # datas.rename(columns={'count':'计数(条)','mean':'均值','std':'标准差','min':'最小值','25%':'四分之一位','50%':'中位数','75%':'四分之三位','max':'最大值','mode':'众数'},inplace=True)
        # VEHSERINAME_ID2alpha={1:'A(180档)',2:'B(240档)',3:'C(300档)',4:'D(380档)',5:'E(450档)',6:'F(500档)',7:'G(600档)',8:'H(680档)',9:'J(800档)',10:'K(1000档)'}
        with open('high_brand/group2id.json', encoding='utf-8') as f1:
            group2id = json.load(f1)
        id2group = {v: k for k, v in group2id.items()}
        datas['VEHSERINAME_GROUP'] = datas['VEHSERINAME_ID'].map(id2group)
        # alpha2VEHSERINAME_ID={v:k for k,v in VEHSERINAME_ID2alpha.items()}
        # datas=datas[datas['喷漆类型']=='全喷']
        # datas['statistic1']=datas.apply(lambda x:round(np.median([x['mean'],x['50%'],x['mode']])),axis=1)
        # datas['statistic2']=datas.apply(lambda x:round(np.mean([x['mean'],x['50%'],x['mode']])),axis=1)

        # XGboost测试
        test_data = datas.copy()
        ss = joblib.load("high_brand/banjin/banjin_ss.model")  ## 加载模型
        xgb_model = joblib.load("high_brand/banjin/banjin_xgb.model")  ## 加载模型

        with open('high_brand/banjin/component2id.json', encoding='utf-8') as reader2:
            component2id = json.load(reader2)

        # with open('high_brand/banjin/id2component.json', encoding='utf-8') as reader3:
        #     id2component = json.load(reader3)

        with open('high_brand/banjin/region2id.json', encoding='utf-8') as reader4:
            region2id = json.load(reader4)
        # test_data1=pd.DataFrame()
        test_data['compnent'] = test_data[args.COMPNAME].map(component2id)
        # test_data['painting_type'] = test_data['PAINTING_TYPE'].apply(paint_trainsform)
        # test_data['VEHSERINAME_ID'] = test_data['VEHSERINAME_GROUP'].map(alpha2VEHSERINAME_ID)
        test_data['REGION_ID'] = test_data['REGION'].map(region2id)
        test_data.dropna(subset=['REGION_ID', 'compnent', 'VEHSERINAME_ID'], how='any', axis=0, inplace=True)
        test_data = test_data.reset_index(drop=True)
        df2 = pd.DataFrame(test_data, columns=['REGION_ID', 'compnent', 'VEHSERINAME_ID', args.IS4S])

        x_data = ss.transform(df2)
        datas_pre = list(np.exp(xgb_model.predict(x_data)))
        df3 = pd.DataFrame()
        df3['XGBOOST_VALUE'] = datas_pre
        df3['参考值'] = df3['XGBOOST_VALUE'].map(lambda x: round(x, -1))
        df3['上限值'] = df3['参考值'].map(lambda x: round(1.1 * x, -1))
        df4 = pd.concat([test_data, df3], axis=1)
        df4 = pd.DataFrame(df4, columns=['REGION', args.COMPNAME, 'VEHSERINAME_GROUP', args.IS4S,
                                         'WORK_TYPE', 'count', 'mean', 'std', 'min', '50%', 'max', 'mode', '上限值',
                                         '参考值'])
        df4.rename(columns={'50%': 'median', 'mode': 'mode1'}, inplace=True)
        df4.to_csv('high_brand/banjin/25个高端品牌钣金统计数据及模型预测.csv', index=None, encoding='utf-8')
        # oracle = useOracle("VDATA", "xdf123", "LBORA170")
        # oracle.creatDataFrame(df4,'banjin_statis')
        # oracle.insertDataToTable(df4,'banjin_statis')

    def chaizhuang_statistics_25_pre(self,group_dict,group2id):
        '''25个高端品牌拆装统计及预测'''
        df = self.get_25_chaizhuang_data(group_dict,group2id)
        df = self.delete_outlier_initial(df)
        df = self.gd_outlier_process_stac(df)
        # df[args.CAR] = df[args.CAR].apply(get_upper)
        # df[args.CAR]=df[args.CAR].apply(chexi_standard)
        df1 = df[['REGION', 'VEHSERINAME_ID', args.IS4S, args.COMPNAME, args.SUMVERILOSS]]
        # 数据转换
        df1['WORK_TYPE'] = '拆装'
        statistic_data = df1.groupby(['REGION', args.COMPNAME, 'VEHSERINAME_ID', args.IS4S, 'WORK_TYPE'])[
            args.SUMVERILOSS].describe().reset_index()
        mode = df1.groupby(['REGION', args.COMPNAME, 'VEHSERINAME_ID', args.IS4S, 'WORK_TYPE'])[
            args.SUMVERILOSS].agg(lambda x: x.value_counts().index[0]).reset_index()
        mode.rename(columns={args.SUMVERILOSS: 'mode'}, inplace=True)
        datas = pd.merge(statistic_data, mode, on=['REGION', args.COMPNAME, 'VEHSERINAME_ID', args.IS4S, 'WORK_TYPE'],
                         how='inner')
        # datas.rename(columns={'count':'计数(条)','mean':'均值','std':'标准差','min':'最小值','25%':'四分之一位','50%':'中位数','75%':'四分之三位','max':'最大值','mode':'众数'},inplace=True)
        # VEHSERINAME_ID2alpha={1:'A(180档)',2:'B(240档)',3:'C(300档)',4:'D(380档)',5:'E(450档)',6:'F(500档)',7:'G(600档)',8:'H(680档)',9:'J(800档)',10:'K(1000档)'}
        # datas['VEHSERINAME_GROUP']=datas['VEHSERINAME_ID'].map(VEHSERINAME_ID2alpha)
        with open('high_brand/group2id.json', encoding='utf-8') as f1:
            group2id = json.load(f1)
        id2group = {v: k for k, v in group2id.items()}
        datas['VEHSERINAME_GROUP'] = datas['VEHSERINAME_ID'].map(id2group)
        # alpha2VEHSERINAME_ID={v:k for k,v in VEHSERINAME_ID2alpha.items()}
        # datas=datas[datas['喷漆类型']=='全喷']
        # datas['statistic1']=datas.apply(lambda x:round(np.median([x['mean'],x['50%'],x['mode']])),axis=1)
        # datas['statistic2']=datas.apply(lambda x:round(np.mean([x['mean'],x['50%'],x['mode']])),axis=1)

        # XGboost测试
        test_data = datas.copy()
        ss = joblib.load("high_brand/chaizhuang/chaizhuang_ss.model")  ## 加载模型
        xgb_model = joblib.load("high_brand/chaizhuang/chaizhuang_xgb.model")  ## 加载模型

        with open('high_brand/chaizhuang/component2id.json', encoding='utf-8') as reader2:
            component2id = json.load(reader2)

        # with open('high_brand/chaizhuang/id2component.json', encoding='utf-8') as reader3:
        #     id2component = json.load(reader3)

        with open('high_brand/chaizhuang/region2id.json', encoding='utf-8') as reader4:
            region2id = json.load(reader4)
        # test_data1=pd.DataFrame()
        test_data['compnent'] = test_data[args.COMPNAME].map(component2id)
        # test_data['painting_type'] = test_data['PAINTING_TYPE'].apply(paint_trainsform)
        # test_data['VEHSERINAME_ID'] = test_data['VEHSERINAME_GROUP'].map(alpha2VEHSERINAME_ID)
        test_data['REGION_ID'] = test_data['REGION'].map(region2id)
        test_data.dropna(subset=['REGION_ID', 'compnent', 'VEHSERINAME_ID'], how='any', axis=0, inplace=True)
        test_data = test_data.reset_index(drop=True)
        df2 = pd.DataFrame(test_data, columns=['REGION_ID', 'compnent', 'VEHSERINAME_ID', args.IS4S])

        x_data = ss.transform(df2)
        datas_pre = list(np.exp(xgb_model.predict(x_data)))
        df3 = pd.DataFrame()
        df3['XGBOOST_VALUE'] = datas_pre
        df3['参考值'] = df3['XGBOOST_VALUE'].map(lambda x: round(x, -1))
        df3['上限值'] = df3['参考值'].map(lambda x: round(1.1 * x, -1))
        df4 = pd.concat([test_data, df3], axis=1)
        df4 = pd.DataFrame(df4, columns=['REGION', args.COMPNAME, 'VEHSERINAME_GROUP', args.IS4S,
                                         'WORK_TYPE', 'count', 'mean', 'std', 'min', '50%', 'max', 'mode', '上限值',
                                         '参考值'])
        df4.rename(columns={'50%': 'median', 'mode': 'mode1'}, inplace=True)
        df4.to_csv('high_brand/chaizhuang/25个高端品牌拆装统计数据及模型预测.csv', index=None, encoding='utf-8')
        # oracle = useOracle("VDATA", "xdf123", "LBORA170")
        # oracle.creatDataFrame(df4,'banjin_statis')
        # oracle.insertDataToTable(df4,'banjin_statis')

    def jixiu_statistics_25_pre(self,group_dict,group2id):
        '''25个高端品牌机修统计'''
        df = self.get_25_jixiu_data(group_dict,group2id)
        df = self.outlier_process_stac(df)
        # df[args.CAR] = df[args.CAR].apply(get_upper)
        # df[args.CAR]=df[args.CAR].apply(chexi_standard)
        df1 = df[['REGION', 'VEHSERINAME_ID', args.IS4S, args.COMPNAME, args.SUMVERILOSS]]
        # 数据转换
        df1['WORK_TYPE'] = '机修'
        statistic_data = df1.groupby(['REGION', args.COMPNAME, 'VEHSERINAME_ID', args.IS4S, 'WORK_TYPE'])[
            args.SUMVERILOSS].describe().reset_index()
        mode = df1.groupby(['REGION', args.COMPNAME, 'VEHSERINAME_ID', args.IS4S, 'WORK_TYPE'])[
            args.SUMVERILOSS].agg(lambda x: x.value_counts().index[0]).reset_index()
        mode.rename(columns={args.SUMVERILOSS: 'mode'}, inplace=True)
        datas = pd.merge(statistic_data, mode, on=['REGION', args.COMPNAME, 'VEHSERINAME_ID', args.IS4S, 'WORK_TYPE'],
                         how='inner')
        # datas.rename(columns={'count':'计数(条)','mean':'均值','std':'标准差','min':'最小值','25%':'四分之一位','50%':'中位数','75%':'四分之三位','max':'最大值','mode':'众数'},inplace=True)
        # VEHSERINAME_ID2alpha={1:'A(180档)',2:'B(240档)',3:'C(300档)',4:'D(380档)',5:'E(450档)',6:'F(500档)',7:'G(600档)',8:'H(680档)',9:'J(800档)',10:'K(1000档)'}
        # datas['VEHSERINAME_GROUP']=datas['VEHSERINAME_ID'].map(VEHSERINAME_ID2alpha)
        with open('high_brand/group2id.json', encoding='utf-8') as f1:
            group2id = json.load(f1)
        id2group = {v: k for k, v in group2id.items()}
        datas['VEHSERINAME_GROUP'] = datas['VEHSERINAME_ID'].map(id2group)
        df4 = pd.DataFrame(datas, columns=['REGION', args.COMPNAME, 'VEHSERINAME_GROUP', args.IS4S,
                                           'WORK_TYPE', 'count', 'mean', 'std', 'min', '50%', 'max', 'mode'])
        df4.rename(columns={'50%': 'median', 'mode': 'mode1'}, inplace=True)
        df4.to_csv('high_brand/jixiu/25个高端品牌机修统计数据.csv', index=None, encoding='utf-8')
        # alpha2VEHSERINAME_ID={v:k for k,v in VEHSERINAME_ID2alpha.items()}
        # datas['statistic1']=datas.apply(lambda x:round(np.median([x['mean'],x['50%'],x['mode']])),axis=1)
        # datas['statistic2']=datas.apply(lambda x:round(np.mean([x['mean'],x['50%'],x['mode']])),axis=1)

    def diangong_statistics_25_pre(self,group_dict,group2id):
        '''25个高端品牌电工统计'''
        df = self.get_25_diangong_data(group_dict,group2id)
        df = self.outlier_process_stac(df)
        # df[args.CAR] = df[args.CAR].apply(get_upper)
        # df[args.CAR]=df[args.CAR].apply(chexi_standard)
        df1 = df[['REGION', 'VEHSERINAME_ID', args.IS4S, args.COMPNAME, args.SUMVERILOSS]]
        # 数据转换
        df1['WORK_TYPE'] = '电工'
        statistic_data = df1.groupby(['REGION', args.COMPNAME, 'VEHSERINAME_ID', args.IS4S, 'WORK_TYPE'])[
            args.SUMVERILOSS].describe().reset_index()
        mode = df1.groupby(['REGION', args.COMPNAME, 'VEHSERINAME_ID', args.IS4S, 'WORK_TYPE'])[
            args.SUMVERILOSS].agg(
            lambda x: x.value_counts().index[0]).reset_index()
        mode.rename(columns={args.SUMVERILOSS: 'mode'}, inplace=True)
        datas = pd.merge(statistic_data, mode, on=['REGION', args.COMPNAME, 'VEHSERINAME_ID', args.IS4S, 'WORK_TYPE'],
                         how='inner')
        # datas.rename(columns={'count':'计数(条)','mean':'均值','std':'标准差','min':'最小值','25%':'四分之一位','50%':'中位数','75%':'四分之三位','max':'最大值','mode':'众数'},inplace=True)
        # VEHSERINAME_ID2alpha={1:'A(180档)',2:'B(240档)',3:'C(300档)',4:'D(380档)',5:'E(450档)',6:'F(500档)',7:'G(600档)',8:'H(680档)',9:'J(800档)',10:'K(1000档)'}
        # datas['VEHSERINAME_GROUP']=datas['VEHSERINAME_ID'].map(VEHSERINAME_ID2alpha)
        with open('high_brand/group2id.json', encoding='utf-8') as f1:
            group2id = json.load(f1)
        id2group = {v: k for k, v in group2id.items()}
        datas['VEHSERINAME_GROUP'] = datas['VEHSERINAME_ID'].map(id2group)
        df4 = pd.DataFrame(datas, columns=['REGION', args.COMPNAME, 'VEHSERINAME_GROUP', args.IS4S,
                                           'WORK_TYPE', 'count', 'mean', 'std', 'min', '50%', 'max', 'mode'])
        df4.rename(columns={'50%': 'median', 'mode': 'mode1'}, inplace=True)
        df4.to_csv('high_brand/diangong/25个高端品牌电工统计数据.csv', index=None, encoding='utf-8')
        # alpha2VEHSERINAME_ID={v:k for k,v in VEHSERINAME_ID2alpha.items()}
        # datas['statistic1']=datas.apply(lambda x:round(np.median([x['mean'],x['50%'],x['mode']])),axis=1)
        # datas['statistic2']=datas.apply(lambda x:round(np.mean([x['mean'],x['50%'],x['mode']])),axis=1)

    def system_value_oracle(self):
        '''大地自定义全国工时价格处理后上传至数据库'''
        path = 'data/全国各机构工时数据汇总20200219.xlsx'
        # datas=pd.read_excel(path)
        datas = pd.ExcelFile(path)

        sheet_names = datas.sheet_names  # see all sheet names
        df1 = pd.read_excel(path,sheet_name='汇总表')
        df1.dropna(subset=['机构'], how='any', axis=0, inplace=True)
        jigou_ls = list(set(df1['机构'].tolist()))
        # for jigou in jigou_ls:
        #     if jigou in ['云南','深圳','江苏']:
        #         jigou_ls.remove(jigou)
        for sheet in sheet_names:
            if '车系分档' in sheet:
                sheet_names.remove(sheet)
        sheet_names.remove('汇总表')
        def transform():
            df = pd.DataFrame()
            for sheet_name in sheet_names:
                for jigou in jigou_ls:
                    try:
                        if jigou in sheet_name and '高端品牌' in sheet_name:
                            df2 = pd.read_excel(path, sheet_name=sheet_name, skiprows=1)
                            df2.drop(0, inplace=True)
                            col = df2.columns[:35]
                            df2 = pd.DataFrame(df2, columns=col)
                            df2 = df2.set_index(['Unnamed: 0', 'Unnamed: 1']).stack().reset_index()
                            df2.rename(
                                columns={'Unnamed: 0': '工时组', 'Unnamed: 1': '项目名称', 'level_2': 'VEHSERINAME_GRADE',
                                         0: 'SYSTEM_VALUE'}, inplace=True)
                            df2['REGION'] = jigou
                            df2['VEHSERINAME_TYPE'] = '高端品牌'
                            df2['IS4S'] = '服务站'
                            df2 = pd.DataFrame(df2, columns=['REGION', 'VEHSERINAME_TYPE', 'IS4S', '工时组', '项目名称',
                                                             'VEHSERINAME_GRADE', 'SYSTEM_VALUE'])

                        elif jigou in sheet_name and '服务站' in sheet_name:
                            df2 = pd.read_excel(path, sheet_name=sheet_name, skiprows=1)
                            col=df2.columns[:12]
                            df2=pd.DataFrame(df2,columns=col)
                            df2['REGION'] = jigou
                            df2['VEHSERINAME_TYPE'] = '乘用车'
                            df2['IS4S'] = '服务站'
                            df2 = df2.set_index(
                                ['REGION', 'VEHSERINAME_TYPE', 'IS4S', '工时组', '项目名称']).stack().reset_index()
                            df2.rename(columns={'level_5': 'VEHSERINAME_GRADE', 0: 'SYSTEM_VALUE'}, inplace=True)

                        elif jigou in sheet_name and '综修厂' in sheet_name:
                            df2 = pd.read_excel(path, sheet_name=sheet_name, skiprows=1)
                            col = df2.columns[:12]
                            df2 = pd.DataFrame(df2, columns=col)
                            df2['REGION'] = jigou
                            df2['VEHSERINAME_TYPE'] = '乘用车'
                            df2['IS4S'] = '综修厂'
                            df2 = df2.set_index(
                                ['REGION', 'VEHSERINAME_TYPE', 'IS4S', '工时组', '项目名称']).stack().reset_index()
                            df2.rename(columns={'level_5': 'VEHSERINAME_GRADE', 0: 'SYSTEM_VALUE'}, inplace=True)

                        else:
                            continue
                        df = pd.concat([df, df2], axis=0)
                    except Exception as e:
                        print(e)

            return df

        df = transform()
        df['VEHSERINAME_GRADE'].replace('（', '(', regex=True, inplace=True)
        df['VEHSERINAME_GRADE'].replace('）', ')', regex=True, inplace=True)
        df.drop_duplicates(subset=['REGION', 'VEHSERINAME_TYPE', 'IS4S', '工时组', '项目名称', 'VEHSERINAME_GRADE'],keep='first',inplace=True)
        df.rename(columns={'工时组': 'REPAIRTYPE', '项目名称': 'COMPNAME'}, inplace=True)
        # df.to_excel('data/大地系统数据.xlsx',index=None,encoding='utf-8')
        df['SYSTEM_VALUE'].replace(' ',0,inplace=True)
        df = df.astype(str)
        df.replace('nan', '', inplace=True)
        # df['SYSTEM_VALUE']=df['SYSTEM_VALUE'].astype(float)
        table_name = 'LB_GS_NATIONAL_GS_DATA'
        self.oracle.BatchinsertDataToTable(df, table_name, self.account)
        self.logger.info(('system_value done!'))
        # print('system_value done!')

    def generate_system(self):
        '''生成243个工时项目体系，上传至数据库'''
        path1 = 'data/工时价格标准采集模板(发联保).xlsx'
        df1 = pd.read_excel(path1, sheet_name='服务站工时标准（范例）', skiprows=1)
        df1.fillna(1,inplace=True)
        df1 = df1.set_index(['工时组', '项目名称']).stack().reset_index()
        df1.rename(columns={'level_2': '车系档次'}, inplace=True)
        df1['车系档次'].replace('（', '(', regex=True, inplace=True)
        df1['车系档次'].replace('）', ')', regex=True, inplace=True)
        df1 = pd.DataFrame(df1, columns=['工时组', '项目名称', '车系档次'])

        areas = ['large_area', 'jiangsu', 'shenzhen', 'yunnan']
        # 档次平滑
        def dangci_pinghua(ddd):
            '''档次平滑'''
            ddd['A(180档)'] = ddd.apply(lambda x: pinhua1(x['A(180档)'], x['B(240档)']), axis=1)
            ddd['B(240档)'] = ddd.apply(lambda x: pinhua(x['A(180档)'], x['B(240档)']), axis=1)
            ddd['C(300档)'] = ddd.apply(lambda x: pinhua(x['B(240档)'], x['C(300档)']), axis=1)
            ddd['D(380档)'] = ddd.apply(lambda x: pinhua(x['C(300档)'], x['D(380档)']), axis=1)
            ddd['E(450档)'] = ddd.apply(lambda x: pinhua(x['D(380档)'], x['E(450档)']), axis=1)
            ddd['F(500档)'] = ddd.apply(lambda x: pinhua(x['E(450档)'], x['F(500档)']), axis=1)
            ddd['G(600档)'] = ddd.apply(lambda x: pinhua(x['F(500档)'], x['G(600档)']), axis=1)
            ddd['H(680档)'] = ddd.apply(lambda x: pinhua(x['G(600档)'], x['H(680档)']), axis=1)
            ddd['J(800档)'] = ddd.apply(lambda x: pinhua(x['H(680档)'], x['J(800档)']), axis=1)
            ddd['K(1000档)'] = ddd.apply(lambda x: pinhua(x['J(800档)'], x['K(1000档)']), axis=1)
            return ddd

        def ascending_process(ddd):
            '''档次升序'''
            def ascending_order(ddd):
                ddd_list = list(ddd.values)
                ddd_list = sorted(ddd_list)
                return ddd_list

            bbb = ddd.iloc[:, -10:].apply(ascending_order, axis=1).apply(pd.Series)
            ddd = pd.concat([ddd.iloc[:, :-10], bbb], axis=1)
            ddd.rename(columns={0: 'A(180档)', 1: 'B(240档)', 2: 'C(300档)', 3: 'D(380档)', 4: 'E(450档)', 5: 'F(500档)',
                                6: 'G(600档)', 7: 'H(680档)', 8: 'J(800档)', 9: 'K(1000档)'}, inplace=True)
            return ddd

        def mapping(df4):
            '''左右配件对称'''
            df = pd.read_excel('data/map.xlsx', sep='\t')
            left2right = dict(zip(df['左侧项目'], df['右侧项目']))
            df4 = df4.pivot_table(index=['区域', '工时组', '车系档次', 'IS4S'], columns=['项目名称'])
            df4 = df4.stack(level=0).reset_index()
            df4.rename(columns={'level_4': 'VALUE_TYPE'}, inplace=True)
            # df4.to_excel('gongshi.xlsx',encoding='utf-8',index=None)
            cols = df4.columns
            for xm in left2right.keys():
                if xm in cols:
                    try:
                        df4[xm] = df4[left2right[xm]]
                    except Exception as e:
                        print(e)
            df4 = df4.pivot_table(index=['区域', '工时组', '车系档次', 'IS4S'], columns=['VALUE_TYPE'])
            df4 = df4.stack(level=0).reset_index()
            return df4

        def youqi_pre_result(df1):
            def youqi_model_predict(area,df1):
                '''
                油漆模型预测
                :param area: 区域：large_area(33个区域的模型)，江苏，深圳，云南的模型
                :type area: str
                :param df1: 生成的所有区域的243个工时项目的工时数据
                :type df1: dataframe
                :return: 每个模型预测的工时数据
                :rtype: dataframe
                '''
                with open('{}/youqi_region2id.json'.format(area), encoding='utf-8') as f1:
                    region2id = json.load(f1)
                dd_all = pd.DataFrame()
                for region in region2id.keys():
                    df1['区域'] = region
                    dd_all = pd.concat([dd_all, df1])
                print(len(dd_all))
                datas2_fb = dd_all.loc[dd_all['工时组'] == '油漆']
                is4s_list=[1,0]
                datas2=pd.DataFrame()
                for is4s in is4s_list:
                    datas2_fb[args.IS4S] = is4s
                    datas2 = pd.concat([datas2, datas2_fb])
                # XGboost测试
                test_data = datas2.copy()
                ss = joblib.load("{}/youqi_data_ss.model".format(area))  ## 加载模型
                xgb_model = joblib.load("{}/youqi_xgb.model".format(area))  ## 加载模型

                with open('{}/youqi_component2id.json'.format(area), encoding='utf-8') as reader2:
                    component2id = json.load(reader2)

                # with open('{}/youqi_id2component.json'.format(area), encoding='utf-8') as reader3:
                #     id2component = json.load(reader3)

                with open('{}/youqi_region2id.json'.format(area), encoding='utf-8') as reader4:
                    region2id = json.load(reader4)
                test_data['compnent'] = test_data['项目名称'].map(component2id)
                test_data['VEHSERINAME_ID'] = test_data['车系档次'].map(alpha2VEHSERINAME_ID)
                test_data['REGION_ID'] = test_data['区域'].map(region2id)
                test_data1=test_data.copy()
                test_data1.dropna(subset=['REGION_ID', 'compnent', 'VEHSERINAME_ID'], how='any', axis=0, inplace=True)
                test_data1 = test_data1.reset_index(drop=True)
                df2 = pd.DataFrame(test_data1, columns=['REGION_ID', 'compnent', 'VEHSERINAME_ID', args.IS4S])

                x_data = ss.transform(df2)
                datas_pre = list(np.exp(xgb_model.predict(x_data)))
                # df3 = pd.DataFrame()
                df2['XGBOOST_VALUE'] = datas_pre
                df2['参考值'] = df2['XGBOOST_VALUE'].map(lambda x: round(x, -1))
                df2['上限值'] = df2['参考值'].map(lambda x: round(1.1 * x, -1))
                df4 = pd.merge(test_data,df2,how='left',on=['REGION_ID', 'compnent', 'VEHSERINAME_ID', args.IS4S])
                # df4.to_excel('出数据/ceshi_data.xlsx',index=None,encoding='utf-8')
                df4 = pd.DataFrame(df4, columns=['区域', '工时组', '项目名称', '车系档次', args.IS4S, '参考值', '上限值'])
                return df4

            youqi_result=pd.DataFrame()
            for area in areas:
                youqi_predict=youqi_model_predict(area,df1)
                youqi_result = pd.concat([youqi_result, youqi_predict])
            # ddd = youqi_result.pivot_table(index=['区域', '工时组', '项目名称', 'IS4S'], columns=['车系档次'])
            # ddd = ddd.stack(level=0).reset_index()
            # ddd.rename(columns={'level_4': 'VALUE_TYPE'}, inplace=True)
            # ddd = ascending_process(ddd)
            # # ddd = dangci_pinghua(ddd)
            # ddd = ddd.pivot_table(index=['区域', '工时组', '项目名称', 'IS4S'], columns=['VALUE_TYPE'])
            # df4 = ddd.stack(level=0).reset_index()
            # df4.rename(columns={'level_4': '车系档次'}, inplace=True)
            youqi_result.fillna(0,inplace=True)
            youqi_result = mapping(youqi_result)
            youqi_result['参考值'].replace(0,np.nan,inplace=True)
            youqi_result['上限值'].replace(0,np.nan,inplace=True)
            youqi_result=pd.DataFrame(youqi_result,columns=['区域','工时组','项目名称','车系档次','IS4S','参考值','上限值'])
            if not os.path.exists('youqi'):
                os.makedirs('youqi')
            youqi_result.to_csv('youqi/油漆模型预测.csv', index=None, encoding='utf-8')
            return youqi_result

        def banjin_pre_result(df1):
            def banjin_model_predict(area,df1):
                with open('{}/banjin_region2id.json'.format(area), encoding='utf-8') as f1:
                    region2id = json.load(f1)
                dd_all = pd.DataFrame()
                for region in region2id.keys():
                    df1['区域'] = region
                    dd_all = pd.concat([dd_all, df1])
                print(len(dd_all))
                datas3_fb = dd_all.loc[dd_all['工时组'] == '钣金']
                is4s_list = [1, 0]
                datas3 = pd.DataFrame()
                for is4s in is4s_list:
                    datas3_fb[args.IS4S] = is4s
                    datas3 = pd.concat([datas3, datas3_fb])
                # XGboost测试
                test_data = datas3.copy()
                ss = joblib.load("{}/banjin_data_ss.model".format(area))  ## 加载模型
                xgb_model = joblib.load("{}/banjin_xgb.model".format(area))  ## 加载模型

                with open('{}/banjin_component2id.json'.format(area), encoding='utf-8') as reader2:
                    component2id = json.load(reader2)

                # with open('{}/banjin_id2component.json'.format(area), encoding='utf-8') as reader3:
                #     id2component = json.load(reader3)

                with open('{}/banjin_region2id.json'.format(area), encoding='utf-8') as reader4:
                    region2id = json.load(reader4)
                # test_data1=pd.DataFrame()
                test_data['compnent'] = test_data['项目名称'].map(component2id)
                # test_data['painting_type'] = test_data['PAINTING_TYPE'].apply(paint_trainsform)
                test_data['VEHSERINAME_ID'] = test_data['车系档次'].map(alpha2VEHSERINAME_ID)
                test_data['REGION_ID'] = test_data['区域'].map(region2id)
                test_data1=test_data.copy()
                test_data1.dropna(subset=['REGION_ID', 'compnent', 'VEHSERINAME_ID'], how='any', axis=0, inplace=True)
                test_data1 = test_data1.reset_index(drop=True)
                df2 = pd.DataFrame(test_data1, columns=['REGION_ID', 'compnent', 'VEHSERINAME_ID', args.IS4S])

                x_data = ss.transform(df2)
                datas_pre = list(np.exp(xgb_model.predict(x_data)))
                df2['XGBOOST_VALUE'] = datas_pre
                df2['参考值'] = df2['XGBOOST_VALUE'].map(lambda x: round(x, -1))
                df2['上限值'] = df2['参考值'].map(lambda x: round(1.1 * x, -1))
                df4 = pd.merge(test_data, df2, how='left', on=['REGION_ID', 'compnent', 'VEHSERINAME_ID', args.IS4S])
                df4 = pd.DataFrame(df4, columns=['区域', '工时组', '项目名称', '车系档次', args.IS4S, '参考值', '上限值'])
                return df4

            banjin_result = pd.DataFrame()
            for area in areas:
                banjin_predict = banjin_model_predict(area, df1)
                banjin_result = pd.concat([banjin_result, banjin_predict])
            # ddd = banjin_result.pivot_table(index=['区域', '工时组', '项目名称', 'IS4S'], columns=['车系档次'])
            # ddd = ddd.stack(level=0).reset_index()
            # ddd.rename(columns={'level_4': 'VALUE_TYPE'}, inplace=True)
            # ddd = ascending_process(ddd)
            # # ddd = dangci_pinghua(ddd)
            # ddd = ddd.pivot_table(index=['区域', '工时组', '项目名称', 'IS4S'], columns=['VALUE_TYPE'])
            # df4 = ddd.stack(level=0).reset_index()
            # df4.rename(columns={'level_4': '车系档次'}, inplace=True)
            banjin_result.fillna(0, inplace=True)
            banjin_result = mapping(banjin_result)
            banjin_result['参考值'].replace(0, np.nan, inplace=True)
            banjin_result['上限值'].replace(0, np.nan, inplace=True)
            banjin_result = pd.DataFrame(banjin_result, columns=['区域', '工时组', '项目名称', '车系档次', 'IS4S', '参考值', '上限值'])
            if not os.path.exists('banjin'):
                os.makedirs('banjin')
            banjin_result.to_csv('banjin/钣金模型预测.csv', index=None, encoding='utf-8')
            return banjin_result

        def chaizhuang_pre_result(df1):
            def chaizhuang_model_predict(area, df1):
                with open('{}/chaizhuang_region2id.json'.format(area), encoding='utf-8') as f1:
                    region2id = json.load(f1)
                dd_all = pd.DataFrame()
                for region in region2id.keys():
                    df1['区域'] = region
                    dd_all = pd.concat([dd_all, df1])
                print(len(dd_all))
                datas4_fb = dd_all.loc[dd_all['工时组'] == '拆装']
                is4s_list = [1, 0]
                datas4 = pd.DataFrame()
                for is4s in is4s_list:
                    datas4_fb[args.IS4S] = is4s
                    datas4 = pd.concat([datas4, datas4_fb])
                # XGboost测试
                test_data = datas4.copy()
                ss = joblib.load("{}/chaizhuang_data_ss.model".format(area))  ## 加载模型
                xgb_model = joblib.load("{}/chaizhuang_xgb.model".format(area))  ## 加载模型

                with open('{}/chaizhuang_component2id.json'.format(area), encoding='utf-8') as reader2:
                    component2id = json.load(reader2)

                # with open('{}/chaizhuang_id2component.json'.format(area), encoding='utf-8') as reader3:
                #     id2component = json.load(reader3)

                with open('{}/chaizhuang_region2id.json'.format(area), encoding='utf-8') as reader4:
                    region2id = json.load(reader4)
                # test_data1=pd.DataFrame()
                test_data['compnent'] = test_data['项目名称'].map(component2id)
                # test_data['painting_type'] = test_data['PAINTING_TYPE'].apply(paint_trainsform)
                test_data['VEHSERINAME_ID'] = test_data['车系档次'].map(alpha2VEHSERINAME_ID)
                test_data['REGION_ID'] = test_data['区域'].map(region2id)
                test_data1=test_data.copy()
                test_data1.dropna(subset=['REGION_ID', 'compnent', 'VEHSERINAME_ID'], how='any', axis=0, inplace=True)
                test_data1 = test_data1.reset_index(drop=True)
                df2 = pd.DataFrame(test_data1, columns=['REGION_ID', 'compnent', 'VEHSERINAME_ID', args.IS4S])
                x_data = ss.transform(df2)
                datas_pre = list(np.exp(xgb_model.predict(x_data)))
                df2['XGBOOST_VALUE'] = datas_pre
                df2['参考值'] = df2['XGBOOST_VALUE'].map(lambda x: round(x, -1))
                df2['上限值'] = df2['参考值'].map(lambda x: round(1.1 * x, -1))
                df4 = pd.merge(test_data, df2, how='left', on=['REGION_ID', 'compnent', 'VEHSERINAME_ID', args.IS4S])
                df4 = pd.DataFrame(df4, columns=['区域', '工时组', '项目名称', '车系档次', args.IS4S, '参考值', '上限值'])
                return df4

            chaizhuang_result = pd.DataFrame()
            for area in areas:
                chaizhuang_predict = chaizhuang_model_predict(area, df1)
                chaizhuang_result = pd.concat([chaizhuang_result, chaizhuang_predict])

            # ddd = chaizhuang_result.pivot_table(index=['区域', '工时组', '项目名称', 'IS4S'], columns=['车系档次'])
            # ddd = ddd.stack(level=0).reset_index()
            # ddd.rename(columns={'level_4': 'VALUE_TYPE'}, inplace=True)
            # ddd = ascending_process(ddd)
            # # ddd = dangci_pinghua(ddd)
            # ddd = ddd.pivot_table(index=['区域', '工时组', '项目名称', 'IS4S'], columns=['VALUE_TYPE'])
            # df4 = ddd.stack(level=0).reset_index()
            # df4.rename(columns={'level_4': '车系档次'}, inplace=True)

            chaizhuang_result.fillna(0, inplace=True)
            chaizhuang_result = mapping(chaizhuang_result)
            chaizhuang_result['参考值'].replace(0, np.nan, inplace=True)
            chaizhuang_result['上限值'].replace(0, np.nan, inplace=True)
            chaizhuang_result = pd.DataFrame(chaizhuang_result, columns=['区域', '工时组', '项目名称', '车系档次', 'IS4S', '参考值', '上限值'])
            if not os.path.exists('chaizhuang'):
                os.makedirs('chaizhuang')
            chaizhuang_result.to_csv('chaizhuang/拆装模型预测.csv', index=None, encoding='utf-8')
            return chaizhuang_result

        youqi_data=youqi_pre_result(df1)
        banjin_data=banjin_pre_result(df1)
        chaizhaung_data=chaizhuang_pre_result(df1)

        def model_statistics_merge(youqi_data,banjin_data,chaizhaung_data):
            '''
            模型数据与统计数据拼接
            :param youqi_data: 油漆模型预测数据
            :type youqi_data: dataframe
            :param banjin_data: 钣金模型预测数据
            :type banjin_data: dataframe
            :param chaizhaung_data: 拆装模型预测数据
            :type chaizhaung_data: dataframe
            :return:
            :rtype:
            '''
            def statistics_data_concat(areas):
                youqi_statistics_data=pd.DataFrame()
                banjin_statistics_data=pd.DataFrame()
                chaizhuang_statistics_data=pd.DataFrame()
                for area in areas:
                    path1=area+'/油漆统计数据及模型预测.csv'
                    path2=area+'/钣金统计数据及模型预测.csv'
                    path3=area+'/拆装统计数据及模型预测.csv'
                    df1=pd.read_csv(path1)
                    df2=pd.read_csv(path2)
                    df3=pd.read_csv(path3)
                    youqi_statistics_data=pd.concat([youqi_statistics_data,df1],axis=0)
                    banjin_statistics_data=pd.concat([banjin_statistics_data,df2],axis=0)
                    chaizhuang_statistics_data=pd.concat([chaizhuang_statistics_data,df3],axis=0)
                return youqi_statistics_data,banjin_statistics_data,chaizhuang_statistics_data

            youqi_statistics_data, banjin_statistics_data, chaizhuang_statistics_data=statistics_data_concat(areas)
            youqi_statistics_data.rename(columns={'REGION': '区域', 'COMPNAME': '项目名称', 'VEHSERINAME_GROUP': '车系档次', 'WORK_TYPE': '工时组'},
                        inplace=True)
            banjin_statistics_data.rename(columns={'REGION': '区域', 'COMPNAME': '项目名称', 'VEHSERINAME_GROUP': '车系档次', 'WORK_TYPE': '工时组'},
                        inplace=True)
            chaizhuang_statistics_data.rename(columns={'REGION': '区域', 'COMPNAME': '项目名称', 'VEHSERINAME_GROUP': '车系档次', 'WORK_TYPE': '工时组'},
                        inplace=True)
            youqi_statistics_data = pd.DataFrame(youqi_statistics_data, columns=['区域', '工时组', '项目名称', '车系档次', 'IS4S', 'count', 'mean'])
            banjin_statistics_data = pd.DataFrame(banjin_statistics_data, columns=['区域', '工时组', '项目名称', '车系档次', 'IS4S', 'count', 'mean'])
            chaizhuang_statistics_data = pd.DataFrame(chaizhuang_statistics_data, columns=['区域', '工时组', '项目名称', '车系档次', 'IS4S', 'count', 'mean'])

            dd1 = pd.merge(youqi_data, youqi_statistics_data, how='left', on=['区域', '工时组', '项目名称', '车系档次', 'IS4S'])
            dd2 = pd.merge(banjin_data, banjin_statistics_data, how='left', on=['区域', '工时组', '项目名称', '车系档次', 'IS4S'])
            dd3 = pd.merge(chaizhaung_data, chaizhuang_statistics_data, how='left', on=['区域', '工时组', '项目名称', '车系档次', 'IS4S'])

            # 油漆，钣金，拆装预测值拼接
            dd = pd.concat([dd1, dd2, dd3], axis=0)
            dd['VEHSERINAME_TYPE'] = '乘用车'
            dd['IS4S']=dd['IS4S'].replace(1,'服务站').replace(0,'综修厂')

            ddd = pd.DataFrame(dd, columns=['区域', 'VEHSERINAME_TYPE', 'IS4S', '工时组', '项目名称', '车系档次', 'count', 'mean', '参考值',
                                            '上限值'])

            # dd=dd.set_index(['区域','VEHSERINAME','IS4S','工时组','项目名称','车系档次']).stack().reset_index()
            # dd.rename(columns={'level_6':'VALUE_TYPE'},inplace=True)
            # # dd=dd.set_index(['区域','VEHSERINAME','IS4S','工时组','项目名称','VALUE_TYPE']).unstack().reset_index()
            # ddd=dd.set_index(['区域','VEHSERINAME','IS4S','工时组','项目名称']).stack(level=0)
            # ddd=dd.pivot_table(index=['区域','VEHSERINAME_TYPE','IS4S','工时组','项目名称'],columns=['车系档次'])

            # ddd=ddd.stack(level=0).reset_index()
            # ddd.rename(columns={'level_5':'VALUE_TYPE'},inplace=True)

            ddd.to_csv('data/油漆钣金拆装工时系统.csv', index=None, encoding='utf-8')

            # ddd=ddd.set_index(['区域','VEHSERINAME','IS4S','工时组','项目名称']).stack(level=1).reset_index()
            return ddd
        model_statistics_system_data=model_statistics_merge(youqi_data,banjin_data,chaizhaung_data)
        def statistics_merge(df1):
            '''机修和电工数据合并'''
            regions=[]
            for area in areas:
                path=area+'/youqi_region2id.json'
                with open(path,encoding='utf-8') as reader:
                    region=list(json.load(reader).keys())
                regions+=region
            dd_all=pd.DataFrame()
            for region in regions:
                df1['区域'] = region
                dd_all = pd.concat([dd_all, df1])

            datas5_fb = dd_all.loc[dd_all['工时组'] == '机修']
            is4s_list = [1, 0]
            datas5 = pd.DataFrame()
            for is4s in is4s_list:
                datas5_fb[args.IS4S] = is4s
                datas5 = pd.concat([datas5, datas5_fb])

            datas6_fb = dd_all.loc[dd_all['工时组'] == '电工']
            datas6=pd.DataFrame()
            for is4s in is4s_list:
                datas6_fb[args.IS4S] = is4s
                datas6 = pd.concat([datas6, datas6_fb])

            def jx_dg_statistics_data_concat(areas):
                jiuxiu_statistics_data = pd.DataFrame()
                diangong_statistics_data = pd.DataFrame()
                for area in areas:
                    path1 = area + '/机修统计数据.csv'
                    path2 = area + '/电工统计数据.csv'
                    df1 = pd.read_csv(path1)
                    df2 = pd.read_csv(path2)
                    jiuxiu_statistics_data = pd.concat([jiuxiu_statistics_data, df1], axis=0)
                    diangong_statistics_data = pd.concat([diangong_statistics_data, df2], axis=0)
                return jiuxiu_statistics_data, diangong_statistics_data

            jiuxiu_statistics_data, diangong_statistics_data=jx_dg_statistics_data_concat(areas)
            jiuxiu_statistics_data['参考值'] = jiuxiu_statistics_data['median']
            jiuxiu_statistics_data.rename(columns={'REGION': '区域', 'WORK_TYPE': '工时组', 'COMPNAME': '项目名称', 'VEHSERINAME_GROUP': '车系档次'},
                         inplace=True)
            jiuxiu_statistics_data = pd.DataFrame(jiuxiu_statistics_data, columns=['区域', '工时组', '项目名称', '车系档次','IS4S','count','mean','参考值'])

            datas5 = pd.merge(datas5, jiuxiu_statistics_data, on=['工时组', '项目名称', '车系档次', '区域','IS4S'], how='left')
            datas5 = pd.DataFrame(datas5, columns=['区域','IS4S', '工时组', '项目名称', '车系档次', 'count','mean','参考值'])
            datas5['mean']=datas5['mean'].apply(lambda x:round(x,2))


            diangong_statistics_data['参考值'] = diangong_statistics_data['median']
            diangong_statistics_data.rename(columns={'REGION': '区域', 'WORK_TYPE': '工时组', 'COMPNAME': '项目名称', 'VEHSERINAME_GROUP': '车系档次'},
                            inplace=True)
            diangong_statistics_data = pd.DataFrame(diangong_statistics_data, columns=['区域', '工时组', '项目名称', '车系档次','IS4S','count','mean','参考值'])
            datas6 = pd.merge(datas6, diangong_statistics_data, on=['工时组', '项目名称', '车系档次', '区域','IS4S'], how='left')
            datas6 = pd.DataFrame(datas6, columns=['区域', 'IS4S', '工时组', '项目名称', '车系档次', 'count','mean', '参考值'])
            dgjx = pd.concat([datas5, datas6], axis=0)
            dgjx['VEHSERINAME_TYPE'] = '乘用车'
            # dgjx=dgjx.pivot_table(index=['区域','VEHSERINAME_TYPE','IS4S','工时组','项目名称'],columns=['车系档次'])
            # dgjx=dgjx.stack(level=0).reset_index()
            # dgjx.rename(columns={'level_5':'VALUE_TYPE'},inplace=True)

            # dgjx=dgjx.pivot_table(index=['区域','工时组','项目名称'],columns=['车系档次'])
            dgjx['IS4S'] = dgjx['IS4S'].replace(1, '服务站').replace(0, '综修厂')
            dgjx = pd.DataFrame(dgjx, columns=['区域', 'VEHSERINAME_TYPE', 'IS4S', '工时组', '项目名称', '车系档次', 'count','mean', '参考值'])
            dgjx.to_csv('data/机修电工工时体系.csv', index=None, encoding='utf-8')
            return dgjx

        dgjx_statistics_system_data=statistics_merge(df1)
        model_statistics_system_data.replace(0.0,10.0,inplace=True)
        def model_statistics_system_large_area_data_process(model_statistics_system_data):
            '''
            33个大区域单独做处理
            :param model_statistics_system_data:
            :type model_statistics_system_data:
            :return:
            :rtype:
            '''
            def get_large_area():
                '''得到33个区域'''
                with open('large_area/youqi_region2id.json', encoding='utf-8') as reader4:
                    regions = list(json.load(reader4).keys())
                    return regions
            large_areas=get_large_area()
            model_statistics_system_large_area_data=model_statistics_system_data.loc[model_statistics_system_data['区域'].isin(large_areas)]
            def national_mean_replace(datas123):
                '''全国均值替换'''
                national_mean = pd.DataFrame(datas123, columns=['VEHSERINAME_TYPE', 'IS4S', '工时组', '项目名称', '车系档次','mean','参考值'])
                national_mean = national_mean.groupby(['VEHSERINAME_TYPE', 'IS4S', '工时组', '项目名称', '车系档次'])['mean'].agg(
                    ['mean']).reset_index()
                national_mean.rename(columns={'mean': 'national_mean'}, inplace=True)

                def transform(x):
                    try:
                        con = round(x, -1)
                    except:
                        con = np.nan
                    return con

                national_mean['national_mean'] = national_mean['national_mean'].apply(transform)
                datas123 = pd.merge(datas123, national_mean, on=['VEHSERINAME_TYPE', 'IS4S', '工时组', '项目名称', '车系档次'],
                                    how='left')
                # 模型预测值与均值的偏差超过一个阈值，用全国均值乘以一个系数去替代
                datas123['coef_national'] = datas123.apply(lambda x: (x['参考值'] - x['national_mean']) / x['national_mean'],
                                                           axis=1)
                datas123['参考值'] = datas123.apply(
                    lambda x: round(1.0 * x['national_mean']) if x['coef_national'] >= 0.5 or x['coef_national'] < -0.45 else
                    x['参考值'], axis=1)
                datas123['上限值'] = datas123['参考值'].apply(lambda x: round(1.1 * x, -1))
                # datas123.to_csv('data1/gongshi_v.csv',index=None,encoding='utf-8')
                datas123 = pd.DataFrame(datas123,
                                        columns=['区域', 'VEHSERINAME_TYPE', 'IS4S', '工时组', '项目名称', '车系档次', 'count', 'mean',
                                                 '参考值', '上限值'])
                return datas123

            model_statistics_system_large_area_data=national_mean_replace(model_statistics_system_large_area_data)
            return model_statistics_system_large_area_data

        def all_regions_process(model_statistics_system_data):
            '''所有区域处理'''
            model_statistics_system_large_area_data=model_statistics_system_large_area_data_process(model_statistics_system_data).reset_index(drop=True)
            single_areas = ['江苏', '深圳', '云南']
            model_statistics_system_single_area_data = model_statistics_system_data.loc[model_statistics_system_data['区域'].isin(single_areas)].reset_index(drop=True)
            model_statistics_system_data=pd.concat([model_statistics_system_large_area_data,model_statistics_system_single_area_data],axis=0)
            def national_mean_ascending(datas123):
                '''10个档次升序'''
                datas123 = pd.DataFrame(datas123, columns=['区域', '工时组', '项目名称', '车系档次', 'IS4S', '参考值', '上限值'])
                ddd = datas123.pivot_table(index=['区域', '工时组', '项目名称', 'IS4S'], columns=['车系档次'])
                ddd = ddd.stack(level=0).reset_index()
                ddd.rename(columns={'level_4': 'VALUE_TYPE'}, inplace=True)
                ddd = ascending_process(ddd)
                ddd = ddd.pivot_table(index=['区域', '工时组', '项目名称', 'IS4S'], columns=['VALUE_TYPE'])
                datas123 = ddd.stack(level=0).reset_index()
                datas123.rename(columns={'level_4': '车系档次'}, inplace=True)
                return datas123

            datas123_new = national_mean_ascending(model_statistics_system_data)
            model_statistics_system_data.drop(['参考值', '上限值'], axis=1, inplace=True)
            model_statistics_system_data = pd.merge(model_statistics_system_data, datas123_new, on=['区域', '工时组', '项目名称', 'IS4S', '车系档次'], how='left')
            model_statistics_system_data = pd.DataFrame(model_statistics_system_data,
                                    columns=['区域', 'VEHSERINAME_TYPE', 'IS4S', '工时组', '项目名称', '车系档次', 'count', 'mean',
                                             '参考值', '上限值'])



            def missing_value_filling(data):
                '''
                江苏的缺失值用浙江的填充，深圳的缺失值用广东的填充,云南的缺失值用四川的填充
                '''
                regions=data['区域'].tolist()
                referrence_values=data['参考值'].tolist()
                referrence_str_values=[str(i) for i in referrence_values]
                upper_limit_values=data['上限值'].tolist()
                counts=data['count'].tolist()
                means=data['mean'].tolist()
                for i,v in  enumerate(referrence_str_values):
                    if v=='nan':
                        index=i
                        region=regions[index]
                        if region=='江苏':
                            index1=regions.index('浙江')
                            referrence_values[index]=referrence_values[index1]
                            upper_limit_values[index]=upper_limit_values[index1]
                        elif region=='深圳':
                            index2 = regions.index('广东')
                            referrence_values[index] = referrence_values[index2]
                            upper_limit_values[index] = upper_limit_values[index2]
                        elif region=='云南':
                            index2 = regions.index('四川')
                            referrence_values[index] = referrence_values[index2]
                            upper_limit_values[index] = upper_limit_values[index2]
                df=pd.DataFrame()
                df['区域']=regions
                df['参考值']=referrence_values
                df['上限值']=upper_limit_values
                df['count']=counts
                df['mean']=means
                return df

            model_statistics_system_data=model_statistics_system_data.groupby(['VEHSERINAME_TYPE', 'IS4S', '工时组', '项目名称', '车系档次'])['区域','参考值','上限值','count','mean'].apply(missing_value_filling).reset_index()
            model_statistics_system_data = pd.DataFrame(model_statistics_system_data,
                                                        columns=['区域', 'VEHSERINAME_TYPE', 'IS4S', '工时组', '项目名称', '车系档次',
                                                                 'count', 'mean','参考值', '上限值'])
            return model_statistics_system_data

        model_statistics_system_data=all_regions_process(model_statistics_system_data)
        dgjx_statistics_system_data['参考值']=dgjx_statistics_system_data.groupby(['工时组','项目名称','VEHSERINAME_TYPE','车系档次','IS4S'])['参考值'].transform(lambda x: x.fillna(x.median()))
        dgjx_statistics_system_data.drop_duplicates(['区域','IS4S','工时组', '项目名称', '车系档次'],
                           keep='first', inplace=True)
        def trans2inter(x):
            try:
                con=round(x)
            except:
                con=np.nan
            return con
        dgjx_statistics_system_data['参考值']=dgjx_statistics_system_data['参考值'].apply(trans2inter)
        dgjx_statistics_system_data['上限值'] = np.nan

        def model_statistics_system_25high_brand_process():
            '''高端品牌只做服务站'''
            # 25个高端品牌
            path2 = 'data/工时价格标准采集模板(发联保).xlsx'
            df2 = pd.read_excel(path2, sheet_name='25个高端品牌服务站工时标准（范例）', skiprows=1)
            df2.drop(0, inplace=True)
            df2.fillna(1,inplace=True)
            df2 = df2.set_index(['Unnamed: 0', 'Unnamed: 1']).stack().reset_index()
            df2.rename(columns={'Unnamed: 0': '工时组', 'Unnamed: 1': '项目名称', 'level_2': '车系档次'}, inplace=True)
            df2 = pd.DataFrame(df2, columns=['工时组', '项目名称', '车系档次'])
            with open('high_brand/youqi/region2id.json', encoding='utf-8') as f1:
                region2id = json.load(f1)
            dd_all = pd.DataFrame()
            for region in region2id.keys():
                df2['区域'] = region
                dd_all = pd.concat([dd_all, df2])
            print(len(dd_all))
            is4s_list = [1]
            def youqi_25_pre(is4s_list):
                datas2_fb = dd_all.loc[dd_all['工时组'] == '油漆']
                datas2 = pd.DataFrame()
                for is4s in is4s_list:
                    datas2_fb[args.IS4S] = is4s
                    datas2 = pd.concat([datas2, datas2_fb])
                # XGboost测试
                test_data = datas2.copy()
                ss = joblib.load("high_brand/youqi/youqi_ss.model")  ## 加载模型
                xgb_model = joblib.load("high_brand/youqi/youqi_xgb.model")  ## 加载模型

                with open('high_brand/youqi/component2id.json', encoding='utf-8') as reader2:
                    component2id = json.load(reader2)

                # with open('high_brand/youqi/id2component.json', encoding='utf-8') as reader3:
                #     id2component = json.load(reader3)

                with open('high_brand/youqi/region2id.json', encoding='utf-8') as reader4:
                    region2id = json.load(reader4)

                with open('high_brand/group2id.json', encoding='utf-8') as f1:
                    group2id = json.load(f1)
                id2group = {v: k for k, v in group2id.items()}
                test_data['compnent'] = test_data['项目名称'].map(component2id)
                test_data['VEHSERINAME_ID'] = test_data['车系档次'].map(group2id)
                test_data['REGION_ID'] = test_data['区域'].map(region2id)
                test_data.dropna(subset=['REGION_ID', 'compnent', 'VEHSERINAME_ID'], how='any', axis=0, inplace=True)
                test_data = test_data.reset_index(drop=True)
                df2 = pd.DataFrame(test_data, columns=['REGION_ID', 'compnent', 'VEHSERINAME_ID', args.IS4S])

                x_data = ss.transform(df2)
                datas_pre = list(np.exp(xgb_model.predict(x_data)))
                df3 = pd.DataFrame()
                df3['XGBOOST_VALUE'] = datas_pre
                df3['参考值'] = df3['XGBOOST_VALUE'].map(lambda x: round(0.97*x, -1))
                df3['上限值'] = df3['参考值'].map(lambda x: round(1.1 * x, -1))
                df4 = pd.concat([test_data, df3], axis=1)
                # df4.to_excel('出数据/ceshi_data.xlsx',index=None,encoding='utf-8')
                df4 = pd.DataFrame(df4, columns=['区域', '工时组', '项目名称', '车系档次', args.IS4S, '参考值', '上限值'])
                def quanche_penqi_pro(df4):
                    df=pd.DataFrame(df4,columns=['区域', '工时组', '项目名称', '车系档次', args.IS4S, '参考值'])
                    ddd=df.pivot_table(index=['区域','工时组','车系档次',args.IS4S],columns=['项目名称'])
                    ddd=ddd.stack(level=0).reset_index()
                    ddd.rename(columns={'level_4':'VALUE_TYPE'},inplace=True)
                    ddd['全车喷漆']=ddd['前保险杠(全喷)'].map(lambda x:round(float(x)*13*0.7,-1))
                    ddd = ddd.pivot_table(index=['区域','工时组','车系档次',args.IS4S], columns=['VALUE_TYPE'])
                    ddd = ddd.stack(level=0).reset_index()
                    ddd['上限值'] = ddd['参考值'].map(lambda x: round(1.1 * x, -1))
                    return ddd
                df4=quanche_penqi_pro(df4)
                df4.to_csv('high_brand/youqi/25个高端品牌油漆模型预测.csv', index=None, encoding='utf-8')
                return df4
            def banjin_25_pre(is4s_list):
                datas3_fb = dd_all.loc[dd_all['工时组'] == '钣金']
                datas3 = pd.DataFrame()
                for is4s in is4s_list:
                    datas3_fb[args.IS4S] = is4s
                    datas3 = pd.concat([datas3, datas3_fb])
                # XGboost测试
                test_data = datas3.copy()
                ss = joblib.load("high_brand/banjin/banjin_ss.model")  ## 加载模型
                xgb_model = joblib.load("high_brand/banjin/banjin_xgb.model")  ## 加载模型

                with open('high_brand/banjin/component2id.json', encoding='utf-8') as reader2:
                    component2id = json.load(reader2)

                # with open('high_brand/banjin/id2component.json', encoding='utf-8') as reader3:
                #     id2component = json.load(reader3)

                with open('high_brand/banjin/region2id.json', encoding='utf-8') as reader4:
                    region2id = json.load(reader4)
                test_data['compnent'] = test_data['项目名称'].map(component2id)
                test_data['VEHSERINAME_ID'] = test_data['车系档次'].map(group2id)
                test_data['REGION_ID'] = test_data['区域'].map(region2id)
                test_data.dropna(subset=['REGION_ID', 'compnent', 'VEHSERINAME_ID'], how='any', axis=0, inplace=True)
                test_data = test_data.reset_index(drop=True)
                df2 = pd.DataFrame(test_data, columns=['REGION_ID', 'compnent', 'VEHSERINAME_ID', args.IS4S])

                x_data = ss.transform(df2)
                datas_pre = list(np.exp(xgb_model.predict(x_data)))
                df3 = pd.DataFrame()
                df3['XGBOOST_VALUE'] = datas_pre
                df3['参考值'] = df3['XGBOOST_VALUE'].map(lambda x: round(0.97*x, -1))
                df3['上限值'] = df3['参考值'].map(lambda x: round(1.1 * x, -1))
                df4 = pd.concat([test_data, df3], axis=1)
                # df4.to_excel('出数据/ceshi_data.xlsx',index=None,encoding='utf-8')
                df4 = pd.DataFrame(df4, columns=['区域', '工时组', '项目名称', '车系档次', args.IS4S, '参考值', '上限值'])
                df4.to_csv('high_brand/banjin/25个高端品牌钣金模型预测.to_csv', index=None, encoding='utf-8')
                return df4
            def chaizhuang_25_pre(is4s_list):
                datas4_fb = dd_all.loc[dd_all['工时组'] == '拆装']
                datas4 = pd.DataFrame()
                for is4s in is4s_list:
                    datas4_fb[args.IS4S] = is4s
                    datas4 = pd.concat([datas4, datas4_fb])
                # XGboost测试
                test_data = datas4.copy()
                ss = joblib.load("high_brand/chaizhuang/chaizhuang_ss.model")  ## 加载模型
                xgb_model = joblib.load("high_brand/chaizhuang/chaizhuang_xgb.model")  ## 加载模型

                with open('high_brand/chaizhuang/component2id.json', encoding='utf-8') as reader2:
                    component2id = json.load(reader2)

                # with open('high_brand/chaizhuang/id2component.json', encoding='utf-8') as reader3:
                #     id2component = json.load(reader3)

                with open('high_brand/chaizhuang/region2id.json', encoding='utf-8') as reader4:
                    region2id = json.load(reader4)
                test_data['compnent'] = test_data['项目名称'].map(component2id)
                test_data['VEHSERINAME_ID'] = test_data['车系档次'].map(group2id)
                test_data['REGION_ID'] = test_data['区域'].map(region2id)
                test_data.dropna(subset=['REGION_ID', 'compnent', 'VEHSERINAME_ID'], how='any', axis=0, inplace=True)
                test_data = test_data.reset_index(drop=True)
                df2 = pd.DataFrame(test_data, columns=['REGION_ID', 'compnent', 'VEHSERINAME_ID', args.IS4S])

                x_data = ss.transform(df2)
                datas_pre = list(np.exp(xgb_model.predict(x_data)))
                df3 = pd.DataFrame()
                df3['XGBOOST_VALUE'] = datas_pre
                df3['参考值'] = df3['XGBOOST_VALUE'].map(lambda x: round(0.97*x, -1))
                df3['上限值'] = df3['参考值'].map(lambda x: round(1.1 * x, -1))
                df4 = pd.concat([test_data, df3], axis=1)
                # df4.to_excel('出数据/ceshi_data.xlsx',index=None,encoding='utf-8')
                df4 = pd.DataFrame(df4, columns=['区域', '工时组', '项目名称', '车系档次', args.IS4S, '参考值', '上限值'])
                df4.to_csv('high_brand/chaizhuang/25个高端品牌拆装模型预测.csv', index=None, encoding='utf-8')
                return df4

            def high_ybc_concat(is4s_list):
                '''高端品牌油漆钣金拆装数据拼接'''
                youqi_25_data=youqi_25_pre(is4s_list)
                banjin_25_data=banjin_25_pre(is4s_list)
                chaizhaung_25_data=chaizhuang_25_pre(is4s_list)
                path11 = 'high_brand/youqi/25个高端品牌油漆统计数据及模型预测.csv'
                path22 = 'high_brand/banjin/25个高端品牌钣金统计数据及模型预测.csv'
                path33 = 'high_brand/chaizhuang/25个高端品牌拆装统计数据及模型预测.csv'

                dd11 = pd.read_csv(path11)
                dd22 = pd.read_csv(path22)
                dd33 = pd.read_csv(path33)

                dd11.rename(columns={'REGION': '区域', 'COMPNAME': '项目名称', 'VEHSERINAME_GROUP': '车系档次', 'WORK_TYPE': '工时组'},
                            inplace=True)
                dd22.rename(columns={'REGION': '区域', 'COMPNAME': '项目名称', 'VEHSERINAME_GROUP': '车系档次', 'WORK_TYPE': '工时组'},
                            inplace=True)
                dd33.rename(columns={'REGION': '区域', 'COMPNAME': '项目名称', 'VEHSERINAME_GROUP': '车系档次', 'WORK_TYPE': '工时组'},
                            inplace=True)
                dd11 = pd.DataFrame(dd11, columns=['区域', '工时组', '项目名称', '车系档次', 'IS4S', 'count', 'mean'])
                dd22 = pd.DataFrame(dd22, columns=['区域', '工时组', '项目名称', '车系档次', 'IS4S', 'count', 'mean'])
                dd33 = pd.DataFrame(dd33, columns=['区域', '工时组', '项目名称', '车系档次', 'IS4S', 'count', 'mean'])

                dd1 = pd.merge(youqi_25_data, dd11, how='left', on=['区域', '工时组', '项目名称', '车系档次', 'IS4S'])
                dd2 = pd.merge(banjin_25_data, dd22, how='left', on=['区域', '工时组', '项目名称', '车系档次', 'IS4S'])
                dd3 = pd.merge(chaizhaung_25_data, dd33, how='left', on=['区域', '工时组', '项目名称', '车系档次', 'IS4S'])
                dd = pd.concat([dd1, dd2, dd3], axis=0)
                dd['IS4S']=dd['IS4S'].replace(1,'服务站').replace(0,'综修厂')
                dd['VEHSERINAME_TYPE'] = '高端品牌'
                ddd = pd.DataFrame(dd, columns=['区域', 'VEHSERINAME_TYPE', 'IS4S', '工时组', '项目名称', '车系档次', 'count', 'mean', '参考值',
                                                '上限值'])

                def national_25_mean_replace(datas123):
                    '''25个高端品牌全国均值替换'''
                    national_mean = pd.DataFrame(datas123,
                                                 columns=['VEHSERINAME_TYPE', 'IS4S', '工时组', '项目名称', '车系档次', 'mean',
                                                          '参考值'])
                    national_mean = national_mean.groupby(['VEHSERINAME_TYPE', 'IS4S', '工时组', '项目名称', '车系档次'])[
                        'mean'].agg(
                        ['mean']).reset_index()
                    national_mean.rename(columns={'mean': 'national_mean'}, inplace=True)

                    def transform(x):
                        try:
                            con = round(x, -1)
                        except:
                            con = np.nan
                        return con

                    national_mean['national_mean'] = national_mean['national_mean'].apply(transform)
                    datas123 = pd.merge(datas123, national_mean, on=['VEHSERINAME_TYPE', 'IS4S', '工时组', '项目名称', '车系档次'],
                                        how='left')
                    # 模型预测值与均值的偏差超过一个阈值，用全国均值乘以一个系数去替代
                    datas123['coef_national'] = datas123.apply(
                        lambda x: (x['参考值'] - x['national_mean']) / x['national_mean'],
                        axis=1)
                    datas123['参考值'] = datas123.apply(
                        lambda x: round(1.0 * x['national_mean']) if x['coef_national'] >= 1 or x[
                            'coef_national'] < -0.45 else
                        x['参考值'], axis=1)
                    datas123['上限值'] = datas123['参考值'].apply(lambda x: round(1.1 * x, -1))
                    # datas123.to_csv('data1/gongshi_v.csv',index=None,encoding='utf-8')
                    datas123 = pd.DataFrame(datas123,
                                            columns=['区域', 'VEHSERINAME_TYPE', 'IS4S', '工时组', '项目名称', '车系档次', 'count',
                                                     'mean',
                                                     '参考值', '上限值'])
                    return datas123

                # ddd=dd.pivot_table(index=['区域','VEHSERINAME_TYPE','IS4S','工时组','项目名称'],columns=['车系档次'])
                # ddd=ddd.stack(level=0).reset_index()
                # ddd.rename(columns={'level_5':'VALUE_TYPE'},inplace=True)
                # ddd=national_25_mean_replace(ddd)
                ddd.to_csv('data/25个高端品牌油漆钣金拆装工时系统.csv', index=None, encoding='utf-8')
                return ddd



            def jd_25_concat():
                '''机修和电工拼接'''
                path2 = 'data/工时价格标准采集模板(发联保).xlsx'
                df2 = pd.read_excel(path2, sheet_name='25个高端品牌服务站工时标准（范例）', skiprows=1)
                df2.drop(0, inplace=True)
                df2.fillna(1, inplace=True)
                df2 = df2.set_index(['Unnamed: 0', 'Unnamed: 1']).stack().reset_index()
                df2.rename(columns={'Unnamed: 0': '工时组', 'Unnamed: 1': '项目名称', 'level_2': '车系档次'}, inplace=True)
                df2 = pd.DataFrame(df2, columns=['工时组', '项目名称', '车系档次'])
                with open('high_brand/youqi/region2id.json', encoding='utf-8') as f1:
                    region2id = json.load(f1)
                dd_all = pd.DataFrame()
                for region in region2id.keys():
                    df2['区域'] = region
                    dd_all = pd.concat([dd_all, df2])
                print(len(dd_all))

                datas7_fb = dd_all.loc[dd_all['工时组'] == '机修']
                is4s_list = [1]
                datas7 = pd.DataFrame()
                for is4s in is4s_list:
                    datas7_fb[args.IS4S] = is4s
                    datas7 = pd.concat([datas7, datas7_fb])

                datas8_fb = dd_all.loc[dd_all['工时组'] == '电工']
                datas8 = pd.DataFrame()
                for is4s in is4s_list:
                    datas8_fb[args.IS4S] = is4s
                    datas8 = pd.concat([datas8, datas8_fb])

                def jixiu_25_merge(datas7):
                    path4 = 'high_brand/jixiu/25个高端品牌机修统计数据.csv'
                    jixiu = pd.read_csv(path4)
                    jixiu['参考值'] = jixiu['median'].map(lambda x: round(x, -1))
                    jixiu.rename(columns={'REGION': '区域', 'WORK_TYPE': '工时组', 'COMPNAME': '项目名称', 'VEHSERINAME_GROUP': '车系档次'},
                                 inplace=True)
                    jixiu = pd.DataFrame(jixiu, columns=['区域', '工时组', '项目名称', '车系档次','IS4S','count','mean', '参考值'])
                    datas7 = pd.merge(datas7, jixiu, on=['工时组', '项目名称', '车系档次', '区域','IS4S'], how='left')

                    datas7['VEHSERINAME_TYPE'] = '高端品牌'
                    datas7['IS4S']=datas7['IS4S'].replace(1,'服务站').replace(0,'综修厂')
                    datas7 = pd.DataFrame(datas7, columns=['区域', 'VEHSERINAME_TYPE', 'IS4S', '工时组', '项目名称', '车系档次', 'count','mean', '参考值'])
                    return datas7
                def diangong_25_merge(datas8):
                    path5 = 'high_brand/diangong/25个高端品牌电工统计数据.csv'
                    diangong = pd.read_csv(path5)
                    diangong['参考值'] = diangong['median'].map(lambda x: round(x, -1))
                    diangong.rename(columns={'REGION': '区域', 'WORK_TYPE': '工时组', 'COMPNAME': '项目名称', 'VEHSERINAME_GROUP': '车系档次'},
                                    inplace=True)
                    diangong = pd.DataFrame(diangong, columns=['区域', '工时组', '项目名称', '车系档次','IS4S','count','mean', '参考值'])
                    datas8 = pd.merge(datas8, diangong, on=['工时组', '项目名称', '车系档次', '区域','IS4S'], how='left')
                    datas8['VEHSERINAME_TYPE'] = '高端品牌'
                    datas8['IS4S'] = datas8['IS4S'].replace(1, '服务站').replace(0, '综修厂')
                    datas8 = pd.DataFrame(datas8, columns=['区域', 'VEHSERINAME_TYPE', 'IS4S', '工时组', '项目名称', '车系档次', 'count','mean','参考值'])
                    return datas8

                def dj_concat(datas7,datas8):
                    datas7=jixiu_25_merge(datas7)
                    datas8=diangong_25_merge(datas8)
                    dgjx_25 = pd.concat([datas7, datas8], axis=0)
                    dgjx_25.to_csv('data/25个高端品牌机修电工工时体系.csv', index=None, encoding='utf-8')
                    return dgjx_25

                dgjx_25=dj_concat(datas7,datas8)
                # dgjx=dgjx.pivot_table(index=['区域','VEHSERINAME_TYPE','IS4S','工时组','项目名称'],columns=['车系档次'])
                # dgjx=dgjx.stack(level=0).reset_index()
                # dgjx.rename(columns={'level_5':'VALUE_TYPE'},inplace=True)
                return dgjx_25

            high_ybc_data = high_ybc_concat(is4s_list)
            dgjx_25=jd_25_concat()
            dgjx_25['参考值'] =dgjx_25.groupby(['工时组', '项目名称', 'VEHSERINAME_TYPE', '车系档次', 'IS4S'])['参考值'].transform(
                lambda x: x.fillna(x.median()))
            dgjx_25.drop_duplicates(['区域', 'IS4S', '工时组', '项目名称', '车系档次'],keep='first', inplace=True)
            dgjx_25['参考值'] = dgjx_25['参考值'].apply(trans2inter)
            dgjx_25['上限值'] = np.nan
            gongshi_25_data = pd.concat([high_ybc_data, dgjx_25], axis=0)
            return gongshi_25_data

        model_statistics_system_25high_brand_data=model_statistics_system_25high_brand_process()


        all_data = pd.concat([model_statistics_system_data, dgjx_statistics_system_data,model_statistics_system_25high_brand_data], axis=0)
        all_data.rename(columns={'区域': 'REGION', '工时组': 'REPAIRTYPE', '项目名称': 'COMPNAME', '车系档次': 'VEHSERINAME_GRADE',
                                 '参考值': 'REFERENCE_VALUE', '上限值': 'UPPER_LIMIT_VALUE'}, inplace=True)
        def digit_transform1(x):
            '''值大于3位的，取整到百位'''
            try:
                if len(str(round(x))) > 3 :
                    con=round(x, -2)
                elif len(str(round(x))) <= 3 :
                    con=round(x, -1)
            except:
                con=x
            return con

        def digit_transform2(x):
            '''值大于3位的，取整到百位'''
            try:
                if  len(str(round(1.1*x))) > 3 :
                    con=round(1.1*x, -2)
                elif len(str(round(1.1*x))) <= 3 :
                    con=round(1.1*x, -1)
            except:
                con=x
            return con
        all_data['REFERENCE_VALUE'] = all_data['REFERENCE_VALUE'].apply(digit_transform1)

        all_data['UPPER_LIMIT_VALUE'] = all_data['REFERENCE_VALUE'].apply(digit_transform2)

        def banjin_ascending(data):
            data=data[data['REPAIRTYPE']=='钣金']
            COMPNAMEs=data['COMPNAME'].tolist()
            REFERENCE_VALUEs=data['REFERENCE_VALUE'].tolist()
            UPPER_LIMIT_VALUEs=data['UPPER_LIMIT_VALUE'].tolist()

            result_COMPNAMEs=[]
            result_REFERENCE_VALUEs=[]
            result_UPPER_LIMIT_VALUEs=[]
            for i in range(len(COMPNAMEs)):
                if '(' in COMPNAMEs[i] and COMPNAMEs[i] not in result_COMPNAMEs:
                    list_C = []
                    list_R = []
                    list_U=[]
                    list_C.append(COMPNAMEs[i])
                    list_R.append(REFERENCE_VALUEs[i])
                    list_U.append(UPPER_LIMIT_VALUEs[i])
                    left_1=COMPNAMEs[i].split('(')[0]
                    right_1=COMPNAMEs[i].split('(')[-1].split(')')[0]
                    for j in range(i+1,len(COMPNAMEs)):
                        if '(' in COMPNAMEs[j]:
                            left_2 = COMPNAMEs[j].split('(')[0]
                            right_2= COMPNAMEs[j].split('(')[-1].split(')')[0]
                            if left_1==left_2:
                                list_C.append(COMPNAMEs[j])
                                list_R.append(REFERENCE_VALUEs[j])
                                list_U.append(UPPER_LIMIT_VALUEs[j])
                    if (len(list_C))>1:
                        if (len(list_C))==2:
                            list_C[0]=left_1+'(小)'
                            list_C[1]=left_1+'(大)'
                        if (len(list_C))==3:
                            list_C[0]=left_1+'(小)'
                            list_C[1]=left_1+'(中)'
                            list_C[2]=left_1+'(大)'
                        list_R=sorted(list_R)
                        list_U=sorted(list_U)
                        if len(list_R) == 3:
                            count_result = pd.Series(list_R).value_counts()
                            count_dict = count_result.to_dict()
                            for i in count_dict:
                                if count_dict[i] > 1:
                                    list_R[1] = round((list_R[0]+list_R[2])/2,-1)
                                    list_U[1] = round(list_R[1]*1.1,-1)
                                    break
                        result_COMPNAMEs.extend(list_C)
                        result_REFERENCE_VALUEs.extend(list_R)
                        result_UPPER_LIMIT_VALUEs.extend(list_U)
            df=pd.DataFrame()
            df['COMPNAME']=result_COMPNAMEs
            df['REFERENCE_VALUE1']=result_REFERENCE_VALUEs
            df['UPPER_LIMIT_VALUE1']=result_UPPER_LIMIT_VALUEs
            df['REPAIRTYPE'] = '钣金'
            return df

        def youqi_ascending(data):
            data=data[data['REPAIRTYPE']=='油漆']
            COMPNAMEs=data['COMPNAME'].tolist()
            REFERENCE_VALUEs=data['REFERENCE_VALUE'].tolist()
            UPPER_LIMIT_VALUEs=data['UPPER_LIMIT_VALUE'].tolist()

            result_COMPNAMEs=[]
            result_REFERENCE_VALUEs=[]
            result_UPPER_LIMIT_VALUEs=[]
            for i in range(len(COMPNAMEs)):
                if '(' in COMPNAMEs[i] and COMPNAMEs[i] not in result_COMPNAMEs:
                    list_C = []
                    list_R = []
                    list_U=[]
                    list_C.append(COMPNAMEs[i])
                    list_R.append(REFERENCE_VALUEs[i])
                    list_U.append(UPPER_LIMIT_VALUEs[i])
                    left_1=COMPNAMEs[i].split('(')[0]
                    right_1=COMPNAMEs[i].split('(')[-1].split(')')[0]
                    for j in range(i+1,len(COMPNAMEs)):
                        if '(' in COMPNAMEs[j]:
                            left_2 = COMPNAMEs[j].split('(')[0]
                            right_2= COMPNAMEs[j].split('(')[-1].split(')')[0]
                            if left_1==left_2:
                                list_C.append(COMPNAMEs[j])
                                list_R.append(REFERENCE_VALUEs[j])
                                list_U.append(UPPER_LIMIT_VALUEs[j])
                    if (len(list_C))>1:
                        if (len(list_C))==2:
                            list_C[0]=left_1+'(半喷)'
                            list_C[1]=left_1+'(全喷)'
                        if (len(list_C))==3:
                            list_C[0]=left_1+'(抛光)'
                            list_C[1]=left_1+'(半喷)'
                            list_C[2]=left_1+'(全喷)'
                        list_R=sorted(list_R)
                        list_U=sorted(list_U)
                        if len(list_R) == 3:
                            count_result = pd.Series(list_R).value_counts()
                            count_dict = count_result.to_dict()
                            for i in count_dict:
                                if count_dict[i] > 1:
                                    list_R[1] = round(list_R[2]/2,-1)
                                    list_U[1] = round(list_R[1]*1.1,-1)
                                    break
                        result_COMPNAMEs.extend(list_C)
                        result_REFERENCE_VALUEs.extend(list_R)
                        result_UPPER_LIMIT_VALUEs.extend(list_U)
            df=pd.DataFrame()
            df['COMPNAME']=result_COMPNAMEs
            df['REFERENCE_VALUE1']=result_REFERENCE_VALUEs
            df['UPPER_LIMIT_VALUE1']=result_UPPER_LIMIT_VALUEs
            df['REPAIRTYPE'] = '油漆'
            return df
        #钣金排序处理
        all_data_1=all_data.groupby(['REGION','VEHSERINAME_TYPE','IS4S','VEHSERINAME_GRADE'])['REPAIRTYPE','COMPNAME','REFERENCE_VALUE','UPPER_LIMIT_VALUE'].apply(banjin_ascending).apply(pd.Series).reset_index()
        all_data_1.drop('level_4',axis=1,inplace=True)
        all_data=pd.merge(all_data,all_data_1,on=['REGION','VEHSERINAME_TYPE','IS4S','VEHSERINAME_GRADE','REPAIRTYPE','COMPNAME'],how='left')
        all_data['REFERENCE_VALUE'] = all_data.apply(
            lambda x: x['REFERENCE_VALUE'] if str(x['REFERENCE_VALUE1']) == 'nan' else x['REFERENCE_VALUE1'], axis=1)
        all_data['UPPER_LIMIT_VALUE'] = all_data.apply(
            lambda x: x['UPPER_LIMIT_VALUE'] if str(x['UPPER_LIMIT_VALUE1']) == 'nan' else x['UPPER_LIMIT_VALUE1'],
            axis=1)
        all_data.drop(['REFERENCE_VALUE1', 'UPPER_LIMIT_VALUE1'], axis=1, inplace=True)
        #油漆排序处理
        all_data_2=all_data.groupby(['REGION','VEHSERINAME_TYPE','IS4S','VEHSERINAME_GRADE'])['REPAIRTYPE','COMPNAME','REFERENCE_VALUE','UPPER_LIMIT_VALUE'].apply(youqi_ascending).apply(pd.Series).reset_index()
        all_data_2.drop('level_4', axis=1, inplace=True)
        all_data = pd.merge(all_data, all_data_2,
                            on=['REGION', 'VEHSERINAME_TYPE', 'IS4S', 'VEHSERINAME_GRADE', 'REPAIRTYPE', 'COMPNAME'],
                            how='left')
        all_data['REFERENCE_VALUE']=all_data.apply(lambda x: x['REFERENCE_VALUE'] if str(x['REFERENCE_VALUE1'])=='nan' else x['REFERENCE_VALUE1'],axis=1)
        all_data['UPPER_LIMIT_VALUE']=all_data.apply(lambda x: x['UPPER_LIMIT_VALUE'] if str(x['UPPER_LIMIT_VALUE1'])=='nan' else x['UPPER_LIMIT_VALUE1'],axis=1)
        all_data.drop(['REFERENCE_VALUE1','UPPER_LIMIT_VALUE1'],axis=1,inplace=True)

        def f1(x):
            '''均值取小数位2位'''
            try:
                con = round(float(x), 2)
            except:
                con = np.nan
            return con

        all_data['mean'] = all_data['mean'].apply(f1)
        all_data['ID'] = [i for i in range(1, len(all_data) + 1)]
        all_data['STATUS']=1
        all_data['REGION_ID'] = all_data['REGION'].map(region2code).astype(str)
        table_name = 'LB_GS_NATIONAL_GS_DATA'
        commit = "select * from {}".format(table_name)
        national_system_data = self.oracle.getData(commit, self.account)
        national_system_data=national_system_data.drop_duplicates()
        all_data = pd.merge(all_data, national_system_data,
                            on=['REGION', 'VEHSERINAME_TYPE', 'IS4S', 'REPAIRTYPE', 'COMPNAME', 'VEHSERINAME_GRADE'],
                            how='left')
        all_data.drop_duplicates(subset=['ID'],keep='first',inplace=True)
        num = all_data.groupby(['REGION', 'VEHSERINAME_TYPE', 'IS4S', 'VEHSERINAME_GRADE']).count().reset_index()
        num['TEMPLATE_ID'] = [id for id in range(100001, 100001 + len(num))]
        num = pd.DataFrame(num, columns=['REGION', 'VEHSERINAME_TYPE', 'IS4S', 'VEHSERINAME_GRADE', 'TEMPLATE_ID'])
        all_data = pd.merge(all_data, num, on=['REGION', 'VEHSERINAME_TYPE', 'IS4S', 'VEHSERINAME_GRADE'], how='left')
        print(all_data.info())
        all_data['INSERT_TIME']=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        all_data = pd.DataFrame(all_data,columns=['ID', 'REGION_ID', 'REGION', 'VEHSERINAME_TYPE', 'IS4S', 'REPAIRTYPE','COMPNAME',
                                         'VEHSERINAME_GRADE', 'count', 'mean', 'REFERENCE_VALUE', 'UPPER_LIMIT_VALUE',
                                         'SYSTEM_VALUE', 'TEMPLATE_ID','LAST_TIME','UPDATEPERSON','STATUS','INSERT_TIME'])
        all_data = all_data.astype(str)
        all_data.replace('nan', '', inplace=True)
        all_data.replace(0.0,10.0, inplace=True)
        table_name = 'LB_GS_SYSTEM'
        time1 = time.time()
        self.oracle.Batch_gs_insertDataToTable(all_data, table_name, self.account)
        print("插入数据耗时{}".format(get_time_dif(time1)))
        # ddd = datas123.pivot_table(index=['区域', 'VEHSERINAME_TYPE', 'IS4S', '工时组', '项目名称'], columns=['车系档次'])
        # ddd.to_excel('出数据\乘用车油漆钣金拆装工时体系数据.xlsx', encoding='utf-8')
        # ddd = df21.pivot_table(index=['区域', 'VEHSERINAME_TYPE', 'IS4S', '工时组', '项目名称'], columns=['车系档次'])
        # ddd.to_excel('出数据/25个高端品牌油漆钣金拆装工时体系.xlsx', encoding='utf-8')
        # ddd = datas124.pivot_table(index=['区域', 'VEHSERINAME_TYPE', 'IS4S', '工时组', '项目名称'], columns=['车系档次'])
        # ddd.to_excel('出数据/乘用车机修电工工时体系.xlsx', encoding='utf-8')
        # ddd = df22.pivot_table(index=['区域', 'VEHSERINAME_TYPE', 'IS4S', '工时组', '项目名称'], columns=['车系档次'])
        # ddd.to_excel('出数据/25个高端品牌机修电工工时体系.xlsx', encoding='utf-8')
        def sql_excute():
            '''sql语句应用表'''
            com1='''truncate table lb_gs_use_data'''
            com2='''insert into lb_gs_use_data 
select region_id,REGION,brand_name,cate_name,x.VEHSERINAME_TYPE,REPAIRTYPE,COMPNAME,
REFERENCE_VALUE ,is4s,GRADE_NUM from(select distinct  region_id,REGION,VEHSERINAME_TYPE,REPAIRTYPE,COMPNAME,
REFERENCE_VALUE ,is4s,vehseriname_grade  from  LB_GS_SYSTEM )x 
left join (select distinct jigou,VEHSERINAME_GRADE,VEHSERINAME_TYPE,brand_name,cate_name,a.GRADE_NUM
from lb_area_grade a left join  lb_cate_GRADE b on a.grade_num=b.grade_num) y
on x.region=y.jigou and x.vehseriname_type=y.vehseriname_type 
and x.vehseriname_grade =y.vehseriname_grade'''
            com3='''truncate table LB_GS_ORIGINAL_new'''
            com4='''truncate  table  lb_TEMPLATE'''
            com5='''insert into lb_TEMPLATE
select lb_TEMPLATE_id.nextval, m.* from
(select distinct region_id , REGION,grade_code,x.VEHSERINAME_GRADE,x.VEHSERINAME_TYPE,IS4S,
TEMPLATE_ID
from LB_GS_SYSTEM x left join (select jigou,VEHSERINAME_GRADE,VEHSERINAME_TYPE,grade_code
from lb_area_grade a left join  lb_cate_GRADE b on a.grade_num=b.grade_num) y
on x.region=y.jigou and x.vehseriname_type=y.vehseriname_type 
and x.vehseriname_grade =y.vehseriname_grade) m'''
            com6='''truncate table LB_GD_BRAND_DATA'''
            list1=[com1,com2,com3,com4,com5,com6]
            for i,com in enumerate(list1):
                # print(i)
                self.oracle.executeCommitSubmit(com,self.account)
            return

        def gd_brand_table(all_data):
            '''高端品牌数据应用表'''
            with open('high_brand/group2id.json',encoding='utf-8') as f:
                group2id=json.load(f)
            gd_grades=list(group2id.keys())
            all_data=all_data[['REGION','REPAIRTYPE','COMPNAME','VEHSERINAME_GRADE','REFERENCE_VALUE']]
            all_data_gd=all_data.loc[all_data['VEHSERINAME_GRADE'].isin(gd_grades)].reset_index(drop=True)
            all_data_gd['VEHSERINAME_GRADE'].replace('·','',regex=True,inplace=True)
            all_data_gd['REFERENCE_VALUE'].replace('',0,inplace=True)
            all_data_gd['REFERENCE_VALUE']=all_data_gd['REFERENCE_VALUE'].astype(float)
            all_data_gd=all_data_gd.pivot_table(index=['REGION', 'REPAIRTYPE', 'COMPNAME'], columns=['VEHSERINAME_GRADE'])
            all_data_gd = all_data_gd.stack(level=0).reset_index()
            all_data_gd.drop('level_3',axis=1,inplace=True)
            all_data_gd=pd.DataFrame(all_data_gd,columns=['REGION','REPAIRTYPE','COMPNAME','JeepA组','JeepB组','之诺','保时捷','兰博基尼','凯迪拉克','劳斯莱斯','奔驰A组','奔驰B组','奔驰C组','奥迪A组','奥迪B组','奥迪C组',\
                             '奥迪D组','宝马A组','宝马B组','宝马C组','宾利','悍马','捷豹','林肯','沃尔沃','法拉利','特斯拉','玛莎拉蒂','英菲尼迪','讴歌','路虎','迈凯轮','迈巴赫','阿尔法罗密欧','阿斯顿马丁','雷克萨斯'])
            all_data_gd.replace(0,'',inplace=True)
            all_data_gd=all_data_gd.astype(str)
            table_name1 = 'LB_GD_BRAND_DATA'
            self.oracle.BatchinsertDataToTable(all_data_gd,table_name1,self.account)

        sql_excute()
        gd_brand_table(all_data)
        self.logger.info(('all done!'))
        # print('all done!')


if __name__=='__main__':
    handle = Process("DDPJCXBUSI", "ccic8519", "ccicgis","10.1.88.75")
    handle.generate_system()