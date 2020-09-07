import pandas as pd
import os
import shutil
import datetime
from ForCall01 import *

def create_all_table():
    #车系档次表
    com1='''CREATE TABLE LB_GS_cate_grade(
ID VARCHAR2(30) NOT NULL PRIMARY KEY,YZT_CATE  VARCHAR2(100),YZT_CATE_CODE VARCHAR2(30),CATE_ID VARCHAR2(30),GRADE_ID VARCHAR2(30),VEHSERINAME_TYPE VARCHAR2(50),
VEHSERINAME_GRADE VARCHAR2(100),
LAST_TIME  DATE,Update_Person VARCHAR2(30),STATUS NUMBER(1),INSERT_TIME DATE )'''

    #原始数据载入表
    com2='''CREATE TABLE LB_GS_ORIGINAL_DATA_LOAD( ID VARCHAR2(30) NOT NULL PRIMARY KEY,
LOSSAPPROVALID VARCHAR2(40),LOSSAPPROVALCOMCODE VARCHAR2(10),VEHSERINAME VARCHAR2(150),IS4S VARCHAR2(2),REPAIRTYPE VARCHAR2(2),
COMPNAME VARCHAR2(150),REGISTNO VARCHAR2(225),SUMVERILOSS NUMBER(14,2),DC VARCHAR2(60),VERIFYFINALDATE DATE)  '''

    #原始数据处理表
    com3='''CREATE TABLE LB_GS_ORIGINAL_HANDLE( ID VARCHAR2(30) NOT NULL PRIMARY KEY,
LOSSAPPROVALID VARCHAR2(40),LOSSAPPROVALCOMCODE VARCHAR2(10),VEHSERINAME VARCHAR2(150),IS4S VARCHAR2(2),REPAIRTYPE VARCHAR2(2),
COMPNAME VARCHAR2(150),REGISTNO VARCHAR2(225),SUMVERILOSS NUMBER(14,2),DC VARCHAR2(60),VERIFYFINALDATE DATE)  '''

    # 工时模块异常表
    com4 = '''CREATE TABLE LB_GS_original_abnormal(LOSSAPPROVALID VARCHAR2(40),REPAIRTYPE VARCHAR2(40),
REGION VARCHAR2(40),IS4S NUMBER(2),VEHSERINAME VARCHAR2(40),VEHSERINAME_GRADE VARCHAR2(20),
COMPNAME VARCHAR2(120),SUMVERILOSS NUMBER(14,2))'''

    #全国各机构工时数据表
    com5='''CREATE TABLE LB_GS_NATIONAL_GS_DATA( REGION VARCHAR2(50),VEHSERINAME_TYPE VARCHAR2(50),
    IS4S VARCHAR2(50),REPAIRTYPE VARCHAR2(40),COMPNAME VARCHAR2(120),VEHSERINAME_GRADE VARCHAR2(100),
    SYSTEM_VALUE NUMBER(14,2))'''

    #工时模板关联工时项目表
    com5='''CREATE TABLE LB_GS_SYSTEM( ID VARCHAR2(30) NOT NULL PRIMARY KEY,
REGION_ID VARCHAR2(8),REGION VARCHAR2(50),VEHSERINAME_TYPE VARCHAR2(50),IS4S VARCHAR2(50),
REPAIRTYPE VARCHAR2(40),COMPNAME VARCHAR2(50),VEHSERINAME_GRADE VARCHAR2(100),COUNT NUMBER(14),
MEAN NUMBER(14,2),REFERENCE_VALUE NUMBER(14,2),UPPER_LIMIT_VALUE NUMBER(14,2),SYSTEM_VALUE NUMBER(14,2),
TEMPLATE_ID VARCHAR2(30),LAST_TIME  DATE,Update_Person VARCHAR2(30),STATUS NUMBER(1),INSERT_TIME DATE)'''

    #工时模板表
    com6='''CREATE TABLE LB_GS_TEMPLATE(
ORDER_ID NUMBER(10) NOT NULL PRIMARY KEY,FORM_PLATE_ID VARCHAR2(30),TEMPLATE_ID VARCHAR2(30),
REGION_ID VARCHAR2(4),LEVEL_NAME VARCHAR2(30),LAST_TIME  DATE,Update_Person VARCHAR2(30),
STATUS NUMBER(1),INSERT_TIME DATE, NOTES VARCHAR2(30))'''

    #工时模板关联车系表
    com7='''CREATE TABLE LB_GS_TEMPLATE_CATE(ID VARCHAR2(30) NOT NULL PRIMARY KEY,TEMPLATE_ID VARCHAR2(30),
    BRAND_ID VARCHAR2(30),CATE_ID VARCHAR2(30),LAST_TIME  DATE,Update_Person VARCHAR2(30),STATUS NUMBER(1),
    INSERT_TIME DATE )'''

    #工时模板关联工时项目修改记录表
    com8='''CREATE TABLE LB_GS_MODIFY_RECORD(ID VARCHAR2(30) NOT NULL PRIMARY KEY,RELATEDNESS_ID VARCHAR2(30),
VALUE_TYPE VARCHAR2(30),UPDATE_VALUE NUMBER(14,2),LAST_TIME  DATE,Update_Person VARCHAR2(30))'''

    #工时实际使用偏差记录表
    com9='''CREATE TABLE LB_GS_DEVIATION_RECORD( ID VARCHAR2(30) NOT NULL PRIMARY KEY,
BRAND_ID VARCHAR2(30),BRAND VARCHAR2(30),
MODEL_FACTORY VARCHAR2(30),REPAIRTYPE VARCHAR2(40),COMPNAME VARCHAR2(40),CATE_ID VARCHAR2(30),
CATE_NAME VARCHAR2(30),REGISTNO VARCHAR2(225),REAL_VALUE NUMBER(14,2),REFERENCE_VALUE NUMBER(14,2),
DEVIATION NUMBER(14,2),REGION VARCHAR2(40),Damage_Fixer  VARCHAR2(40),INSERT_TIME DATE,STATUS NUMBER(1),
LAST_TIME DATE,Update_Person VARCHAR2(30))'''

def system_value_oracle():
    path='data/全国各机构工时数据汇总20200219.xlsx'
    # datas=pd.read_excel(path)
    datas = pd.ExcelFile(path)

    sheet_names=datas.sheet_names# see all sheet names
    df1=pd.read_excel(path)
    df1.dropna(subset=['机构'],how='any',axis=0,inplace=True)
    jigou_ls=list(set(df1['机构'].tolist()))
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
                        df2=pd.read_excel(path,sheet_name=sheet_name,skiprows=1)
                        df2.drop(0, inplace=True)
                        df2 = df2.set_index(['Unnamed: 0', 'Unnamed: 1']).stack().reset_index()
                        df2.rename(columns={'Unnamed: 0': '工时组', 'Unnamed: 1': '项目名称', 'level_2': 'VEHSERINAME_GRADE',0:'SYSTEM_VALUE'}, inplace=True)
                        df2['REGION']=jigou
                        df2['VEHSERINAME_TYPE']='高端品牌'
                        df2['IS4S']='服务站'
                        df2 = pd.DataFrame(df2,columns=['REGION','VEHSERINAME_TYPE','IS4S','工时组','项目名称','VEHSERINAME_GRADE','SYSTEM_VALUE'])

                    elif jigou in sheet_name and '服务站' in sheet_name:
                        df2=pd.read_excel(path,sheet_name=sheet_name,skiprows=1)
                        df2['REGION']=jigou
                        df2['VEHSERINAME_TYPE']='乘用车'
                        df2['IS4S']='服务站'
                        df2 = df2.set_index(['REGION','VEHSERINAME_TYPE','IS4S','工时组','项目名称']).stack().reset_index()
                        df2.rename(columns={'level_5': 'VEHSERINAME_GRADE',0:'SYSTEM_VALUE'}, inplace=True)

                    elif jigou in sheet_name and '综修厂' in sheet_name:
                        df2=pd.read_excel(path,sheet_name=sheet_name,skiprows=1)
                        df2['REGION']=jigou
                        df2['VEHSERINAME_TYPE']='乘用车'
                        df2['IS4S']='综修厂'
                        df2 = df2.set_index(['REGION', 'VEHSERINAME_TYPE', 'IS4S', '工时组', '项目名称']).stack().reset_index()
                        df2.rename(columns={'level_5': 'VEHSERINAME_GRADE', 0: 'SYSTEM_VALUE'}, inplace=True)

                    else:
                        continue
                    df = pd.concat([df, df2], axis=0)
                except Exception as e:
                    print(e)

        return df
    df=transform()
    # df.to_excel('大地系统数据.xlsx',index=None,encoding='utf-8')
    df=df.drop_duplicates(['REGION','VEHSERINAME_TYPE','IS4S','工时组','项目名称','VEHSERINAME_GRADE'], keep='first')
    df.rename(columns={'工时组':'REPAIRTYPE','项目名称':'COMPNAME'},inplace=True)
    # df['SYSTEM_VALUE']=df['SYSTEM_VALUE'].astype(float)
    oracle = useOracle("VDATA", "xdf123", "LBORA170")
    table_name='national_gongshi_data'
    account="vdata/xdf123@10.9.1.170/lbora"
    # oracle.creatDataFrame(all_data,table_name,account)
    oracle.BatchinsertDataToTable(df,table_name,account)


if __name__=='__main__':
    system_value_oracle()

