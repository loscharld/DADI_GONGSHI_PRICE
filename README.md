#项目名称
AI自助定损系统之智能工时模型

# 核损工时价格预测
从数据库里抽取数据集，预测全国每个区域车型的核损工时价格

# 项目介绍
为了降低工时费在车险理赔中的不确定因素性，完善理赔数据，为业务发展和KPI考核方案制定带来保障，智能工时标准化项目的实施刻不容缓。


# 项目运行方式
1、配置要求：要求配置cx_Oracle数据库及相应的python依赖包。cx_Oracle数据库用户名密码记住。
2、all_model_run.py  主程序运行
3、branch_code/dadi_loader.py 辅助文件（一些函数）
4、chexi_id.py  车系转换id文件
5、sql_oracle.py 项目的核心文件
6、ForCall01.py 数据库关联文件

#部署到服务器运行方式：
#cd到项目路径下
1、cd /home/appuser/lihui/DADI_GONGSHI_PRICE_RESULT
#后台运行
1、nohup python all_model_run.py >> /home/appuser/lihui/DADI_GONGSHI_PRICE_RESULT/data/my.log 2>&1 &
关联到的文件有 ForCall01.py，sql_oracle.py，dadi_loader.py，chexi_id.py

#后台接口运行
1、nohup python interface_gs.py >> /home/appuser/lihui/DADI_GONGSHI_PRICE_RESULT/data/my.log 2>&1 &  后台运行xmlrpc接口，供java调用

# 总结
1、本项目以cx_Oracle作为存储，从数据库里提取数据集，以预测全国36个区域车型的油漆、钣金、拆装的核损工时价格
2、数据量：取1年的数据，大概1000万数据
3、以业务驱动，经过数据清洗，过滤，得到高质量数据，特征工程化后训练XGBoost模型
4、采用箱线图异常值检查方法去除异常值，特征工程中增加车系档次划分，工时项目划分
5、统计模型统计每一个区域的工时项目的核损工时价格，包括样本数，均值，中位数，众数等
6、采用机器学习框架SKlearn中的xgboost-gpu模型训练，GridSearchCV自动调参，保存最优模型
7、使用已保存的最优模型生成36个区域243个工时项目的所有数据，并保存在数据库中。
8、数据校准：模型预测价格，采用数据平滑，数据对齐，数据排序，数据填充校准价格。同时用统计模型统计出的均值，中位数，众数等参考对比校准价格。




