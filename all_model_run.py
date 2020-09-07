#!/usr/bin/env python 
# -*- coding:utf-8 -*-

import chexi_id
import os
from sql_oracle import Process
import time
from branch_code.dadi_loader import get_time_dif

def main():

    def large_area_model(handle):
        '''
        模型训练是训练33个区域的油漆，钣金，拆装模型及统计数据
        '''
        chexi2id=chexi_id.chexi_transform_id()
        handle.youqi_model('youqi','large_area',chexi2id)
        handle.banjin_model('banjin','large_area',chexi2id)
        handle.chaizhaung_model('chaizhuang','large_area',chexi2id)
        handle.youqi_statistics_pre('large_area',chexi2id)
        handle.banjin_statistics_pre('large_area',chexi2id)
        handle.chaizhuang_statistics_pre('large_area',chexi2id)
        handle.jixiu_statistics_pre('large_area',chexi2id)
        handle.diangong_statistics_pre('large_area',chexi2id)
        print('大区域3个模型完成！')

    def single_area_model(handle):
        '''
        分别训练江苏，深圳，云南的油漆，钣金，拆装模型及统计数据
        '''
        jigous=['jiangsu','shenzhen','yunnan']
        for jigou in jigous:
            if jigou=='jiangsu':
                chexi2id=chexi_id.jiangsu_chexi_transform_id()
            elif jigou=='shenzhen':
                chexi2id = chexi_id.shenzhen_chexi_transform_id()
            elif jigou=='yunnan':
                chexi2id = chexi_id.yunnan_chexi_transform_id()
            handle.single_area_youqi_model('youqi',jigou,chexi2id)
            handle.single_area_banjin_model('banjin', jigou,chexi2id)
            handle.single_area_chaizhaung_model('chaizhuang', jigou,chexi2id)
            handle.youqi_statistics_pre(jigou, chexi2id)
            handle.banjin_statistics_pre(jigou, chexi2id)
            handle.chaizhuang_statistics_pre(jigou, chexi2id)
            handle.jixiu_statistics_pre(jigou, chexi2id)
            handle.diangong_statistics_pre(jigou, chexi2id)
        print('3个区域3个模型完成！')

    def high_end_brand_model(handle):
        '''全国25个高端品牌油漆，钣金，拆装模型及统计数据'''
        group_dict,group2id = chexi_id.gaoduan_chexi_transform_id()
        handle.youqi_25_model(group_dict,group2id)
        handle.banjin_25_model(group_dict,group2id)
        handle.chaizhaung_25_model(group_dict,group2id)
        handle.youqi_statistics_25_pre(group_dict,group2id)
        handle.banjin_statistics_25_pre(group_dict,group2id)
        handle.chaizhuang_statistics_25_pre(group_dict,group2id)
        handle.jixiu_statistics_25_pre(group_dict,group2id)
        handle.diangong_statistics_25_pre(group_dict,group2id)
        print('高端品牌模型完成！')

    start_time_model = time.time()
    handle=Process("DDPJCXBUSI", "ccic8519", "ccicgis","10.1.88.75")
    handle.trunct_table()
    handle.handle2oracle()
    large_area_model(handle)
    single_area_model(handle)
    high_end_brand_model(handle)
    handle.outlier()
    handle.system_value_oracle()
    handle.generate_system()
    print('总耗时为{}'.format(get_time_dif(start_time_model)))

if __name__=='__main__':
    main()