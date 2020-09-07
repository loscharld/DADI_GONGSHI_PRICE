#!/usr/bin/env python
# -*- coding:utf-8 -*-
import xmlrpc.client
import datetime
import json
server = xmlrpc.client.ServerProxy("http://10.9.1.45:5200")
start_time=datetime.datetime.now()
result=server.start_code()
end_time=datetime.datetime.now()
print(result)
print('总耗时：%.1f秒' % ((end_time - start_time).seconds))
