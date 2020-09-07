#! usr/bin/env python3
# -*- coding:utf-8 -*-
from all_model_run import main
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler

class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

# Create server
with SimpleXMLRPCServer(("0.0.0.0", 5200),requestHandler=RequestHandler) as server:
    server.register_introspection_functions()


    # 开始
    def start_code():
        try:
            main()
            return 100
        except:
            return 400


    server.register_function(start_code, 'start_code')
    # server.register_instance(vn)
    print("server is start...........")
    # Run the server's main loop
    server.serve_forever()
    print("server is end...........")