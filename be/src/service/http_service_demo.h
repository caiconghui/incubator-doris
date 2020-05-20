// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <common/utils.h>
#include <common/status.h>
#include <runtime/stream_load/stream_load_context.h>
#include "gen_cpp/http_service.pb.h"
#include "service/brpc.h"

namespace doris {
    class ExecEnv;

    class HttpServiceDemoImpl : public doris_demo::HttpService {

    public :

        HttpServiceDemoImpl(ExecEnv *exec_env);

        virtual ~HttpServiceDemoImpl();

        virtual void Echo(google::protobuf::RpcController *cntl_base,
                          const doris_demo::HttpRequest * /*request*/,
                          doris_demo::HttpResponse * /*response*/,
                          google::protobuf::Closure *done) override;
    private :
        ExecEnv *_exec_env;

        bool parse_basic_auth(brpc::Controller* cntl, AuthInfo* auth);
        bool parse_basic_auth(brpc::HttpHeader &header, std::string* user, std::string* passwd);

        void free_handler_ctx(void* ctx);
        Status _on_header(brpc::Controller* cntl, StreamLoadContext* ctx);
        Status _handle(StreamLoadContext* ctx);
        Status _data_saved_path(StreamLoadContext* ctx, std::string* file_path);
        Status _process_put(brpc::Controller* cntl, StreamLoadContext* ctx);
    };
}
