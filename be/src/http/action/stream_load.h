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

#include <functional>

#include "gen_cpp/PlanNodes_types.h"
#include "http/http_handler.h"
#include "runtime/client_cache.h"
#include "runtime/message_body_sink.h"
#include "util/priority_thread_pool.hpp"

namespace doris {

class ExecEnv;
class Status;
class StreamLoadContext;

class StreamLoadAction : public HttpHandler {
public:

    typedef std::function<void (StreamLoadContext*)> HandleFinishCallback;

    StreamLoadAction(ExecEnv* exec_env);
    ~StreamLoadAction() override;

    void handle(HttpRequest *req) override;

    bool request_will_be_read_progressively() override { return true; }

    int on_header(HttpRequest* req) override;

    void on_chunk_data(HttpRequest* req) override;
    void free_handler_ctx(void* ctx) override;

private:
    Status _on_header(HttpRequest* http_req, StreamLoadContext* ctx);
    Status _handle(StreamLoadContext* ctx);
    Status _data_saved_path(HttpRequest* req, std::string* file_path);
    Status _execute_plan_fragment(StreamLoadContext* ctx);
    Status _process_put(HttpRequest* http_req, StreamLoadContext* ctx);
    void _process_header(HttpRequest* req, StreamLoadContext* ctx);
    void _finalize(HttpRequest* req, StreamLoadContext* ctx, HandleFinishCallback cb);

private:
    ExecEnv* _exec_env;
    PriorityThreadPool _thread_pool;
};

}
