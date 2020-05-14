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

#include <brpc/closure_guard.h>
#include <brpc/uri.h>
#include <brpc/restful.h>
#include "http_service_demo.h"

namespace doris {

HttpServiceDemoImpl::HttpServiceDemoImpl() {}

HttpServiceDemoImpl::~HttpServiceDemoImpl() {}

void HttpServiceDemoImpl::Echo(google::protobuf::RpcController* cntl_base,
        const doris_demo::HttpRequest* /*request*/,
        doris_demo::HttpResponse* /*response*/,
        google::protobuf::Closure* done) {

        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
        LOG(INFO) << "execute echo for http request";
        // body是纯文本
        cntl->http_response().set_content_type("text/plain");

        // 把请求的query-string和body打印结果作为回复内容。
        butil::IOBufBuilder os;
        os << "queries:";
        for (brpc::URI::QueryIterator it = cntl->http_request().uri().QueryBegin();
            it != cntl->http_request().uri().QueryEnd(); ++it) {
            os << ' ' << it->first << '=' << it->second;
        }

        cntl->request_attachment().size();
        os << "\nbody: " << cntl->http_request().unresolved_path() << "\n" <<
        "size " << cntl->request_attachment().size() << "\n" <<
        cntl->request_attachment() << '\n';
        os.move_to(cntl->response_attachment());
    }
}
