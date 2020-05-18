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

#include <http/http_common.h>
#include <http/http_headers.h>
#include <util/url_coding.h>
#include "http_service_demo.h"

#include "runtime/stream_load/stream_load_context.h"


namespace doris {

HttpServiceDemoImpl::HttpServiceDemoImpl(ExecEnv *exec_env) :
    _exec_env(exec_env) {}

HttpServiceDemoImpl::~HttpServiceDemoImpl() {}

void HttpServiceDemoImpl::Echo(google::protobuf::RpcController* cntl_base,
        const doris_demo::HttpRequest* /*request*/,
        doris_demo::HttpResponse* /*response*/,
        google::protobuf::Closure* done) {

        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
        LOG(WARNING) << "execute echo for http request";
        // body是纯文本
        cntl->http_response().set_content_type("text/plain");
        std::shared_ptr<StreamLoadContext> ctx(new StreamLoadContext(_exec_env));

        ctx->load_type = TLoadType::MANUL_LOAD;
        ctx->load_src_type = TLoadSourceType::RAW;
        const std::string &unresolved_path =cntl->http_request().unresolved_path();
        std::string::size_type position = unresolved_path.find('/');
        if (position != unresolved_path.npos) {
            ctx->db = unresolved_path.substr(0, position);
            ctx->table = unresolved_path.substr(position + 1, unresolved_path.size() - position - 1);
        }
        if (cntl->http_request().GetHeader(HTTP_LABEL_KEY) != NULL) {
            ctx->label = *cntl->http_request().GetHeader(HTTP_LABEL_KEY);
        }

        if (ctx->label.empty()) {
            ctx->label = generate_uuid_string();
        }

        LOG(INFO) << "new income streaming load request." << ctx->brief()
              << ", db=" << ctx->db << ", tbl=" << ctx->table;

        // auth information
        if (!parse_basic_auth(cntl, &ctx->auth)) {
            LOG(WARNING) << "parse basic authorization failed." << ctx->brief();
            ctx->status =Status::InternalError("no valid Basic authorization");
        }


        // 把请求的query-string和body打印结果作为回复内容。
        butil::IOBufBuilder os;
        os << "queries:";
        for (brpc::URI::QueryIterator it = cntl->http_request().uri().QueryBegin();
            it != cntl->http_request().uri().QueryEnd(); ++it) {
            os << ' ' << it->first << '=' << it->second;
        }

        auto iter = cntl->http_request().HeaderBegin();
        while (iter != cntl->http_request().HeaderEnd()) {
            os << (*iter).first << " " << (*iter).second << " \n";
            iter++;
        }
        os << "\nbody: " << cntl->http_request().unresolved_path() << "\n" <<
        "size " << cntl->request_attachment().size() << "\n" <<
        "db " << ctx->db << " table " << ctx->table << " \n" <<
        "method " << cntl->http_request().method() << "\n" <<
        ctx->to_json() << ctx->auth.user << " " << ctx->auth.passwd << " " << ctx->auth.user_ip << '\n';
        os.move_to(cntl->response_attachment());
    }

bool HttpServiceDemoImpl::parse_basic_auth(brpc::HttpHeader &header, std::string* user, std::string* passwd) {
    const char k_basic[] = "Basic ";
    auto auth = header.GetHeader(HttpHeaders::AUTHORIZATION);
    if (auth == NULL || auth->compare(0, sizeof(k_basic) - 1, k_basic, sizeof(k_basic) - 1) != 0) {
        return false;
    }
    std::string encoded_str = auth->substr(sizeof(k_basic) - 1);
    std::string decoded_auth;
    if (!base64_decode(encoded_str, &decoded_auth)) {
        return false;
    }

    auto pos = decoded_auth.find(':');
    if (pos == std::string::npos) {
        return false;
    }

    user->assign(decoded_auth.c_str(), pos);
    passwd->assign(decoded_auth.c_str() + pos + 1);
    return true;
}

bool HttpServiceDemoImpl::parse_basic_auth(brpc::Controller* cntl, AuthInfo* auth) {
    std::string full_user;
    if (!parse_basic_auth(cntl->http_request(), &full_user, &auth->passwd)) {
        return false;
    }
    auto pos = full_user.find('@');
    if (pos != std::string::npos) {
        auth->user.assign(full_user.data(), pos);
        auth->cluster.assign(full_user.data() + pos + 1);
    } else {
        auth->user = full_user;
    }
    auth->user_ip.assign(butil::ip2str(cntl->remote_side().ip).c_str());

    return true;
}
}
