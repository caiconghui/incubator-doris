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
#include "util/thrift_rpc_helper.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/FrontendService_types.h"
#include "gen_cpp/HeartbeatService_types.h"
#include "runtime/stream_load/stream_load_context.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/load_path_mgr.h"

namespace doris {

static TFileFormatType::type parse_format(const std::string& format_str) {
    if (boost::iequals(format_str, "CSV")) {
        return TFileFormatType::FORMAT_CSV_PLAIN;
    }
    return TFileFormatType::FORMAT_UNKNOWN;
}

static bool is_format_support_streaming(TFileFormatType::type format) {
    switch (format) {
        case TFileFormatType::FORMAT_CSV_PLAIN:
            return true;
        default:
            return false;
    }
}

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
        StreamLoadContext *ctx = new StreamLoadContext(_exec_env);
        ctx->ref();
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

        auto st = _on_header(cntl, ctx);
        if (!st.ok()) {
            ctx->status = st;
            if (ctx->need_rollback) {
                _exec_env->stream_load_executor()->rollback_txn(ctx);
                ctx->need_rollback = false;
            }
            if (ctx->body_sink.get() != nullptr) {
                ctx->body_sink->cancel();
            }
        }
        LOG(INFO) << "finish on header";
        int64_t begin_read_buffer = MonotonicNanos();
        if (st.ok()) {
            while (ctx->receive_bytes < cntl->request_attachment().size()) {
                LOG(WARNING) << "total size " << cntl->request_attachment().size();
                auto bb = ByteBuffer::allocate(4096);
                auto remove_bytes = cntl->request_attachment().copy_to_cstr(bb->ptr, bb->capacity - 1, ctx->receive_bytes);
                bb->pos = remove_bytes;
                bb->flip();
                auto st = ctx->body_sink->append(bb);
                if (!st.ok()) {
                    LOG(WARNING) << "append body content failed. errmsg=" << st.get_error_msg()
                                 << ctx->brief();
                    ctx->status = st;
                    return;
                }
                ctx->receive_bytes += remove_bytes;
                LOG(WARNING) << "copy remove bytes " << remove_bytes << " receive bytes " << ctx->receive_bytes << " total size " << cntl->request_attachment().size() ;
            }
        }
        ctx->read_buffer_cost_nanos = MonotonicNanos() - begin_read_buffer;
        LOG(INFO) << "finish on chunk";
        // status already set to fail
        if (ctx->status.ok()) {
            ctx->status = _handle(ctx);
            if (!ctx->status.ok() && ctx->status.code() != TStatusCode::PUBLISH_TIMEOUT) {
                LOG(WARNING) << "handle streaming load failed, id=" << ctx->id
                         << ", errmsg=" << ctx->status.get_error_msg();
            }
        }
        ctx->load_cost_nanos = MonotonicNanos() - ctx->start_nanos;

        if (!ctx->status.ok() && ctx->status.code() != TStatusCode::PUBLISH_TIMEOUT) {
            if (ctx->need_rollback) {
                _exec_env->stream_load_executor()->rollback_txn(ctx);
                ctx->need_rollback = false;
            }
            if (ctx->body_sink.get() != nullptr) {
                ctx->body_sink->cancel();
            }
        }
        LOG(INFO) << "publish finish";
        // 把请求的query-string和body打印结果作为回复内容。
        butil::IOBufBuilder os;
        for (brpc::URI::QueryIterator it = cntl->http_request().uri().QueryBegin();
            it != cntl->http_request().uri().QueryEnd(); ++it) {
            os << ' ' << it->first << '=' << it->second;
        }

        os << ctx->to_json();
        os.move_to(cntl->response_attachment());
        free_handler_ctx(ctx);
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

void HttpServiceDemoImpl::free_handler_ctx(void* param) {
    StreamLoadContext* ctx = (StreamLoadContext*) param;
    if (ctx == nullptr) {
        return;
    }
    // sender is going, make receiver know it
    if (ctx->body_sink != nullptr) {
        ctx->body_sink->cancel();
    }
    if (ctx->unref()) {
        LOG(INFO) << "free stream load context";
        delete ctx;
    }

}
Status HttpServiceDemoImpl::_on_header(brpc::Controller* cntl, StreamLoadContext* ctx) {
    // auth information
    if (!parse_basic_auth(cntl, &ctx->auth)) {
        LOG(WARNING) << "parse basic authorization failed." << ctx->brief();
        ctx->status =Status::InternalError("no valid Basic authorization");
    }

    // check content length
    ctx->body_bytes = 0;
    size_t max_body_bytes = config::streaming_load_max_mb * 1024 * 1024;
    if (cntl->http_request().GetHeader(HttpHeaders::CONTENT_LENGTH) != NULL && !cntl->http_request().GetHeader(HttpHeaders::CONTENT_LENGTH)->empty()) {
        ctx->body_bytes = std::stol(*cntl->http_request().GetHeader(HttpHeaders::CONTENT_LENGTH));
        if (ctx->body_bytes > max_body_bytes) {
            LOG(WARNING) << "body exceed max size." << ctx->brief();

            std::stringstream ss;
            ss << "body exceed max size, max_body_bytes=" << max_body_bytes;
            return Status::InternalError(ss.str());
        }
    }

    auto format = cntl->http_request().GetHeader(HTTP_FORMAT_KEY);
    // get format of this put
    if (format == NULL || format->empty()) {
        ctx->format = TFileFormatType::FORMAT_CSV_PLAIN;
    } else {
        ctx->format = parse_format(*format);
        if (ctx->format == TFileFormatType::FORMAT_UNKNOWN) {
            LOG(WARNING) << "unknown data format." << ctx->brief();
            std::stringstream ss;
            ss << "unknown data format, format=" << *format;
            return Status::InternalError(ss.str());
        }
    }

    auto http_timeout = cntl->http_request().GetHeader(HTTP_TIMEOUT);
    if (http_timeout != NULL && !http_timeout->empty()) {
        try {
            ctx->timeout_second = std::stoi(*http_timeout);
        } catch (const std::invalid_argument& e) {
            return Status::InvalidArgument("Invalid timeout format");
        }
    }

    int64_t start_begin_txn = MonotonicNanos();
    // begin transaction
    RETURN_IF_ERROR(_exec_env->stream_load_executor()->begin_txn(ctx));
    ctx->begin_txn_cost_nanos = MonotonicNanos() - start_begin_txn;
    // process put file
    return _process_put(cntl, ctx);
}

Status HttpServiceDemoImpl::_handle(StreamLoadContext* ctx) {
    if (ctx->body_bytes > 0 && ctx->receive_bytes != ctx->body_bytes) {
        LOG(WARNING) << "recevie body don't equal with body bytes, body_bytes="
                     << ctx->body_bytes << ", receive_bytes=" << ctx->receive_bytes
                     << ", id=" << ctx->id;
        return Status::InternalError("receive body dont't equal with body bytes");
    }
    if (!ctx->use_streaming) {
        // if we use non-streaming, we need to close file first,
        // then execute_plan_fragment here
        // this will close file
        ctx->body_sink.reset();
        RETURN_IF_ERROR(_exec_env->stream_load_executor()->execute_plan_fragment(ctx));
    } else {
        RETURN_IF_ERROR(ctx->body_sink->finish());
    }

    // wait stream load finish
    RETURN_IF_ERROR(ctx->future.get());
    int64_t begin_commit_and_publish = MonotonicNanos();
    // If put file succeess we need commit this load
    auto st = _exec_env->stream_load_executor()->commit_txn(ctx);
    ctx->commit_and_publish_cost_nanos = MonotonicNanos() - begin_commit_and_publish;
    return st;
}
Status HttpServiceDemoImpl::_data_saved_path(StreamLoadContext* ctx, std::string* file_path) {
    std::string prefix;
    RETURN_IF_ERROR(_exec_env->load_path_mgr()->allocate_dir(ctx->db, "", &prefix));
    timeval tv;
    gettimeofday(&tv, nullptr);
    struct tm tm;
    time_t cur_sec = tv.tv_sec;
    localtime_r(&cur_sec, &tm);
    char buf[64];
    strftime(buf, 64, "%Y%m%d%H%M%S", &tm);
    std::stringstream ss;
    ss << prefix << "/" << ctx->table << "." << buf << "." << tv.tv_usec;
    *file_path = ss.str();
    return Status::OK();
}
Status HttpServiceDemoImpl::_process_put(brpc::Controller* cntl, StreamLoadContext* ctx) {
    // Now we use stream
    ctx->use_streaming = is_format_support_streaming(ctx->format);

    // put request
    TStreamLoadPutRequest request;
    set_request_auth(&request, ctx->auth);
    request.db = ctx->db;
    request.tbl = ctx->table;
    request.txnId = ctx->txn_id;
    request.formatType = ctx->format;
    request.__set_loadId(ctx->id.to_thrift());
    if (ctx->use_streaming) {
        auto pipe = std::make_shared<StreamLoadPipe>();
        RETURN_IF_ERROR(_exec_env->load_stream_mgr()->put(ctx->id, pipe));
        request.fileType = TFileType::FILE_STREAM;
        ctx->body_sink = pipe;
    } else {
        RETURN_IF_ERROR(_data_saved_path(ctx, &request.path));
        auto file_sink = std::make_shared<MessageBodyFileSink>(request.path);
        RETURN_IF_ERROR(file_sink->open());
        request.__isset.path = true;
        request.fileType = TFileType::FILE_LOCAL;
        ctx->body_sink = file_sink;
    }
    auto& http_req = cntl->http_request();
    if (http_req.GetHeader(HTTP_COLUMNS) != NULL) {
        request.__set_columns(*http_req.GetHeader(HTTP_COLUMNS));
    }
    if (http_req.GetHeader(HTTP_WHERE) != NULL) {
        request.__set_where(*http_req.GetHeader(HTTP_WHERE));
    }
    if (http_req.GetHeader(HTTP_COLUMN_SEPARATOR) != NULL) {
        request.__set_columnSeparator(*http_req.GetHeader(HTTP_COLUMN_SEPARATOR));
    }

    if (http_req.GetHeader(HTTP_PARTITIONS) != NULL) {
        request.__set_partitions(*http_req.GetHeader(HTTP_PARTITIONS));
        request.__set_isTempPartition(false);
        if (http_req.GetHeader(HTTP_TEMP_PARTITIONS) != NULL) {
            return Status::InvalidArgument("Can not specify both partitions and temporary partitions");
        }
    }

    if (http_req.GetHeader(HTTP_TEMP_PARTITIONS) != NULL) {
        request.__set_partitions(*http_req.GetHeader(HTTP_TEMP_PARTITIONS));
        request.__set_isTempPartition(true);
        if (http_req.GetHeader(HTTP_PARTITIONS) != NULL) {
            return Status::InvalidArgument("Can not specify both partitions and temporary partitions");
        }
    }

    if (http_req.GetHeader(HTTP_NEGATIVE) != NULL && strcmp(http_req.GetHeader(HTTP_NEGATIVE)->c_str(), "true") == 0) {
        request.__set_negative(true);
    } else {
        request.__set_negative(false);
    }

    if (http_req.GetHeader(HTTP_STRICT_MODE) != NULL) {
        if (boost::iequals(*http_req.GetHeader(HTTP_STRICT_MODE), "false")) {
            request.__set_strictMode(false);
        } else if (boost::iequals(*http_req.GetHeader(HTTP_STRICT_MODE), "true")) {
            request.__set_strictMode(true);
        } else {
            return Status::InvalidArgument("Invalid strict mode format. Must be bool type");
        }
    }
    if (http_req.GetHeader(HTTP_TIMEZONE) != NULL) {
        request.__set_timezone(*http_req.GetHeader(HTTP_TIMEZONE));
    }
    if (http_req.GetHeader(HTTP_EXEC_MEM_LIMIT) != NULL) {
        try {
            request.__set_execMemLimit(std::stoll(*http_req.GetHeader((HTTP_EXEC_MEM_LIMIT))));
        } catch (const std::invalid_argument& e) {
            return Status::InvalidArgument("Invalid mem limit format");
        }
    }

    if (ctx->timeout_second != -1) {
        request.__set_timeout(ctx->timeout_second);
    }

    // plan this load
    TNetworkAddress master_addr = _exec_env->master_info()->network_address;
#ifndef BE_TEST
    if (http_req.GetHeader(HTTP_MAX_FILTER_RATIO) != NULL) {
        ctx->max_filter_ratio = strtod((*http_req.GetHeader(HTTP_MAX_FILTER_RATIO)).c_str(), nullptr);
    }
    int64_t begin_stream_load_put = MonotonicNanos();
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, ctx] (FrontendServiceConnection& client) {
                client->streamLoadPut(ctx->put_result, request);
            }));
#else
    ctx->put_result = k_stream_load_put_result;
#endif
    ctx->stream_load_put_cost_nanos = MonotonicNanos() - begin_stream_load_put;
    Status plan_status(ctx->put_result.status);
    if (!plan_status.ok()) {
        LOG(WARNING) << "plan streaming load failed. errmsg=" << plan_status.get_error_msg()
                     << ctx->brief();
        return plan_status;
    }

    // if we not use streaming, we must download total content before we begin
    // to process this load
    if (!ctx->use_streaming) {
        return Status::OK();
    }
    return _exec_env->stream_load_executor()->execute_plan_fragment(ctx);
}

}
