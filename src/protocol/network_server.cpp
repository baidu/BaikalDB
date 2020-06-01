// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "network_server.h"
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <fcntl.h>
#include <unistd.h>
#include "physical_planner.h"
#include "transaction_manager_node.h"
#include "log.h"
#include <gflags/gflags.h>
#include <time.h>

namespace bthread {
DECLARE_int32(bthread_concurrency); //bthread.cpp
}

namespace baikaldb {

DEFINE_int32(backlog, 1024, "Size of waitting queue in listen()");
DEFINE_int32(baikal_port, 28282, "Server port");
DEFINE_int32(epoll_timeout, 2000, "Epoll wait timeout in epoll_wait().");
DEFINE_int32(check_interval, 10, "interval for conn idle timeout");
DEFINE_int32(connect_idle_timeout_s, 1800, "connection idle timeout threshold (second)");
DEFINE_int32(slow_query_timeout_s, 60, "slow query threshold (second)");
DEFINE_int32(baikal_heartbeat_interval_us, 10 * 1000 * 1000, "baikal_heartbeat_interval(us)");
DEFINE_int32(print_agg_sql_interval_s, 10, "print_agg_sql_interval_s");
DEFINE_bool(fetch_instance_id, false, "fetch baikaldb instace id, used for generate transaction id");
DEFINE_string(recovery_db_path, "./db", "db path for transaction recovery, default: ./db");
DEFINE_string(hostname, "HOSTNAME", "matrix instance name");

static const std::string instance_table_name = "INTERNAL.baikaldb.__baikaldb_instance";
uint8_t NetworkServer::transaction_prefix = 0x01;

// 1. read meta_db to get prepared (yet not committed) transactions left by last baikaldb instance
// 2. try to commit transaction until success
// 3. remove the transaction entry from the meta_db
void NetworkServer::recovery_transactions() {
    rocksdb::ReadOptions read_options;
    read_options.total_order_seek = false;
    read_options.prefix_same_as_start = true;
    std::unique_ptr<rocksdb::Iterator> iter(_meta_db->new_iterator(read_options, _meta_handle));
    MutTableKey txn_key;
    txn_key.append_u8(transaction_prefix);
    for (iter->Seek(txn_key.data()); iter->Valid(); iter->Next()) {
        TableKey key(iter->key(), true);
        int pos = 1;
        uint64_t txn_id = key.extract_u64(pos);
        uint64_t instance = txn_id >> 40;
        if (instance == _instance_id) {
            continue;
        }
        DB_WARNING("recover txn_id: %lu", txn_id);
        pb::CachePlan commit_plan;
        if (!commit_plan.ParseFromArray(iter->value().data(), iter->value().size())) {
            DB_FATAL("TransactionError: parse recovery plan failed, txn_id: %lu", txn_id);
            continue;
        }
        TransactionManagerNode::remove_commit_log_entry(txn_id);
        SmartSocket dummy_client = SmartSocket(new (std::nothrow)NetworkSocket);
        dummy_client->txn_id = txn_id;
        dummy_client->seq_id = commit_plan.seq_id();
        for (auto& region_info : commit_plan.regions()) {
            dummy_client->region_infos[region_info.region_id()] = region_info;
        }
        RuntimeState state;
        state.set_client_conn(dummy_client.get());
        state.txn_id = txn_id;
        ExecNode* commit_root = nullptr;
        ExecNode::create_tree(commit_plan.plan(), &commit_root);
        TransactionManagerNode commit_manager_node;
        commit_manager_node.set_op_type(pb::OP_COMMIT);
        int ret = commit_manager_node.exec_commit_node(&state, commit_root);
        delete commit_root;
        if (ret < 0) {
            DB_FATAL("TransactionError: execute recovery plan failed, txn_id: %lu", txn_id);
            continue;
        }
        DB_WARNING("TransactionNote: execute recovery plan, txn_id: %lu", txn_id);
    }
}

static void request_log(pb::BaikalHeartBeatRequest& request) {
    static int reduce_log = 0;
    if (reduce_log++ % 10 != 0) {
        return;
    }
    for (auto& schema_info : request.schema_infos()) {
        SELF_TRACE("heartbeat request(table version): table_id: %ld, table_version:%ld", 
                schema_info.table_id(), schema_info.version());
        std::string region_version;
        int count = 0;
        for (auto& region_heart : schema_info.regions()) {
            region_version += region_heart.ShortDebugString() + ", ";
            ++count;
            if (count % 50 == 0) {
                SELF_TRACE("heartbeat request(region version): %s", region_version.c_str());
                region_version.clear();
            }
        }
        if (!region_version.empty()) {
            SELF_TRACE("heartbeat request(region version): %s", region_version.c_str());
        }
    }
}

static void response_log(pb::BaikalHeartBeatResponse& response) {
    static int reduce_log = 0;
    if (reduce_log++ % 10 != 0) {
        return;
    }
    int count = 0;
    std::string str_response;
    for (auto& schema_change_info : response.schema_change_info()) {
        str_response += schema_change_info.ShortDebugString() + ", ";
        ++count;
        if (count % 5 == 0) {
            SELF_TRACE("heartbeat response(schema version):%s", str_response.c_str());
            str_response.clear();
        }
    }
    if (!str_response.empty()) {
        SELF_TRACE("heartbeat response(schema version):%s", str_response.c_str());
    }
    for (auto& region_change_info : response.region_change_info()) {
        SELF_TRACE("heartbeat response(region info):%s", 
                region_change_info.ShortDebugString().c_str());
    }
    for (auto& privilege_change_info : response.privilege_change_info()) {
        SELF_TRACE("heartbeat response(privilege info):%s", 
                privilege_change_info.ShortDebugString().c_str());
    }
    if (response.has_idc_info()) {
        SELF_TRACE("heartbeat response(idc_info):%s",
                response.idc_info().ShortDebugString().c_str());
    }
}

void NetworkServer::report_heart_beat() {
    while (!_shutdown) {
        pb::BaikalHeartBeatRequest request;
        pb::BaikalHeartBeatResponse response;
        //1、construct heartbeat request
        construct_heart_beat_request(request);
        request_log(request);
        //2、send heartbeat request to meta server
        if (MetaServerInteract::get_instance()->send_request("baikal_heartbeat", request, response) == 0) {
            //处理心跳
            process_heart_beat_response(response);
            response_log(response);
        } else {
            DB_WARNING("send heart beat request to meta server fail");
        }
        bthread_usleep_fast_shutdown(FLAGS_baikal_heartbeat_interval_us, _shutdown);
    }
}

void NetworkServer::print_agg_sql() {
    while (!_shutdown) {
        BvarMap sample = StateMachine::get_instance()->sql_agg_cost.reset();
        SchemaFactory* factory = SchemaFactory::get_instance();
        time_t timep;
        struct tm tm;
        time(&timep);
        localtime_r(&timep, &tm);
        
        for (auto& pair : sample.internal_map) {
            if (!pair.first.empty()) {
                for (auto& pair2 : pair.second) {
                    uint64_t out[2];
                    int64_t version;
                    std::string description;
                    factory->get_schema_conf_op_info(pair2.second.table_id, version, description);
                    butil::MurmurHash3_x64_128(pair.first.c_str(), pair.first.size(), 0x1234, out);
                    SQL_TRACE("date_hour_min=[%04d-%02d-%02d\t%02d\t%02d] sum_pv_avg_affected_scan_filter=[%ld\t%ld\t%ld\t%ld\t%ld\t%ld] sign_hostname_index=[%llu\t%s\t%s] sql_agg: %s "
                        "op_version_desc=[%ld\t%s]", 
                        1900 + tm.tm_year, 1 + tm.tm_mon, tm.tm_mday, tm.tm_hour, tm.tm_min,
                        pair2.second.sum, pair2.second.count,
                        pair2.second.count == 0 ? 0 : pair2.second.sum / pair2.second.count,
                        pair2.second.affected_rows, pair2.second.scan_rows, pair2.second.filter_rows,
                        out[0], FLAGS_hostname.c_str(), factory->get_index_name(pair2.first).c_str(), pair.first.c_str(),  
                        version, description.c_str());
                }
            }
        }
        bthread_usleep_fast_shutdown(FLAGS_print_agg_sql_interval_s * 1000 * 1000LL, _shutdown);
    }
}

void NetworkServer::construct_heart_beat_request(pb::BaikalHeartBeatRequest& request) {
    SchemaFactory* factory = SchemaFactory::get_instance();
    auto schema_read_recallback = [&request, factory](const SchemaMapping& schema){
        auto& table_statistics_mapping = schema.table_statistics_mapping;
        for (auto& info_pair : schema.table_info_mapping) {
            if (info_pair.second->engine != pb::ROCKSDB &&
                    info_pair.second->engine != pb::ROCKSDB_CSTORE) {
                continue;
            }
            //主键索引和全局二级索引都需要传递region信息
            for (auto& index_id : info_pair.second->indices) {
                auto& index_info_mapping = schema.index_info_mapping;
                if (index_info_mapping.count(index_id) == 0) {
                    continue;
                }
                IndexInfo index = *index_info_mapping.at(index_id);
                //主键索引
                if (index.type != pb::I_PRIMARY && !index.is_global) {
                    continue;
                }
                auto req_info = request.add_schema_infos();
                req_info->set_table_id(index_id);
                req_info->set_version(1);
                int64_t version = 0;
                auto iter = table_statistics_mapping.find(index_id);
                if (iter != table_statistics_mapping.end()) {
                    version = iter->second->version();
                }
                req_info->set_statis_version(version);

                if (index_id == info_pair.second->id) {
                    req_info->set_version(info_pair.second->version);
                } 
                std::map<int64_t, pb::RegionInfo> region_infos;
                //
                // TODO：读多个double buffer，可能死锁？
                //
                factory->get_region_by_key(index, NULL, region_infos);
                for (auto& pair : region_infos) {
                    auto region = req_info->add_regions();
                    auto& region_info = pair.second;
                    region->set_region_id(region_info.region_id());
                    region->set_version(region_info.version());
                    region->set_conf_version(region_info.conf_version());
                }
            }
        }
    };
    request.set_last_updated_index(factory->last_updated_index());
    factory->schema_info_scope_read(schema_read_recallback);
    
}

void NetworkServer::process_heart_beat_response(const pb::BaikalHeartBeatResponse& response) {
    SchemaFactory* factory = SchemaFactory::get_instance();
    for (auto& info : response.schema_change_info()) {
        factory->update_table(info);
    }
    factory->update_regions(response.region_change_info());
    if (response.has_idc_info()) {
        factory->update_idc(response.idc_info());
    }
    for (auto& info : response.privilege_change_info()) {
        factory->update_user(info);
    }
    factory->update_show_db(response.db_info());
    if (response.statistics().size() > 0) {
        factory->update_statistics(response.statistics());
    }
    if (response.has_last_updated_index() && 
        response.last_updated_index() > factory->last_updated_index()) {
        factory->set_last_updated_index(response.last_updated_index());
    }
}

void NetworkServer::process_heart_beat_response_sync(const pb::BaikalHeartBeatResponse& response) {
    TimeCost cost;
    SchemaFactory* factory = SchemaFactory::get_instance();
    factory->update_tables_double_buffer_sync(response.schema_change_info());

    if (response.has_idc_info()) {
        factory->update_idc(response.idc_info());
    }
    for (auto& info : response.privilege_change_info()) {
        factory->update_user(info);
    }
    factory->update_show_db(response.db_info());
    if (response.statistics().size() > 0) {
        factory->update_statistics(response.statistics());
    }

    factory->update_regions_double_buffer_sync(response.region_change_info());
    if (response.has_last_updated_index() && 
        response.last_updated_index() > factory->last_updated_index()) {
        factory->set_last_updated_index(response.last_updated_index());
    }
    DB_NOTICE("sync time:%ld", cost.get_time());
}

void NetworkServer::connection_timeout_check() {
    auto check_func = [this]() {
        time_t time_now = time(NULL);
        if (time_now == (time_t)-1) {
            DB_WARNING("get current time failed.");
            return;
        }
        if (_epoll_info == NULL) {
            DB_WARNING("_epoll_info not initialized yet.");
            return;
        }

        for (int32_t idx = 0; idx < CONFIG_MPL_EPOLL_MAX_SIZE; ++idx) {
            SmartSocket sock = _epoll_info->get_fd_mapping(idx);
            if (sock == NULL || sock->is_free || sock->fd == -1) {
                continue;
            }

            // 处理客户端Hang住的情况，server端没有发送handshake包或者auth_result包
            timeval current;
            gettimeofday(&current, NULL);
            int64_t diff_us = (current.tv_sec - sock->connect_time.tv_sec) * 1000000
                + (current.tv_usec - sock->connect_time.tv_usec);
            if (!sock->is_authed && diff_us >= 1000000) {
                // 待现有工作处理完成，需要获取锁
                if (sock->mutex.try_lock() == false) {
                    continue;
                }
                if (sock->is_free || sock->fd == -1) {
                    DB_WARNING("sock is already free.");
                    sock->mutex.unlock();
                    continue;
                }
                DB_WARNING("close un_authed connection [fd=%d][ip=%s][port=%d].",
                        sock->fd, sock->ip.c_str(), sock->port);
                sock->shutdown = true;
                MachineDriver::get_instance()->dispatch(sock, _epoll_info,
                        sock->shutdown || _shutdown);
                continue;
            }
            time_now = time(NULL);
            if (sock->query_ctx != nullptr && 
                    sock->query_ctx->mysql_cmd != COM_SLEEP) {
                int query_time_diff = time_now - sock->query_ctx->stat_info.start_stamp.tv_sec;
                if (query_time_diff > FLAGS_slow_query_timeout_s) {
                    DB_NOTICE("query is slow, [cost=%d][fd=%d][ip=%s:%d][now=%ld][active=%ld][user=%s][log_id=%lu][sql=%s]",
                            query_time_diff, sock->fd, sock->ip.c_str(), sock->port,
                            time_now, sock->last_active,
                            sock->user_info->username.c_str(),
                            sock->query_ctx->stat_info.log_id,
                            sock->query_ctx->sql.c_str());
                    continue;
                }
            }
            // 处理连接空闲时间过长的情况，踢掉空闲连接
            double diff = difftime(time_now, sock->last_active);
            if ((int32_t)diff < FLAGS_connect_idle_timeout_s) {
                continue;
            }
            // 待现有工作处理完成，需要获取锁
            if (sock->mutex.try_lock() == false) {
                continue;
            }
            if (sock->is_free || sock->fd == -1) {
                DB_WARNING("sock is already free.");
                sock->mutex.unlock();
                continue;
            }
            DB_NOTICE("close idle connection [fd=%d][ip=%s:%d][now=%ld][active=%ld][user=%s]",
                    sock->fd, sock->ip.c_str(), sock->port,
                    time_now, sock->last_active,
                    sock->user_info->username.c_str());
            sock->shutdown = true;
            MachineDriver::get_instance()->dispatch(sock, _epoll_info,
                    sock->shutdown || _shutdown);
        }
    };
    while (!_shutdown) {
        check_func();
        bthread_usleep_fast_shutdown(FLAGS_check_interval * 1000 * 1000LL, _shutdown);
    }
}

// Gracefully shutdown.
void NetworkServer::graceful_shutdown() {
    _shutdown = true;
}

NetworkServer::NetworkServer():
        _is_init(false),
        _shutdown(false),
        _epoll_info(NULL) {
}

NetworkServer::~NetworkServer() {
    // Free epoll info.
    if (_epoll_info != NULL) {
        delete _epoll_info;
        _epoll_info = NULL;
    }
}

int NetworkServer::fetch_instance_info() {
    SchemaFactory* factory = SchemaFactory::get_instance();
    int64_t instance_tableid = -1;
    if (0 != factory->get_table_id(instance_table_name, instance_tableid)) {
        DB_WARNING("unknown instance table: %s", instance_table_name.c_str());
        return -1;
    }

    // 请求meta来获取自增id
    pb::MetaManagerRequest request;
    pb::MetaManagerResponse response;
    request.set_op_type(pb::OP_GEN_ID_FOR_AUTO_INCREMENT);
    auto auto_increment_ptr = request.mutable_auto_increment();
    auto_increment_ptr->set_table_id(instance_tableid);
    auto_increment_ptr->set_count(1);
    if (MetaServerInteract::get_instance()->send_request("meta_manager", 
                                                          request, 
                                                          response) != 0) {
        DB_FATAL("fetch_instance_info from meta_server fail");
        return -1;
    }
    if (response.start_id() + 1 != response.end_id()) {
        DB_FATAL("gen id count not equal to 1");
        return -1;
    }
    _instance_id = response.start_id();
    DB_NOTICE("baikaldb instance_id: %lu", _instance_id);
    return 0;
}

bool NetworkServer::init() {
    // init val 
    _driver_thread_num = bthread::FLAGS_bthread_concurrency;
    TimeCost cost;
    // 先把meta数据都获取到
    pb::BaikalHeartBeatRequest request;
    pb::BaikalHeartBeatResponse response;
    //1、构造心跳请求
    construct_heart_beat_request(request);
    //2、发送请求
    if (MetaServerInteract::get_instance()->send_request("baikal_heartbeat", request, response) == 0) {
        //处理心跳
        process_heart_beat_response_sync(response);
        //DB_WARNING("req:%s  \nres:%s", request.DebugString().c_str(), response.DebugString().c_str());
    } else {
        DB_FATAL("send heart beat request to meta server fail");
        return false;
    }
    DB_NOTICE("sync time2:%ld", cost.get_time());
    if (FLAGS_fetch_instance_id) {
        if (fetch_instance_info() != 0) {
            return false;
        }        
    }
    DB_WARNING("get instance_id: %lu", _instance_id);

    _meta_db = RocksWrapper::get_instance();
    if (!_meta_db) {
        DB_FATAL("create meta database failed");
        return false;
    }
    int32_t res = _meta_db->init(FLAGS_recovery_db_path);
    if (res != 0) {
        DB_FATAL("rocksdb init failed: code:%d", res);
        return false;
    }
    _meta_handle = _meta_db->get_meta_info_handle();
    _is_init = true;
    return true;
}

void NetworkServer::stop() {
    _heartbeat_bth.join();

    if (_epoll_info == nullptr) {
        DB_WARNING("_epoll_info not initialized yet.");
        return;
    }
    for (int32_t idx = 0; idx < CONFIG_MPL_EPOLL_MAX_SIZE; ++idx) {
        SmartSocket sock = _epoll_info->get_fd_mapping(idx);
        if (!sock) {
            continue;
        }
        if (sock == nullptr || sock->fd == 0) {
            continue;
        }

        // 待现有工作处理完成，需要获取锁
        if (sock->mutex.try_lock()) {
            sock->shutdown = true;
            MachineDriver::get_instance()->dispatch(sock, _epoll_info, true, false);
        }
    }
    return;
}

bool NetworkServer::start() {
    if (!_is_init) {
        DB_FATAL("Network server is not initail.");
        return false;
    }
    if (0 != make_worker_process()) {
        DB_FATAL("Start event loop failed.");
        return false;
    }
    return true;
}

SmartSocket NetworkServer::create_listen_socket() {
    // Fetch a socket.
    SocketFactory* socket_pool = SocketFactory::get_instance();
    SmartSocket sock = socket_pool->create(SERVER_SOCKET);
    if (sock == NULL) {
        DB_FATAL("Failed to fetch socket from poll.type:[%u]", SERVER_SOCKET);
        return SmartSocket();
    }
    // Bind.
    int val = 1;
    if (setsockopt(sock->fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val)) != 0) {
        DB_FATAL("setsockopt fail");
        return SmartSocket();
    }
    struct sockaddr_in listen_addr;
    listen_addr.sin_family = AF_INET;
    listen_addr.sin_addr.s_addr = INADDR_ANY;
    listen_addr.sin_port = htons(FLAGS_baikal_port);
    if (0 > bind(sock->fd, (struct sockaddr *) &listen_addr, sizeof(listen_addr))) {
        DB_FATAL("bind() errno=%d, error=%s", errno, strerror(errno));
        return SmartSocket();
    }

    // Listen.
    if (0 > listen(sock->fd, FLAGS_backlog)) {
        DB_FATAL("listen() failed fd=%d, bakclog=%d, errno=%d, error=%s",
                sock->fd, FLAGS_backlog, errno, strerror(errno));
        return SmartSocket();
    }
    sock->shutdown = false;
    // Set socket attribute.
    if (!set_fd_flags(sock->fd)) {
        DB_FATAL("create listen socket but set fd flags error.");
        return SmartSocket();
    }
    return sock;
}

int NetworkServer::make_worker_process() {
    _last_time.resize(_driver_thread_num);
    if (MachineDriver::get_instance()->init(_driver_thread_num, _last_time) != 0) {
        DB_FATAL("Failed to init machine driver.");
        exit(-1);
    }
    _conn_check_bth.run([this]() {connection_timeout_check();});
    _heartbeat_bth.run([this]() {report_heart_beat();});
    _recover_bth.run([this]() {recovery_transactions();});
    _agg_sql_bth.run([this]() {print_agg_sql();});

    // Create listen socket.
    _service = create_listen_socket();
    if (_service == nullptr) {
        DB_FATAL("Failed to create listen socket.");
        return -1;
    }

    // Initail epoll info.
    _epoll_info = new EpollInfo();
    if (!_epoll_info->init()) {
        DB_FATAL("initial epoll info failed.");
        return -1;
    }
    if (!_epoll_info->poll_events_add(_service, EPOLLIN)) {
        DB_FATAL("poll_events_add add socket[%d] error", _service->fd);
        return -1;
    }
    // Process epoll events.
    int listen_fd = _service->fd;
    SocketFactory* socket_pool = SocketFactory::get_instance();
    while (!_shutdown) {
        int fd_cnt = _epoll_info->wait(FLAGS_epoll_timeout);
        if (_shutdown) {
            // Delete event from epoll.
            _epoll_info->poll_events_delete(_service);
        }

        for (int cnt = 0; cnt < fd_cnt; ++cnt) {
            int fd = _epoll_info->get_ready_fd(cnt);
            int event = _epoll_info->get_ready_events(cnt);

            // New connection.
            if (!_shutdown && listen_fd == fd) {
                // Accept and check new client socket.
                struct sockaddr_in client_addr;
                socklen_t client_len = sizeof(client_addr);
                int client_fd = accept(fd, (struct sockaddr*)&client_addr, &client_len);
                if (client_fd <= 0) {
                    DB_WARNING("Wrong fd:[%d] errno:%d", client_fd, errno);
                    continue;
                }
                if (client_fd >= CONFIG_MPL_EPOLL_MAX_SIZE) {
                    DB_WARNING("Wrong fd.fd=%d >= CONFIG_MENU_EPOLL_MAX_SIZE", client_fd);
                    close(client_fd);
                    continue;
                }
                // Set flags of client socket.
                if (!set_fd_flags(client_fd)) {
                    DB_WARNING("client_fd=%d set_fd_flags error close(client)", client_fd);
                    close(client_fd);
                    continue;
                }
                // Create NetworkSocket for new client socket.
                SmartSocket client_socket = socket_pool->create(CLIENT_SOCKET);
                if (client_socket == NULL) {
                    DB_WARNING("Failed to create NetworkSocket from pool.fd:[%d]", client_fd);
                    close(client_fd);
                    continue;
                }

                // Set attribute of client socket.
                char *ip_address = inet_ntoa(client_addr.sin_addr);
                if (NULL != ip_address) {
                    client_socket->ip = ip_address;
                }
                client_socket->fd = client_fd;
                client_socket->state = STATE_CONNECTED_CLIENT;
                client_socket->port = ntohs(client_addr.sin_port);
                client_socket->addr = client_addr;
                client_socket->server_instance_id = _instance_id;

                // Set socket mapping and event.
                if (!_epoll_info->set_fd_mapping(client_socket->fd, client_socket)) {
                    DB_FATAL("Failed to set fd mapping.");
                    return -1;
                }
                _epoll_info->poll_events_add(client_socket, 0);

                // New connection will be handled immediately.
                fd = client_fd;
                //DB_NOTICE("Accept new connect [ip=%s, port=%d, client_fd=%d]",
                DB_WARNING("Accept new connect [ip=%s, port=%d, client_fd=%d]",
                        ip_address,
                        client_socket->port,
                        client_socket->fd);
            }

            // Check if socket in fd_mapping or not.
            SmartSocket sock = _epoll_info->get_fd_mapping(fd);
            if (sock == NULL) {
                DB_DEBUG("Can't find fd in fd_mapping, fd:[%d], listen_fd:[%d], fd_cnt:[%d]",
                            fd, listen_fd, cnt);
                continue;
            }
            if (fd != sock->fd) {
                DB_WARNING("current [fd=%d][sock_fd=%d]"
                    "[event=%d][fd_cnt=%d][state=%s]",
                    fd,
                    sock->fd,
                    event,
                    fd_cnt,
                    state2str(sock).c_str());
                continue;
            }
            sock->last_active = time(NULL);

            // Check socket event.
            // EPOLLHUP: closed by client. because of protocol of sending package is wrong.
            if (event & EPOLLHUP || event & EPOLLERR) {
                if (sock->socket_type == CLIENT_SOCKET) {
                    if ((event & EPOLLHUP) && sock->shutdown == false) {
                        DB_WARNING("CLIENT EPOLL event is EPOLLHUP, fd=%d event=0x%x",
                                        fd, event);
                    } else if ((event & EPOLLERR) && sock->shutdown == false) {
                        DB_WARNING("CLIENT EPOLL event is EPOLLERR, fd=%d event=0x%x",
                                        fd, event);
                    }
                } else {
                    DB_WARNING("socket type is wrong, fd %d event=0x%x", fd, event);
                }
                sock->shutdown = true;
            }

            // Handle client socket event by status machine.
            if (sock->socket_type == CLIENT_SOCKET) {
                // the socket has just connected, no need to require lock
                if (sock->mutex.try_lock() == false) {
                    continue;
                }
                if (sock->is_free || sock->fd == -1) {
                    DB_WARNING("sock is already free.");
                    sock->mutex.unlock();
                    continue;
                }
                // close the socket event on epoll when the sock is being process
                // and reopen it when finish process
                _epoll_info->poll_events_mod(sock, 0);
                MachineDriver::get_instance()->dispatch(sock, _epoll_info,
                    sock->shutdown || _shutdown);
            } else {
                DB_WARNING("unknown network socket type[%d].", sock->socket_type);
            }
        }
    }
    DB_NOTICE("Baikal instance exit.");
    return 0;
}

std::string NetworkServer::state2str(SmartSocket client) {
    switch (client->state) {
    case STATE_CONNECTED_CLIENT: {
        return "STATE_CONNECTED_CLIENT";
    }
    case STATE_SEND_HANDSHAKE: {
        return "STATE_SEND_HANDSHAKE";
    }
    case STATE_READ_AUTH: {
        return "STATE_READ_AUTH";
    }
    case STATE_SEND_AUTH_RESULT: {
        return "STATE_SEND_AUTH_RESULT";
    }
    case STATE_READ_QUERY_RESULT: {
        return "STATE_READ_QUERY_RESULT";
    }
    case STATE_ERROR_REUSE: {
        return "STATE_ERROR_REUSE";
    }
    case STATE_ERROR: {
        return "STATE_ERROR";
    }
    default: {
        return "unknown state";
    }
    }
    return "unknown state";
}

bool NetworkServer::set_fd_flags(int fd) {
    if (fd < 0) {
        DB_FATAL("wrong fd:[%d].", fd);
        return false;
    }
    int opts = fcntl(fd, F_GETFL);
    if (opts < 0) {
        DB_FATAL("set_fd_flags() fd=%d fcntl(fd, F_GETFL) error", fd);
        return false;
    }
    opts = opts | O_NONBLOCK;
    if (fcntl(fd, F_SETFL, opts) < 0) {
        DB_FATAL("set_fd_flags() fd=%d fcntl(fd, F_SETFL, opts) error", fd);
        return false;
    }
    struct linger li;
    li.l_onoff = 1;
    li.l_linger = 0;

    int ret = setsockopt(fd, SOL_SOCKET, SO_LINGER, (const char*) &li, sizeof(li));
    if (ret != 0) {
        DB_FATAL("set_fd_flags() fd=%d setsockopt linger error", fd);
        return false;
    }
    int var = 1;
    ret = setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &var, sizeof(var));
    if (ret != 0) {
        DB_FATAL("set_fd_flags() fd=%d setsockopt tcp_nodelay error", fd);
        return false;
    }
    return true;
}

} // namespace baikal
