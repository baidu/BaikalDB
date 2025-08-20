#pragma once

#include "exchange_receiver_node.h"

namespace baikaldb {

class DataStreamManager {
public:
    static DataStreamManager* get_instance() {
        static DataStreamManager instance;
        return &instance;
    }
    ~DataStreamManager() {}
    std::shared_ptr<DataStreamReceiver> create_receiver(
        const uint64_t log_id, const uint64_t fragment_instance_id, const int32_t node_id,
        RuntimeState* state, ExchangeReceiverNode* node);
    std::shared_ptr<DataStreamReceiver> get_receiver(
        const uint64_t log_id, const uint64_t fragment_instance_id, const int32_t node_id);
    int remove_receiver(
        const uint64_t log_id, const uint64_t fragment_instance_id, const int32_t node_id);
    void cancel_receivers(const uint64_t log_id) {
        std::vector<std::string> receiver_str_vec;
        {
            BAIDU_SCOPED_LOCK(_mtx);
            for (const auto& [key, receiver] : _receiver_map) {
                if (key.log_id != log_id) {
                    continue;
                }
                if (receiver == nullptr) {
                    DB_FATAL("receiver is nullptr");
                    continue;
                }
                receiver_str_vec.emplace_back(receiver->to_string());
                receiver->set_cancelled();
            }
        }
        for (const auto& receiver_str : receiver_str_vec) {
            if (!receiver_str.empty()) {
                DB_WARNING("receiver_str: %s", receiver_str.c_str());
            }
        }
    }

private:
    DataStreamManager() {}
    struct ReceiverKey {
        uint64_t log_id = 0;
        uint64_t fragment_instance_id = 0;
        int32_t node_id = -1;
        bool operator==(const ReceiverKey& key) const {
            return log_id == key.log_id 
                        && fragment_instance_id == key.fragment_instance_id
                        && node_id == key.node_id;
        }
        std::string to_string() const {
            return std::to_string(log_id) + "_" 
                    + std::to_string(fragment_instance_id) + "_" 
                    + std::to_string(node_id);
        }
    };
    struct ReceiverHasher {
        size_t operator()(const ReceiverKey& key) const {
            return std::hash<int64_t>{}(key.log_id) ^ 
                   std::hash<uint64_t>{}(key.fragment_instance_id) ^ 
                   std::hash<int32_t>{}(key.node_id);
        }
    };
    bthread::Mutex _mtx;
    std::unordered_map<ReceiverKey, std::shared_ptr<DataStreamReceiver>, ReceiverHasher> _receiver_map;
};

} // namespace baikaldb