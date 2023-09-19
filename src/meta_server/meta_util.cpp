#include "meta_util.h"

namespace baikaldb {

namespace partition_utils {

DEFINE_int32(max_dynamic_partition_attribute_end, 500, "max_dynamic_partition_attribute_end");

int64_t compare(const pb::Expr& left, const pb::Expr& right) {
    if (left.nodes_size() == 0 || right.nodes_size() == 0) {
        return -1;
    }
    ExprValue left_value;
    if (get_partition_value(left, left_value) != 0) {
        return -1;
    }
    ExprValue right_value;
    if (get_partition_value(right, right_value) != 0) {
        return -1;
    }
    left_value.cast_to(left.nodes(0).col_type());
    right_value.cast_to(right.nodes(0).col_type());
    return left_value.compare(right_value);
}

pb::Expr min(const pb::Expr& left, const pb::Expr& right) {
    if (compare(left, right) <= 0) {
        return left;
    }
    return right;
}

pb::Expr max(const pb::Expr& left, const pb::Expr& right) {
    if (compare(left, right) <= 0) {
        return right;
    }
    return left;
}

bool is_equal(const pb::RangePartitionInfo& left, const pb::RangePartitionInfo& right) {
    if (!left.has_range() || !right.has_range()) {
        return false;
    }
    if (compare(left.range().left_value(), right.range().left_value()) == 0 &&
            compare(left.range().right_value(), right.range().right_value()) == 0) {
        return true;
    }
    return false;
}

int create_partition_expr(
        const pb::PrimitiveType col_type, const std::string& str_val, pb::Expr& expr) {
    pb::ExprNode* p_expr_node = expr.add_nodes();
    if (p_expr_node == nullptr) {
        DB_WARNING("p_expr_node is nullptr");
        return -1;
    }
    p_expr_node->set_num_children(0);
    p_expr_node->set_node_type(pb::STRING_LITERAL);
    p_expr_node->set_col_type(col_type);
    p_expr_node->mutable_derive_node()->set_string_val(str_val);
    return 0;
}

// lower_or_upper_bound: false表示分区下界，true表示分区上界
int check_partition_expr(const pb::Expr& expr, bool lower_or_upper_bound) {
    if (expr.nodes_size() != 1) {
        DB_WARNING("expr has invalid num nodes, %d", (int)expr.nodes_size());
        return -1;
    }
    if (expr.nodes(0).num_children() != 0) {
        DB_WARNING("expr has invalid num_children, %d", expr.nodes(0).num_children());
        return -1;
    }
    if (expr.nodes(0).node_type() == pb::MAXVALUE_LITERAL) {
        return 0;
    }
    if (expr.nodes(0).node_type() != pb::STRING_LITERAL && expr.nodes(0).node_type() != pb::INT_LITERAL) {
        DB_WARNING("expr has invalid node_type, %d", (int)expr.nodes(0).node_type());
        return -1;
    }
    // 判断是否在合法区间
    pb::PrimitiveType partition_col_type = expr.nodes(0).col_type();
    std::string partition_min_str;
    std::string partition_max_str;
    if (get_min_partition_value(partition_col_type, partition_min_str) != 0) {
        DB_WARNING("Fail to get_min_partition_value, type: %d", partition_col_type);
        return -1;
    }
    if (get_max_partition_value(partition_col_type, partition_max_str) != 0) {
        DB_WARNING("Fail to get_min_partition_value, type: %d", partition_col_type);
        return -1;
    }
    pb::Expr partition_min_expr;
    pb::Expr partition_max_expr;
    if (create_partition_expr(partition_col_type, partition_min_str, partition_min_expr) != 0) {
        DB_WARNING("Fail to create_partition_expr");
        return -1;
    }
    if (create_partition_expr(partition_col_type, partition_max_str, partition_max_expr) != 0) {
        DB_WARNING("Fail to create_partition_expr");
        return -1;
    }
    // 范围的下界需要小于最大值
    if (!lower_or_upper_bound) {
        if(compare(expr, partition_min_expr) < 0 || compare(expr, partition_max_expr) >= 0) {
            DB_WARNING("Expr not in valid range, bound: %d, expr: %s, in: %s, max: %s", 
                        lower_or_upper_bound, expr.ShortDebugString().c_str(), 
                        partition_min_expr.ShortDebugString().c_str(), partition_max_expr.ShortDebugString().c_str());
            return -1;
        }
    }
    // 范围的上界需要大于最小值
    if (lower_or_upper_bound) {
        if(compare(expr, partition_min_expr) <= 0 || compare(expr, partition_max_expr) > 0) {
            DB_WARNING("Expr not in valid range, bound: %d, expr: %s, min: %s, max: %s", 
                        lower_or_upper_bound, expr.ShortDebugString().c_str(), 
                        partition_min_expr.ShortDebugString().c_str(), partition_max_expr.ShortDebugString().c_str());
            return -1;
        }
    }
    return 0;
}

int get_partition_value(const pb::Expr& expr, ExprValue& value) {
    if (expr.nodes_size() != 1) {
        return -1;
    }
    ExprNode* p_partition_node = nullptr;
    if (ExprNode::create_expr_node(expr.nodes(0), &p_partition_node) != 0) {
        return -1;
    }
    value = p_partition_node->get_value(nullptr);
    delete p_partition_node;
    return 0;
}

int get_min_partition_value(const pb::PrimitiveType col_type, std::string& str_val) {
    switch (col_type) {
    case pb::DATE:
        str_val = MIN_DATE;
        break;
    case pb::DATETIME:
        str_val = MIN_DATETIME;
        break;
    case pb::INT8:
        str_val = std::to_string(std::numeric_limits<int8_t>::min());
        break;
    case pb::INT16:
        str_val = std::to_string(std::numeric_limits<int16_t>::min());
        break;
    case pb::INT32:
        str_val = std::to_string(std::numeric_limits<int32_t>::min());
        break;
    case pb::INT64:
        str_val = std::to_string(std::numeric_limits<int64_t>::min());
        break;
    case pb::UINT8:
        str_val = std::to_string(std::numeric_limits<uint8_t>::min());
        break;
    case pb::UINT16:
        str_val = std::to_string(std::numeric_limits<uint16_t>::min());
        break;
    case pb::UINT32:
        str_val = std::to_string(std::numeric_limits<uint32_t>::min());
        break;
    case pb::UINT64:
        str_val = std::to_string(std::numeric_limits<uint64_t>::min());
        break;
    default:
        return -1;
    }
    return 0;
}

int get_max_partition_value(const pb::PrimitiveType col_type, std::string& str_val) {
    switch (col_type) {
    case pb::DATE:
        str_val = MAX_DATE;
        break;
    case pb::DATETIME:
        str_val = MAX_DATETIME;
        break;
    case pb::INT8:
        str_val = std::to_string(std::numeric_limits<int8_t>::max());
        break;
    case pb::INT16:
        str_val = std::to_string(std::numeric_limits<int16_t>::max());
        break;
    case pb::INT32:
        str_val = std::to_string(std::numeric_limits<int32_t>::max());
        break;
    case pb::INT64:
        str_val = std::to_string(std::numeric_limits<int64_t>::max());
        break;
    case pb::UINT8:
        str_val = std::to_string(std::numeric_limits<uint8_t>::max());
        break;
    case pb::UINT16:
        str_val = std::to_string(std::numeric_limits<uint16_t>::max());
        break;
    case pb::UINT32:
        str_val = std::to_string(std::numeric_limits<uint32_t>::max());
        break;
    case pb::UINT64:
        str_val = std::to_string(std::numeric_limits<uint64_t>::max());
        break;
    default:
        return -1;
    }
    return 0;
}

int create_dynamic_range_partition_info(
        const std::string& partition_prefix, const pb::PrimitiveType partition_col_type, 
        const time_t current_ts, const int32_t offset, const TimeUnit time_unit, 
        pb::RangePartitionInfo& range_partition_info) {
    time_t left_ts;
    time_t right_ts;
    std::string left_value_str;
    std::string right_value_str;
    
    // Partition range left/right expr
    get_specified_timestamp(current_ts, offset, time_unit, left_ts);
    get_specified_timestamp(current_ts, offset + 1, time_unit, right_ts);
    if (partition_col_type == pb::DATE) {
        timestamp_to_format_str(left_ts, DATE_FORMAT, left_value_str);
        timestamp_to_format_str(right_ts, DATE_FORMAT, right_value_str);
    } else {
        timestamp_to_format_str(left_ts, DATETIME_FORMAT, left_value_str);
        timestamp_to_format_str(right_ts, DATETIME_FORMAT, right_value_str);
    }
    pb::PartitionRange* p_range = range_partition_info.mutable_range();
    if (p_range == nullptr) {
        DB_WARNING("p_range is nullptr");
        return -1;
    }
    pb::Expr* p_left_value = p_range->mutable_left_value();
    if (p_left_value == nullptr) {
        DB_WARNING("p_left_value is nullptr");
        return -1;
    }
    pb::Expr* p_right_value = p_range->mutable_right_value();
    if (p_left_value == nullptr) {
        DB_WARNING("p_left_value is nullptr");
        return -1;
    }
    create_partition_expr(partition_col_type, left_value_str, *p_left_value);
    create_partition_expr(partition_col_type, right_value_str, *p_right_value);

    // Partition name
    std::string partition_name = partition_prefix;
    std::string format_str;
    if (time_unit == TimeUnit::DAY) {
        timestamp_to_format_str(left_ts, DAY_FORMAT, format_str);
        partition_name += format_str;
    } else {
        timestamp_to_format_str(left_ts, MONTH_FORMAT, format_str);
        partition_name += format_str;
    }
    range_partition_info.set_partition_name(partition_name);

    return 0;
}

bool check_range_partition_info(const pb::RangePartitionInfo& range_partition_info) {
    if (range_partition_info.has_less_value()) {
        if (check_partition_expr(range_partition_info.less_value(), true) != 0) {
            DB_WARNING("Fail to check partition less value");
            return false;
        }
    }
    if (range_partition_info.has_range()) {
        if (check_partition_expr(range_partition_info.range().left_value(), false) != 0) {
            DB_WARNING("Fail to check partition range left value");
            return false;
        }
        if (check_partition_expr(range_partition_info.range().right_value(), true) != 0) {
            DB_WARNING("Fail to check partition range right value");
            return false;
        }
        if (compare(range_partition_info.range().left_value(), range_partition_info.range().right_value()) >= 0) {
            DB_WARNING("range_partition_info range left_value >= right_value");
            return false;
        }
    }
    return true;
}

bool check_dynamic_partition_attr(
        const pb::DynamicPartitionAttr& dynamic_partition_attr) {
    if (!dynamic_partition_attr.enable()) {
        return true;
    }
    if (!dynamic_partition_attr.has_time_unit() ||
            (!boost::algorithm::iequals(dynamic_partition_attr.time_unit(), "DAY") && 
             !boost::algorithm::iequals(dynamic_partition_attr.time_unit(), "MONTH"))) {
        DB_WARNING("Dynamic partition miss 'time_unit' attribute or has invalid 'time_unit' attribute");
        return false;
    }
    if (dynamic_partition_attr.has_start() && dynamic_partition_attr.start() >= 0) {
        DB_WARNING("Dynamic partition invalid 'start' attribute");
        return false;
    }
    if (dynamic_partition_attr.has_cold() && dynamic_partition_attr.cold() >= 0) {
        DB_WARNING("Dynamic partition invalid 'cold' attribute");
        return false;
    }
    if (!dynamic_partition_attr.has_end() || dynamic_partition_attr.end() < 0 || 
            dynamic_partition_attr.end() > FLAGS_max_dynamic_partition_attribute_end) {
        DB_WARNING("Dynamic partition miss 'end' attribute or has invalid 'end' attribute");
        return false;
    }
    if (dynamic_partition_attr.has_start_day_of_month() && 
            !(dynamic_partition_attr.start_day_of_month() >= MIN_START_DAY_OF_MONTH && 
              dynamic_partition_attr.start_day_of_month() <= MAX_START_DAY_OF_MONTH)) {
        DB_WARNING("Dynamic partition invalid 'start_day_of_month' attribute, %d", 
                                            dynamic_partition_attr.start_day_of_month());
        return false;
    }
    return true;
}

// 两个区间左端点的较大值「小于」两个区间右端点的较小值，则区间重叠
bool check_partition_overlapped(
        const std::vector<pb::RangePartitionInfo>& partitions_vec, 
        const pb::RangePartitionInfo& partition) {
    const pb::Expr& new_left_value = partition.range().left_value();
    const pb::Expr& new_right_value = partition.range().right_value();
    for (auto& range_partition_info : partitions_vec) {
        if (range_partition_info.partition_name() == partition.partition_name()) {
            // 跳过自身
            continue;
        }
        if (!is_specified_partition(range_partition_info, partition)) {
            continue;
        }
        const pb::Expr& left_value = range_partition_info.range().left_value();
        const pb::Expr& right_value = range_partition_info.range().right_value();
        if (compare(max(left_value, new_left_value), min(right_value, new_right_value)) < 0) {
            return true;
        }
    }
    return false;
}

// 两个区间左端点的较大值「小于」两个区间右端点的较小值，则区间重叠
bool check_partition_overlapped(
        const ::google::protobuf::RepeatedPtrField<pb::RangePartitionInfo>& partitions_vec, 
        const pb::RangePartitionInfo& partition) {
    const pb::Expr& new_left_value = partition.range().left_value();
    const pb::Expr& new_right_value = partition.range().right_value();
    for (auto& range_partition_info : partitions_vec) {
        if (range_partition_info.partition_name() == partition.partition_name()) {
            // 跳过自身
            continue;
        }
        if (!is_specified_partition(range_partition_info, partition)) {
            continue;
        }
        const pb::Expr& left_value = range_partition_info.range().left_value();
        const pb::Expr& right_value = range_partition_info.range().right_value();
        if (compare(max(left_value, new_left_value), min(right_value, new_right_value)) < 0) {
            return true;
        }
    }
    return false;
}

// 获取「小于等于」指定时间的分区
int get_specifed_partitions(
        const pb::Expr& specified_expr,
        const ::google::protobuf::RepeatedPtrField<pb::RangePartitionInfo>& partitions_vec,
        ::google::protobuf::RepeatedPtrField<pb::RangePartitionInfo>& specified_partitions_vec,
        bool is_cold) {
    for (const auto& range_partition_info : partitions_vec) {
        if (!range_partition_info.has_range()) {
            continue;
        }
        if (!range_partition_info.range().has_right_value()) {
            continue;
        }
        // 获取冷区间时，如果区间已经为冷则跳过
        if (is_cold && range_partition_info.is_cold()) {
            continue;
        }
        if (compare(range_partition_info.range().right_value(), specified_expr) <= 0) {
            pb::RangePartitionInfo new_range_partition_info;
            new_range_partition_info.CopyFrom(range_partition_info);
            pb::RangePartitionInfo* p_range_partition_info = specified_partitions_vec.Add();
            if (p_range_partition_info == nullptr) {
                DB_WARNING("p_range_partition_info is nullptr");
                continue;
            }
            p_range_partition_info->Swap(&new_range_partition_info);
        }
    }
    return 0;
}

// 将less_value转换成range
int convert_to_partition_range(
        const pb::PrimitiveType partition_col_type, 
        pb::RangePartitionInfo& range_partition_info) {
    if (!range_partition_info.has_less_value()) {
        return 0;
    }
    if (range_partition_info.mutable_range() == nullptr) {
        DB_WARNING("range_partition_info mutable_range is nullptr");
        return -1;
    }
    pb::Expr* p_left_value = range_partition_info.mutable_range()->mutable_left_value();
    if (p_left_value == nullptr) {
        DB_WARNING("p_left_value is nullptr");
        return -1;
    }
    std::string partition_str_val;
    if (get_min_partition_value(partition_col_type, partition_str_val) != 0) {
        DB_WARNING("Fail to get_min_partition_value");
        return -1;
    }
    if (create_partition_expr(partition_col_type, partition_str_val, *p_left_value) != 0) {
        DB_WARNING("Fail to create_partition_expr");
        return -1;
    }
    pb::Expr* p_right_value = range_partition_info.mutable_range()->mutable_right_value();
    if (p_right_value == nullptr) {
        DB_WARNING("p_right_value is nullptr");
        return -1;
    }
    p_right_value->Swap(range_partition_info.mutable_less_value());
    range_partition_info.clear_less_value();
    return 0;
}

// 将partition value的类型设置为分区列类型
int set_partition_col_type(
        const pb::PrimitiveType partition_col_type,
        pb::RangePartitionInfo& range_partition_info) {
    if (range_partition_info.has_less_value()) {
        if (range_partition_info.mutable_less_value() == nullptr || 
                range_partition_info.mutable_less_value()->nodes_size() != 1 || 
                range_partition_info.mutable_less_value()->mutable_nodes(0) == nullptr) {
            DB_WARNING("p_range_partition_info mutable_less_value is nullptr");
            return -1;
        }
        if (range_partition_info.less_value().nodes(0).node_type() != pb::MAXVALUE_LITERAL) {
            range_partition_info.mutable_less_value()->mutable_nodes(0)->set_col_type(partition_col_type);
        }
    }
    if (range_partition_info.has_range()) {
        if (range_partition_info.mutable_range() == nullptr) {
            DB_WARNING("range_partition_info mutable_range is nullptr");
            return -1;
        }
        if (range_partition_info.mutable_range()->mutable_left_value() == nullptr ||
                range_partition_info.mutable_range()->mutable_left_value()->nodes_size() != 1 || 
                range_partition_info.mutable_range()->mutable_left_value()->mutable_nodes(0) == nullptr) {
            DB_WARNING("range_partition_info range mutable_left_value is nullptr");
            return -1;
        }
        if (range_partition_info.mutable_range()->mutable_right_value() == nullptr ||
                range_partition_info.mutable_range()->mutable_right_value()->nodes_size() != 1 || 
                range_partition_info.mutable_range()->mutable_right_value()->mutable_nodes(0) == nullptr) {
            DB_WARNING("range_partition_info range mutable_right_value is nullptr");
            return -1;
        }
        if (range_partition_info.range().left_value().nodes(0).node_type() != pb::MAXVALUE_LITERAL) {
            range_partition_info.mutable_range()->mutable_left_value()->mutable_nodes(0)->set_col_type(partition_col_type);
        }
        if (range_partition_info.range().right_value().nodes(0).node_type() != pb::MAXVALUE_LITERAL) {
            range_partition_info.mutable_range()->mutable_right_value()->mutable_nodes(0)->set_col_type(partition_col_type);
        }
    }
    return 0;
}

int get_partition_range(
        const pb::RangePartitionInfo& range_partition_info,
        std::pair<std::string, std::string>& range) {
    ExprValue left_value;
    if (get_partition_value(range_partition_info.range().left_value(), left_value) != 0) {
        DB_WARNING("Fail to get_partition_range, %s", range_partition_info.ShortDebugString().c_str());
        return -1;
    }
    ExprValue right_value;
    if (get_partition_value(range_partition_info.range().right_value(), right_value) != 0) {
        DB_WARNING("Fail to get_partition_range, %s", range_partition_info.ShortDebugString().c_str());
        return -1;
    }
    range = std::make_pair(left_value.get_string(), right_value.get_string());
    return 0;
}

} // namespace partition_utils

} // namespace baikaldb