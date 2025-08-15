#include "literal.h"
#include "mysql_interact.h"

namespace baikaldb {

std::string Literal::to_sql(const std::unordered_map<int32_t, std::string>& slotid_fieldname_map, 
                   baikal::client::MysqlShortConnection* conn) {
    if (_node_type == pb::NULL_LITERAL) {
        return "NULL";
    }
    std::string res;
    if (_node_type == pb::STRING_LITERAL 
            || _node_type == pb::DATE_LITERAL 
            || _node_type == pb::DATETIME_LITERAL
            || _node_type == pb::TIME_LITERAL
            || _node_type == pb::TIMESTAMP_LITERAL) {
        res = "\"" + MysqlInteract::mysql_escape_string(conn, _value.get_string()) + "\"";
    } else {
        res = _value.get_string();
    }
    return res;
}

} // namespace baikaldb