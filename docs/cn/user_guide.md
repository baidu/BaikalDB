# 常见问题

1. count(\*) vs count(1)：count(*)是经过优化的，性能是最好的。不过BaikalDB会自动把count(1)优化成count（\*）
2. a,b字段建联合索引，where b=3 能否用到索引？：不能用到，联合索引必须完全匹配前缀才能用到索引，例如where a=3 ；where a=3 and b=4;均可用到索引
3. 只查询几行数据，但是耗时很久？：第一先确认是否建立二级索引加速查询，如果有二级索引还很慢，可以尝试`use index(index_name)`来指定索引



# 索引类型

| 类型         | 中文     | 描述                                                         |
| ------------ | -------- | ------------------------------------------------------------ |
| I\_PRIMARY   | 主键索引 | 主键聚簇，可联合，不允许NULL                                 |
| I\_UNIQ      | 唯一索引 | 可多列，不可重复，不允许NULL                                 |
| I\_KEY       | 索引     | 可多列，可重复，不允许NULL                                   |
| I\_FULLTEXT  | 全文索引 | 只能单列，可选wordrank/wordseg/单字切词，只支持gbk，允许NULL |
| I\_RECOMMEND | 推荐索引 | 图片推荐专用索引，外部用户可忽略                             |

**与MySQL不同，BaikalDB天然顺序写，无需指定无意义的顺序id作为主键。一般情况使用联合主键会获得更好的性能。**

**由于主键聚簇，这么指定主键可以使相关数据物理上也聚集在一起，能够大幅提升性能。**

**好的案例（对于凤巢业务库举例）：**

* **plan表主键可设置为userid,planid。**
* **unit表主键可设置为userid，planid，unitid。**
* **wordinfo表主键可设置为userid，planid，unitid，winfoid。**
* **以此类推。**

# 自增列

BaikalDB一个表只允许指定一个自增，并且取消了索引限制，可以指定在任意的整数类型上。

insert时如果指定了自增列的值，并且这个值大于系统分配的最大值，则系统最大值会更新为此值。

由于是分布式系统，使用自增id分配需要网络开销，建议批量insert降低开销。

**不建议使用自增列，支持自增列只是为了和MySQL兼容。**

**不建议使用唯一的自增列作为主键，会导致写压力在最后一个节点，会影响分布式系统性能。**

**如果非要使用自增列，建议联合索引后缀列使用自增列，例如主键指定为userid,planid，planid可设置为自增列。**

# 数据类型

- NULL类型

NULL\_TYPE
基于实现性能考虑，除全文索引外，索引字段不允许为NULL

- 整数类型

| 类型     | 长度(字节) | 取值范围                      |
| ------ | ------ | ------------------------- |
| BOOL   | 1      | [false, true],[0,1]       |
| INT8   | 1      | [-128, 127]               |
| INT16  | 2      | [-32768, 32767]           |
| INT32  | 4      | [-2147483648, 2147483647] |
| INT64  | 8      | [-2^63, 2^63-1]           |
| UINT8  | 1      | [0, 255]                  |
| UINT16 | 2      | [0, 65535]                |
| UINT32 | 4      | [0, 4294967295]           |
| UINT64 | 8      | [0, 2^64-1]               |

- 浮点数

| 类型     | 长度(字节) |                         |
| ------ | ------ | ----------------------- |
| FLOAT  | 4      | -3.40E+38 ~ +3.40E+38   |
| DOUBLE | 8      | -1.79E+308 ~ +1.79E+308 |

- 字符串

STRING 长度限制64M，使用尽量不要超过1M

* HyperLogLog类型

HLL固定占用1k存储，直接使用impala的实现（HyperLogLog in Practice (paper from google with some improvements)）。HyperLogLog算法是用来估算UV(COUNT DISTINCT ），因此有一定的误差率。HLL相关的函数：hll_add，hll_merge，hll_estimate。聚合函数：hll_add_agg，hll_merge_agg。

- 日期时间

| 类型      | 长度(字节) | 描述                                                         |
| --------- | ---------- | ------------------------------------------------------------ |
| DATETIME  | 8          | [1000-01-01 00:00:00.000000, 9999-12-31 00:00:00.000000] 精确到us |
| TIMESTAMP | 4          | [1970-01-01 00:00:01, 2038-01-19 03:14:07] 精确到s           |
| DATE      | 4          | [1000-01-01, 9999-12-31]精确到天                             |
| TIME      | 4          | [-838:59:59,838:59:59]精确到s                                |

# 字面常量

* 数字字面常量

整型类型（TINYINT, SMALLINT, INT, 和BIGINT）的字面常量是一系列数字，这些数字前可以加些0。

浮点类型（DOUBLE）的字面常量是一系列数字，并且可选加上十进制的点（.字符）。

整型类型在需要时可以根据上下文提升到浮点类型。

在描述字面常量时，可以使用指数符号（e 字符）。例如1e+6 表示10 的6 次方（1 百万）。包含指数符号的字面常量会被识别成浮点类型。

* 字符串字面常量

字符串字面常量被单引号或者双引号括起来。字符串字面常量也包含其他形式：字符串字面常量为包含单引号的字符串，外面被双引号括起来；字符串字面常量为包含双引号的字符串，外面被单引号括起来。

为了描述字符串字面常量的特殊字符，需要在特殊字符前加入转义字符（\字符）。

\t 表示tab键

\n 表示换行符

\r 表示回车符

\b 表示回退符

\0 表示ASCII码的空字符（和SQL语言的NULL不同）

\Z 表示dos 的文本结束符

\%和\_用来转义传给LIKE操作符的字符串中的通配符

\\防止反斜线符号被解释成转义字符

如果字符串字面常量被单引号或双引号括起来，则反斜线符号可以用来转义该字符串字面常量中出现的单引号或双引号。

如果\后面出现的字符不是上面列举的转移字符，则该字符保持不变，不会被转义。

* 日期字面常量

BaikalDB会自动将STRING类型字面常量转成时间类型字面常量。BaikalDB接受的时间类型字面常量的输入格式为成YYYY-MM-DD HH:MM:SS.ssssss，或者只包含日期。其中上述格式中小数点后面的数字(微秒数)可带可不带。例如，用户可以指定时间类型为"2010-01-01"，"2010-01-01 10:10:10"，或者"2010-01-01  10:10:10.112112"。

# SQL操作符

目前支持的操作符如下表,计算规则同MySQL 

| 操作符     | 符号                     | 描述                                                    |
| ---------- | ------------------------ | ------------------------------------------------------- |
| 算术运算   | `+-*/`                   | 特殊的除以0返回NULL，+-可以单目                         |
| 位运算     | `<<>>~|^&`               | 同C语言                                                 |
| 逻辑运算   | `and or not && || !`     | 可多列，可重复，允许NULL                                |
| 关系运算符 | `  < = != >= <= between` | between等价于&gt;= &amp;&amp; &lt;=，!=可以写作&lt;&gt; |
| 其他运算符 | `like，is null，in`      | 特殊的，对全文索引列做like，会进行全文检索              |
| 条件运算符 | `case when else`         | 同MySQL，条件选择时候用                                 |

# SQL语句

BaikalDB支持与MySQL类似的语法，具体用法参考MySQL

- show databases/show tables/show create table/show full columns
- alter table add/drop/rename column/alter table rename table [as|to] 
- rename table old to new
- create table
  - create table支持default value
- use db
- select
  - 支持where、group by、order by、having、limit，语法同MySQL
  - join：left join，inner join，right join，只支持等值join
  - 支持use index (index_name)来指定索引
  - 子查询功能正在开发中

即将支持union/union all，子查询

- insert族
  - insert：
  - replace：覆盖已存在的数据
  - insert ignore：忽略已存在的数据
  - insert … on duplicate update：已存在的数据使用update功能，update子句中可使用values(col)函数指定前面的数据
- update
- delete

其他语法正在支持中

# 聚合函数

当查询指定使用GROUP BY从句时，则每个group by的值都会返回1条结果。不使用 GROUP BY 子句时只返回一条结果。

| 函数            | 返回类型                                | 描述                                       |
| ------------- | ----------------------------------- | ---------------------------------------- |
| AVG           | double                              | 该聚合函数返回集合中的平均数。该函数只有1个参数，该参数可以是数字类型的列，返回值是数字的函数，或者计算结果是数字的表达式。包含NULL值的行将被忽略。如果该表是空的或者AVG 的参数都是NULL，则该函数返回NULL。 |
| COUNT         | INT64                               | COUNT(\*) 会计算包含NULL 值的行。COUNT(column\_name)仅会计算非NULL值的行。COUNT(distinct col\_name...)会先对数据去重，然后再计算多个列的组合出现的次数。 |
| MAX           | 和输入参数相同的类型                          | 该聚合函数返回集合中的最大值。该函数只有1个参数，该参数可以是数字类型的列，返回值是数字的函数，或者计算结果是数字的表达式。包含NULL值的行将被忽略。如果该表是空的或者MAX的参数都是NULL，则该函数返回NULL。 |
| MIN           | 和输入参数相同的类型                          | 该聚合函数返回集合中的最小值。该函数只有1个参数，该参数可以是数字类型的列，返回值是数字的函数，或者计算结果是数字的表达式。包含NULL 值的行将被忽略。如果该表是空的或者MIN 的参数都是NULL，则该函数返回NULL。 |
| SUM           | 如果参数整型，则返回INT64，如果参数是浮点型则返回double类型 | 聚合函数返回集合中所有值的和。该函数只有1个参数，该参数可以是数字类型的列，返回值是数字的函数，或者计算结果是数字的表达式。包含NULL值的行将被忽略。如果该表是空的或者MIN的参数都是NULL，则该函数返回NULL。 |
| hll_add_agg   | HLL                                 | 对普通元素进行HLL估算，可以视为低消耗版本的count distinct    |
| hll_merge_agg | HLL                                 | 对表格中存储的HLL类型进行聚合，比如存储着小时级的HLL，可以聚合出天级别的HLL |

# 内置函数

* 字符串函数

| 函数               | 返回类型   | 描述                                    |
| ---------------- | ------ | ------------------------------------- |
| length           | UINT32 | 返回STRING长度                            |
| lower            | STRING | STRING转为小写并返回                         |
| upper            | STRING | STRING转为大写并返回                         |
| concat           | STRING | 该函数可以接受任意个参数，返回拼接后的字符串，参数有NULL则返回NULL |
| substr/substring | STRING | 获取子串，下标从1开始，同MySQL                    |
| left             | STRING | 获取左边N个字符                              |
| right            | STRING | 获取右边N个字符                              |

* 日期时间函数

| 函数           | 返回类型  | 描述                                                         |
| -------------- | --------- | ------------------------------------------------------------ |
| now            | DATETIME  | 返回当前时间，精确到us                                       |
| unix_timestamp | UINT32    | 若无参数则返回当前的时间戳，也可接收一个时间类型，返回对应的时间戳 |
| from_unixtime  | TIMESTAMP | 给定一个int32的时间戳，返回内部TIMESTAMP类型`2018-01-01 23:11:11` |
| date_format    | STRING    | 目前同strftime http://www.runoob.com/cprogramming/c-function-strftime.html |
| timestampdiff  | INT64     | 返回两个时间差，详见MySQL，不过单位只支持SECOND，MINUT，HOUR，DAY |
| timediff       | TIME      | 返回两个时间的差，详见MySQL                                  |

- HyperLogLog函数

| 函数           | 返回类型  | 描述                                       |
| ------------ | ----- | ---------------------------------------- |
| hll_add      | HLL   | hll_add(hll, other)，给原HLL加一个元素，返回新的HLL。hll为NULL时会初始化一个hll |
| hll_merge    | HLL   | hll_merge(hll1，hll2)，两个hll合并成一个hll，返回合并后的HLL。hll为NULL时会初始化一个hll |
| hll_estimate | INT64 | hll_estimate(hll),对hll求估算值。特殊的，select 返回一个HLL类型没有意义，因此会自动进行一次hll_estimate来返回估算值。hll为NULL时返回NULL |



逐步实现中
​        