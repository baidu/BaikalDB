# 索引选择的规则

### 主键的选择

BaikalDB内部是有分片的，而且分片是按照主键范围划分。主键是聚簇索引。（注：聚簇索引的顺序就是数据的物理存储顺序，而对非聚簇索引的索引顺序与数据物理排列顺序无关。）

除了主键外，**BaikalDB都是local index**。local index：索引数据与主表数据存储在一起，并且跟随主表的分片来分布。与此相对的，global index：索引数据与主表数据不存储在一起，索引数据有自己的分片规则。

举个例子：

* 一个表的单主键是id，id范围是0-100，那么可能的分片是[0,20),[20,80),[80,∞)
* 一个表的联合主键是userid（0-100）,planid（0-10000），可能的分片是[0-0,30-1000),[30-1000,80-3000),[80-3000,80-7000)，[80-7000,∞)。其中80号用户是大客户，可能分散在多个分片中可并行计算。

主键的选择影响了数据的分片和物理分布，合理的主键选择可以使得相关数据物理上也聚集在一起，能够大幅提升性能，**主键的选择是最重要的**。

好的案例（对于凤巢业务库举例）：

- **plan表主键可设置为userid,planid。**
- **unit表主键可设置为userid，planid，unitid。**
- **wordinfo表主键可设置为userid，planid，unitid，winfoid。**

上面的主键选择，可以让同一个小客户的全部数据在物理上也聚集在一起。并且对于大客户来说，会自动进行分裂成多个分片，可以并行计算。



那么对于一条sql，BaikalDB是如何选择正确的分片的呢？

答案是：**where条件中只有命中了主键前缀，才能选择到合适的分片。否则就是广播查找！**

[0-0,30-1000),[30-1000,80-3000),[80-3000,80-7000)，[80-7000,∞)，以这些分片举例，where useid=15，系统能选到0分片。where userid=80，系统会选到1,2,3分片。where userid=80 and planid=5000，系统会选到2分片。

注意：索引的选择与分片的选择无关，不是说选择到了分片就一定会用主键索引的。上述分片，如果还有个unique索引unitid，where userid=15 and unitid=700；分片可以选择到0，索引可以选择到unitid。



### 索引的选择

目前BaikalDB没有实现代价模型，因此所有的索引选择都是基于规则的：

1. 如果有FULLTEXT则最优先匹配
2. 如果order by可以用到索引，则优先使用该索引
3. 全部命中比部分命中优先
4. 优先级：主键 > 唯一 > 普通



如果发现BaikalDB查询很慢，可以尝试用hint：use index(index_name)来强制指定索引。示例：`select * from b use index(primary) where id > 5 and x = 3;`

联合索引的建立**顺序很重要**，遵循最左前缀匹配原则：只有前缀相等的情况，才会使用下一级索引（范围匹配也不行）。

举例：

1. 索引(a,b)，比如where b = 2；则不符合最左前缀原则，无法使用该索引。

2. 索引(a,b,c,d)，比如where a = 1 and b = 2 and c > 3 and d = 4；则d用不到索引。如果索引是(a,b,d,c)，则可以用到全部索引列。


# 索引类型

| 类型         | 中文     | 描述                                                         |
| ------------ | -------- | ------------------------------------------------------------ |
| I\_PRIMARY   | 主键索引 | 主键聚簇，可联合，不允许NULL                                 |
| I\_UNIQ      | 唯一索引 | 可多列，不可重复，不允许NULL                                 |
| I\_KEY       | 索引     | 可多列，可重复，不允许NULL                                   |
| I\_FULLTEXT  | 全文索引 | 只能单列，可选wordrank/wordseg/单字切词，只支持gbk，允许NULL |
| I\_RECOMMEND | 推荐索引 | 图片推荐专用索引，外部用户可忽略                             |

**与MySQL不同，BaikalDB天然顺序写，无需指定无意义的顺序id作为主键。一般情况使用联合主键会获得更好的性能。**



# 自增列

BaikalDB一个表只允许指定一个自增列，并且取消了索引限制，可以指定在任意的整数类型列上。

insert时如果指定了自增列的值，并且这个值大于系统分配的最大值，则系统最大值会更新为此值。

由于是分布式系统，使用自增id分配**需要网络开销**，建议批量insert降低开销。



不建议使用自增列，支持自增列只是为了和MySQL兼容。

不建议使用唯一的自增列作为主键，会导致写压力在最后一个节点，会影响分布式系统性能。

如果非要使用自增列，建议联合索引**后缀列**使用自增列，例如主键指定为userid,planid，planid可设置为自增列。



# 举例说明

`CREATE TABLE OCPC_API.ocpc_convert_stat (`
  `userid bigint(20) NOT NULL COMMENT '用户id',`
  `clktype tinyint(1) NOT NULL DEFAULT '0' COMMENT '点击类型: 1凤巢无线; 2凤巢PC, 3feed',`
  `time bigint(20) NOT NULL COMMENT '时间yyyyMMddHHmm',`
  `clk int(10) NOT NULL DEFAULT '0' COMMENT '点击数',`
  `cv int(10) NOT NULL DEFAULT '0' COMMENT '转化数',`
  `addcv int(10) NOT NULL DEFAULT '0' COMMENT '回传转化数',`
  `ctime datetime NOT NULL COMMENT '新建时间',`
  `mtime datetime NOT NULL COMMENT '修改时间',`
  `PRIMARY KEY (userid, clktype, time),`
  `key index_userid_time (userid, time),`
  `KEY index_time (time)`
`) ENGINE=Rocksdb DEFAULT CHARSET=gbk AVG_ROW_LENGTH=100 COMMENT='{"resource_tag":"e0-nj", "namespace":"FENGCHAO"}';`

如上表所示

对于where userid=3 and clktype = 5 and time >= 11; where userid=3 and clktype = 5; where userid=3;这些条件均可以用到主键索引。并且还会根据主键前缀来选择分片。

对于where userid=3 and time<222;可以用到index_userid_time 索引。并且可以根据useid字段来选择合适的分片。

对于where time = 333；则可以用到index_time 索引，但是由于没有主键前缀条件，因此选择不到分片，会把请求发送给全部分片处理。

另外，对于userid比较少，每个userid数据量比较多的情况，也就是userid区分度不高，则`key index_userid_time (userid, time)`索引意义不大，如果没有这个索引，则where userid=3 and time<222;可以选择到index_time索引，并且可以根据userid=3 选择到合适分片。

