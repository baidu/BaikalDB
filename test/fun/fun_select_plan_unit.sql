### select_plan_unit功能测试 ###

# join
SELECT unitinfo.unitid,planinfo.pcpricefactor,planinfo.targeturl,planinfo.mpricefactor FROM Baikaltest.unitinfo as unitinfo JOIN Baikaltest.planinfo as planinfo ON planinfo.userid = unitinfo.userid  WHERE planinfo.userid = 630152  AND unitinfo.userid = 630152 ;
SELECT unitinfo.unitid,planinfo.pcpricefactor FROM Baikaltest.unitinfo as unitinfo left JOIN Baikaltest.planinfo as planinfo ON planinfo.userid = unitinfo.userid  WHERE planinfo.userid = 630152  AND unitinfo.userid = 630152 ;
# between...and... + group by + having
select userid,count(*) as count  from Baikaltest.planinfo where modtime between '2020-03-25 00:04:34' and '2020-03-26 05:29:11' group by userid having  count > 3;
# between...and... + count
select count(*)  from Baikaltest.planinfo where modtime between '2020-03-25 00:04:34' and '2020-03-26 05:29:11';
# between...and... + int
select userid from Baikaltest.planinfo where modtime between '2020-03-25 00:04:34' and '2020-03-26 05:29:11';
# group by + count
select modtime,count(*) from Baikaltest.planinfo group by modtime;
# group by + int
select userid,count(*) from Baikaltest.planinfo group by userid;
# limit + int
select userid from Baikaltest.unitinfo limit 1,10;
# limit + *
select * from Baikaltest.unitinfo limit 1,10;
# count
select count(*) from Baikaltest.planinfo where userid=630152;
select count(*) from Baikaltest.unitinfo where userid=630152;
# group by + bool
select rstat,count(*)  from Baikaltest.unitinfo group by rstat;
# in
select * from Baikaltest.unitinfo where userid in (630152);
# match...against...
# 初始化数据address2
insert into Baikaltest.t_student2(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(1,'zhangsan1','zhangsan2',10,11,100,101,'zhangsanaddress1','zhangsanaddress2|lisiaddress2|wangwuaddress2',1000,1001);
# select *  from Baikaltest.t_student2 where  MATCH (address2) Against ('zhangsan') and age1 in (10,11) ORDER BY id DESC LIMIT 0,5;
# like
select *  from Baikaltest.t_student2 where address2 like "%zhangsan%";
# not like
select *  from Baikaltest.t_student2 where address2 not like "%zhangsan%";
select *  from Baikaltest.t_student2 where address2 not like "%zhangsang%";
# case...when...
update Baikaltest.t_student2 set class1=case class1 when 100 then 1001 else 1009 end;
select *  from Baikaltest.t_student2;
update Baikaltest.t_student2 set class1=case class1 when 1001 then 100 else 1010 end;
select *  from Baikaltest.t_student2;
# explain 语句
explain select * from Baikaltest.ideainfo where planid<8;

################# 倒排索引——共8种类型，正常的case全部覆盖了 #################
# 倒排索引1=S_WORDRANK_Q2B_ICASE
select * from Baikaltest.ideainfo where ideaname like "%ideaname%" ;
# 倒排索引1=S_WORDRANK_Q2B_ICASE | gbk
select * from Baikaltest.ideainfo where ideaname like "%开屏创意%" ;

# 倒排索引2=S_WORDSEG_BASIC
select * from Baikaltest.t_student2 where address2 like "%lisiaddress%";
# 倒排索引2=S_WORDSEG_BASIC | gbk
select * from Baikaltest.t_student2 where address2 like "%李四地址%";
# 倒排索引2=S_WORDSEG_BASIC
update Baikaltest.ideainfo set riskscore = "test测试";
select * from Baikaltest.ideainfo where riskscore like "%test%";
# 倒排索引2=S_WORDSEG_BASIC | gbk
select * from Baikaltest.ideainfo where riskscore like "%测试%";

# 倒排索引3=S_WORDRANK
select * from Baikaltest.unitinfo where unitname like "%uname%";
# 倒排索引3=S_WORDRANK | gbk
select * from Baikaltest.unitinfo where unitname like "%门店单元%";

# 倒排索引4=S_UNIGRAMS
select * from Baikaltest.t_student2 where address1 like "%lisiaddress%";
# 倒排索引4=S_UNIGRAMS | gbk
select * from Baikaltest.t_student2 where address1 like "%李四地址%";

# 倒排索引5=S_DEFAULT
select * from Baikaltest.t_student2 where name2 like "%lisiname%";
# 倒排索引5=S_DEFAULT | gbk
select * from Baikaltest.t_student2 where name2 like "%李四名字%";

# or like case 补充
select * from Baikaltest.t_student2 where name2 like "%李四名字%" or address1 like "%lisiaddress%";


# 倒排索引6=S_NO_SEGMENT
select * from Baikaltest.t_student2 where name1 like "%lisiname%";
# 倒排索引6=S_NO_SEGMENT | gbk
select * from Baikaltest.t_student2 where name1 like "%李四名字%";

# 倒排索引7=S_BIGRAMS
select * from Baikaltest.t_student3 where address2 like "%lisiaddress%";
# 倒排索引7=S_BIGRAMS | gbk
select * from Baikaltest.t_student3 where address2 like "%李四地址%";

# 倒排索引8=S_ES_STANDARD
select * from Baikaltest.unitsetting where content like "%pageId=%";
# 倒排索引8=S_ES_STANDARD | gbk
update Baikaltest.unitsetting set content='单元测试' where unitid=4501383668;
select * from Baikaltest.unitsetting where content like "%单元测试%";
# 倒排索引 + and
################# 倒排索引 ——end—— #################


################# 普通索引 ——start—— #################
# 普通索引-primary key 两个字段均有值
select * from Baikaltest.ideainfo where userid=8 and ideaid=54370408271;
# 普通索引-primary key 仅第一个字段有值
select * from Baikaltest.ideainfo where userid=8 and ideaid=54370414090;
# 普通索引-primary key 两个字段都没值
select * from Baikaltest.ideainfo where userid=9 and ideaid=54370414090;
# 普通索引-key 有值
select * from Baikaltest.ideainfo where unitid=4501406201;
# 普通索引-key 无值
select * from Baikaltest.ideainfo where unitid=54370412700;
# 普通索引-UNIQUE key 有值
select * from Baikaltest.ideainfo where ideaid=54370408271;
# 普通索引-UNIQUE key 无值
select * from Baikaltest.ideainfo where ideaid=54370408300;
# 普通索引-key 有值
select * from Baikaltest.ideainfo where modtime>"2020-04-07 04:51:11";
# 普通索引-key 无值
select * from Baikaltest.ideainfo where modtime<"2020-03-07 04:51:11";
# 普通索引-key 有值
select * from Baikaltest.ideainfo where planid>89977919;
# 普通索引-key 有值
select * from Baikaltest.ideainfo where planid<8;
# 强制使用索引
select * from Baikaltest.ideainfo use index(index_ideaid) where  ideaid=54370411209 and userid>8 and ideaname like "%idea%";
select * from Baikaltest.ideainfo use index(index_ideaid) where  ideaid=54370411209 and userid=8 and ideaname like "%idea%";
explain select * from Baikaltest.ideainfo where  ideaid=54370411209 and userid>8 and ideaname like "%idea%";
explain select * from Baikaltest.ideainfo use index(index_ideaid) where  ideaid=54370411209 and userid>8 and ideaname like "%idea%";
# 虽然指定了使用index_ideaid，但是由于userid + ideaid 是主键，所以还是选择了主键
explain select * from Baikaltest.ideainfo where  ideaid=54370411209 and userid=8 and ideaname like "%idea%";
explain select * from Baikaltest.ideainfo use index(index_ideaid) where  ideaid=54370411209 and userid=8 and ideaname like "%idea%";
# primary_key(userid,ideaid),index_planid(planid) 优先选择，index_planid
explain select * from Baikaltest.ideainfo  where  userid=8 and planid=89980069;
# primary_key(userid,ideaid),index_planid(planid) 优先选择，index_planid,use index(primary_key) 后使用primary_key
explain select * from Baikaltest.ideainfo use index(primary_key) where  userid=8 and planid=89980069;
# 忽略使用索引
select * from Baikaltest.ideainfo ignore index(ideaname_key) where  ideaid=54370411209 and userid=8 and ideaname like "%idea%";
select * from Baikaltest.ideainfo ignore index(index_ideaid) where  ideaid=54370411209 and userid>8 and ideaname like "%idea%";
# 倒排索引优先级最高 使用ideaname_key
explain select * from Baikaltest.ideainfo where  ideaid=54370411209 and userid=8 and ideaname like "%idea%";
# 倒排索引优先级最高 使用ideaname_key,忽略倒排索引后，使用主键primary_key
explain select * from Baikaltest.ideainfo ignore index(ideaname_key) where  ideaid=54370411209 and userid=8 and ideaname like "%idea%";
# 倒排索引优先级最高 使用ideaname_key,忽略倒排索引后，使用index_ideaid
explain select * from Baikaltest.ideainfo ignore index(ideaname_key) where  ideaid=54370411209  and ideaname like "%idea%";
################# 普通索引 ——end—— #################

