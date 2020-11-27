# 添加列
alter table Baikaltest.DDLtable add column status int(4) default 0 comment "状态：0-上线；1-下线";
sleep 10
desc Baikaltest.DDLtable;

# 修改列名
alter table Baikaltest.DDLtable RENAME COLUMN status TO status2;
sleep 10
desc Baikaltest.DDLtable;

# 删除列
alter table Baikaltest.DDLtable drop column status2;
sleep 10
desc Baikaltest.DDLtable;


# 添加索引
alter table Baikaltest.DDLtable add unique index index_name1(name1);
sleep 10
desc  Baikaltest.DDLtable;

# 删除索引
alter table BBaikaltest.DDLtable2 drop index class1_key ;
sleep 10
desc  Baikaltest.DDLtable2;


# 修改表名
alter table Baikaltest.DDLtable rename to Baikaltest.DDLtable22;
sleep 10
use Baikaltest;
show tables;
# 表名再改回来
alter table Baikaltest.DDLtable22 rename to Baikaltest.DDLtable;
sleep 10
use Baikaltest;
show tables;

# drop table 
drop table Baikaltest.DDLtable;
sleep 10
use Baikaltest;
show tables;

# restore table
restore table Baikaltest.DDLtable;
sleep 10
use Baikaltest;
show tables;

alter table Baikaltest.DDLtable add unique index global global_index_name2(name2);
alter table Baikaltest.DDLtable drop index global_index_name2 ;
alter table Baikaltest.DDLtable add index global global_index_class1(class1);
alter table Baikaltest.DDLtable drop index global_index_class1 ;
sleep 10
desc  Baikaltest.DDLtable;
use Baikaltest;
show tables;

# 权限异常分支验证
