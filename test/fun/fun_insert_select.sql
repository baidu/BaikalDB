# 初始化数据 Baikaltest.t_student1
delete from Baikaltest.t_student1 ;
insert into Baikaltest.t_student1(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(1,'zhangsan1','zhangsan11',10,11,100,101,'zhangsanaddress1','zhangsanaddress11',1000,1001);
insert into Baikaltest.t_student1(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(2,'lisi2','lisi22',20,21,200,201,'lisiaddress2','lisiaddress22',2000,2001);
insert into Baikaltest.t_student1(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(3,'wangwu3','wangwu33',30,31,300,301,'wangwuaddress3','wangwuaddress33',3000,3001);
insert into Baikaltest.t_student1(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(4,'zhaoliu4','zhaoliu44',40,41,400,401,'zhaoliuaddress4','zhaoliuaddress44',4000,4001);
select * from Baikaltest.t_student1;
# 初始化数据 Baikaltest.t_student2
delete from Baikaltest.t_student2 ;
# 就这一条数据是重复的
insert into Baikaltest.t_student2(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(4,'test4','test44',4040,4141,400400,401401,'testaddress4','testaddress44',40004000,40014001);
insert into Baikaltest.t_student2(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(7,'zhangsan7','zhangsan77',70,71,700,701,'zhangsanaddress7','zhangsanaddress77',7000,7001);
insert into Baikaltest.t_student2(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(8,'lisi8','lisi88',80,81,800,801,'lisiaddress8','lisiaddress88',8000,8001);
insert into Baikaltest.t_student2(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(9,'wangwu9','wangwu99',90,91,900,901,'wangwuaddress9','wangwuaddress99',9000,9001);
insert into Baikaltest.t_student2(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(10,'zhaoliu10','zhaoliu1010',100,101,1000,1001,'zhaoliuaddress10','zhaoliuaddress1010',10000,10001);
select * from Baikaltest.t_student2;



###  insert ###
# 单表全字段 全部写入新表
# 验证所有字段分别插入新表的结果
delete from `Baikaltest`.`insert_select`;
insert into `Baikaltest`.`insert_select` select * from `Baikaltest`.`t_student1`;
select * from `Baikaltest`.`insert_select`;
# 预期结果：有重复数据整条失败
insert into `Baikaltest`.`insert_select` select * from `Baikaltest`.`t_student2`;
select * from `Baikaltest`.`insert_select`;
delete from `Baikaltest`.`insert_select`;
insert into `Baikaltest`.`insert_select`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) select id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2 from `Baikaltest`.`t_student1`;
select * from `Baikaltest`.`insert_select`;
delete from `Baikaltest`.`insert_select`;

# 验证所有单一字段分别插入新表的结果
insert into `Baikaltest`.`insert_select`(id) select id from `Baikaltest`.`t_student1`;
select * from `Baikaltest`.`insert_select`;
delete from `Baikaltest`.`insert_select`;
insert into `Baikaltest`.`insert_select`(name1) select name1 from `Baikaltest`.`t_student1`;
select * from `Baikaltest`.`insert_select`;
delete from `Baikaltest`.`insert_select`;
insert into `Baikaltest`.`insert_select`(name2) select name2 from `Baikaltest`.`t_student1`;
select * from `Baikaltest`.`insert_select`;
delete from `Baikaltest`.`insert_select`;
insert into `Baikaltest`.`insert_select`(age1) select age1 from `Baikaltest`.`t_student1`;
select * from `Baikaltest`.`insert_select`;
delete from `Baikaltest`.`insert_select`;
insert into `Baikaltest`.`insert_select`(age2) select age2 from `Baikaltest`.`t_student1`;
select * from `Baikaltest`.`insert_select`;
delete from `Baikaltest`.`insert_select`;
insert into `Baikaltest`.`insert_select`(class1) select class1 from `Baikaltest`.`t_student1`;
select * from `Baikaltest`.`insert_select`;
delete from `Baikaltest`.`insert_select`;
insert into `Baikaltest`.`insert_select`(class2) select class2 from `Baikaltest`.`t_student1`;
select * from `Baikaltest`.`insert_select`;
delete from `Baikaltest`.`insert_select`;
insert into `Baikaltest`.`insert_select`(address1) select address1 from `Baikaltest`.`t_student1`;
select * from `Baikaltest`.`insert_select`;
delete from `Baikaltest`.`insert_select`;
insert into `Baikaltest`.`insert_select`(address2) select address2 from `Baikaltest`.`t_student1`;
select * from `Baikaltest`.`insert_select`;
delete from `Baikaltest`.`insert_select`;
insert into `Baikaltest`.`insert_select`(height1) select height1 from `Baikaltest`.`t_student1`;
select * from `Baikaltest`.`insert_select`;
delete from `Baikaltest`.`insert_select`;
insert into `Baikaltest`.`insert_select`(height2) select height2 from `Baikaltest`.`t_student1`;
select * from `Baikaltest`.`insert_select`;
delete from `Baikaltest`.`insert_select`;
insert into `Baikaltest`.`insert_select` select id from `Baikaltest`.`t_student1`;
select * from `Baikaltest`.`insert_select`;
delete from `Baikaltest`.`insert_select`;
insert into `Baikaltest`.`insert_select` select name1 from `Baikaltest`.`t_student1`;
select * from `Baikaltest`.`insert_select`;
delete from `Baikaltest`.`insert_select`;
insert into `Baikaltest`.`insert_select` select name2 from `Baikaltest`.`t_student1`;
select * from `Baikaltest`.`insert_select`;
delete from `Baikaltest`.`insert_select`;
insert into `Baikaltest`.`insert_select` select age1 from `Baikaltest`.`t_student1`;
select * from `Baikaltest`.`insert_select`;
delete from `Baikaltest`.`insert_select`;
insert into `Baikaltest`.`insert_select` select age2 from `Baikaltest`.`t_student1`;
select * from `Baikaltest`.`insert_select`;
delete from `Baikaltest`.`insert_select`;
insert into `Baikaltest`.`insert_select` select class1 from `Baikaltest`.`t_student1`;
select * from `Baikaltest`.`insert_select`;
delete from `Baikaltest`.`insert_select`;
insert into `Baikaltest`.`insert_select` select class2 from `Baikaltest`.`t_student1`;
select * from `Baikaltest`.`insert_select`;
delete from `Baikaltest`.`insert_select`;
insert into `Baikaltest`.`insert_select` select address1 from `Baikaltest`.`t_student1`;
select * from `Baikaltest`.`insert_select`;
delete from `Baikaltest`.`insert_select`;
insert into `Baikaltest`.`insert_select` select address2 from `Baikaltest`.`t_student1`;
select * from `Baikaltest`.`insert_select`;
delete from `Baikaltest`.`insert_select`;
insert into `Baikaltest`.`insert_select` select height1 from `Baikaltest`.`t_student1`;
select * from `Baikaltest`.`insert_select`;
delete from `Baikaltest`.`insert_select`;
insert into `Baikaltest`.`insert_select` select height2 from `Baikaltest`.`t_student1`;
select * from `Baikaltest`.`insert_select`;
delete from `Baikaltest`.`insert_select`;
insert into `Baikaltest`.`insert_select` select id,name1,name2,age1,age2,class1,class2 from `Baikaltest`.`t_student1`;
select * from `Baikaltest`.`insert_select`;
delete from `Baikaltest`.`insert_select`;
insert into `Baikaltest`.`insert_select`(id,name1,name2,age1,age2,class1,class2) select id,name1,name2,age1,age2,class1,class2 from `Baikaltest`.`t_student1`;
select * from `Baikaltest`.`insert_select`;
delete from `Baikaltest`.`insert_select`;

# union 都不加where条件——有重复数据，预期结果：整条失败
insert into `Baikaltest`.`insert_select` (select * from `Baikaltest`.`t_student1`  union select * from `Baikaltest`.`t_student2` );
select * from `Baikaltest`.`insert_select`;
delete from `Baikaltest`.`insert_select`;
# union 前加where条件，后不加where条件——无重复数据，预期结果：成功
insert into `Baikaltest`.`insert_select` (select * from `Baikaltest`.`t_student1` where id < 3  union select * from `Baikaltest`.`t_student2` );
select * from `Baikaltest`.`insert_select`;
delete from `Baikaltest`.`insert_select`;
# union 前不加where条件，后加where条件——无重复数据，预期结果：成功
insert into `Baikaltest`.`insert_select` (select * from `Baikaltest`.`t_student1`  union select * from `Baikaltest`.`t_student2` where id> 6 );
select * from `Baikaltest`.`insert_select`;
delete from `Baikaltest`.`insert_select`;
# union union前后都有数据——无重复数据，预期结果：成功
insert into `Baikaltest`.`insert_select` (select * from `Baikaltest`.`t_student1` where id < 2 union select * from `Baikaltest`.`t_student2` where id > 2);
select * from `Baikaltest`.`insert_select`;
delete from `Baikaltest`.`insert_select`;
# union union前后都没有数据——无数据，预期结果：成功
insert into `Baikaltest`.`insert_select` (select * from `Baikaltest`.`t_student1` where id < 0 union select * from `Baikaltest`.`t_student2` where id > 100);
select * from `Baikaltest`.`insert_select`;
delete from `Baikaltest`.`insert_select`;
# union union前无数据，后有数据——无重复数据
insert into `Baikaltest`.`insert_select` (select * from `Baikaltest`.`t_student1` where id < 0 union select * from `Baikaltest`.`t_student2` where id > 3);
select * from `Baikaltest`.`insert_select`;
delete from `Baikaltest`.`insert_select`;
# union union前有数据，后无数据——无重复数据
insert into `Baikaltest`.`insert_select` (select * from `Baikaltest`.`t_student1` where id < 3 union select * from `Baikaltest`.`t_student2` where id > 100);
select * from `Baikaltest`.`insert_select`;
delete from `Baikaltest`.`insert_select`;


### insert ignore ###
# union 都不加where条件——有重复数据，预期结果：成功，冲突的数据忽略，不插入
insert ignore  `Baikaltest`.`insert_select` (select * from `Baikaltest`.`t_student1`  union select * from `Baikaltest`.`t_student2` );
select * from `Baikaltest`.`insert_select`;
# 更新一条数据后再验证ignore的结果
update `Baikaltest`.`insert_select` set age1=100001 where id=4;
select * from `Baikaltest`.`insert_select`;
# union 命中t_student2中的id=4数据，预期结果：冲突的数据忽略，不插入
insert ignore  `Baikaltest`.`insert_select` (select * from `Baikaltest`.`t_student1` where id < 3  union select * from `Baikaltest`.`t_student2` );
select * from `Baikaltest`.`insert_select`;
# union  命中t_student1中的id=4数据，预期结果：冲突的数据忽略，不插入
insert ignore  `Baikaltest`.`insert_select` (select * from `Baikaltest`.`t_student1`  union select * from `Baikaltest`.`t_student2` where id> 6 );
select * from `Baikaltest`.`insert_select`;
delete from `Baikaltest`.`insert_select`;

# union union前后都有数据——无重复数据，预期结果：插入成功
insert ignore  `Baikaltest`.`insert_select` (select * from `Baikaltest`.`t_student1` where id < 2 union select * from `Baikaltest`.`t_student2` where id > 2);
select * from `Baikaltest`.`insert_select`;
delete from `Baikaltest`.`insert_select`;

# union union前后都没有数据——无数据，预期结果：插入成功
insert ignore  `Baikaltest`.`insert_select` (select * from `Baikaltest`.`t_student1` where id < 0 union select * from `Baikaltest`.`t_student2` where id > 100);
select * from `Baikaltest`.`insert_select`;
delete from `Baikaltest`.`insert_select`;

# union union前无数据，后有数据——无重复数据，预期结果：插入成功
insert ignore  `Baikaltest`.`insert_select` (select * from `Baikaltest`.`t_student1` where id < 0 union select * from `Baikaltest`.`t_student2` where id > 3);
select * from `Baikaltest`.`insert_select`;
delete from `Baikaltest`.`insert_select`;

# union union前有数据，后无数据——无重复数据，预期结果：插入成功
insert ignore  `Baikaltest`.`insert_select` (select * from `Baikaltest`.`t_student1` where id < 3 union select * from `Baikaltest`.`t_student2` where id > 100);
select * from `Baikaltest`.`insert_select`;
delete from `Baikaltest`.`insert_select`;

###  replace ###
# union 都不加where条件——有重复数据，预期结果：成功，冲突的数据：覆盖
replace into `Baikaltest`.`insert_select` (select * from `Baikaltest`.`t_student1`  union select * from `Baikaltest`.`t_student2` );
select * from `Baikaltest`.`insert_select`;
# 更新一条数据后再验证replace的结果
update `Baikaltest`.`insert_select` set age1=100001 where id=4;
select * from `Baikaltest`.`insert_select`;
# union  命中t_student1中的id=4数据
replace into `Baikaltest`.`insert_select` (select * from `Baikaltest`.`t_student1`  union select * from `Baikaltest`.`t_student2` where id> 6 );
select * from `Baikaltest`.`insert_select`;
# union 命中t_student2中的id=4数据
replace into `Baikaltest`.`insert_select` (select * from `Baikaltest`.`t_student1` where id < 3  union select * from `Baikaltest`.`t_student2` );
select * from `Baikaltest`.`insert_select`;
delete from `Baikaltest`.`insert_select`;

# union union前后都有数据——无重复数据，预期结果：成功
replace into `Baikaltest`.`insert_select` (select * from `Baikaltest`.`t_student1` where id < 2 union select * from `Baikaltest`.`t_student2` where id > 2);
select * from `Baikaltest`.`insert_select`;
delete from `Baikaltest`.`insert_select`;
# union union前后都没有数据——无数据，预期结果：成功
replace into `Baikaltest`.`insert_select` (select * from `Baikaltest`.`t_student1` where id < 0 union select * from `Baikaltest`.`t_student2` where id > 100);
select * from `Baikaltest`.`insert_select`;
delete from `Baikaltest`.`insert_select`;
# union union前无数据，后有数据——无重复数据，预期结果：成功
replace into `Baikaltest`.`insert_select` (select * from `Baikaltest`.`t_student1` where id < 0 union select * from `Baikaltest`.`t_student2` where id > 3);
select * from `Baikaltest`.`insert_select`;
delete from `Baikaltest`.`insert_select`;
# union union前有数据，后无数据——无重复数据，预期结果：成功
replace into `Baikaltest`.`insert_select` (select * from `Baikaltest`.`t_student1` where id < 3 union select * from `Baikaltest`.`t_student2` where id > 100);
select * from `Baikaltest`.`insert_select`;
delete from `Baikaltest`.`insert_select`;



###  insert into ... on duplicate key update ... ###
# union 都不加where条件——有重复数据，预期结果：成功，冲突的数据忽略，不插入
insert ignore  `Baikaltest`.`insert_select` (select * from `Baikaltest`.`t_student1`  union select * from `Baikaltest`.`t_student2` );
select * from `Baikaltest`.`insert_select`;
# union 更新id=3的数据height1=100000
insert into `Baikaltest`.`insert_select` (select * from `Baikaltest`.`t_student1` where id = 3  union select * from `Baikaltest`.`t_student2` where id = 100) on duplicate key update  height1 = 100000;
select * from `Baikaltest`.`insert_select`;
# union 更新id=9和10的数据height2=200000
insert into `Baikaltest`.`insert_select` (select * from `Baikaltest`.`t_student1` where id = 0 union select * from `Baikaltest`.`t_student2` where id> 8 ) on duplicate key update  height2 = 200000;
select * from `Baikaltest`.`insert_select`;
delete from `Baikaltest`.`insert_select`;

# union 都不加where条件——有重复数据，预期结果：成功，冲突的数据忽略，不插入
insert ignore  `Baikaltest`.`insert_select` (select * from `Baikaltest`.`t_student1`  union select * from `Baikaltest`.`t_student2` );
select * from `Baikaltest`.`insert_select`;
# union 更新id=3的数据 name1="zhangsan1" ,预期结果：失败，因为插入的name1字段是uniq key 与表中的数据冲突
insert into `Baikaltest`.`insert_select` (select * from `Baikaltest`.`t_student1` where id = 3  union select * from `Baikaltest`.`t_student2` where id = 100) on duplicate key update  name1="zhangsan1";
select * from `Baikaltest`.`insert_select`;
# union 更新id=9和10的数据 class2=1001 ,预期结果：失败，因为插入的name1字段是uniq key 与表中的数据冲突
insert into `Baikaltest`.`insert_select` (select * from `Baikaltest`.`t_student1` where id = 0 union select * from `Baikaltest`.`t_student2` where id  = 8 ) on duplicate key update  class2=1001;
select * from `Baikaltest`.`insert_select`;
# union 更新id=9和10的数据 class2=1099 ,预期结果：成功，因为插入的name1字段是uniq key 与表中的数据冲突
insert into `Baikaltest`.`insert_select` (select * from `Baikaltest`.`t_student1` where id = 0 union select * from `Baikaltest`.`t_student2` where id =  9 ) on duplicate key update  class2=1099;
select * from `Baikaltest`.`insert_select`;
# union 更新id=9和10的数据 class2=1099 ,预期结果：成功，因为插入的name1字段是uniq key 与表中的数据冲突————class2更新为空，class1未变
insert into `Baikaltest`.`insert_select` (select * from `Baikaltest`.`t_student1` where id = 0 union select * from `Baikaltest`.`t_student2` where id =  9 ) on duplicate key update  class2=1099 , class1= 1088;
select * from `Baikaltest`.`insert_select`;
delete from `Baikaltest`.`insert_select`;



# union
insert ignore `Baikaltest`.`insert_select` (select * from `Baikaltest`.`t_student1` where id < 5 union select * from `Baikaltest`.`t_student2` where id > 2);
select * from `Baikaltest`.`insert_select`;
# union ——未命中数据
insert into `Baikaltest`.`insert_select` (select * from `Baikaltest`.`t_student1` where id < 0 union select * from `Baikaltest`.`t_student2` where id > 100) on duplicate key update  height2 = 200000;
select * from `Baikaltest`.`insert_select`;
# union —— 命中2条数据，update唯一索引，唯一索引是字符串&&冲突，预期结果：失败，因为插入的name1字段是uniq key 更新的两条冲突
insert into `Baikaltest`.`insert_select` (select * from `Baikaltest`.`t_student1` where id = 3 union select * from `Baikaltest`.`t_student2` where id = 10) on duplicate key update  name1 = "name|name|name";
select * from `Baikaltest`.`insert_select`;
# union  —— 命中2条数据，update唯一索引，唯一索引是int&&冲突
insert into `Baikaltest`.`insert_select` (select * from `Baikaltest`.`t_student1` where id = 4 union select * from `Baikaltest`.`t_student2` where id = 9) on duplicate key update  class1 = 100101;
select * from `Baikaltest`.`insert_select`;