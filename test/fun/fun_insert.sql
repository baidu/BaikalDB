### insert 功能测试 ###
# 初始化数据
delete from `Baikaltest`.`insert` ;
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(1,'zhangsan1','zhangsan11',10,11,100,101,'zhangsanaddress1','zhangsanaddress11',1000,1001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(2,'lisi2','lisi22',20,21,200,201,'lisiaddress2','lisiaddress22',2000,2001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(3,'wangwu3','wangwu33',30,31,300,301,'wangwuaddress3','wangwuaddress33',3000,3001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(4,'zhaoliu4','zhaoliu44',40,41,400,401,'zhaoliuaddress4','zhaoliuaddress44',4000,4001);
select * from `Baikaltest`.`insert`;

# insert into 主键冲突————预期结果insert fail
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(1,'zhangsan9','zhangsan99',90,91,900,901,'zhangsanaddress9','zhangsanaddress99',9000,9001);
select * from `Baikaltest`.`insert`;

# insert into 唯一键冲突————预期结果insert fail
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(9,'zhangsan1','zhangsan99',90,91,900,901,'zhangsanaddress9','zhangsanaddress99',9000,9001);
select * from `Baikaltest`.`insert`;

# insert into 两条数据，其中一条主键冲突————预期结果insert两条都fail
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(1,'zhangsan9','zhangsan99',90,91,900,901,'zhangsanaddress9','zhangsanaddress99',9000,9001),(11,'zhangsan11','zhangsan1111',1100,1101,1100,1101,'zhangsanaddress11','zhangsanaddress1111',11000,11001);
select * from `Baikaltest`.`insert`;

# insert into 两条数据，其中一条唯一键冲突————预期结果insert两条都fail
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(9,'zhangsan9','zhangsan99',90,91,900,901,'zhangsanaddress9','zhangsanaddress99',9000,9001),(11,'zhangsan1','zhangsan1111',1100,1101,1100,1101,'zhangsanaddress11','zhangsanaddress1111',11000,11001);
select * from `Baikaltest`.`insert`;

# insert into 两条数据，与表内无冲突，前后两条主键冲突————预期结果insert两条都fail
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(9,'zhangsan9','zhangsan99',90,91,900,901,'zhangsanaddress9','zhangsanaddress99',9000,9001),(9,'zhangsan11','zhangsan1111',1100,1101,1100,1101,'zhangsanaddress11','zhangsanaddress1111',11000,11001);
select * from `Baikaltest`.`insert`;

# insert into 两条数据，与表内无冲突，前后两条唯一键冲突————预期结果insert两条都fail
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(9,'zhangsan9','zhangsan99',90,91,900,901,'zhangsanaddress9','zhangsanaddress99',9000,9001),(11,'zhangsan9','zhangsan1111',1100,1101,1100,1101,'zhangsanaddress11','zhangsanaddress1111',11000,11001);
select * from `Baikaltest`.`insert`;

# insert into 两条数据，与表内无冲突, 前后两条也不冲突————预期结果 insert 两条success
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(9,'zhangsan9','zhangsan99',90,91,900,901,'zhangsanaddress9','zhangsanaddress99',9000,9001),(11,'zhangsan11','zhangsan1111',1100,1101,1100,1101,'zhangsanaddress11','zhangsanaddress1111',11000,11001);
select * from `Baikaltest`.`insert`;


### insert ignore 功能测试 ###
# 初始化数据
delete from `Baikaltest`.`insert` ;
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(1,'zhangsan1','zhangsan11',10,11,100,101,'zhangsanaddress1','zhangsanaddress11',1000,1001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(2,'lisi2','lisi22',20,21,200,201,'lisiaddress2','lisiaddress22',2000,2001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(3,'wangwu3','wangwu33',30,31,300,301,'wangwuaddress3','wangwuaddress33',3000,3001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(4,'zhaoliu4','zhaoliu44',40,41,400,401,'zhaoliuaddress4','zhaoliuaddress44',4000,4001);
select * from `Baikaltest`.`insert`;

# insert ignore 主键冲突————预期结果：语句不报错，数据insert fail
insert ignore `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(1,'zhangsan9','zhangsan99',90,91,900,901,'zhangsanaddress9','zhangsanaddress99',9000,9001);
select * from `Baikaltest`.`insert`;

# insert ignore 唯一键冲突————预期结果：语句不报错，数据insert fail
insert ignore `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(9,'zhangsan1','zhangsan99',90,91,900,901,'zhangsanaddress9','zhangsanaddress99',9000,9001);
select * from `Baikaltest`.`insert`;

# insert ignore 两条数据，其中一条主键冲突————预期结果：不冲突的一条数据insert success，冲突的一条insert fail
insert ignore `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(1,'zhangsan9','zhangsan99',90,91,900,901,'zhangsanaddress9','zhangsanaddress99',9000,9001),(11,'zhangsan11','zhangsan1111',1100,1101,1100,1101,'zhangsanaddress11','zhangsanaddress1111',11000,11001);
select * from `Baikaltest`.`insert`;
# 再次初始化数据
delete from `Baikaltest`.`insert` ;
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(1,'zhangsan1','zhangsan11',10,11,100,101,'zhangsanaddress1','zhangsanaddress11',1000,1001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(2,'lisi2','lisi22',20,21,200,201,'lisiaddress2','lisiaddress22',2000,2001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(3,'wangwu3','wangwu33',30,31,300,301,'wangwuaddress3','wangwuaddress33',3000,3001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(4,'zhaoliu4','zhaoliu44',40,41,400,401,'zhaoliuaddress4','zhaoliuaddress44',4000,4001);
select * from `Baikaltest`.`insert`;

# insert ignore 两条数据，其中一条唯一键冲突————预期结果：不冲突的一条数据insert success，冲突的一条insert fail
insert ignore `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(9,'zhangsan9','zhangsan99',90,91,900,901,'zhangsanaddress9','zhangsanaddress99',9000,9001),(11,'zhangsan1','zhangsan1111',1100,1101,1100,1101,'zhangsanaddress11','zhangsanaddress1111',11000,11001);
select * from `Baikaltest`.`insert`;
# 再次初始化数据
delete from `Baikaltest`.`insert` ;
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(1,'zhangsan1','zhangsan11',10,11,100,101,'zhangsanaddress1','zhangsanaddress11',1000,1001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(2,'lisi2','lisi22',20,21,200,201,'lisiaddress2','lisiaddress22',2000,2001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(3,'wangwu3','wangwu33',30,31,300,301,'wangwuaddress3','wangwuaddress33',3000,3001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(4,'zhaoliu4','zhaoliu44',40,41,400,401,'zhaoliuaddress4','zhaoliuaddress44',4000,4001);
select * from `Baikaltest`.`insert`;

# insert ignore 两条数据，与表内无冲突，前后两条主键冲突————预期结果：先插入的success，后插入的fail
insert ignore `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(9,'zhangsan9','zhangsan99',90,91,900,901,'zhangsanaddress9','zhangsanaddress99',9000,9001),(9,'zhangsan11','zhangsan1111',1100,1101,1100,1101,'zhangsanaddress11','zhangsanaddress1111',11000,11001);
select * from `Baikaltest`.`insert`;
# 再次初始化数据
delete from `Baikaltest`.`insert` ;
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(1,'zhangsan1','zhangsan11',10,11,100,101,'zhangsanaddress1','zhangsanaddress11',1000,1001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(2,'lisi2','lisi22',20,21,200,201,'lisiaddress2','lisiaddress22',2000,2001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(3,'wangwu3','wangwu33',30,31,300,301,'wangwuaddress3','wangwuaddress33',3000,3001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(4,'zhaoliu4','zhaoliu44',40,41,400,401,'zhaoliuaddress4','zhaoliuaddress44',4000,4001);
select * from `Baikaltest`.`insert`;

# insert ignore 两条数据，与表内无冲突，前后两条唯一键冲突————预期结果：先插入的success，后插入的fail
insert ignore `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(9,'zhangsan9','zhangsan99',90,91,900,901,'zhangsanaddress9','zhangsanaddress99',9000,9001),(11,'zhangsan9','zhangsan1111',1100,1101,1100,1101,'zhangsanaddress11','zhangsanaddress1111',11000,11001);
select * from `Baikaltest`.`insert`;
# 再次初始化数据
delete from `Baikaltest`.`insert` ;
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(1,'zhangsan1','zhangsan11',10,11,100,101,'zhangsanaddress1','zhangsanaddress11',1000,1001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(2,'lisi2','lisi22',20,21,200,201,'lisiaddress2','lisiaddress22',2000,2001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(3,'wangwu3','wangwu33',30,31,300,301,'wangwuaddress3','wangwuaddress33',3000,3001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(4,'zhaoliu4','zhaoliu44',40,41,400,401,'zhaoliuaddress4','zhaoliuaddress44',4000,4001);
select * from `Baikaltest`.`insert`;

# insert ignore 两条数据，与表内无冲突, 前后两条也不冲突————预期结果 insert 两条success
insert ignore `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(9,'zhangsan9','zhangsan99',90,91,900,901,'zhangsanaddress9','zhangsanaddress99',9000,9001),(11,'zhangsan11','zhangsan1111',1100,1101,1100,1101,'zhangsanaddress11','zhangsanaddress1111',11000,11001);
select * from `Baikaltest`.`insert`;



### insert into ... on duplicate key update ... 功能测试 ###
# 初始化数据
delete from `Baikaltest`.`insert` ;
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(1,'zhangsan1','zhangsan11',10,11,100,101,'zhangsanaddress1','zhangsanaddress11',1000,1001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(2,'lisi2','lisi22',20,21,200,201,'lisiaddress2','lisiaddress22',2000,2001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(3,'wangwu3','wangwu33',30,31,300,301,'wangwuaddress3','wangwuaddress33',3000,3001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(4,'zhaoliu4','zhaoliu44',40,41,400,401,'zhaoliuaddress4','zhaoliuaddress44',4000,4001);
select * from `Baikaltest`.`insert`;

# insert into ... on duplicate key update ...  主键冲突————预期结果：把冲突行的数据id更新成100
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(1,'zhangsan9','zhangsan99',90,91,900,901,'zhangsanaddress9','zhangsanaddress99',9000,9001) on duplicate key update id = 100;
select * from `Baikaltest`.`insert`;
# 再次初始化数据
delete from `Baikaltest`.`insert` ;
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(1,'zhangsan1','zhangsan11',10,11,100,101,'zhangsanaddress1','zhangsanaddress11',1000,1001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(2,'lisi2','lisi22',20,21,200,201,'lisiaddress2','lisiaddress22',2000,2001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(3,'wangwu3','wangwu33',30,31,300,301,'wangwuaddress3','wangwuaddress33',3000,3001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(4,'zhaoliu4','zhaoliu44',40,41,400,401,'zhaoliuaddress4','zhaoliuaddress44',4000,4001);
select * from `Baikaltest`.`insert`;

# insert into ... on duplicate key update ... 唯一键冲突————预期结果：把冲突行的数据name1更新成nametest
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(9,'zhangsan1','zhangsan99',90,91,900,901,'zhangsanaddress9','zhangsanaddress99',9000,9001) on duplicate key update name1 = "nametest";
select * from `Baikaltest`.`insert`;
# 再次初始化数据
delete from `Baikaltest`.`insert` ;
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(1,'zhangsan1','zhangsan11',10,11,100,101,'zhangsanaddress1','zhangsanaddress11',1000,1001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(2,'lisi2','lisi22',20,21,200,201,'lisiaddress2','lisiaddress22',2000,2001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(3,'wangwu3','wangwu33',30,31,300,301,'wangwuaddress3','wangwuaddress33',3000,3001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(4,'zhaoliu4','zhaoliu44',40,41,400,401,'zhaoliuaddress4','zhaoliuaddress44',4000,4001);
select * from `Baikaltest`.`insert`;

# insert into ... on duplicate key update ... 主键冲突&唯一键同时冲突，共冲突两条————预期结果：把主键冲突的数据height1更新成5000，更新一条
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(1,'lisi2','zhangsan99',90,91,900,901,'zhangsanaddress9','zhangsanaddress99',9000,9001) on duplicate key update height1 = 5000;
select * from `Baikaltest`.`insert`;
# 再次初始化数据
delete from `Baikaltest`.`insert` ;
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(1,'zhangsan1','zhangsan11',10,11,100,101,'zhangsanaddress1','zhangsanaddress11',1000,1001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(2,'lisi2','lisi22',20,21,200,201,'lisiaddress2','lisiaddress22',2000,2001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(3,'wangwu3','wangwu33',30,31,300,301,'wangwuaddress3','wangwuaddress33',3000,3001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(4,'zhaoliu4','zhaoliu44',40,41,400,401,'zhaoliuaddress4','zhaoliuaddress44',4000,4001);
select * from `Baikaltest`.`insert`;

# insert into ... on duplicate key update ... insert两条 主键冲突 & 唯一键冲突，共冲突两条————预期结果：把冲突行的数据height1更新成5000，更新2条
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(1,'zhangsan9','zhangsan99',90,91,900,901,'zhangsanaddress9','zhangsanaddress99',9000,9001),(11,'zhangsan1','zhangsan1111',1100,1101,1100,1101,'zhangsanaddress11','zhangsanaddress1111',11000,11001) on duplicate key update height1 = 5000;
select * from `Baikaltest`.`insert`;
# 再次初始化数据
delete from `Baikaltest`.`insert` ;
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(1,'zhangsan1','zhangsan11',10,11,100,101,'zhangsanaddress1','zhangsanaddress11',1000,1001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(2,'lisi2','lisi22',20,21,200,201,'lisiaddress2','lisiaddress22',2000,2001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(3,'wangwu3','wangwu33',30,31,300,301,'wangwuaddress3','wangwuaddress33',3000,3001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(4,'zhaoliu4','zhaoliu44',40,41,400,401,'zhaoliuaddress4','zhaoliuaddress44',4000,4001);
select * from `Baikaltest`.`insert`;

# insert into ... on duplicate key update ... insert两条 主键冲突 & 唯一键冲突，共冲突两条————预期结果：把冲突行的数据class1更新成500，classs1为全局唯一索引, 仅成功第一条
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(1,'zhangsan9','zhangsan99',90,91,900,901,'zhangsanaddress9','zhangsanaddress99',9000,9001),(11,'zhangsan1','zhangsan1111',1100,1101,1100,1101,'zhangsanaddress11','zhangsanaddress1111',11000,11001) on duplicate key update class1 = 500;
select * from `Baikaltest`.`insert`;
# 再次初始化数据
delete from `Baikaltest`.`insert` ;
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(1,'zhangsan1','zhangsan11',10,11,100,101,'zhangsanaddress1','zhangsanaddress11',1000,1001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(2,'lisi2','lisi22',20,21,200,201,'lisiaddress2','lisiaddress22',2000,2001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(3,'wangwu3','wangwu33',30,31,300,301,'wangwuaddress3','wangwuaddress33',3000,3001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(4,'zhaoliu4','zhaoliu44',40,41,400,401,'zhaoliuaddress4','zhaoliuaddress44',4000,4001);
select * from `Baikaltest`.`insert`;

# insert into ... on duplicate key update ... insert两条，其中一条主键冲突————预期结果：不冲突的一条数据insert success，冲突的一条执行update  class1 = 500
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(1,'zhangsan9','zhangsan99',90,91,900,901,'zhangsanaddress9','zhangsanaddress99',9000,9001),(11,'zhangsan11','zhangsan1111',1100,1101,1100,1101,'zhangsanaddress11','zhangsanaddress1111',11000,11001) on duplicate key update class1 = 500;
select * from `Baikaltest`.`insert`;
# 再次初始化数据
delete from `Baikaltest`.`insert` ;
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(1,'zhangsan1','zhangsan11',10,11,100,101,'zhangsanaddress1','zhangsanaddress11',1000,1001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(2,'lisi2','lisi22',20,21,200,201,'lisiaddress2','lisiaddress22',2000,2001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(3,'wangwu3','wangwu33',30,31,300,301,'wangwuaddress3','wangwuaddress33',3000,3001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(4,'zhaoliu4','zhaoliu44',40,41,400,401,'zhaoliuaddress4','zhaoliuaddress44',4000,4001);
select * from `Baikaltest`.`insert`;

# insert into ... on duplicate key update ... 两条数据，其中一条唯一键冲突————预期结果：不冲突的一条数据insert success，冲突的一条执行update height1 = 5000
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(9,'zhangsan9','zhangsan99',90,91,900,901,'zhangsanaddress9','zhangsanaddress99',9000,9001),(11,'zhangsan1','zhangsan1111',1100,1101,1100,1101,'zhangsanaddress11','zhangsanaddress1111',11000,11001) on duplicate key update height1 = 5000;
select * from `Baikaltest`.`insert`;
# 再次初始化数据
delete from `Baikaltest`.`insert` ;
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(1,'zhangsan1','zhangsan11',10,11,100,101,'zhangsanaddress1','zhangsanaddress11',1000,1001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(2,'lisi2','lisi22',20,21,200,201,'lisiaddress2','lisiaddress22',2000,2001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(3,'wangwu3','wangwu33',30,31,300,301,'wangwuaddress3','wangwuaddress33',3000,3001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(4,'zhaoliu4','zhaoliu44',40,41,400,401,'zhaoliuaddress4','zhaoliuaddress44',4000,4001);
select * from `Baikaltest`.`insert`;

# insert into ... on duplicate key update ... 两条数据，与表内无冲突，前后两条主键冲突————预期结果：先插入的success，后插入的把前面一条更新 update height1 = 5000
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(9,'zhangsan9','zhangsan99',90,91,900,901,'zhangsanaddress9','zhangsanaddress99',9000,9001),(9,'zhangsan11','zhangsan1111',1100,1101,1100,1101,'zhangsanaddress11','zhangsanaddress1111',11000,11001) on duplicate key update height1 = 5000;
select * from `Baikaltest`.`insert`;
# 再次初始化数据
delete from `Baikaltest`.`insert` ;
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(1,'zhangsan1','zhangsan11',10,11,100,101,'zhangsanaddress1','zhangsanaddress11',1000,1001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(2,'lisi2','lisi22',20,21,200,201,'lisiaddress2','lisiaddress22',2000,2001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(3,'wangwu3','wangwu33',30,31,300,301,'wangwuaddress3','wangwuaddress33',3000,3001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(4,'zhaoliu4','zhaoliu44',40,41,400,401,'zhaoliuaddress4','zhaoliuaddress44',4000,4001);
select * from `Baikaltest`.`insert`;

# insert into ... on duplicate key update ... 两条数据，与表内无冲突，前后两条唯一键冲突————预期结果：先插入的success，后插入的把前面一条更新 update class1 = 500
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(9,'zhangsan9','zhangsan99',90,91,900,901,'zhangsanaddress9','zhangsanaddress99',9000,9001),(11,'zhangsan9','zhangsan1111',1100,1101,1100,1101,'zhangsanaddress11','zhangsanaddress1111',11000,11001) on duplicate key update class1 = 500;
select * from `Baikaltest`.`insert`;
# 再次初始化数据
delete from `Baikaltest`.`insert` ;
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(1,'zhangsan1','zhangsan11',10,11,100,101,'zhangsanaddress1','zhangsanaddress11',1000,1001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(2,'lisi2','lisi22',20,21,200,201,'lisiaddress2','lisiaddress22',2000,2001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(3,'wangwu3','wangwu33',30,31,300,301,'wangwuaddress3','wangwuaddress33',3000,3001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(4,'zhaoliu4','zhaoliu44',40,41,400,401,'zhaoliuaddress4','zhaoliuaddress44',4000,4001);
select * from `Baikaltest`.`insert`;

# insert into ... on duplicate key update ... 两条数据，与表内无冲突, 前后两条也不冲突————预期结果 insert 两条success 不执行update
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(9,'zhangsan9','zhangsan99',90,91,900,901,'zhangsanaddress9','zhangsanaddress99',9000,9001),(11,'zhangsan11','zhangsan1111',1100,1101,1100,1101,'zhangsanaddress11','zhangsanaddress1111',11000,11001) on duplicate key update height1 = 5000;
select * from `Baikaltest`.`insert`;




### replace into 功能测试 ###
# 初始化数据
delete from `Baikaltest`.`insert` ;
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(1,'zhangsan1','zhangsan11',10,11,100,101,'zhangsanaddress1','zhangsanaddress11',1000,1001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(2,'lisi2','lisi22',20,21,200,201,'lisiaddress2','lisiaddress22',2000,2001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(3,'wangwu3','wangwu33',30,31,300,301,'wangwuaddress3','wangwuaddress33',3000,3001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(4,'zhaoliu4','zhaoliu44',40,41,400,401,'zhaoliuaddress4','zhaoliuaddress44',4000,4001);
select * from `Baikaltest`.`insert`;


# replace into  主键冲突————预期结果：冲突的数据replace成功
replace into  `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(1,'zhangsan9','zhangsan99',90,91,900,901,'zhangsanaddress9','zhangsanaddress99',9000,9001);
select * from `Baikaltest`.`insert`;
# 再次初始化数据
delete from `Baikaltest`.`insert` ;
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(1,'zhangsan1','zhangsan11',10,11,100,101,'zhangsanaddress1','zhangsanaddress11',1000,1001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(2,'lisi2','lisi22',20,21,200,201,'lisiaddress2','lisiaddress22',2000,2001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(3,'wangwu3','wangwu33',30,31,300,301,'wangwuaddress3','wangwuaddress33',3000,3001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(4,'zhaoliu4','zhaoliu44',40,41,400,401,'zhaoliuaddress4','zhaoliuaddress44',4000,4001);
select * from `Baikaltest`.`insert`;

# replace into  唯一键冲突————预期结果：冲突的数据replace成功
replace into  `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(9,'zhangsan1','zhangsan99',90,91,900,901,'zhangsanaddress9','zhangsanaddress99',9000,9001);
select * from `Baikaltest`.`insert`;
# 再次初始化数据
delete from `Baikaltest`.`insert` ;
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(1,'zhangsan1','zhangsan11',10,11,100,101,'zhangsanaddress1','zhangsanaddress11',1000,1001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(2,'lisi2','lisi22',20,21,200,201,'lisiaddress2','lisiaddress22',2000,2001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(3,'wangwu3','wangwu33',30,31,300,301,'wangwuaddress3','wangwuaddress33',3000,3001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(4,'zhaoliu4','zhaoliu44',40,41,400,401,'zhaoliuaddress4','zhaoliuaddress44',4000,4001);
select * from `Baikaltest`.`insert`;

# replace into  两条数据，其中一条主键冲突————预期结果：不冲突的一条数据insert success，冲突的一条replace成功
replace into  `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(1,'zhangsan9','zhangsan99',90,91,900,901,'zhangsanaddress9','zhangsanaddress99',9000,9001),(11,'zhangsan11','zhangsan1111',1100,1101,1100,1101,'zhangsanaddress11','zhangsanaddress1111',11000,11001);
select * from `Baikaltest`.`insert`;
# 再次初始化数据
delete from `Baikaltest`.`insert` ;
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(1,'zhangsan1','zhangsan11',10,11,100,101,'zhangsanaddress1','zhangsanaddress11',1000,1001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(2,'lisi2','lisi22',20,21,200,201,'lisiaddress2','lisiaddress22',2000,2001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(3,'wangwu3','wangwu33',30,31,300,301,'wangwuaddress3','wangwuaddress33',3000,3001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(4,'zhaoliu4','zhaoliu44',40,41,400,401,'zhaoliuaddress4','zhaoliuaddress44',4000,4001);
select * from `Baikaltest`.`insert`;

# replace into  两条数据，其中一条唯一键冲突————预期结果：不冲突的一条数据insert success，冲突的一条replace成功
replace into  `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(9,'zhangsan9','zhangsan99',90,91,900,901,'zhangsanaddress9','zhangsanaddress99',9000,9001),(11,'zhangsan1','zhangsan1111',1100,1101,1100,1101,'zhangsanaddress11','zhangsanaddress1111',11000,11001);
select * from `Baikaltest`.`insert`;
# 再次初始化数据
delete from `Baikaltest`.`insert` ;
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(1,'zhangsan1','zhangsan11',10,11,100,101,'zhangsanaddress1','zhangsanaddress11',1000,1001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(2,'lisi2','lisi22',20,21,200,201,'lisiaddress2','lisiaddress22',2000,2001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(3,'wangwu3','wangwu33',30,31,300,301,'wangwuaddress3','wangwuaddress33',3000,3001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(4,'zhaoliu4','zhaoliu44',40,41,400,401,'zhaoliuaddress4','zhaoliuaddress44',4000,4001);
select * from `Baikaltest`.`insert`;

# replace into  两条数据，与表内无冲突，前后两条主键冲突————预期结果：先插入的success，后插入的replace前一条成功
replace into  `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(9,'zhangsan9','zhangsan99',90,91,900,901,'zhangsanaddress9','zhangsanaddress99',9000,9001),(9,'zhangsan11','zhangsan1111',1100,1101,1100,1101,'zhangsanaddress11','zhangsanaddress1111',11000,11001);
select * from `Baikaltest`.`insert`;
# 再次初始化数据
delete from `Baikaltest`.`insert` ;
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(1,'zhangsan1','zhangsan11',10,11,100,101,'zhangsanaddress1','zhangsanaddress11',1000,1001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(2,'lisi2','lisi22',20,21,200,201,'lisiaddress2','lisiaddress22',2000,2001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(3,'wangwu3','wangwu33',30,31,300,301,'wangwuaddress3','wangwuaddress33',3000,3001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(4,'zhaoliu4','zhaoliu44',40,41,400,401,'zhaoliuaddress4','zhaoliuaddress44',4000,4001);
select * from `Baikaltest`.`insert`;

# replace into  两条数据，与表内无冲突，前后两条唯一键冲突————预期结果：先插入的success，后插入的fail
replace into  `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(9,'zhangsan9','zhangsan99',90,91,900,901,'zhangsanaddress9','zhangsanaddress99',9000,9001),(11,'zhangsan9','zhangsan1111',1100,1101,1100,1101,'zhangsanaddress11','zhangsanaddress1111',11000,11001);
select * from `Baikaltest`.`insert`;
# 再次初始化数据
delete from `Baikaltest`.`insert` ;
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(1,'zhangsan1','zhangsan11',10,11,100,101,'zhangsanaddress1','zhangsanaddress11',1000,1001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(2,'lisi2','lisi22',20,21,200,201,'lisiaddress2','lisiaddress22',2000,2001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(3,'wangwu3','wangwu33',30,31,300,301,'wangwuaddress3','wangwuaddress33',3000,3001);
insert into `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(4,'zhaoliu4','zhaoliu44',40,41,400,401,'zhaoliuaddress4','zhaoliuaddress44',4000,4001);
select * from `Baikaltest`.`insert`;

# replace into  两条数据，与表内无冲突, 前后两条也不冲突————预期结果 insert 两条success
replace into  `Baikaltest`.`insert`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(9,'zhangsan9','zhangsan99',90,91,900,901,'zhangsanaddress9','zhangsanaddress99',9000,9001),(11,'zhangsan11','zhangsan1111',1100,1101,1100,1101,'zhangsanaddress11','zhangsanaddress1111',11000,11001);
select * from `Baikaltest`.`insert`;

