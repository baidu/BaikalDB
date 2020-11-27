### update 功能测试 ###
# 初始化数据
delete from `Baikaltest`.`update` ;
insert into `Baikaltest`.`update`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(1,'zhangsan1','zhangsan11',10,11,100,101,'zhangsanaddress1','zhangsanaddress11',1000,1001);
insert into `Baikaltest`.`update`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(2,'lisi2','lisi22',20,21,200,201,'lisiaddress2','lisiaddress22',2000,2001);
insert into `Baikaltest`.`update`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(3,'wangwu3','wangwu33',30,31,300,301,'wangwuaddress3','wangwuaddress33',3000,3001);
insert into `Baikaltest`.`update`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(4,'zhaoliu4','zhaoliu44',40,41,400,401,'zhaoliuaddress4','zhaoliuaddress44',4000,4001);
select * from `Baikaltest`.`update`;

# update 执行全表，依次执行所有字段，包括主键，唯一键，普通索引
update `Baikaltest`.`update` set id = 10;
select * from `Baikaltest`.`update`;
update `Baikaltest`.`update` set name1 = "name1test";
select * from `Baikaltest`.`update`;
update `Baikaltest`.`update` set name2 = "name2test";
select * from `Baikaltest`.`update`;
update `Baikaltest`.`update` set age1 = 99;
select * from `Baikaltest`.`update`;
update `Baikaltest`.`update` set age2 = 98;
select * from `Baikaltest`.`update`;
update `Baikaltest`.`update` set class1 = 990;
select * from `Baikaltest`.`update`;
update `Baikaltest`.`update` set class2 = 980;
select * from `Baikaltest`.`update`;
update `Baikaltest`.`update` set address1 = "address1test";
select * from `Baikaltest`.`update`;
update `Baikaltest`.`update` set address2 = "address2test";
select * from `Baikaltest`.`update`;
update `Baikaltest`.`update` set height1 = 999;
select * from `Baikaltest`.`update`;
update `Baikaltest`.`update` set height2 = 998;
select * from `Baikaltest`.`update`;


# update 执行where条件命中两条数据，依次执行所有字段，包括主键，唯一键，普通索引
# 重新初始化数据
delete from `Baikaltest`.`update` ;
insert into `Baikaltest`.`update`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(1,'zhangsan1','zhangsan11',10,11,100,101,'zhangsanaddress1','zhangsanaddress11',1000,1001);
insert into `Baikaltest`.`update`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(2,'lisi2','lisi22',20,21,200,201,'lisiaddress2','lisiaddress22',2000,2001);
insert into `Baikaltest`.`update`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(3,'wangwu3','wangwu33',30,31,300,301,'wangwuaddress3','wangwuaddress33',3000,3001);
insert into `Baikaltest`.`update`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(4,'zhaoliu4','zhaoliu44',40,41,400,401,'zhaoliuaddress4','zhaoliuaddress44',4000,4001);
select * from `Baikaltest`.`update`;

update `Baikaltest`.`update` set id = 10 where id > 2;
select * from `Baikaltest`.`update`;
update `Baikaltest`.`update` set name1 = "name1test" where id > 2;
select * from `Baikaltest`.`update`;
update `Baikaltest`.`update` set name2 = "name2test" where id > 2;
select * from `Baikaltest`.`update`;
update `Baikaltest`.`update` set age1 = 99 where id > 2;
select * from `Baikaltest`.`update`;
update `Baikaltest`.`update` set age2 = 98 where id > 2;
select * from `Baikaltest`.`update`;
update `Baikaltest`.`update` set class1 = 990 where id > 2;
select * from `Baikaltest`.`update`;
update `Baikaltest`.`update` set class2 = 980 where id > 2;
select * from `Baikaltest`.`update`;
update `Baikaltest`.`update` set address1 = "address1test" where id > 2;
select * from `Baikaltest`.`update`;
update `Baikaltest`.`update` set address2 = "address2test" where id > 2;
select * from `Baikaltest`.`update`;
update `Baikaltest`.`update` set height1 = 999 where id > 2;
select * from `Baikaltest`.`update`;
update `Baikaltest`.`update` set height2 = 998 where id > 2;
select * from `Baikaltest`.`update`;



# update 执行where条件命中一条数据，依次执行所有字段，包括主键，唯一键，普通索引
# 重新初始化数据
delete from `Baikaltest`.`update` ;
insert into `Baikaltest`.`update`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(1,'zhangsan1','zhangsan11',10,11,100,101,'zhangsanaddress1','zhangsanaddress11',1000,1001);
insert into `Baikaltest`.`update`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(2,'lisi2','lisi22',20,21,200,201,'lisiaddress2','lisiaddress22',2000,2001);
insert into `Baikaltest`.`update`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(3,'wangwu3','wangwu33',30,31,300,301,'wangwuaddress3','wangwuaddress33',3000,3001);
insert into `Baikaltest`.`update`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(4,'zhaoliu4','zhaoliu44',40,41,400,401,'zhaoliuaddress4','zhaoliuaddress44',4000,4001);
select * from `Baikaltest`.`update`;
# 主键
update `Baikaltest`.`update` set id = 10 where id > 3;
select * from `Baikaltest`.`update`;
# 重新初始化数据
delete from `Baikaltest`.`update` ;
insert into `Baikaltest`.`update`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(1,'zhangsan1','zhangsan11',10,11,100,101,'zhangsanaddress1','zhangsanaddress11',1000,1001);
insert into `Baikaltest`.`update`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(2,'lisi2','lisi22',20,21,200,201,'lisiaddress2','lisiaddress22',2000,2001);
insert into `Baikaltest`.`update`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(3,'wangwu3','wangwu33',30,31,300,301,'wangwuaddress3','wangwuaddress33',3000,3001);
insert into `Baikaltest`.`update`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(4,'zhaoliu4','zhaoliu44',40,41,400,401,'zhaoliuaddress4','zhaoliuaddress44',4000,4001);
select * from `Baikaltest`.`update`;
# 唯一键，普通索引，普通数据
update `Baikaltest`.`update` set name1 = "name1test" where id > 3;
select * from `Baikaltest`.`update`;
update `Baikaltest`.`update` set name2 = "name2test" where id > 3;
select * from `Baikaltest`.`update`;
update `Baikaltest`.`update` set age1 = 99 where id > 3;
select * from `Baikaltest`.`update`;
update `Baikaltest`.`update` set age2 = 98 where id > 3;
select * from `Baikaltest`.`update`;
update `Baikaltest`.`update` set class1 = 990 where id > 3;
select * from `Baikaltest`.`update`;
update `Baikaltest`.`update` set class2 = 980 where id > 3;
select * from `Baikaltest`.`update`;
update `Baikaltest`.`update` set address1 = "address1test" where id > 3;
select * from `Baikaltest`.`update`;
update `Baikaltest`.`update` set address2 = "address2test" where id > 3;
select * from `Baikaltest`.`update`;
update `Baikaltest`.`update` set height1 = 999 where id > 3;
select * from `Baikaltest`.`update`;
update `Baikaltest`.`update` set height2 = 998 where id > 3;
select * from `Baikaltest`.`update`;



# update 执行where条件未命中数据，依次执行所有字段，包括主键，唯一键，普通索引
# 重新初始化数据
delete from `Baikaltest`.`update` ;
insert into `Baikaltest`.`update`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(1,'zhangsan1','zhangsan11',10,11,100,101,'zhangsanaddress1','zhangsanaddress11',1000,1001);
insert into `Baikaltest`.`update`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(2,'lisi2','lisi22',20,21,200,201,'lisiaddress2','lisiaddress22',2000,2001);
insert into `Baikaltest`.`update`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(3,'wangwu3','wangwu33',30,31,300,301,'wangwuaddress3','wangwuaddress33',3000,3001);
insert into `Baikaltest`.`update`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(4,'zhaoliu4','zhaoliu44',40,41,400,401,'zhaoliuaddress4','zhaoliuaddress44',4000,4001);
select * from `Baikaltest`.`update`;

update `Baikaltest`.`update` set id = 10 where id > 4;
select * from `Baikaltest`.`update`;
update `Baikaltest`.`update` set name1 = "name1test" where id > 4;
select * from `Baikaltest`.`update`;
update `Baikaltest`.`update` set name2 = "name2test" where id > 4;
select * from `Baikaltest`.`update`;
update `Baikaltest`.`update` set age1 = 99 where id > 4;
select * from `Baikaltest`.`update`;
update `Baikaltest`.`update` set age2 = 98 where id > 4;
select * from `Baikaltest`.`update`;
update `Baikaltest`.`update` set class1 = 990 where id > 4;
select * from `Baikaltest`.`update`;
update `Baikaltest`.`update` set class2 = 980 where id > 4;
select * from `Baikaltest`.`update`;
update `Baikaltest`.`update` set address1 = "address1test" where id > 4;
select * from `Baikaltest`.`update`;
update `Baikaltest`.`update` set address2 = "address2test" where id > 4;
select * from `Baikaltest`.`update`;
update `Baikaltest`.`update` set height1 = 999 where id > 4;
select * from `Baikaltest`.`update`;
update `Baikaltest`.`update` set height2 = 998 where id > 4;
select * from `Baikaltest`.`update`;