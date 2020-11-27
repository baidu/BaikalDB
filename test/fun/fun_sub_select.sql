### 子查询 功能测试 ###
# 初始化数据
delete from `Baikaltest`.`subselect` ;
insert into `Baikaltest`.`subselect`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(1,'zhangsan1','zhangsan11',10,11,100,101,'zhangsanaddress1','zhangsanaddress11',1000,1001);
insert into `Baikaltest`.`subselect`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(2,'lisi2','lisi22',20,21,200,201,'lisiaddress2','lisiaddress22',2000,2001);
insert into `Baikaltest`.`subselect`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(3,'wangwu3','wangwu33',30,31,300,301,'wangwuaddress3','wangwuaddress33',3000,3001);
insert into `Baikaltest`.`subselect`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(4,'zhaoliu4','zhaoliu44',40,41,400,401,'zhaoliuaddress4','zhaoliuaddress44',4000,4001);
select * from `Baikaltest`.`subselect`;


# join 子查询语句
SELECT t1.id, t2.name1 FROM `Baikaltest`.`subselect` as t1 join `Baikaltest`.`subselect` as t2 on t1.id=t2.id;
SELECT t1.id, t2.name1 FROM `Baikaltest`.`subselect` as t1 join `Baikaltest`.`subselect` as t2 on t1.id=t2.id where t2.name1 = 'zhangsan1';
SELECT t1.id, t2.name1 FROM `Baikaltest`.`subselect` as t1 left join `Baikaltest`.`subselect` as t2 on t1.id=t2.id where t2.name1 = 'zhangsan1';
SELECT t1.id, t2.name1 FROM `Baikaltest`.`subselect` as t1 right join `Baikaltest`.`subselect` as t2 on t1.id=t2.id where t2.name1 = 'zhangsan1';