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
# normal select
select id,name1,name2,age1,age2,class1,class2 from (select id,name1,name2,age1,age2,class1,class2 from Baikaltest.t_student1) as t1;
select id+1,name1,name2,age1-1,age2,class1,class2 from (select id,name1,name2,age1,age2,class1,class2 from Baikaltest.t_student1) as t1;
select class2,class1,age1,age2,id,name1,name2 from (select id,name1,name2,age1,age2,class1,class2 from Baikaltest.t_student1) as t1;
select *,t1.k from (select id,name1,1 as k from Baikaltest.t_student1) as t1;
# agg
select id,name1,name2,sum(age1),avg(age2),class1,class2 from (select id,name1,name2,age1,age2,class1,class2 from Baikaltest.t_student1) as t1 group by class1 order by id;
select id,name1,name2,sum(age1),avg(age2),class1,class2 from (select id,name1,name2,age1,age2,class1,class2 from Baikaltest.t_student1) as t1 group by class1 order by id;
select id,name1,name2,min(age1),max(age2),class1,class2 from (select id,name1,name2,age1,age2,class1,class2 from Baikaltest.t_student1) as t1 group by class1 order by id;
# union
select id,name1,name2,age1,age2,class1,class2 from (select id,name1,name2,age1,age2,class1,class2 from Baikaltest.t_student1 union select id,name1,name2,age1,age2,class1,class2 from Baikaltest.t_student2) as t1 group by class1 order by id;
select id,name1,name2,age1,age2,class1,class2 from (select id,name1,name2,age1,age2,class1,class2 from Baikaltest.t_student1 union all select id,name1,name2,age1,age2,class1,class2 from Baikaltest.t_student2) as t1 group by class1 order by id;
# nest
select id,name1,name2,age1 from (select id,name1,name2,age1,age2,class1,class2 from (select id,name1,name2,age1,age2,class1,class2 from Baikaltest.t_student1) as t1) as t2;
select id,name1,name2,age1 from (select id,name1,name2,age1,age2,class1,class2 from (select id,name1,name2,age1,age2,class1,class2 from Baikaltest.t_student1) as t1 union select id,name1,name2,age1,age2,class1,class2 from Baikaltest.t_student2) as t2;
# join
SELECT t1.id,t2.name1 FROM Baikaltest.t_student1 as t1 join Baikaltest.t_student1 as t2 on t1.id=t2.id;
select t1.id,t2.name1 from Baikaltest.t_student1 as t1 join (select id,name1,age1 from Baikaltest.t_student2 group by age1) as t2 on t1.id=t2.id join Baikaltest.t_student2 as t3 on t1.id=t3.id;