### select 功能测试 ###
# 初始化数据
delete from `Baikaltest`.`select` ;
insert into `Baikaltest`.`select`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(1,'zhangsan1','zhangsan11',10,11,100,101,'zhangsanaddress1','zhangsanaddress11',1000,1001);
insert into `Baikaltest`.`select`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(2,'lisi2','lisi22',20,21,200,201,'lisiaddress2','lisiaddress22',2000,2001);
insert into `Baikaltest`.`select`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(3,'wangwu3','wangwu33',30,31,300,301,'wangwuaddress3','wangwuaddress33',3000,3001);
insert into `Baikaltest`.`select`(id,name1,name2,age1,age2,class1,class2,address1,address2,height1,height2) values(4,'zhaoliu4','zhaoliu44',40,41,400,401,'zhaoliuaddress4','zhaoliuaddress44',4000,4001);
select * from `Baikaltest`.`select`;

# 条件主键, =/>=/>/</<=/between... and.../in/not in/limit/count(*)/count(1)/count(id)/like/not like/limit/group by/order by/having
select * from `Baikaltest`.`select` where id = 1;
select * from `Baikaltest`.`select` where id > 1;
select * from `Baikaltest`.`select` where id >= 1;
select * from `Baikaltest`.`select` where id < 1;
select * from `Baikaltest`.`select` where id <= 1;
select * from `Baikaltest`.`select` where id between 1  and 2;
select * from `Baikaltest`.`select` where id in ( 1 , 2 );
select * from `Baikaltest`.`select` where id not in ( 1 , 2 );
select count(*) from `Baikaltest`.`select` where id in ( 1 , 2 );
select count(1) from `Baikaltest`.`select` where id not in ( 1 , 2 );
select count(id) from `Baikaltest`.`select` where id not in ( 1 , 2 );
select * from `Baikaltest`.`select` where id like  "%1%";
select * from `Baikaltest`.`select` where id not like  "%1%";
select * from `Baikaltest`.`select` limit 1;
select * from `Baikaltest`.`select` order by id desc ;
select id,count(*) from `Baikaltest`.`select` group by id having id > 3 ;

# 条件uniqkey
select * from `Baikaltest`.`select` where name1 = "zhangsan1";
select * from `Baikaltest`.`select` where name1 > "zhangsan1";
select * from `Baikaltest`.`select` where name1 >= "zhangsan1";
select * from `Baikaltest`.`select` where name1 < "zhangsan1";
select * from `Baikaltest`.`select` where name1 <= "zhangsan1";
select * from `Baikaltest`.`select` where name1 between "zhangsan1"  and "lisi2";
select * from `Baikaltest`.`select` where name1 in ( "zhangsan1" , "lisi2" );
select * from `Baikaltest`.`select` where name1 not in ( "zhangsan1" , "lisi2" );
select count(*) from `Baikaltest`.`select` where name1 in ( "zhangsan1" , "lisi2" );
select count(1) from `Baikaltest`.`select` where name1 not in ( "zhangsan1" , "lisi2" );
select count(id) from `Baikaltest`.`select` where name1 not in ( "zhangsan1" , "lisi2" );
select * from `Baikaltest`.`select` where name1 like  "%zhangsan1%";
select * from `Baikaltest`.`select` where name1 not like  "%zhangsan1%";
select * from `Baikaltest`.`select` order by name1 desc ;
select name1,count(*) from `Baikaltest`.`select` group by name1 having name1 like "%zhangsan1%";

# 条件普通索引
select * from `Baikaltest`.`select` where address1 = "zhangsanaddress1";
select * from `Baikaltest`.`select` where address1 > "zhangsanaddress1";
select * from `Baikaltest`.`select` where address1 >= "zhangsanaddress1";
select * from `Baikaltest`.`select` where address1 < "zhangsanaddress1";
select * from `Baikaltest`.`select` where address1 <= "zhangsanaddress1";
select * from `Baikaltest`.`select` where address1 between "zhangsanaddress1"  and "lisiaddress2";
select * from `Baikaltest`.`select` where address1 in ( "zhangsanaddress1" , "lisiaddress2" );
select * from `Baikaltest`.`select` where address1 not in ( "zhangsanaddress1" , "lisiaddress2" );
select count(*) from `Baikaltest`.`select` where address1 in ( "zhangsanaddress1" , "lisiaddress2" );
select count(1) from `Baikaltest`.`select` where address1 not in ( "zhangsanaddress1" , "lisiaddress2" );
select count(id) from `Baikaltest`.`select` where address1 not in ( "zhangsanaddress1" , "lisiaddress2" );
select * from `Baikaltest`.`select` where address1 like  "%zhangsanaddress1%";
select * from `Baikaltest`.`select` where address1 not like  "%zhangsanaddress1%";
select * from `Baikaltest`.`select` order by address1 desc ;
select address1,count(*) from `Baikaltest`.`select` group by address1 having address1 like "%zhangsanaddress1%";

# 条件普通字段
select * from `Baikaltest`.`select` where height1 = 1000;
select * from `Baikaltest`.`select` where height1 > 1000;
select * from `Baikaltest`.`select` where height1 >= 1000;
select * from `Baikaltest`.`select` where height1 < 1000;
select * from `Baikaltest`.`select` where height1 <= 1000;
select * from `Baikaltest`.`select` where height1 between 1000  and 2000;
select * from `Baikaltest`.`select` where height1 in ( 1000 , 2000 );
select * from `Baikaltest`.`select` where height1 not in ( 1000 , 2000 );
select count(*) from `Baikaltest`.`select` where height1 in ( 1000 , 2000 );
select count(1) from `Baikaltest`.`select` where height1 not in ( 1000 , 2000 );
select count(id) from `Baikaltest`.`select` where height1 not in ( 1000 , 2000 );
select * from `Baikaltest`.`select` where height1 like  "%1000%";
select * from `Baikaltest`.`select` where height1 not like  "%1000%";
select * from `Baikaltest`.`select` order by height1 desc ;
select height1,count(*) from `Baikaltest`.`select` group by height1 having height1 like "%1000%";


# 条件主键 & uniqkey
select * from `Baikaltest`.`select` where id = 1 and height1 not in ( 1000 , 2000 );
select * from `Baikaltest`.`select` where id > 1 and height1  in ( 1000 , 2000 );
select * from `Baikaltest`.`select` where id >= 1 and height1 not in ( 1000 , 2000 );
select * from `Baikaltest`.`select` where id < 1 and height1 not like  "%1000%" ;
select * from `Baikaltest`.`select` where id <= 1 and height1 like  "%1000%" ;
select * from `Baikaltest`.`select` where id between 1  and 2 and height1 not in ( 1000 , 2000 );
select * from `Baikaltest`.`select` where id in ( 1 , 2 )  and height1 not in ( 1000 , 2000 );
select * from `Baikaltest`.`select` where id not in ( 1 , 2 ) and height1 in ( 1000 , 2000 );
select count(*) from `Baikaltest`.`select` where id in ( 1 , 2 ) and height1 <= 1000;
select count(1) from `Baikaltest`.`select` where id not in ( 1 , 2 ) and height1 < 1000;
select count(id) from `Baikaltest`.`select` where id not in ( 1 , 2 ) and height1 >= 1000;
select * from `Baikaltest`.`select` where id like  "%1%" or height1 > 1000;
select * from `Baikaltest`.`select` where id not like  "%1%" or height1 = 1000;
select * from `Baikaltest`.`select` order by id, height1 desc ;
select id,height1,count(*) from `Baikaltest`.`select` group by id,height1 having id like "%1%";


# 条件主键 & 普通字段
select * from `Baikaltest`.`select` where id = 1 and name1 not like  "%zhangsan1%";
select * from `Baikaltest`.`select` where id > 1 and name1 like  "%zhangsan1%";
select * from `Baikaltest`.`select` where id >= 1 and name1 not in ( "zhangsan1" , "lisi2" );
select * from `Baikaltest`.`select` where id < 1 and name1 not in ( "zhangsan1" , "lisi2" );
select * from `Baikaltest`.`select` where id <= 1 and name1 in ( "zhangsan1" , "lisi2" );
select * from `Baikaltest`.`select` where id between 1  and 2 and name1 in ( "zhangsan1" , "lisi2" );
select * from `Baikaltest`.`select` where id in ( 1 , 2 )  and address1 <= "zhangsanaddress1";
select * from `Baikaltest`.`select` where id not in ( 1 , 2 ) and name1 in ( "zhangsan1" , "lisi2" );
select count(*) from `Baikaltest`.`select` where id in ( 1 , 2 ) and name1 <= "zhangsan1";
select count(1) from `Baikaltest`.`select` where id not in ( 1 , 2 ) and name1 < "zhangsan1";
select count(id) from `Baikaltest`.`select` where id not in ( 1 , 2 ) and name1 >= "zhangsan1";
select * from `Baikaltest`.`select` where id like  "%1%" or name1 > "zhangsan1";
select * from `Baikaltest`.`select` where id not like  "%1%" or name1 = "zhangsan1";
select * from `Baikaltest`.`select` order by id, name1 desc ;
select id,name1,count(*) from `Baikaltest`.`select` group by id,address1 having id like "%1%";


# 条件uniqkey & 普通字段
select * from `Baikaltest`.`select` where height1 = 1000 and name1 not like  "%zhangsan1%";
select * from `Baikaltest`.`select` where height1 > 1000 and name1 like  "%zhangsan1%";
select * from `Baikaltest`.`select` where height1 >= 1000 and name1 not in ( "zhangsan1" , "lisi2" );
select * from `Baikaltest`.`select` where height1 < 1000 and name1 not in ( "zhangsan1" , "lisi2" );
select * from `Baikaltest`.`select` where height1 <= 1000 and name1 in ( "zhangsan1" , "lisi2" );
select * from `Baikaltest`.`select` where height1 between 1000  and 2000 and name1 in ( "zhangsan1" , "lisi2" );
select * from `Baikaltest`.`select` where height1 in ( 1000 , 2000 )  and address1 <= "zhangsanaddress1";
select * from `Baikaltest`.`select` where height1 not in ( 1000 , 2000 ) and name1 in ( "zhangsan1" , "lisi2" );
select count(*) from `Baikaltest`.`select` where height1 in ( 1000 , 2000 ) and name1 <= "zhangsan1";
select count(1) from `Baikaltest`.`select` where height1 not in ( 1000 , 2000 ) and name1 < "zhangsan1";
select count(id) from `Baikaltest`.`select` where height1 not in ( 1000 , 2000 ) and name1 >= "zhangsan1";
select * from `Baikaltest`.`select` where height1 like  "%1000%" or name1 > "zhangsan1";
select * from `Baikaltest`.`select` where height1 not like  "%1000%" or name1 = "zhangsan1";
select * from `Baikaltest`.`select` order by height1, name1 desc ;
select height1,name1,count(*) from `Baikaltest`.`select` group by height1,address1 having height1 like "%1000%";



# 条件普通索引 & 普通字段
select * from `Baikaltest`.`select` where height1 = 1000 and address1 like  "%zhangsanaddress1%";
select * from `Baikaltest`.`select` where height1 > 1000 and address1 not like  "%zhangsanaddress1%";
select * from `Baikaltest`.`select` where height1 >= 1000 and address1 not in ( "zhangsanaddress1" , "lisiaddress2" );
select * from `Baikaltest`.`select` where height1 < 1000 and address1 in ( "zhangsanaddress1" , "lisiaddress2" );
select * from `Baikaltest`.`select` where height1 <= 1000 and address1 between "zhangsanaddress1"  and "lisiaddress2";
select * from `Baikaltest`.`select` where height1 between 1000  and 2000 and address1 < "zhangsanaddress1";
select * from `Baikaltest`.`select` where height1 in ( 1000 , 2000 )  and address1 <= "zhangsanaddress1";
select * from `Baikaltest`.`select` where height1 not in ( 1000 , 2000 ) and address1 >= "zhangsanaddress1";
select count(*) from `Baikaltest`.`select` where height1 in ( 1000 , 2000 ) and address1 = "zhangsanaddress1";
select count(1) from `Baikaltest`.`select` where height1 not in ( 1000 , 2000 ) and address1 > "zhangsanaddress1";
select count(id) from `Baikaltest`.`select` where height1 not in ( 1000 , 2000 ) and address1 not in ( "zhangsanaddress1" , "lisiaddress2" );
select * from `Baikaltest`.`select` where height1 like  "%1000%" or address1 not in ( "zhangsanaddress1" , "lisiaddress2" );
select * from `Baikaltest`.`select` where height1 not like  "%1000%" or address1 not in ( "zhangsanaddress1" , "lisiaddress2" );
select * from `Baikaltest`.`select` order by height1, address1 desc ;
select height1,address1,count(*) from `Baikaltest`.`select` group by height1,address1 having height1 like "%1000%";

