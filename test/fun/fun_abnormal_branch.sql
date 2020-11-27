# unix_timestamp
select unix_timestamp(1);
select unix_timestamp();
select unix_timestamp(null);
# 随机数
SELECT RAND();
select RAND(100);
# 现在时间
select now();

# sql_parser error
select * Frmo Baikaltest.planinfo;
select select * Frmo Baikaltest.planinfo;
selectt  * from Baikaltest.planinfo;
insert ioto Baikaltest.t_student2 values(,,,,);
replace ioto Baikaltest.t_student2 values(,,,,);
delete Form Baikaltest.planinfo;
# 不支持正则，现在报语法错误
SELECT name1 FROM Baikaltest.t_student2 WHERE name1 REGEXP '^[aeiou]|ok$';
select 'beijing' REGEXP 'jing';
select 'beijing' REGEXP 'xi';
select 2<=>3;
select null<=>null;

# null
select name1 from Baikaltest.t_student2 group by NULL;
select name1,count(*) from Baikaltest.t_student2 group by NULL;

