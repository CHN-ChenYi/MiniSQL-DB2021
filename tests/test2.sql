-- 为了使执行时间更有对比度，以下测试仅在10w数据集上进行，1w数据集仅进行装载/删除表操作

-- time begin without index
insert into student2 values (56789,'abcd',50);
select * from student2 where name = "name90000"; --name9000
-- time end

-- time begin with index
create index stunameidx on student2 ( name );
select * from student2 where name = "name90000"; --name9000
drop index stunameidx on student2;
-- time end

-- test delete
delete from student2 where name = "name90000";
select * from student2 where name = "name90000";

-- test and
select * from student2 where score = 100 and name="name90000"; --(x))    "name89958"(✔)

-- test > < <> (optional)
select * from student2 where score > 99;
