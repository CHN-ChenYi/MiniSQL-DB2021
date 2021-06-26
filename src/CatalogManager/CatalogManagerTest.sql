create table student (
        sno char(8),
        sname char(16) unique,
        sage int,
        sgender char (1),
        score float,
        primary key ( sno )
);
create index stunameidx on student ( sname );
drop index stunameidx on student;
create index stunameidx on student ( sname );
drop table student;
