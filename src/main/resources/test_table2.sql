create table ads.t_t1
(
    col1 string,
    col2 string,
    col3 string
) comment '最终结果表t_t1';

create table temp.t_temp1
(
    col1 string,
    col2 string,
    col3 string
);

insert overwrite table temp.t_temp1
select col1, col2, col3
from ods.origin_1;

insert overwrite table ads.t_t1
select col1, col2, col3
from temp.t_temp1;

drop table temp.t_temp1;
