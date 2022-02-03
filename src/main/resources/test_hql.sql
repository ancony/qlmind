INSERT OVERWRITE TABLE b.a
SELECT col1, col2
FROM b.t_a;
-- comment
INSERT OVERWRITE TABLE b.b
SELECT col1, col2
FROM b.a;

INSERT OVERWRITE TABLE b.c
SELECT a.col1, b.col2
FROM b.a a
         left join b.b b on a.col1 = b.col2;
