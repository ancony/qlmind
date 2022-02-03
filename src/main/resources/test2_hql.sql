INSERT OVERWRITE TABLE b.d
SELECT col1, col2
FROM b.t_a;
-- comment
INSERT OVERWRITE TABLE b.e
SELECT col1, col2
FROM b.a;

INSERT OVERWRITE TABLE b.f
SELECT a.col1, b.col2
FROM b.e a
         left join b.d b on a.col1 = b.col2;
