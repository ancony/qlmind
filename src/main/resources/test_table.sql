CREATE TABLE ads.test_table
(
    id   INT,
    name STRING,
    info STRING
)
    COMMENT '测试表'
    PARTITIONED BY (dat STRING);

CREATE TABLE temp.tmp_table AS
SELECT a.id AS id, b.name as name
FROM ods.before_table a
         LEFT JOIN dwd.base_table b ON a.id = b.uid;

CREATE TABLE temp.tmp_table2 AS
SELECT a.id AS id, b.name as name
FROM ods.before_table a
         LEFT JOIN temp.tmp_table b ON a.id = b.uid;

INSERT OVERWRITE TABLE ads.test_table PARTITION (dat = '20220102')
SELECT id, name, 'info' AS info
FROM temp.tmp_table a
         LEFT JOIN temp.tmp_table2 b
                   on a.id = b.id
;

DROP TABLE temp.tmp_table;
DROP TABLE temp.tmp_table2;