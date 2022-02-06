package cn.ancony.qlmind.parse

import org.apache.hadoop.hive.ql.parse.ParseUtils
import org.scalatest.funsuite.AnyFunSuite

class TestCreateTable extends AnyFunSuite {
  val hql1 =
    """
      |create table table_name (
      |  id                int,
      |  dtDontQuery       string,
      |  name              string
      |)
      |comment '测试表'
      |partitioned by (dat string)
      |
      |""".stripMargin
  val hql2 =
    """
      |CREATE TABLE new_key_value_store
      |   ROW FORMAT SERDE "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe"
      |   STORED AS RCFile
      |   AS
      |SELECT (key % 1024) new_key, concat(key, value) key_value_pair
      |FROM key_value_store
      |SORT BY new_key, key_value_pair
      |""".stripMargin
  val hql3 =
    """
      |CREATE TABLE page_view(viewTime INT, userid BIGINT,
      |     page_url STRING, referrer_url STRING,
      |     ip STRING COMMENT 'IP Address of the User')
      | COMMENT 'This is the page view table'
      | PARTITIONED BY(dt STRING, country STRING)
      | CLUSTERED BY(userid) SORTED BY(viewTime) INTO 32 BUCKETS
      | ROW FORMAT DELIMITED
      |   FIELDS TERMINATED BY '\001'
      |   COLLECTION ITEMS TERMINATED BY '\002'
      |   MAP KEYS TERMINATED BY '\003'
      | STORED AS SEQUENCEFILE
      |""".stripMargin
  val hql4 =
    """
      |CREATE TABLE list_bucket_single (key STRING, value STRING)
      |  SKEWED BY (key) ON (1,5,6) STORED AS DIRECTORIES
      |""".stripMargin
  val hql5 =
    """
      |CREATE TEMPORARY TABLE list_bucket_multiple (col1 STRING, col2 int, col3 STRING)
      |
      |""".stripMargin
  val hql6 =
    """
      |CREATE TRANSACTIONAL TABLE transactional_table_test(key string, value string) PARTITIONED BY(ds string) STORED AS ORC
      |
      |""".stripMargin
  val hql7 =
    """
      |create table pk(id1 integer, id2 integer,
      |  primary key(id1, id2) disable novalidate)
      |""".stripMargin
  val hql8 =
    """
      |create table fk(id1 integer, id2 integer,
      |  constraint c1 foreign key(id1, id2) references pk(id2, id1) disable novalidate)
      |""".stripMargin

  //  @Test
  //  def testCreatTable1(): Unit = {
  //    val node = ParseUtils.parse(hql2)
  //    println(node.dump())
  //    val topic = Node2Topic.tpcCreateTable(node)
  //    Node2Topic.save(topic, "ct", "ct.xmind")
  //  }

  test("test create table") {
    val node = ParseUtils.parse(hql2)
    println(node.dump())
    val topic = Node2Topic.tpcCreateTable(node)
    Node2Topic.save(topic, "ct", "ct.xmind")
  }
}
