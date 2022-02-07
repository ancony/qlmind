package cn.ancony.qlmind.parse

import org.apache.hadoop.hive.ql.parse.ParseUtils
import org.scalatest.funsuite.AnyFunSuite

class TestSelect extends AnyFunSuite {
  val hql = "SELECT *\nFROM source TABLESAMPLE(BUCKET 3 OUT OF 32 ON rand()) s"
  val hql2 =
    """
      |SELECT NULL as col1, col2, row_number() over() row_num, co13/cast(col4 as bigint)
      |FROM ads.tbl_name a
      |LATERAL VIEW EXPLODE(col,"^") a AS ex_col
      |WHERE a.pt_d='20220207'
      |""".stripMargin
  val hql3 = "SELECT pageid, adid FROM pageAds LATERAL VIEW explode(adid_list) adTable as adid"

  test("test select") {
    val node = ParseUtils.parse(hql)
    println(node.dump())
    val topic = Node2Topic.tpcQuery(node)
    Node2Topic.save(topic, "ct", "ct.xmind")
  }
  test("test select2") {
    val node = ParseUtils.parse(hql2)
    println(node.dump())
    val topic = Node2Topic.tpcQuery(node)
    Node2Topic.save(topic, "ct", "ct.xmind")
  }
}
