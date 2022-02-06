package cn.ancony.qlmind.parse

import org.apache.hadoop.hive.ql.parse.ParseUtils
import org.scalatest.funsuite.AnyFunSuite

class TestSelect extends AnyFunSuite {
  val hql = "SELECT *\nFROM source TABLESAMPLE(BUCKET 3 OUT OF 32 ON rand()) s"

  test("test select") {
    val node = ParseUtils.parse(hql)
    println(node.dump())
    val topic = Node2Topic.tpcQuery(node)
    Node2Topic.save(topic, "ct", "ct.xmind")
  }
}
