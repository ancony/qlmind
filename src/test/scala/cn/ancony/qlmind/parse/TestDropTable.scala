package cn.ancony.qlmind.parse

import cn.ancony.qlmind.util.FileUtils.save
import org.apache.hadoop.hive.ql.parse.ParseUtils
import org.scalatest.funsuite.AnyFunSuite

class TestDropTable extends AnyFunSuite {
  val hql1 = "DROP TABLE IF EXISTS table_name PURGE"
  val hql2 =
    """
      |TRUNCATE TABLE table_name PARTITION (source_type = 'music', pt_d = '20210123')
      |""".stripMargin
  val hql3 = "TRUNCATE TABLE table_name"

  test("test drop table") {
    val node = ParseUtils.parse(hql3)
    println(node.dump())
    val topic = Node2Topic.tpcTruncateTable(node)
    save(topic, "ct", "ct.xmind")
  }
  test("test drop table2") {
    val node = ParseUtils.parse(hql3)
    println(node.dump())
    val topic = Node2Topic.tpcTruncateTable(node)
    save(topic, "ct", "ct.xmind")
  }
}
