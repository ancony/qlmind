package cn.ancony.qlmind.parse

import cn.ancony.qlmind.QMDStarter
import cn.ancony.qlmind.parse.Node2Topic.topic
import cn.ancony.qlmind.util.Combine
import org.apache.hadoop.hive.ql.parse.ParseUtils
import org.scalatest.funsuite.AnyFunSuite

class TestHqlFile extends AnyFunSuite {
  val hql1 = "create table ads.test_table\n(\n    id          int,\n    dtDontQuery string,\n    name        string\n)\n    comment '测试表'\n    partitioned by (dat string)"
  val hql2 = "create table temp.tmp_tabl1 as\nselect a, b\nfrom ads.before_table a\n         left join ads.base_table b on a.id = b.uid"
  val hql3 = "insert overwrite table ads.test_table partition (dat = '20220102')\nselect a, b, 'name'\nfrom temp.tmp_tabl1"
  val hql4 = "drop table temp.tmp_tabl1"

  test("union create,insert,drop hql") {
    val topics = new Nodes2Topic().combine("D:\\mixed\\qlmind\\src\\main\\resources\\test_table.sql")

        val root = Node2Topic.wb.getPrimarySheet.getRootTopic
        root.setTitleText("test")
        Node2Topic.wb.save("hqlFile.xmind")

//    val tpc = topic("test")
//    topics.foreach(tpc.add)
//    Node2Topic.save(tpc, "qmd", "hqlFile.xmind")
  }
}
