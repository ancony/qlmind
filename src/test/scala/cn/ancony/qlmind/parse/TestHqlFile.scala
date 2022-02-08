package cn.ancony.qlmind.parse

import cn.ancony.qlmind.util.FileUtils.save
import cn.ancony.qlmind.util.Compact
import org.scalatest.funsuite.AnyFunSuite

class TestHqlFile extends AnyFunSuite {
  val hql1 = "create table ads.test_table\n(\n    id          int,\n    dtDontQuery string,\n    name        string\n)\n    comment '测试表'\n    partitioned by (dat string)"
  val hql2 = "create table temp.tmp_tabl1 as\nselect a, b\nfrom ads.before_table a\n         left join ads.base_table b on a.id = b.uid"
  val hql3 = "insert overwrite table ads.test_table partition (dat = '20220102')\nselect a, b, 'name'\nfrom temp.tmp_tabl1"
  val hql4 = "drop table temp.tmp_tabl1"

  test("test sql in file2") {
//    new TestHqlFile().file("D:\\mixed\\qlmind\\src\\main\\resources\\test_table2.sql")
//    val tpc=Combine.fil("D:\\mixed\\qlmind\\src\\main\\resources\\test_table2.sql")
//    Node2Topic.save(tpc,"test","test.xmind")
    val tpc=Compact.fileMerge("D:\\mixed\\qlmind\\src\\main\\resources\\test_table2.sql",true)
    save(tpc,"test","test.xmind")
  }
}
