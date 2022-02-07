package cn.ancony.qlmind.parse

import cn.ancony.qlmind.QMDStarter
import cn.ancony.qlmind.QMDStarter.{combineAndSave, getAllTopicsInFile, simpleName}
import cn.ancony.qlmind.parse.Node2Topic.{emptyContentTopic, notesContent, topic, tpcCreateTable, tpcDropTable, tpcQuery}
import cn.ancony.qlmind.util.Combine
import org.apache.hadoop.hive.ql.parse.{HiveParser, ParseUtils}
import org.scalatest.funsuite.AnyFunSuite
import org.xmind.core.ITopic

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

  def getAllTopicsInFile2(fileName: String): Map[String, Array[ITopic]] = {
    val text = QMDStarter.readFromFile(fileName)
    val res = for (hql <- text.split(";").toList) yield {
      val node = ParseUtils.parse(hql)
      node.getType match {
        case HiveParser.TOK_QUERY => ("_query", tpcQuery(node))
        case HiveParser.TOK_CREATETABLE => ("_create", tpcCreateTable(node))
        case HiveParser.TOK_DROPTABLE => ("_drop", tpcDropTable(node))
      }
    }
    val rtn = res.groupBy(_._1).map(i => (i._1, i._2.map(_._2).toArray))
    rtn
  }

  def compactLabel(topics: Map[String, Array[ITopic]]): Unit = {
    val createAndInsert = topics.filterNot(i => i._1.equals("_drop")).values.flatten
    val grouped = createAndInsert.groupBy(_.getTitleText)
    val hasLb = (tpc: ITopic) => tpc.getLabels.size() > 0
    for (pair <- grouped) {
      val pairArr = pair._2.toArray
      if (pairArr.length > 1 && pairArr.exists(hasLb)) {
        val hasLabel = pairArr.filter(hasLb).head
        pairArr.filterNot(hasLb).foreach(i => i.setLabels(hasLabel.getLabels))
      }
    }
  }

  def getToLink(topics: Map[String, Array[ITopic]]): (Array[ITopic], Array[String]) = {
    val noChildTopic: ITopic => Boolean = (tpc: ITopic) => tpc.getAllChildren.size() == 0

    val toLinkTopicNames = topics.filter(_._1.equals("_drop")).values.flatten.map(_.getTitleText).toArray
    val flatten = topics.values.flatten
    (flatten.filterNot(noChildTopic).toArray, toLinkTopicNames)
  }

  def file(fileName: String): Unit = {
    val topics = getAllTopicsInFile2(fileName)
    compactLabel(topics)
    val toLink = getToLink(topics)
    val tpc = topic("test")
    toLink._1.foreach(tpc.add)
    println(toLink._2.mkString("\n"))
    //todo link
    Node2Topic.save(tpc, "qmd", "hqlFile.xmind")
  }

  test("test sql in file2") {
    new TestHqlFile().file("D:\\mixed\\qlmind\\src\\main\\resources\\test_table2.sql")
  }
}
