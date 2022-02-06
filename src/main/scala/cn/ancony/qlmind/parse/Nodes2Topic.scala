package cn.ancony.qlmind.parse

import cn.ancony.qlmind.QMDStarter.readFromFile
import cn.ancony.qlmind.parse.Node2Topic.notesContent
import cn.ancony.qlmind.util.Combine
import org.apache.hadoop.hive.ql.parse.{HiveParser, ParseUtils}
import org.xmind.core.ITopic

import java.util
import scala.collection.mutable
import scala.collection.JavaConverters._

class Nodes2Topic {
  /**
   * key: 文件中topic的类型。value:某类型下面的topic的集合。
   * topic的类型。
   * 0- 创建表的topic，仅仅是创建。不包括CTAS
   * 1- 临时表。包括temporary类型的表，以及在文件中创建，使用后删除掉的表
   * 2- 需要展示的表。如果最终展示的表只有一个，不管是否与文件同名，直接使用表名。
   * 如果最终展示的表多于一个，那么将所有要展示的topic挂在以文件名的topic之上。
   */
  val topics: mutable.Map[Int, Array[ITopic]] = mutable.Map[Int, Array[ITopic]]()

  val textTopic: ITopic => Boolean = (tpc: ITopic) => tpc != null && tpc.getTitleText.nonEmpty
  val labelTopic: ITopic => Boolean = (tpc: ITopic) => tpc != null && tpc.getLabels.asScala.nonEmpty
  val contentTopic: ITopic => Boolean = (tpc: ITopic) => tpc != null && notesContent(tpc).getTextContent.nonEmpty

  def getAllTopicsInFile2(fileName: String): Unit = {
    val hqlList = readFromFile(fileName).split(";").toList
    val res = for (hql <- hqlList) yield {
      val node = ParseUtils.parse(hql)
      node.getType match {
        case HiveParser.TOK_QUERY => ("_query", Node2Topic.tpcQuery(node))
        case HiveParser.TOK_CREATETABLE => ("_create", Node2Topic.tpcCreateTable(node))
        case HiveParser.TOK_DROPTABLE => ("_drop", Node2Topic.tpcDropTable(node))
      }
    }
    val topic_1: ITopic => Boolean = (tpc: ITopic) => textTopic(tpc) && labelTopic(tpc) && !contentTopic(tpc)
    //    val topic_2: ITopic => Boolean = (tpc: ITopic) => textTopic(tpc) && !labelTopic(tpc) && !contentTopic(tpc)
    val topic_3: ITopic => Boolean = (tpc: ITopic) => textTopic(tpc) && !labelTopic(tpc) && !contentTopic(tpc)
    val _0 = res.filter { case (id, tpc) => id.equals("_create") && topic_1(tpc) }.map(_._2)
    val _1 = res.filter { case (id, tpc) => id.equals("_drop") && topic_3(tpc) }.map(_._2)
    val _2 = res.map(_._2).filter(t => !_1.map(i => i.getTitleText).exists(i => t.getTitleText.equals(i)))
    topics += (0 -> _0.toArray)
    topics += (1 -> _1.toArray)
    topics += (2 -> _2.toArray)
    println()
  }

  def getAllTopicsInFile3(fileName: String): Map[String, Array[ITopic]] = {
    val hqlList = readFromFile(fileName).split(";").toList
    val res = for (hql <- hqlList) yield {
      val node = ParseUtils.parse(hql)
      node.getType match {
        case HiveParser.TOK_QUERY => ("_query", Node2Topic.tpcQuery(node))
        case HiveParser.TOK_CREATETABLE => ("_create", Node2Topic.tpcCreateTable(node))
        case HiveParser.TOK_DROPTABLE => ("_drop", Node2Topic.tpcDropTable(node))
      }
    }
    res.groupBy(_._1).map(i => (i._1, i._2.map(_._2).toArray))
  }

  def combineSqlInSingleFile(topics: Map[String, Array[ITopic]]): Array[ITopic] = {
    //要挂载的topic
    val toLinkTopicsText = topics("_drop").map(_.getTitleText)
    val createTopics = topics("_create")
    //要添加label的topic
    val toLinkLabelTopics = createTopics.filter(_.getLabels.size() > 0)
    val toLinkTopicsLabelMap = toLinkLabelTopics.map(i => (i.getTitleText, i.getLabels)).toMap
    //要处理的其他的topic
    val toDeal = createTopics.filter(_.getLabels.size() == 0) ++ topics("_query")
    toDeal.foreach(i => {
      val text = i.getTitleText
      //添加label
      if (toLinkTopicsLabelMap.contains(text)) {
        i.setLabels(toLinkTopicsLabelMap(text))
      }
    })
    //添加引用
    Combine.linkAndRemoveTopics(toDeal, toLinkTopicsText)
//    Combine.linkTopics(toDeal)
    toDeal.filter(i => toLinkTopicsText.exists(i.getTitleText.equals))
//    toDeal
  }

  def combine(fileName: String): Array[ITopic] = {
    combineSqlInSingleFile(getAllTopicsInFile3(fileName))
  }
}

object Nodes2Topic {

}