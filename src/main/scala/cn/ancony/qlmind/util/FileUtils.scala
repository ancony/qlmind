package cn.ancony.qlmind.util

import cn.ancony.qlmind.parse.Node2Topic.{tpcCreateTable, tpcDropTable, tpcQuery}
import cn.ancony.qlmind.util.TopicUtils.wb
import org.apache.hadoop.hive.ql.parse.{HiveParser, ParseUtils}
import org.xmind.core.ITopic

import scala.collection.mutable
import scala.io.Source

object FileUtils {

  def readFromFile(fileName: String): String = {
    val fn = Source.fromFile(fileName)
    val lines = fn.getLines().map(_.trim).filterNot(_.startsWith("--")).filterNot(_.startsWith("#")).mkString("\n")
    fn.close()
    lines
  }

  def getExtendName(name: String): String = {
    val dotPos = name.lastIndexOf(".")
    name.slice(dotPos + 1, name.length)
  }

  def simpleName(name: String): String = {
    val idx = name.lastIndexOf("\\")
    val dotPos = name.lastIndexOf(".")
    val stop = if (dotPos == -1) name.length else dotPos
    if (idx == -1) name.slice(0, stop) else name.slice(idx + 1, stop)
  }

  def getAllTopicsInFile(fileName: String): Map[String, mutable.Buffer[ITopic]] = {
    val text = readFromFile(fileName)
    val res = for (hql <- text.split(";").toList) yield {
      val node = ParseUtils.parse(hql)
      node.getType match {
        case HiveParser.TOK_QUERY => ("_query", tpcQuery(node))
        case HiveParser.TOK_CREATETABLE => ("_create", tpcCreateTable(node))
        case HiveParser.TOK_DROPTABLE => ("_drop", tpcDropTable(node))
      }
    }
    val rtn = res.groupBy(_._1).map(i => (i._1, i._2.map(_._2).toBuffer))
    rtn
  }

  def save(topic: ITopic, rootText: String, file: String): Unit = {
    val root = wb.getPrimarySheet.getRootTopic
    root.setTitleText(rootText)
    root.add(topic)
    wb.save(file)
  }

}
