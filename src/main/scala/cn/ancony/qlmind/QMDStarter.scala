package cn.ancony.qlmind

import cn.ancony.qlmind.parse.Node2Topic
import cn.ancony.qlmind.parse.Node2Topic.topic
import cn.ancony.qlmind.util.Combine
import org.apache.hadoop.hive.ql.parse.ParseUtils
import org.xmind.core.ITopic

import java.io.{File, FileFilter}
import scala.io.Source

object QMDStarter {
  def main(args: Array[String]): Unit = {
    //    if (args.length != 2) {
    //      println(
    //        s"""
    //           |Usage: qmd -e "select a from b"
    //           |qmd -f a.hql
    //           |qmd -d .
    //           |""".stripMargin)
    //      System.exit(0)
    //    }
    //    args(0) match {
    //      case "-e" => string(args(1))
    //      case "-f" =>
    //      case "-d" =>
    //    }
    //    file("D:\\mixed\\qlmind\\src\\main\\resources\\test_table.sql")
    dir("D:\\mixed\\qlmind\\src\\main\\resources",false)
  }

  def dir(dirName: String, merge: Boolean): Unit = {
    val files = getSqlFiles(dirName)
    if (merge) combineAndSave(files.flatMap(getAllTopicsInFile), simpleName(dirName))
    else files.foreach(file)
  }

  def file(fileName: String): Unit = {
    val topics = getAllTopicsInFile(fileName)
    combineAndSave(topics, simpleName(fileName))
  }

  def getSqlFiles(dirName: String): Array[String] = new File(dirName)
    .listFiles(new FileFilter {
      override def accept(f: File): Boolean = f.isFile
    })
    .map(_.getAbsolutePath)
    .filter(getExtendName(_).equals("sql"))

  def combineAndSave(topics: Array[ITopic], supTopicTitleText: String): Unit = {
    Combine.linkTopics(topics)
    val tpc = topic(supTopicTitleText)
    topics.foreach(tpc.add)
    Node2Topic.save(tpc, "qmd", "hqlFile.xmind")
  }

  def getAllTopicsInFile(fileName: String): Array[ITopic] = readFromFile(fileName)
    .split(";")
    .map { hql => Node2Topic.tpcQuery(ParseUtils.parse(hql)) }

  def readFromFile(fileName: String): String = {
    val fn = Source.fromFile(fileName)
    val lines = fn.getLines().map(_.trim).filterNot(_.startsWith("--")).mkString("\n")
    fn.close()
    lines
  }

  def simpleName(name: String): String = {
    val idx = name.lastIndexOf("\\")
    val dotPos = name.lastIndexOf(".")
    val stop = if (dotPos == -1) name.length else dotPos
    if (idx == -1) name.slice(0, stop) else name.slice(idx + 1, stop)
  }

  def string(hql: String): Unit = {
    val node = ParseUtils.parse(hql)
    val topic = Node2Topic.tpcQuery(node)
    Node2Topic.save(topic, "qmd", "hqlString.xmind")
  }

  def getExtendName(name: String): String = {
    val dotPos = name.lastIndexOf(".")
    name.slice(dotPos + 1, name.length)
  }
}
