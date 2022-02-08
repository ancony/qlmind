package cn.ancony.qlmind

import cn.ancony.qlmind.parse.Node2Topic
import cn.ancony.qlmind.util.Compact
import cn.ancony.qlmind.util.FileUtils.{getExtendName, save}
import org.apache.hadoop.hive.ql.parse.ParseUtils

import java.io.{File, FileFilter}

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
    file("D:\\mixed\\qlmind\\src\\main\\resources\\test_table.sql")
  }

  def file(fileName: String): Unit = {
    val topic = Compact.fileMerge(fileName, true)
    save(topic, "qmd", "hqlString.xmind")
  }

  def getSqlFiles(dirName: String): Array[String] = new File(dirName)
    .listFiles(new FileFilter {
      override def accept(f: File): Boolean = f.isFile
    })
    .map(_.getAbsolutePath)
    .filter(getExtendName(_).equals("sql"))

  def string(hql: String): Unit = {
    val node = ParseUtils.parse(hql)
    val topic = Node2Topic.tpcQuery(node)
    save(topic, "qmd", "hqlString.xmind")
  }

}
