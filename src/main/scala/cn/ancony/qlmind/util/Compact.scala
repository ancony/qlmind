package cn.ancony.qlmind.util

import cn.ancony.qlmind.util.FileUtils.{getAllTopicsInFile, simpleName}
import cn.ancony.qlmind.util.TopicUtils.{addLink, hasChild, hasLabel, leaves, rmAlias, text, topic}
import org.xmind.core.ITopic

import scala.collection.JavaConverters._

object Compact {

  def linkTopics(topics: Array[ITopic]): Unit = {
    val allLeaves = topics.map(leaves).flatten
    for (c <- topics.filter(hasChild); t = text(c)) {
      allLeaves.filter(l => rmAlias(l).equals(t)).foreach(addLink(_, c))
    }
  }

  def compactLabel(topicMap: Map[String, Array[ITopic]]): Unit = {
    val createAndInsert = topicMap.filterNot(i => i._1.equals("_drop")).values.flatten
    for ((_, tpc) <- createAndInsert.groupBy(text);
         topics = tpc.toArray if topics.length > 1 && topics.exists(hasLabel))
      topics.filterNot(hasLabel).foreach(i => i.setLabels(topics.filter(hasLabel).head.getLabels))
  }

  def mount(topics: Array[ITopic]): Array[ITopic] = {
    val allLeaves = topics.map(leaves).flatten
    val order = allLeaves.map(rmAlias).map((_, 1))
      .groupBy(_._1).mapValues(_.length).toArray
      .sortBy(_._2)(Ordering.Int.reverse).map(_._1)
    for (toMount <- order.intersect(topics.map(text))) {
      val allBeMountCollection = allLeaves.filter(l => rmAlias(l).equals(toMount))
      val head = allBeMountCollection.head
      topics.filter(i => text(i).equals(toMount)).head.getAllChildren.asScala.foreach(head.add)
      allBeMountCollection.tail.foreach(addLink(_, head))
    }
    topics.filter(hasChild)
  }

  //default, merge/link选一个或者no
  def fileMerge(fileName: String, merge: Boolean): ITopic = {
    val topicMap = getAllTopicsInFile(fileName)
    val topics = if (merge) {
      compactLabel(topicMap)
      mount(topicMap.values.flatten.filter(hasChild).toArray)
    } else topicMap.values.flatten.toArray
    rtnTopic(topics, simpleName(fileName))
  }

  def rtnTopic(topics: Array[ITopic], name: String = "topics"): ITopic = {
    require(topics.nonEmpty)
    if (topics.length == 1) topics.head
    else {
      val tpc = topic(name)
      topics.foreach(tpc.add)
      tpc
    }
  }

  def fileLink(fileName: String, link: Boolean): ITopic = {
    val topics = getAllTopicsInFile(fileName).values.flatten.toArray
    if (link) linkTopics(topics)
    rtnTopic(topics, simpleName(fileName))
  }

}
