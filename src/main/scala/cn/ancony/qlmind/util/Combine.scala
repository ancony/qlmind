package cn.ancony.qlmind.util

import org.xmind.core.ITopic

import scala.collection.JavaConverters._

object Combine {

  def combineTopics(topics: Array[ITopic]): Unit = {
    val rmRef = (text: String) => text.slice(0, text.indexOf('('))
    val leaves = (tpc: ITopic) => getSubTopicRecursion(tpc, _ => true)
    val text = (tpc: ITopic) => tpc.getTitleText
    val fromTopics = topics.map(leaves).flatten
    for (elem <- topics) fromTopics
      .filter(f => rmRef(text(f)).equals(text(elem)) || text(f).equals(text(elem)))
      .foreach(addLink(_, elem))
  }

  /**
   * 递归获取topic中符合条件的子topic
   *
   * @param topic 要查找的topic
   * @param func  查找条件
   * @return 返回条件为true的子topic数组
   */
  def getSubTopicRecursion(topic: ITopic, func: ITopic => Boolean): Array[ITopic] = {
    if (topic == null) return Array()
    if (topic.getAllChildren.size() == 0 && func(topic)) Array(topic)
    else (for (child <- topic.getAllChildren.asScala)
      yield getSubTopicRecursion(child, func)).flatten.toArray
  }

  def addLink(from: ITopic, to: ITopic): Unit = from.setHyperlink("xmind:#" + to.getId)

}
