package cn.ancony.qlmind.util

import org.apache.hadoop.hive.ql.parse.ASTNode
import org.xmind.core.{Core, INotes, IPlainNotesContent, ITopic, IWorkbook}

import scala.collection.mutable
import scala.collection.JavaConverters._

object TopicUtils {

  val wb: IWorkbook = Core.getWorkbookBuilder.createWorkbook()

  val childrenASTNode: ASTNode => mutable.Buffer[ASTNode] =
    (node: ASTNode) => node.getChildren.asScala.map(_.asInstanceOf[ASTNode])
  val notesContent: ITopic => IPlainNotesContent = (topic: ITopic) =>
    topic.getNotes.getContent(INotes.PLAIN).asInstanceOf[IPlainNotesContent]
  val emptyTextTopic: ITopic => Boolean = (tpc: ITopic) =>
    tpc == null || tpc.getTitleText.isEmpty
  val emptyTopic: ITopic = topic("")
  val emptyContentTopic: ITopic => Boolean = (tpc: ITopic) =>
    tpc == null || notesContent(tpc).getTextContent.isEmpty
  val leaves: ITopic => Array[ITopic] = (tpc: ITopic) => getSubTopicRecursion(tpc, _ => true)
  val rmAdditionalInfo: String => String = (text: String) => {
    val idx = text.indexOf('(')
    if (idx == -1) text else text.slice(0, idx)
  }
  val text: ITopic => String = (tpc: ITopic) => tpc.getTitleText
  val rmAlias: ITopic => String = (tpc: ITopic) => rmAdditionalInfo(text(tpc))
  val hasChild: ITopic => Boolean = (tpc: ITopic) => tpc.getAllChildren.size() > 0
  val hasLabel: ITopic => Boolean = (tpc: ITopic) => tpc.getLabels.size() > 0

  def topic(text: String, labels: Array[String] = Array(), content: String = ""): ITopic = {
    val tpc = wb.createTopic()
    tpc.setTitleText(text)
    if (labels.nonEmpty) tpc.setLabels(labels.toList.asJava)
    val pc = wb.createNotesContent(INotes.PLAIN).asInstanceOf[IPlainNotesContent]
    pc.setTextContent(content)
    tpc.getNotes.setContent(INotes.PLAIN, pc)
    tpc
  }

  val topicOnlyContent: String => ITopic =
    (content: String) => topic("", Array(), content)

  //合并topic的内容，忽略其他
  def topicMergeContent(topics: ITopic*): ITopic = {
    val content = topics.map(notesContent).map(_.getTextContent).mkString("\n")
    notesContent(topics.head).setTextContent(content)
    topics.head
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
