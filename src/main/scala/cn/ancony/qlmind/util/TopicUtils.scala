package cn.ancony.qlmind.util

import org.apache.hadoop.hive.ql.parse.ASTNode
import org.xmind.core.{Core, INotes, IPlainNotesContent, ITopic, IWorkbook}

import scala.collection.mutable
import scala.collection.JavaConverters._

object TopicUtils {

  val wb: IWorkbook = Core.getWorkbookBuilder.createWorkbook()

  val childrenASTNode: ASTNode => mutable.Buffer[ASTNode] =
    node => node.getChildren.asScala.map(_.asInstanceOf[ASTNode])

  val rmAdditionalInfo: String => String = text => {
    val idx = text.indexOf('(')
    if (idx == -1) text else text.slice(0, idx)
  }

  val notesContent: ITopic => IPlainNotesContent = tpc =>
    tpc.getNotes.getContent(INotes.PLAIN).asInstanceOf[IPlainNotesContent]
  val getText: IPlainNotesContent => String = note => note.getTextContent
  val setText: (IPlainNotesContent, String) => Unit = (note, content) => note.setTextContent(content)

  def complement[A](pre: A => Boolean): A => Boolean = a => !pre(a)

  def any[A](predicates: (A => Boolean)*): A => Boolean =
    a => predicates.exists(p => p(a))

  def none[A](predicates: (A => Boolean)*): A => Boolean =
    complement(any(predicates: _*))

  def every[A](predicates: (A => Boolean)*): A => Boolean =
    none(predicates.view.map(complement(_)): _*)

  type TopicString = ITopic => String
  type TopicFilter = ITopic => Boolean

  val text: TopicString = tpc => tpc.getTitleText
  val rmAlias: TopicString = text.andThen(rmAdditionalInfo)
  val getContent: TopicString = notesContent.andThen(getText)
  val setContent: (ITopic, String) => Unit = (tpc, content) => setText(notesContent(tpc), content)

  val noTitleText: TopicFilter = tpc => text(tpc).isEmpty
  val noContent: TopicFilter = tpc => getContent(tpc).isEmpty
  val noChild: TopicFilter = tpc => tpc.getAllChildren.size() == 0
  val noLabel: TopicFilter = tpc => tpc.getLabels.size() == 0
  val hasTitleText: TopicFilter = complement(noTitleText)
  val hasContent: TopicFilter = complement(noContent)
  val hasChild: TopicFilter = complement(noChild)
  val hasLabel: TopicFilter = complement(noLabel)

  val emptyTopic: ITopic = topic("")
  val topicOnlyContent: String => ITopic =
    (content: String) => topic("", Array(), content)

  val leaves: ITopic => mutable.Buffer[ITopic] = tpc => getSubTopicRecursion(tpc, _ => true)

  def topic(text: String, labels: Array[String] = Array(), content: String = ""): ITopic = {
    val tpc = wb.createTopic()
    tpc.setTitleText(text)
    if (labels.nonEmpty) tpc.setLabels(labels.toList.asJava)
    val pc = wb.createNotesContent(INotes.PLAIN).asInstanceOf[IPlainNotesContent]
    pc.setTextContent(content)
    tpc.getNotes.setContent(INotes.PLAIN, pc)
    tpc
  }

  //合并topic的内容，忽略其他
  def topicMergeContent(topics: ITopic*): ITopic = {
    val content = topics.map(getContent).mkString("\n")
    setContent(topics.head, content)
    topics.head
  }

  /**
   * 递归获取topic中符合条件的子topic
   *
   * @param tpc  要查找的topic
   * @param func 查找条件
   * @return 返回条件为true的子topic数组
   */
  def getSubTopicRecursion(tpc: ITopic, func: TopicFilter): mutable.Buffer[ITopic] = {
    if (tpc == null) return mutable.Buffer()
    if (every(noChild, func)(tpc)) mutable.Buffer(tpc)
    else (for (child <- tpc.getAllChildren.asScala)
      yield getSubTopicRecursion(child, func)).flatten
  }

  def addLink(from: ITopic, to: ITopic): Unit = from.setHyperlink("xmind:#" + to.getId)

}
