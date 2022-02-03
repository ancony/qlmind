package cn.ancony.qlmind.parse

import cn.ancony.qlmind.parse.Node2String._
import org.apache.hadoop.hive.ql.parse.{ASTNode, HiveParser}
import org.xmind.core._

import scala.collection.JavaConverters._
import scala.collection.mutable

object Node2Topic {

  val wb: IWorkbook = Core.getWorkbookBuilder.createWorkbook()

  val childrenASTNode: ASTNode => mutable.Buffer[ASTNode] =
    (node: ASTNode) => node.getChildren.asScala.map(_.asInstanceOf[ASTNode])
  val notesContent: ITopic => IPlainNotesContent = (topic: ITopic) =>
    topic.getNotes.getContent(INotes.PLAIN).asInstanceOf[IPlainNotesContent]
  val emptyTextTopic: ITopic => Boolean = (tpc: ITopic) =>
    tpc == null || tpc.getTitleText.isEmpty
  val emptyContentTopic: ITopic => Boolean = (tpc: ITopic) =>
    tpc == null || notesContent(tpc).getTextContent.isEmpty

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

  def tpcTabName(node: ASTNode): ITopic = topic(getTabName(node))

  def tpcTabRef(node: ASTNode): ITopic = topic(getTabRef(node))

  def tpcEqual(node: ASTNode): ITopic = topicOnlyContent(getRelationshipCompare(node))

  def tpcAnd(node: ASTNode): ITopic = topicOnlyContent(getAnd(node))

  def tpcJoin(node: ASTNode): ITopic = {
    require((joinTypeMap.keys ++ Array(HiveParser.TOK_SUBQUERY)).exists(_ == node.getType))
    val res = for (elem <- childrenASTNode(node)) yield
      elem.getType match {
        case HiveParser.TOK_TABREF => tpcTabRef(elem)
        //关系比较操作返回内容
        case HiveParser.NOTEQUAL |
             HiveParser.EQUAL => tpcEqual(elem)
        case HiveParser.KW_AND => tpcAnd(elem)
        case _ if joinTypeMap.keys.exists(_ == elem.getType) => tpcJoin(elem)
        case HiveParser.TOK_SUBQUERY => tpcSubQuery(elem)
        case _ => throw new RuntimeException(unknownType(elem))
      }

    val contents = res.filter(t => emptyTextTopic(t) && t.getAllChildren.size() == 0)
    //设置contents的连接条件
    contents.map(notesContent).foreach(i => i.setTextContent(joinTypeMap(node.getType) + " ON " + i.getTextContent))
    val tabRef = res.filter(t => !emptyTextTopic(t) && emptyContentTopic(t)) //当前获得的
    val subJoin = res.filter(t => emptyTextTopic(t) && t.getAllChildren.size() != 0) //子循环获得的
    //合并结果为连接条件，放到最后一个topic中。
    topicMergeContent(Array(tabRef.last) ++ contents: _*)
    //获得要返回的topic
    val rtnTpc = if (subJoin.nonEmpty) subJoin.head else topic("")
    tabRef.foreach(rtnTpc.add) //当前的加到子循环的结果上。
    rtnTpc
  }

  def tpcUnionAll(node: ASTNode): ITopic = {
    require(unionType.contains(node.getType))
    val unionString = node.getType match {
      case HiveParser.TOK_UNIONALL => "UNION ALL"
      case HiveParser.TOK_UNIONDISTINCT => "UNION DISTINCT"
    }
    val res = for (elem <- childrenASTNode(node)) yield {
      elem.getType match {
        case HiveParser.TOK_QUERY => tpcQuery(elem)
        case HiveParser.TOK_UNIONALL => tpcUnionAll(elem)
        case _ => throw new RuntimeException(unknownType(elem))
      }
    }
    val tpc = topic("_select")
    res.flatMap(_.getAllChildren.asScala).foreach(tpc.add)
    val ch = tpc.getAllChildren.asScala
    Array(ch.takeRight(2).head).map(notesContent).foreach(i => i.setTextContent(i.getTextContent + "\n" + unionString))
    tpc
  }

  def tpcSubQuery(node: ASTNode): ITopic = {
    require(node.getType == HiveParser.TOK_SUBQUERY)
    val res = for (elem <- childrenASTNode(node)) yield {
      elem.getType match {
        case HiveParser.TOK_QUERY => tpcQuery(elem)
        case HiveParser.Identifier => topic(elem.toString)
        case HiveParser.TOK_UNIONALL => tpcUnionAll(elem)
        case _ => throw new RuntimeException(unknownType(elem))
      }
    }
    val tokTmp = (tpc: ITopic) => tpc.getTitleText.equals("_select")

    val aliasTableName = res.filterNot(tokTmp).head.getTitleText
    val queryTopic = res.filter(tokTmp).head

    queryTopic.setTitleText(aliasTableName)
    queryTopic
  }

  def tpcFrom(node: ASTNode): ITopic = {
    require(node.getType == HiveParser.TOK_FROM)
    val res = for (elem <- childrenASTNode(node)) yield {
      elem.getType match {
        case _ if joinTypeMap.keys.exists(_ == elem.getType) => tpcJoin(elem)
        case HiveParser.TOK_TABREF => tpcTabRef(elem)
        case HiveParser.TOK_SUBQUERY => tpcSubQuery(elem)
        case _ => throw new RuntimeException(unknownType(elem))
      }
    }
    res.head
  }

  def tpcInsert(node: ASTNode): ITopic = {
    require(node.getType == HiveParser.TOK_INSERT)
    val res = for (elem <- childrenASTNode(node)) yield {
      elem.getType match {
        case HiveParser.TOK_DESTINATION => topic(getDestination(elem)) //暂时只支持单表
        case HiveParser.TOK_SELECT => topicOnlyContent(getSelect(elem) + "\n")
        case HiveParser.TOK_WHERE => topicOnlyContent(getWhere(elem))
        case HiveParser.TOK_GROUPBY => topicOnlyContent(getGroupBy(elem))
        case HiveParser.TOK_ORDERBY => topicOnlyContent(getOrderByOrSortBy(elem, "ORDER"))
        case HiveParser.TOK_SORTBY => topicOnlyContent(getOrderByOrSortBy(elem, "SORT"))
        case HiveParser.TOK_CLUSTERBY => topicOnlyContent(getClusterByOrDistributeBy(elem, "CLUSTER"))
        case HiveParser.TOK_DISTRIBUTEBY => topicOnlyContent(getClusterByOrDistributeBy(elem, "DISTRIBUTE"))
        case HiveParser.TOK_SELECTDI => topicOnlyContent(getSelectDi(elem))
        case HiveParser.TOK_HAVING => topicOnlyContent(getHaving(elem))
        case HiveParser.TOK_LIMIT => topicOnlyContent(getLimit(elem))
        case HiveParser.KW_WINDOW => topicOnlyContent(getWindow(elem))
        case _ => throw new RuntimeException(unknownType(elem))
      }
    }
    //暂时先把insert的内容放到 destination里面，后面再移除掉destination中的内容
    val select = res.filter(emptyTextTopic)
    val des = res.filterNot(emptyTextTopic)
    topicMergeContent(des ++ select: _*)
    des.head
  }

  def tpcQuery(node: ASTNode): ITopic = {
    require(node.getType == HiveParser.TOK_QUERY)
    val res = for (elem <- childrenASTNode(node)) yield {
      elem.getType match {
        case HiveParser.TOK_FROM => ("from", tpcFrom(elem))
        case HiveParser.TOK_INSERT => ("insert", tpcInsert(elem))
        case _ => throw new RuntimeException(unknownType(elem))
      }
    }
    val from = res.filter(_._1.equals("from")).map(_._2).head
    val insert = res.filter(_._1.equals("insert")).map(_._2).head
    //要返回的topic
    val returnTopic = if (insert.getAllChildren.size() == 0) topic(insert.getTitleText) else insert
    //把子topic添加到要返回的topic上
    if (emptyTextTopic(from) || from.getTitleText.equals("_u1")) from.getAllChildren.asScala.foreach(returnTopic.add) else returnTopic.add(from)
    val insertTopic = if (from.getTitleText.equals("_u1")) topic("") else insert
    //把insert上的信息合并到返回的topic的第一个子topic上
    topicMergeContent(returnTopic.getAllChildren.get(0), insertTopic)
    returnTopic
  }

  def save(topic: ITopic, rootText: String, file: String): Unit = {
    val root = wb.getPrimarySheet.getRootTopic
    root.setTitleText(rootText)
    root.add(topic)
    wb.save(file)
  }
}
