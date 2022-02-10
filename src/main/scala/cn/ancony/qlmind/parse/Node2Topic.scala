package cn.ancony.qlmind.parse

import cn.ancony.qlmind.parse.Node2String._
import cn.ancony.qlmind.util.TopicUtils._
import org.apache.hadoop.hive.ql.parse.{ASTNode, HiveParser}
import org.xmind.core._

import scala.collection.JavaConverters._

object Node2Topic {

  def tpcCreateTable(node: ASTNode): ITopic = {
    require(node.getType == HiveParser.TOK_CREATETABLE)
    val res = for (elem <- node.getChildren.asScala; child = elem.asInstanceOf[ASTNode]) yield {
      child.getType match {
        case HiveParser.TOK_TABNAME => tpcTabName(child)
        case HiveParser.TOK_QUERY => tpcQuery(child)
        case HiveParser.TOK_TABLECOMMENT =>
          val cmt = child.getChildren.get(0).toString
          val comment = cmt.slice(1, cmt.length - 1) //去除引号
          topic("_label", Array(comment), "")
        case HiveParser.KW_TEMPORARY |
             HiveParser.KW_EXTERNAL |
             HiveParser.TOK_LIKETABLE |
             HiveParser.TOK_TABCOLLIST |
             HiveParser.TOK_TABLEPARTCOLS |
             HiveParser.TOK_ALTERTABLE_BUCKETS |
             HiveParser.TOK_TABLEROWFORMAT |
             HiveParser.TOK_TABLESERIALIZER |
             HiveParser.TOK_FILEFORMAT_GENERIC |
             HiveParser.TOK_TABLESKEWED |
             HiveParser.TOK_IFNOTEXISTS |
             HiveParser.TOK_TABLELOCATION |
             HiveParser.TOK_TABLEPROPERTIES |
             HiveParser.TOK_TABLEPROPERTY => emptyTopic
        case _ => throw new RuntimeException(unknownType(child))
      }
    }
    val rtnTopic = res.head
    val labelTpc = res.filter(_.getTitleText.equals("_label"))
    if (labelTpc.nonEmpty) rtnTopic.setLabels(labelTpc.head.getLabels)
    val subTopic = res.filter(hasTitleText).filterNot(_.getTitleText.equals("_label")) - rtnTopic
    if (subTopic.nonEmpty) {
      val head = subTopic.head
      val add = if (head.getTitleText.equals("_select")) head.getAllChildren.asScala else List(head)
      add.foreach(rtnTopic.add)
    }
    rtnTopic
  }

  def tpcDropTable(node: ASTNode): ITopic = {
    require(node.getType == HiveParser.TOK_DROPTABLE)
    val res = for (elem <- node.getChildren.asScala; child = elem.asInstanceOf[ASTNode]) yield {
      child.getType match {
        case HiveParser.TOK_TABNAME => tpcTabName(child)
        case HiveParser.KW_PURGE | HiveParser.TOK_IFEXISTS => emptyTopic
        case _ => throw new RuntimeException(unknownType(child))
      }
    }
    res.filter(hasTitleText).head
  }

  def tpcTruncateTable(node: ASTNode): ITopic = {
    require(node.getType == HiveParser.TOK_TRUNCATETABLE)
    val res = for (elem <- node.getChildren.asScala; child = elem.asInstanceOf[ASTNode]) yield {
      child.getType match {
        case HiveParser.TOK_TABLE_PARTITION => topic(getTablePartition(child))
        case _ => throw new RuntimeException(unknownType(child))
      }
    }
    res.filter(hasTitleText).head
  }

  def tpcTabName(node: ASTNode): ITopic = topic(getTabName(node))

  def tpcTabRef(node: ASTNode): ITopic = topic(getTabRef(node))

  def tpcRelationshipCompare(node: ASTNode): ITopic = topicOnlyContent(getRelationshipCompare(node))

  def tpcAnd(node: ASTNode): ITopic = topicOnlyContent(getAnd(node))

  def tpcJoin(node: ASTNode): ITopic = {
    require((joinTypeMap.keys ++ Array(HiveParser.TOK_SUBQUERY)).exists(_ == node.getType))
    val res = for (elem <- childrenASTNode(node)) yield
      elem.getType match {
        case HiveParser.KW_AND => tpcAnd(elem)
        case HiveParser.TOK_TABREF => tpcTabRef(elem)
        case HiveParser.TOK_SUBQUERY => tpcSubQuery(elem)
        case _ if relationship.contains(elem.getType) => tpcRelationshipCompare(elem)
        case _ if joinTypeMap.keys.exists(_ == elem.getType) => tpcJoin(elem)
        case _ => throw new RuntimeException(unknownType(elem))
      }

    val contents = res.filter(every(noTitleText, noChild))
    //设置contents的连接条件
    contents.map(notesContent).foreach(i => i.setTextContent(joinTypeMap(node.getType) + " ON " + i.getTextContent))
    val tabRef = res.filter(every(hasTitleText, noContent)) //当前获得的
    val subJoin = res.filter(every(noTitleText, hasChild)) //子循环获得的
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
        case HiveParser.Identifier => topic(elem.toString)
        case HiveParser.TOK_QUERY => tpcQuery(elem)
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

  def tpcLateralView(node: ASTNode): ITopic = {
    require(node.getType == HiveParser.TOK_LATERAL_VIEW)
    val res = for (elem <- childrenASTNode(node)) yield {
      elem.getType match {
        case HiveParser.TOK_TABREF => topic(getTabRef(elem))
        case HiveParser.TOK_SELECT => topic("_select", Array(), getSelect(elem))
        case HiveParser.TOK_SUBQUERY => tpcSubQuery(elem)
        case _ => throw new RuntimeException(unknownType(elem))
      }
    }
    val select = res.filter(_.getTitleText.equals("_select"))
    val rtn = res.filterNot(_.getTitleText.equals("_select")).head
    if (select.nonEmpty)
      notesContent(rtn).setTextContent("LATERAL VIEW " + notesContent(select.head).getTextContent)
    rtn
  }

  def tpcFrom(node: ASTNode): ITopic = {
    require(node.getType == HiveParser.TOK_FROM)
    val res = for (elem <- childrenASTNode(node)) yield {
      elem.getType match {
        case HiveParser.TOK_LATERAL_VIEW => tpcLateralView(elem)
        case HiveParser.TOK_SUBQUERY => tpcSubQuery(elem)
        case HiveParser.TOK_TABREF => tpcTabRef(elem)
        case _ if joinTypeMap.keys.exists(_ == elem.getType) => tpcJoin(elem)
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
    val select = res.filter(noTitleText)
    val des = res.filter(hasTitleText)
    topicMergeContent(des ++ select: _*)
  }

  def tpcQuery(node: ASTNode): ITopic = {
    require(node.getType == HiveParser.TOK_QUERY)
    val res = for (elem <- childrenASTNode(node)) yield {
      elem.getType match {
        case HiveParser.TOK_FROM => ("_from", tpcFrom(elem))
        case HiveParser.TOK_INSERT => ("_insert", tpcInsert(elem))
        case _ => throw new RuntimeException(unknownType(elem))
      }
    }
    val filterTopicHead = (identifier: String) => res
      .filter { case (id, _) => id.equals(identifier) }
      .map { case (_, tpc) => tpc }
      .head
    val from = if (res.length > 1) filterTopicHead("_from") else topic("_from")
    val insert = filterTopicHead("_insert")
    //要返回的topic
    val rtnTpc = if (noChild(insert)) topic(insert.getTitleText) else insert
    val fromU1 = from.getTitleText.equals("_u1")
    //把子topic添加到要返回的topic上
    if (noTitleText(from) || fromU1) from.getAllChildren.asScala.foreach(rtnTpc.add)
    else rtnTpc.add(from)
    val insertTopic = if (fromU1) topic("") else insert
    //把insert上的信息合并到返回的topic的第一个子topic上
    topicMergeContent(rtnTpc.getAllChildren.get(0), insertTopic)
    rtnTpc
  }

}
