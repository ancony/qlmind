package cn.ancony.qlmind.parse

import org.apache.hadoop.hive.ql.parse.{ASTNode, HiveParser}

import scala.collection.JavaConverters._
import scala.collection.mutable

object Node2String {

  val dot = "."
  val space = " "
  val empty = ""
  val lf = "\n"

  val unknownType: ASTNode => String = (node: ASTNode) =>
    s"Unknown type: ${node.getType}, name: ${node.toString} in node ${node.getParent.asInstanceOf[ASTNode].dump()}"

  type ASTNodeString = ASTNode => String

  def childrenLoopString(node: ASTNode, f: ASTNodeString): mutable.Buffer[String] =
    for (elem <- node.getChildren.asScala; child = elem.asInstanceOf[ASTNode]) yield f(child)

  def childrenLoopStr(node: ASTNode): mutable.Buffer[String] =
    for (elem <- node.getChildren.asScala) yield elem.toString

  def getTabName(node: ASTNode): String = {
    require(node.getType == HiveParser.TOK_TABNAME)
    childrenLoopStr(node).mkString(dot)
  }

  def getTablePartition(node: ASTNode): String = {
    require(node.getType == HiveParser.TOK_TABLE_PARTITION)
    val res = childrenLoopString(node, n => n.getType match {
      case HiveParser.TOK_TABNAME => getTabName(n)
      case HiveParser.TOK_PARTSPEC => "" //
      case _ => throw new RuntimeException(unknownType(n))
    })
    res.filter(_.nonEmpty).head
  }

  def getTabRef(node: ASTNode): String = {
    require(node.getType == HiveParser.TOK_TABREF)
    var res = childrenLoopString(node, n => n.getType match {
      case HiveParser.TOK_TABNAME => getTabName(n)
      case HiveParser.TOK_TABLEBUCKETSAMPLE => ""
      case HiveParser.Identifier => n.toString
      case _ => throw new RuntimeException(unknownType(n))
    })
    res = res.filter(_.nonEmpty)
    val ref = if (res.length == 2) s"(${res(1)})" else empty
    res.head + ref
  }

  def getTableOrCol(node: ASTNode): String = {
    require(node.getType == HiveParser.TOK_TABLE_OR_COL)
    node.getChildren.get(0).toString
  }

  def getSelect(node: ASTNode): String = {
    require(node.getType == HiveParser.TOK_SELECT)
    val res = childrenLoopString(node, n => n.getType match {
      case HiveParser.TOK_SELEXPR => getSelExpr(n)
      case HiveParser.QUERY_HINT => "" // /*+ STREAMTABLE(a) */
      case _ => throw new RuntimeException(unknownType(n))
    })
    res.filter(_.nonEmpty).mkString(lf)
  }

  def getSelectDi(node: ASTNode): String = {
    require(node.getType == HiveParser.TOK_SELECTDI)
    val res = childrenLoopString(node, n => n.getType match {
      case HiveParser.TOK_SELEXPR => getSelExpr(n)
      case _ => throw new RuntimeException(unknownType(n))
    })
    s"DISTINCT $lf" + res.filter(_.nonEmpty).mkString(lf)
  }

  def getNot(node: ASTNode): String = {
    require(node.getType == HiveParser.KW_NOT)
    val res = childrenLoopString(node, child => child.getType match {
      case HiveParser.TOK_FUNCTION => getFunction(child)
      case HiveParser.DOT => getDot(child)
      case _ if likeRegexp.contains(child.getType) => getLikeRegexp(child)
      case _ => throw new RuntimeException(unknownType(child))
    })
    val funcStr = res.head.split("\\s+")
    val hd = funcStr.head
    if (funcStr.length == 1) s"NOT $hd$lf"
    else s"$hd NOT ${funcStr.tail.mkString(" ")}$lf"
  }

  def getLikeRegexp(node: ASTNode): String = {
    require(likeRegexp.contains(node.getType))
    val res = childrenLoopString(node, child => child.getType match {
      case HiveParser.DOT => getDot(child)
      case HiveParser.StringLiteral => child.toString
      case HiveParser.TOK_TABLE_OR_COL => getTableOrCol(child)
      case _ => throw new RuntimeException(unknownType(child))
    })
    res.mkString(s" ${node.toString.toUpperCase} ")
  }

  def getOr(node: ASTNode): String = {
    require(node.getType == HiveParser.KW_OR)
    val res = childrenLoopString(node, child => child.getType match {
      case HiveParser.StringLiteral => child.toString
      case HiveParser.KW_AND => getAnd(child)
      case HiveParser.KW_OR => getOr(child)
      case _ if likeRegexp.contains(child.getType) => getLikeRegexp(child)
      case HiveParser.TOK_FUNCTION => getFunction(child)
      case _ if relationship.contains(child.getType) => getRelationshipCompare(child)
      case _ => throw new RuntimeException(unknownType(child))
    })
    val rtn = res.mkString(s" OR ")
    "(" + rtn + ")"
  }

  def getWhere(node: ASTNode): String = {
    require(node.getType == HiveParser.TOK_WHERE)
    val res = childrenLoopString(node, child => child.getType match {
      case _ if relationship.contains(child.getType) => getRelationshipCompare(child)
      case HiveParser.KW_AND => getAnd(child)
      case HiveParser.KW_OR => getOr(child)
      case HiveParser.TOK_FUNCTION => getFunction(child)
      case HiveParser.KW_NOT => getNot(child)
      case _ if likeRegexp.contains(child.getType) => getLikeRegexp(child)
      case _ => throw new RuntimeException(unknownType(child))
    })
    "WHERE" + space + res.mkString(lf)
  }

  def getDot(node: ASTNode): String = {
    require(node.getType == HiveParser.DOT)
    val res = childrenLoopString(node, child => child.getType match {
      case HiveParser.TOK_TABLE_OR_COL => getTableOrCol(child)
      case HiveParser.Identifier => child.toString
      case _ => throw new RuntimeException(unknownType(child))
    })
    res.mkString(dot)
  }

  def getWindowRange(node: ASTNode): String = {
    require(node.getType == HiveParser.TOK_WINDOWRANGE)
    val res = childrenLoopString(node, child => child.getType match {
      case HiveParser.KW_PRECEDING =>
        getPreCeding(child) + " " + child.toString
      case HiveParser.KW_CURRENT =>
        s"${child.toString} ROW"
      case HiveParser.KW_FOLLOWING =>
        child.getChildren.get(0).toString + " " + child.toString
      case _ => throw new RuntimeException(unknownType(child))
    })
    val between = if (res.length == 2) "BETWEEN " else ""
    res.mkString(s"ROWS $between", " AND ", "")
  }

  def getPreCeding(node: ASTNode): String = {
    require(node.getType == HiveParser.KW_PRECEDING)
    val res = childrenLoopString(node, child => child.getType match {
      case HiveParser.KW_UNBOUNDED => "UNBOUNDED"
      case HiveParser.Number => child.toString
      case _ => throw new RuntimeException(unknownType(child))
    })
    res.mkString(dot)
  }

  val isMap: Map[String, String] = Map(
    "ISNULL" -> "IS NULL",
    "ISNOTNULL" -> "IS NOT NULL",
    "ISTRUE" -> "IS TRUE",
    "ISNOTTRUE" -> "IS NOT TRUE",
    "ISFALSE" -> "IS FALSE",
    "ISNOTFALSE" -> "IS NOT FALSE")

  val castMap: Map[String, String] = Map(
    "TOK_INT" -> "INT",
    "TOK_DOUBLE" -> "DOUBLE",
    "TOK_BIGINT" -> "BIGINT",
    "TOK_STRING" -> "STRING"
  )

  val cast: (String, String) => String = (col: String, typ: String) => s"CAST($col AS ${castMap(typ)})"
  val is: (String, String) => String = (col: String, typ: String) => s"$col ${isMap(typ)}"

  val odd: Int => Boolean = (n: Int) => n % 2 == 1
  val even: Int => Boolean = (n: Int) => !odd(n)

  def getCaseWhenString(caseWhen: mutable.Buffer[String], typ: String): String = {
    val (cond, head) = typ match {
      case "CASE" => (odd, Array(caseWhen.head))
      case "WHEN" => (even, Array("CASE", caseWhen.head))
      case _ => throw new RuntimeException("")
    }
    val len = caseWhen.tail.length
    val fill = Array.tabulate(len - 2)(i => if (cond(i)) "THEN" else "WHEN") ++ Array("ELSE", "END")
    val res = head ++ caseWhen.tail.zip(fill).flatMap { case (value, keyword) => Array(value, keyword) }
    res.mkString(" ")
  }

  def getFunction(node: ASTNode): String = {
    require(node.getType == HiveParser.TOK_FUNCTION)
    var win = false
    val res = childrenLoopString(node, child => child.getType match {
      case HiveParser.Identifier => child.toString.toUpperCase
      case HiveParser.TOK_TABLE_OR_COL => getTableOrCol(child)
      case HiveParser.DOT => getDot(child)
      case HiveParser.TOK_WINDOWSPEC =>
        win = true
        getWindowSpec(child)
      case HiveParser.KW_FALSE => empty
      case HiveParser.Number => child.toString
      case HiveParser.StringLiteral => child.toString
      case _ if castMap.contains(child.toString) => child.toString
      case HiveParser.KW_WHEN => child.toString.toUpperCase
      case HiveParser.KW_CASE => child.toString.toUpperCase
      case _ if relationship.contains(child.getType) => getRelationshipCompare(child)
      case HiveParser.TOK_NULL => "NULL"
      case HiveParser.TOK_FUNCTION => getFunction(child)
      case HiveParser.KW_AND => getAnd(child)
      case HiveParser.KW_OR => getOr(child)
      case HiveParser.KW_TRUE => "TRUE"
      case HiveParser.KW_FALSE => "FALSE"
      case _ if arithmetic.contains(child.getType) => getArithmetics(child)
      case _ => throw new RuntimeException(unknownType(child))
    })

    val windowString = if (win) "OVER " + res.last else ""
    val identify = if (win) res.filter(_.nonEmpty).take(res.length - 1) else res.filter(_.nonEmpty)

    val hd = identify.head
    val identifyString = hd match {
      case "BETWEEN" => identify(1) + " BETWEEN " + identify.slice(2, identify.length).mkString(" AND ")
      case "IN" => identify(1) + " IN (" + identify.slice(2, identify.length).mkString(", ") + ")"
      case _ if hd.startsWith("IS") => is(identify(1), hd)
      case _ if hd.equals("CASE") || hd.equals("WHEN") => getCaseWhenString(identify, hd)
      case _ if castMap.keys.exists(hd.equals) => cast(identify(1), hd)
      case _ => hd + "(" + identify.tail.mkString(", ") + ")"
    }
    val tail = if (win) " " + windowString else ""
    identifyString + tail
  }

  def getNulls(node: ASTNode): String = {
    require(nullsFirstLast.contains(node.getType))
    val res = childrenLoopString(node, child => child.getType match {
      case HiveParser.TOK_TABLE_OR_COL => getTableOrCol(child)
      case HiveParser.DOT => getDot(child)
      case _ => throw new RuntimeException(unknownType(child))
    })
    res.head
  }

  //order ????????? DESC ?????? ASC
  def getTabSortColName(node: ASTNode, order: String): String = {
    require(ascDesc.keys.toArray.contains(node.getType))
    val res = childrenLoopString(node, child => child.getType match {
      case _ if nullsFirstLast.contains(child.getType) => getNulls(child)
      case _ => throw new RuntimeException(unknownType(child))
    })
    res.head + " " + order
  }

  //typ?????????ORDER??????SORT
  def getOrderByOrSortBy(node: ASTNode, typ: String): String = {
    require(orderByOrSortBy.contains(node.getType))
    val res = childrenLoopString(node, child => {
      val childTyp = child.getType
      childTyp match {
        case _ if ascDesc.keys.toArray.contains(childTyp) => getTabSortColName(child, ascDesc(childTyp))
        case _ => throw new RuntimeException(unknownType(child))
      }
    })
    typ + " BY " + res.mkString(", ")
  }

  def getHaving(node: ASTNode): String = {
    require(node.getType == HiveParser.TOK_HAVING)
    val res = for (elem <- node.getChildren.asScala; child = elem.asInstanceOf[ASTNode]) yield
      child.getType match {
        case _ if relationship.contains(child.getType) => getRelationshipCompare(child)
        case HiveParser.KW_AND => getAnd(child)
        case _ => throw new RuntimeException(unknownType(child))
      }
    "HAVING " + res.mkString(", ")
  }

  def getLimit(node: ASTNode): String = {
    require(node.getType == HiveParser.TOK_LIMIT)
    val res = for (elem <- node.getChildren.asScala; child = elem.asInstanceOf[ASTNode]) yield
      child.getType match {
        case HiveParser.Number => child.toString
        case _ => throw new RuntimeException(unknownType(child))
      }
    "LIMIT " + res.mkString(", ")
  }

  //typ?????????CLUSTER??????DISTRIBUTE
  def getClusterByOrDistributeBy(node: ASTNode, typ: String): String = {
    require(clusterByDistributeBy.contains(node.getType))
    val res = for (elem <- node.getChildren.asScala; child = elem.asInstanceOf[ASTNode]) yield
      child.getType match {
        case HiveParser.TOK_TABLE_OR_COL => getTableOrCol(child)
        case HiveParser.DOT => getDot(child)
        case HiveParser.TOK_FUNCTION => getFunction(child)
        case _ => throw new RuntimeException(unknownType(child))
      }
    typ + " BY " + res.mkString(", ")
  }

  def getPartitioningSpec(node: ASTNode): String = {
    require(node.getType == HiveParser.TOK_PARTITIONINGSPEC)
    val res = for (elem <- node.getChildren.asScala; child = elem.asInstanceOf[ASTNode]) yield {
      child.getType match {
        case HiveParser.TOK_DISTRIBUTEBY => getClusterByOrDistributeBy(child, "DISTRIBUTE")
        case HiveParser.TOK_ORDERBY => getOrderByOrSortBy(child, "ORDER")
        case _ => throw new RuntimeException(unknownType(child))
      }
    }
    val orderBy = if (res.length == 2) " " + res(1) else ""
    "PARTITION BY " + res.head.replace("DISTRIBUTE BY ", "") + orderBy
  }

  def getWindowSpec(node: ASTNode): String = {
    require(node.getType == HiveParser.TOK_WINDOWSPEC)
    if (node.getChildren == null) return "()"
    var identifier = false
    val res = for (elem <- node.getChildren.asScala; child = elem.asInstanceOf[ASTNode]) yield {
      child.getType match {
        case HiveParser.TOK_PARTITIONINGSPEC => getPartitioningSpec(child)
        case HiveParser.TOK_WINDOWRANGE => getWindowRange(child)
        case HiveParser.Identifier =>
          identifier = true
          child.toString
        case _ => throw new RuntimeException(unknownType(child))
      }
    }
    if (identifier) res.head else s"(${res.mkString(" ")})"
  }

  def getWindowDef(node: ASTNode): String = {
    require(node.getType == HiveParser.TOK_WINDOWDEF)
    val res = for (elem <- node.getChildren.asScala; child = elem.asInstanceOf[ASTNode]) yield {
      child.getType match {
        case HiveParser.Identifier => "WINDOW " + child.toString
        case HiveParser.TOK_WINDOWSPEC => getWindowSpec(child)
        case _ => throw new RuntimeException(unknownType(child))
      }
    }
    res.head + " AS " + res.tail.head
  }

  def getInsert(node: ASTNode): String = {
    require(node.getType == HiveParser.TOK_INSERT)
    val res = for (elem <- node.getChildren.asScala; child = elem.asInstanceOf[ASTNode]) yield {
      child.getType match {
        case HiveParser.TOK_SELECT => getSelect(child)
        case HiveParser.KW_WINDOW => getWindow(child)
        case HiveParser.TOK_DESTINATION => getDestination(child)
        case HiveParser.TOK_WHERE => getWhere(child)
        case HiveParser.TOK_DISTRIBUTEBY => getClusterByOrDistributeBy(child, "DISTRIBUTE")
        case HiveParser.TOK_CLUSTERBY => getClusterByOrDistributeBy(child, "CLUSTER")
        case HiveParser.TOK_SORTBY => getOrderByOrSortBy(child, "SORT")
        case HiveParser.TOK_ORDERBY => getOrderByOrSortBy(child, "ORDER")
        case HiveParser.TOK_GROUPBY => getGroupBy(child)
        case HiveParser.TOK_SELECTDI => getSelectDi(child)
        case _ => throw new RuntimeException(unknownType(child))
      }
    }
    res.mkString("\n")
  }

  def getTab(node: ASTNode): String = {
    require(node.getType == HiveParser.TOK_TAB)
    val res = for (elem <- node.getChildren.asScala; child = elem.asInstanceOf[ASTNode]) yield {
      child.getType match {
        case HiveParser.TOK_TABNAME => getTabName(child)
        case HiveParser.TOK_PARTSPEC => ""
        case _ => throw new RuntimeException(unknownType(child))
      }
    }
    res.filter(_.nonEmpty).head
  }

  def getDestination(node: ASTNode): String = {
    require(node.getType == HiveParser.TOK_DESTINATION)
    val res = for (elem <- node.getChildren.asScala; child = elem.asInstanceOf[ASTNode]) yield {
      child.getType match {
        case HiveParser.TOK_TAB => getTab(child)
        case HiveParser.TOK_DIR => "_select"
        case _ => throw new RuntimeException(unknownType(child))
      }
    }
    res.head
  }

  def getRelationshipCompare(node: ASTNode): String = {
    require(relationship.contains(node.getType))
    val res = for (elem <- node.getChildren.asScala; child = elem.asInstanceOf[ASTNode]) yield {
      child.getType match {
        case HiveParser.DOT => getDot(child)
        case HiveParser.StringLiteral => child.toString
        case HiveParser.TOK_TABLE_OR_COL => getTableOrCol(child)
        case HiveParser.TOK_FUNCTION => getFunction(child)
        case HiveParser.Number => child.toString
        case _ => throw new RuntimeException(unknownType(child))
      }
    }
    res.mkString(s" ${node.toString} ")
  }

  def getGroupBy(node: ASTNode): String = {
    require(node.getType == HiveParser.TOK_GROUPBY)
    val res = for (elem <- node.getChildren.asScala; child = elem.asInstanceOf[ASTNode]) yield {
      child.getType match {
        case HiveParser.TOK_TABLE_OR_COL => getTableOrCol(child)
        case HiveParser.DOT => getDot(child)
        case _ => throw new RuntimeException(unknownType(child))
      }
    }
    "GROUP BY " + res.mkString(", \n")
  }

  def getAnd(node: ASTNode): String = {
    require(node.getType == HiveParser.KW_AND)
    val res = for (elem <- node.getChildren.asScala; child = elem.asInstanceOf[ASTNode]) yield {
      child.getType match {
        case HiveParser.StringLiteral => child.toString
        case HiveParser.KW_AND => getAnd(child)
        case HiveParser.KW_OR => getOr(child)
        case HiveParser.KW_NOT => getNot(child)
        case HiveParser.TOK_FUNCTION => getFunction(child)
        case _ if relationship.contains(child.getType) => getRelationshipCompare(child)
        case _ => throw new RuntimeException(unknownType(child))
      }
    }
    res.mkString(" AND ")
  }

  def getWindow(node: ASTNode): String = {
    require(node.getType == HiveParser.KW_WINDOW)
    val res = for (elem <- node.getChildren.asScala; child = elem.asInstanceOf[ASTNode]) yield {
      child.getType match {
        case HiveParser.TOK_WINDOWDEF => getWindowDef(child)
        case _ => throw new RuntimeException(unknownType(child))
      }
    }
    res.head
  }

  def getFunctionDi(node: ASTNode): String = {
    require(node.getType == HiveParser.TOK_FUNCTIONDI)
    val res = for (elem <- node.getChildren.asScala; child = elem.asInstanceOf[ASTNode]) yield
      child.getType match {
        case HiveParser.Identifier => child.toString.toUpperCase
        case HiveParser.TOK_TABLE_OR_COL => getTableOrCol(child)
        case HiveParser.TOK_WINDOWSPEC => getWindowSpec(child)
        case HiveParser.DOT => getDot(child)
        case _ => throw new RuntimeException(unknownType(child))
      }
    val win = if (res.length == 3) " OVER " + res(2) else ""
    res.head + "(DISTINCT " + res(1) + ") " + win
  }

  def getAllColRef(node: ASTNode): String = {
    require(node.getType == HiveParser.TOK_ALLCOLREF)
    if (node.getChildren == null) return "*"
    val res = for (elem <- node.getChildren.asScala; child = elem.asInstanceOf[ASTNode]) yield
      child.getType match {
        case HiveParser.TOK_TABNAME => getTabName(child) + ".*"
        case _ => throw new RuntimeException(unknownType(child))
      }
    res.mkString(space)
  }

  def getArithmetics(node: ASTNode): String = {
    require(arithmetic.contains(node.getType))
    val res = for (elem <- node.getChildren.asScala; child = elem.asInstanceOf[ASTNode]) yield
      child.getType match {
        case HiveParser.TOK_TABLE_OR_COL => getTableOrCol(child)
        case HiveParser.Number => child.toString
        case HiveParser.TOK_FUNCTION => getFunction(child)
        case HiveParser.DOT => getDot(child)
        case _ if arithmetic.contains(child.getType) => getArithmetics(child)
        case _ => throw new RuntimeException(unknownType(child))
      }
    val opt = node.toString.toUpperCase
    val head = res.head
    if (res.length == 1) s"$opt$head" else s"$head $opt ${res(1)}"
  }

  def getFunctionStar(node: ASTNode): String = {
    require(node.getType == HiveParser.TOK_FUNCTIONSTAR)
    val res = for (elem <- node.getChildren.asScala; child = elem.asInstanceOf[ASTNode]) yield
      child.getType match {
        case HiveParser.Identifier => child.toString.toUpperCase + "(*)"
        case _ => throw new RuntimeException(unknownType(child))
      }
    res.mkString(s" + ")
  }

  def getTabAlias(node: ASTNode): String = {
    require(node.getType == HiveParser.TOK_TABALIAS)
    val res = for (elem <- node.getChildren.asScala; child = elem.asInstanceOf[ASTNode]) yield
      child.getType match {
        case HiveParser.Identifier => child.toString
        case _ => throw new RuntimeException(unknownType(child))
      }
    res.head
  }

  def getLSquare(node: ASTNode): String = {
    require(node.getType == HiveParser.LSQUARE)
    val res = for (elem <- node.getChildren.asScala; child = elem.asInstanceOf[ASTNode]) yield
      child.getType match {
        case HiveParser.TOK_FUNCTION => getFunction(child)
        case HiveParser.Number => child.toString
        case _ => throw new RuntimeException(unknownType(child))
      }

    val other = if (res.length == 2) res(1).mkString("[", ",", "]") else ""
    res.head + other
  }

  def getSelExpr(node: ASTNode): String = {
    require(node.getType == HiveParser.TOK_SELEXPR)
    val res = for (elem <- node.getChildren.asScala; child = elem.asInstanceOf[ASTNode]) yield
      child.getType match {
        case _ if arithmetic.contains(child.getType) => getArithmetics(child)
        case HiveParser.DOT => getDot(child)
        case HiveParser.Identifier => child.toString
        case HiveParser.TOK_FUNCTION => getFunction(child)
        case HiveParser.TOK_TABLE_OR_COL => getTableOrCol(child)
        case HiveParser.TOK_FUNCTIONDI => getFunctionDi(child)
        case HiveParser.TOK_ALLCOLREF => getAllColRef(child)
        case HiveParser.TOK_FUNCTIONSTAR => getFunctionStar(child)
        case HiveParser.Number => child.toString
        case HiveParser.KW_NOT => getNot(child)
        case HiveParser.TOK_NULL => "NULL"
        case HiveParser.TOK_TABALIAS => "_alias_" + getTabAlias(child)
        case HiveParser.StringLiteral => child.toString
        case HiveParser.LSQUARE => getLSquare(child)
        case _ => throw new RuntimeException(unknownType(child))
      }

    val toRtn = res.filter(_.nonEmpty)
    val alias = toRtn.filter(_.startsWith("_alias_"))
    val rtn = if (alias.nonEmpty) handleAlias(res.toArray, alias.head.slice("_alias_".length, alias.head.length))
    else res.toArray
    rtn.mkString(space)
  }

  def handleAlias(arr: Array[String], alias: String): Array[String] = {
    if (arr.length != 3) throw new RuntimeException("to do support.")
    Array(arr.head, alias, "AS", arr(1))
  }

  def getNode(node: ASTNode): Array[String] = {
    val children = node.getChildren
    if (children != null) {
      val res = for (elem <- children.asScala; child = elem.asInstanceOf[ASTNode]) yield {
        child.getType match {
          case HiveParser.TOK_TABNAME => Array(getTabName(child))
          case HiveParser.TOK_TABREF => Array(getTabRef(child))
          case HiveParser.TOK_SELECT => Array(getSelect(child))
          case HiveParser.TOK_INSERT => Array(getInsert(child))
          case _ => getNode(child)
        }
      }
      return res.flatten.toArray
    }
    Array()
  }

}
