package cn.ancony.qlmind

import org.apache.hadoop.hive.ql.parse.HiveParser

package object parse {
  val joinTypeMap = Map(
    HiveParser.TOK_JOIN -> "JOIN",
    HiveParser.TOK_LEFTOUTERJOIN -> "LEFT JOIN",
    HiveParser.TOK_LEFTSEMIJOIN -> "LEFT SEMI JOIN",
    HiveParser.TOK_RIGHTOUTERJOIN -> "RIGHT JOIN",
    HiveParser.TOK_FULLOUTERJOIN -> "FULL OUTER JOIN")

  val unionType: Array[Int] = Array(
    HiveParser.TOK_UNIONALL,
    HiveParser.TOK_UNIONDISTINCT)

  val orderByOrSortBy: Array[Int] = Array(
    HiveParser.TOK_ORDERBY,
    HiveParser.TOK_SORTBY)

  val clusterByDistributeBy: Array[Int] = Array(
    HiveParser.TOK_CLUSTERBY,
    HiveParser.TOK_DISTRIBUTEBY)

  val relationship: Array[Int] = Array(
    HiveParser.EQUAL,
    HiveParser.EQUAL_NS,
    HiveParser.NOTEQUAL,
    HiveParser.LESSTHAN,
    HiveParser.GREATERTHAN,
    HiveParser.AMPERSAND,
    HiveParser.TILDE, //~
    HiveParser.BITWISEOR,
    HiveParser.BITWISEXOR,
    HiveParser.GREATERTHANOREQUALTO,
    HiveParser.LESSTHANOREQUALTO)

  val arithmetic: Array[Int] = Array(
    HiveParser.PLUS,
    HiveParser.MINUS,
    HiveParser.STAR,
    HiveParser.DIVIDE,
    HiveParser.DIV,
    HiveParser.MOD)
}
