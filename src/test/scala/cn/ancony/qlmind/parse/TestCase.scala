package cn.ancony.qlmind.parse

//import cn.ancony.qlmind.parse.Node2String.rmAddCast
import org.apache.hadoop.hive.ql.parse.HiveParser
import org.scalatest.funsuite.AnyFunSuite

class TestCase extends AnyFunSuite {
  val cast: (String, Int) => String = (col, typ) => s"CAST($col AS ${castMap(typ)})"
  val castMap: Map[Int, String] = Map(
    HiveParser.TOK_INT -> "INT",
    HiveParser.TOK_DOUBLE -> "DOUBLE",
    HiveParser.TOK_BIGINT -> "BIGINT",
    HiveParser.TOK_STRING -> "STRING"
  )
  test("test function case") {
    val str = cast("b", HiveParser.TOK_INT)
    println(str)
  }
}
