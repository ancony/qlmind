package cn.ancony.qlmind.parse

import org.scalatest.funsuite.AnyFunSuite

class TestMatchFor extends AnyFunSuite {
  test("test match in for") {
    val a = Array("a", "b", "c")
    val b = Array("d", "g")
    val resA2 = loop(a, {
      case "a" => "a"
      case "b" => "i am b"
      case "c" => "hello c"
    })

    val resB2 = loop(b, {
      case "d" => "this is d"
      case "g" => "g is ok"
    })
    println(resA2.mkString("."))
    println(resB2.mkString("."))
  }

  def loop(arr: Array[String], matchFunc: String => String): Array[String] = {
    for (elem <- arr) yield {
      matchFunc(elem)
    }
  }
}
