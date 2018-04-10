package cn.quantgroup.graph.useraction

import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.native.JsonMethods._
import java.text.SimpleDateFormat
import java.util.Date
//import org.json4s.jackson.JsonMethods._

object UserActionAnalysis{

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("UserAction").getOrCreate()
    actionExtractInfo(spark)
  }

  // 从原始的文件里提取信息
  def actionExtractInfo(spark: SparkSession) :Unit = {
    // 处理extrainfo字段
    val parseExtra = (extra: String) => {
      val tmp = parse(extra).values.asInstanceOf[Map[String, String]]
      val actions = parse(tmp("inputArray")).values.asInstanceOf[List[Map[String,_]]]
      val result = scala.collection.mutable.HashMap.empty[String, Any]
      // 一个完整的action操作需要有一个focus 一个blur
      var start = false
      var end = false
      // 某个action执行了多少次change 多长时间的change
      var changeCounts = 0
      // 哪一天的操作
      var day = ""
      // 完成这个操作需要的时间
      var takeTime = 0

      if (actions.length > 0) {
        for (action <- actions) {
          val name = action("action").asInstanceOf[String]
          if (action.contains("time") && (action("time") != null) && (action("time").toString.length>0)) {
            name match {
              case "focus" =>
                start = true
                val ml = action("time").toString.toLong
                day = new SimpleDateFormat("yyyy-MM-dd").format(new Date(ml))
              case "change" =>
                changeCounts = changeCounts + 1
                takeTime = takeTime + action("time").toString.toInt
              case "blur" =>
                takeTime = takeTime + action("time").toString.toInt
                end = true
              case _ =>
            }
          }
        }
      }

      if (start & end) {
        result += ("isCorrect" -> 1)
      } else {
        result += ("isCorrect" -> 0)
      }
      result += ("identify" -> tmp("identify"))
      result += ("day" -> day)
      result += ("changeCounts" -> changeCounts)
      result += ("takeTime" -> takeTime)
    }

   // val spark = SparkSession.builder().appName("UserAction").getOrCreate()
    spark.sql("select phoneno,extrainfo from statistics.action where type='input' and dt>'20171210'").rdd.map{row =>
      val res = parseExtra(row.getString(1))
      if (res("isCorrect").asInstanceOf[Int] == 1) {
        (row.getString(0), res("day").asInstanceOf[String], res("identify").asInstanceOf[String],
          res("changeCounts").asInstanceOf[Int], res("takeTime").asInstanceOf[Int])
      } else {
        null
      }
    }.filter(_!=null).map(x =>
      x._1 + "," + x._2 + "," + x._3 + "," + x._4 + "," + x._5
    ).repartition(2).saveAsTextFile("/user/jianjun.zhou/userAction/data")
  }

}

