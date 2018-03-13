package com.quantgroup.ml.graph

import org.apache.spark.sql.SparkSession
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.types.StructField
//import org.apache.spark.sql.types.StructType
//import org.apache.spark.sql.types.StringType

object DfOfUsers {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DfOfUsers").getOrCreate()
    getDfOfMobilePhoneUsers(spark, "/user/he.zhou/output/output2")//hadoop
  }

  /**
    * 计算手机用户的DF值
    */
  def getDfOfMobilePhoneUsers(spark: SparkSession, output_path: String): Unit = {
    // 所有注册用户的手机号
    val users = spark.sql("SELECT phone_no,uuid from xyqb.user").rdd.map(x =>
      if (x.size == 2) {
        (x.getAs[String](0), "")
      } else {
        ("", "")
      }
    ).distinct().cache()

    //根据通话时间，接听者电话类型筛选通话记录,包括三家运营商，用户作为主叫，被叫


    //主叫
    val caller_telecom = spark.sql("select phone,receiverphone,substr(ctime,1,7)"+
      " from hbase.telecom_call_info where receiverphonetype=1 and ctime<'2017-07-01 00:00:00'").rdd

    val caller_mobile = spark.sql("select phone,receiverphone,substr(ctime,1,7)"+
      "from hbase.mobile_call_info where receiverphonetype=1 and ctime<'2017-07-01 00:00:00'").rdd

    val caller_unicom = spark.sql("select phone,receiverphone,substr(ctime,1,7)"+
      "from hbase.telecom_call_info where receiverphonetype=1 and ctime<'2017-07-01 00:00:00'").rdd

    val caller = caller_telecom.union(caller_mobile).union(caller_unicom)

    //被叫
    val receiver_telecom = spark.sql("select receiverphone,phone,substr(ctime,1,7)"+
      "from hbase.telecom_call_info where receiverphonetype=1 and ctime<'2017-07-01 00:00:00'").rdd

    val receiver_mobile = spark.sql("select receiverphone,phone,substr(ctime,1,7)"+
      "from hbase.mobile_call_info where receiverphonetype=1 and ctime<'2017-07-01 00:00:00'").rdd

    val receiver_unicom = spark.sql("select receiverphone,phone,substr(ctime,1,7)"+
      "from hbase.telecom_call_info where receiverphonetype=1 and ctime<'2017-07-01 00:00:00'").rdd

    val receiver = receiver_telecom.union(receiver_mobile).union(receiver_unicom)

  /*
    //主叫
    val caller_telecom = spark.sql("select phone,receiverphone,substr(ctime,1,7)"+
      " from hbase.telecom_call_info where receiverphonetype=1 and ctime<'2017-06-15 00:00:00' and ctime>'2017-04-15 00:00:00'").rdd

    val caller_mobile = spark.sql("select phone,receiverphone,substr(ctime,1,7)"+
      "from hbase.mobile_call_info where receiverphonetype=1 and ctime<'2017-07-01 00:00:00' and ctime>'2017-04-15 00:00:00'").rdd

    val caller_unicom = spark.sql("select phone,receiverphone,substr(ctime,1,7)"+
      "from hbase.telecom_call_info where receiverphonetype=1 and ctime<'2017-07-01 00:00:00' and ctime>'2017-04-15 00:00:00'").rdd

    val caller = caller_telecom.union(caller_mobile).union(caller_unicom)

    //被叫
    val receiver_telecom = spark.sql("select receiverphone,phone,substr(ctime,1,7)"+
      "from hbase.telecom_call_info where receiverphonetype=1 and ctime<'2017-07-01 00:00:00' and ctime>'2017-04-15 00:00:00'").rdd

    val receiver_mobile = spark.sql("select receiverphone,phone,substr(ctime,1,7)"+
      "from hbase.mobile_call_info where receiverphonetype=1 and ctime<'2017-07-01 00:00:00' and ctime>'2017-04-15 00:00:00'").rdd

    val receiver_unicom = spark.sql("select receiverphone,phone,substr(ctime,1,7)"+
      "from hbase.telecom_call_info where receiverphonetype=1 and ctime<'2017-07-01 00:00:00' and ctime>'2017-04-15 00:00:00'").rdd

    val receiver = receiver_telecom.union(receiver_mobile).union(receiver_unicom)

  */

    /*
    val schema = StructType(
      Array(
        StructField("phone",StringType,true),
        StructField("ctime_month",StringType,true),
        StructField("receiverphone",StringType,true)
      )
    )
    */

    //主被叫合并,根据号码0，通话月份3groupby
    val phone_relation = caller.union(receiver).map(
      x => ((x.getAs[String](0),x.getAs[String](2)),x.getAs[String](1))).groupBy(_._1).map(
      x => (x._1._1, x._1._2, x._2.size)).repartition(2000).cache()  //最后一个数字是2还是1？

    println(s"Users size: ${users.count()}  Phone relation size: ${phone_relation.count()}")

    //信用钱包用户的每个月通话次数
    val count_permonth =  phone_relation.map(x => (x._1,(x._2,x._3))).join(users)
    // 到底是怎样join的，输出是什么样的
    //  .map(x => (x._2._1, x._2._2,x._2._3))

    //信用钱包用户活跃月份数（DF）
    val count_df = count_permonth.filter(_._2._1._2>0).groupBy(_._1).map(
      x => (x._1,x._2.size)      //这个地方是对的吗?
    )

    //存储输出文件
    count_df.map(x => x._1 + "," + x._2).saveAsTextFile(output_path)

    /*
  val df = sqlContext.createDataFrame(phone_relation,schema)

  df.groupBy(('phone','ctime_month')).agg(('phone','ctime_month'),count('receiverphone').map(row => Row(row(1),row(2),row(3))).cache()
     */
  }
}