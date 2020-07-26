package com.windTa1ker.learnSpark.sql

import com.windTa1ker.photon.PhotonSQL
import org.apache.spark.sql.SparkSession

/**
 * time: 2020/7/2 7:09 下午
 * author: 荆旗
 * Description: 
 */
object SparkSqlExample extends PhotonSQL{
  /**
   * 处理函数
   *
   * @param spark context
   */
  override def handle(spark: SparkSession): Unit = {
    import spark.implicits._
    val df = spark.read.json("learnSpark/src/main/resources/people.json")
    df.show()
    df.printSchema()
    df.select("name").show()
    df.select($"name", $"age" + 1).show()
    df.filter($"age" > 21).show()
    df.groupBy("age").count().show()

  }
}
