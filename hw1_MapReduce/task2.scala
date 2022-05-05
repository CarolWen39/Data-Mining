import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.collection.mutable._
import org.json4s.jackson.Serialization
import java.io._


object task2 {
  def main(args: Array[String]) : Unit = {
    val review_filepath = args(0)
    val output_filepath = args(1)
    val n_partition = args(2)

    val conf = new SparkConf
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    var reviews = sc.textFile(review_filepath).map(line => parse(line)).cache()
    val result = HashMap[String,Any]()

    def mapfunc(x: JValue, id: String): (String, Int) = {
      val str = compact(render(x \ id))
      (str.substring(1, str.length()-1), 1)
    }

    def F(reivews: Any, t: String): HashMap[String,Any] = {
      val res = HashMap[String,Any]()

      if(t != "default") {
        reviews = reviews.repartition(n_partition.toInt)
      }

      res("n_partition") = reviews.getNumPartitions
      res("n_items") = reviews.glom.map(_.length).collect
      val startTime: Long = System.currentTimeMillis

      val topbusiness = reviews.map(x => mapfunc(x, "business_id")).reduceByKey(_+_).takeOrdered(10)(Ordering[(Int, String)].on(s => (-s._2, s._1)))
      val endTime: Long = System.currentTimeMillis

      res("exe_time") = (endTime - startTime) / 1000.0

      return res
    }

    result("default") = F(reviews, "default")
    result("customized") = F(reviews, "customized")

    implicit val format = DefaultFormats
    val writer = new PrintWriter(new File(output_filepath))
    writer.write(Serialization.writePretty(result))
    writer.close()
  }
}
