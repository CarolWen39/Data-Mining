import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.collection.Iterable
import java.io._


object task3 extends Logging with Serializable{
  def main(args: Array[String]) :Unit = {
    val review_filepath = args(0)
    val business_filepath = args(1)
    val outputa_filepath = args(2)
    val outputb_filepath = args(3)

    val conf = new SparkConf
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val reviews = sc.textFile(review_filepath).map(line => parse(line)).cache()
    val business = sc.textFile(business_filepath).map(line => parse(line)).cache()

    val id_star = reviews.map(r => (compact(render(r \ "business_id")), compact(render(r \ "stars"))))
    val id_city = business.map(b => (compact(render(b \ "business_id")), compact(render(b \ "city"))))

    val join_rdd = id_city.leftOuterJoin(id_star)

    val city_stars = join_rdd.map(r => r._2).filter(kv => kv._2 != None).groupByKey()

    def mapfunc(rdd: (String, Iterable[String])): (String,Double)= {
      val c = rdd._1
      val s = rdd._2
      var total_stars = 0.0
      val len = s.size
      for (i <- s)
        total_stars += i.toDouble
      return (c, total_stars / len)
    }

    val flatten_rdd = city_stars.map(kv => (kv._1, kv._2.flatten))
    val sorted_rdd = flatten_rdd.map(kv => mapfunc((kv._1, kv._2)))
      .takeOrdered(flatten_rdd.count().toInt)(Ordering[(Double, String)].on(s => (-s._2, s._1)))
    for(x <- sorted_rdd) {
      println(x)
    }

  }
}
