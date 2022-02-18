import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.collection.mutable._
import org.json4s.jackson.Serialization
import java.io._

object task1 {
  def main(args: Array[String]): Unit = {
    val review_filepath = args(0)
    val output_filepath = args(1)

    val conf = new SparkConf
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val review = sc.textFile(review_filepath).map(line => parse(line)).cache()
    val result = HashMap[String,Any]()

    // A.
    result("n_review") = review.count()
    println(s"total number of reviews: ${result("n_review")}")

    // B.
    result("n_review_2018") = review.filter(r => compact(render(r \ "date"))regionMatches(false, 1, "2018", 0, 4)).count()
    println(s"total number of reviews in 2018: ${result("n_review_2018")}")

    // C.
    result("n_user") = review.map(r => compact(render(r \ "user_id"))).distinct().count()
    println(s"number of distinct users who have written the reviews: ${result("n_user")}")

    // D.
    def mapfunc(x: JValue, id: String): (String, Int) = {
      val str = compact(render(x \ id))
      (str.substring(1, str.length()-1), 1)
    }
    val topusers = review.map(x => mapfunc(x, "user_id")).reduceByKey(_+_).takeOrdered(10)(Ordering[(Int, String)].on(s => (-s._2, s._1)))
    val res:Array[List[Any]] = new Array(topusers.length)
    for (i <- 0 until topusers.length) {
      res(i) = topusers(i).productIterator.toList
    }
    result("top10_user") = res

    // E.
    result("n_business") = review.map(r => compact(render(r \ "business_id"))).distinct().count()

    //F.
    val topbusiness = review.map(x => mapfunc(x, "business_id")).reduceByKey(_+_).takeOrdered(10)(Ordering[(Int, String)].on(s => (-s._2, s._1)))
    val res1:Array[List[Any]] = new Array(topbusiness.length)
    for (i <- 0 until topbusiness.length) {
      res1(i) = topbusiness(i).productIterator.toList
    }
    result("top10_business") = res1

    println(result)

    implicit val formats = DefaultFormats
    val writer = new PrintWriter(new File(output_filepath))
    writer.write(Serialization.writePretty(result))
    writer.close()
  }
}

