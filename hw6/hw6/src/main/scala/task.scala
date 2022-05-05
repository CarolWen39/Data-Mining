import scala.collection.mutable.{ListBuffer, Map}
import scala.io.Source
import scala.util.Random
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import scala.collection.Set
import java.io._
import scala.collection.mutable
import scala.math.pow
import breeze.linalg._
import breeze.stats.distributions._
import scala.util.control.Breaks._

object task {
  def main(args: Array[String]): Unit = {
    val start_time = System.nanoTime

    val input_filepath = args(0)
    val n_cluster: Int = args(1).toInt
    val output_filepath = args(2)

    val spark = new SparkConf()
    val sc = new SparkContext(spark)
    sc.setLogLevel("ERROR")

    var output = "The intermediate results:\n"
    var retain_set:ListBuffer[List[Float]] = ListBuffer()
    val compression_set:mutable.Map[Int, mutable.Map[String, DenseVector[Float]]] = mutable.Map()
    val raw_data:ListBuffer[List[Float]] = ListBuffer()
    for (line <- Source.fromFile(input_filepath).getLines()){
      raw_data += line.split(",").toList.map(x => x.toFloat)
    }
    // (id, list[dimensions])
    val data = for (rd <- raw_data) yield (rd.head.toInt,rd.slice(2, rd.length-1))
    println(data.head)

    val idx_dimensions_dict: mutable.Map[Int, List[Float]] = mutable.Map()
    val dimensions_idx_dict: mutable.Map[List[Float], Int] = mutable.Map()
    for (d <- data) {
      idx_dimensions_dict += (d._1 -> d._2)
      dimensions_idx_dict += (d._2 -> d._1)
    }
    val dimensions_data = for(d <- data) yield d._2
    val dimensions_data_shuffle = Random.shuffle(dimensions_data)
    val sub_len:Int = (dimensions_data_shuffle.length/5)
    val distance_threshold:Float = pow(dimensions_data_shuffle(0).length, 0.5).toFloat * 2

    /* step 1. load 20% data randomly */
    var random_load:ListBuffer[Vector] = ListBuffer()
    for (d <- dimensions_data_shuffle.slice(0, sub_len))
      random_load += Vectors.dense(d.toArray.map(_.toDouble))

    /* step 2. K-means */
    var RDD = sc.parallelize(random_load)
    var kmeans_2 = KMeans.train(RDD, n_cluster * 25, 30)
    var labels = kmeans_2.predict(RDD).collect().toList

    /* step 3. move all the clusters that contain only one point to RS (outliers) */
    // cluster_dict: {cluster id: # of points in the cluster}
    var cluster_dict:Map[Int, Int] = Map()
    for (label <- labels) {
      if (cluster_dict.contains(label)){
        cluster_dict(label) = cluster_dict(label) + 1
      }
      else{
       cluster_dict += (label -> 1)
      }
    }
    // find id of clusters that have only one point in it
    var RS_key:ListBuffer[Int] = ListBuffer()
    for ((k,v) <- cluster_dict)
      if (v < 20)
        RS_key += k
    // find the index of the RS points
    var RS_index:ListBuffer[Int] = ListBuffer()
    if (RS_key != ListBuffer()) {
      for (key <- RS_key){
        for (ii <- labels.zipWithIndex.filter(_._1 == key).map(_._2)){
          RS_index += ii
        }
      }
    }
    // add isolated points into RS
    for (index <- RS_index){
      retain_set += random_load(index)
    }
    // delete points from random load data
    for (index <- RS_index){
      random_load -= random_load(index)
    }

    /* Step 4. Run K-Means to the rest of the data points with K = the number of input clusters */
    var RDD_data:ListBuffer[Vector] = ListBuffer()
    for (dd <- random_load){
      RDD_data += Vectors.dense(dd.toArray.map(_.toDouble))
    }
    RDD = sc.parallelize(RDD_data).cache()
    var kmeans_4 = KMeans.train(RDD, n_cluster, 30)
    labels = kmeans_4.predict(RDD).collect().toList
    var chunk_data = RDD.map(x => DenseVector(x.toArray.map(x => x.toFloat))).collect().toList
    println(dimensions_data_shuffle.length)
    // check the results of initial clusters
    var prediction_RDD = sc.parallelize(labels).map(x => (x, 1)).reduceByKey((x,y) => x+y).map(x => x._2).collect().toList
    breakable{
      while (true){
        if (prediction_RDD.max.toFloat > dimensions_data_shuffle.length.toFloat *(0.2) *(1.8)*(1/n_cluster.toFloat)){
          println("oh no")
          kmeans_4 = KMeans.train(RDD, n_cluster, 30)
          labels = kmeans_4.predict(RDD).collect().toList
          prediction_RDD = sc.parallelize(labels).map(x => (x, 1)).reduceByKey((x,y) => x+y).map(x => x._2).collect().toList
        }
        else {
          break
        }
      }
    }

    /* Write file */
    /*val file = new File(output_filepath)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(output)
    bw.close()*/

    val duration = (System.nanoTime - start_time) / 1e9d
    println("Duration: "+duration)
  }
}
