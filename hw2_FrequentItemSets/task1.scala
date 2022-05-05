import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD._

import scala.collection.mutable.Set
import scala.collection.mutable.Map
import scala.collection.Iterable
import java.io._
import scala.util.control._
import java.io.FileWriter
import java.io.File


object task1 extends Serializable{
  def get_singleitem(baskets: List[Set[String]]): Set[Set[String]] = {
    var res : Set[Set[String]] = Set()
    for(b <- baskets)
      for(i <- b)
        res.add(Set(i))
    return res
  }
  def filter_non_frequent(baskets: List[Iterable[Set[String]]], candidates: Set[Set[String]] ,threshold: Double): Set[Set[String]] = {
    var frequent_candidates_set : Set[Set[String]] = Set()
    val candidates_num = candidates.size
    if(candidates_num == 0)
      return frequent_candidates_set

    var frequent_candidates_dict : Map[Set[String], Int] = Map()
    for(b <- baskets) {
      val new_b = b.flatten.toSet
        for(c <- candidates) {
          if(c.subsetOf(new_b)) {
            if(frequent_candidates_dict.contains(c))
              frequent_candidates_dict(c) += 1
            else
              frequent_candidates_dict += (c -> 1)
          }
        }
    }
    frequent_candidates_dict.keys.foreach{
      k => if(frequent_candidates_dict(k) >= threshold) {
        frequent_candidates_set.add(k)
      }
    }
    return frequent_candidates_set
  }

  def construct_candidates(itemsets : Set[Set[String]], k : Int): Set[Set[String]] = {
    var result : Set[Set[String]] = Set()
    for(c1 <- itemsets) {
      for(c2 <- itemsets) {
        val c = c1 ++ c2
        if (c.size == k)
          result.add(c)
      }
    }
    return result
  }

  def prune_non_frequent(candidates_set : Set[Set[String]], prev_frequent_set : Set[Set[String]], k : Int) : Set[Set[String]] = {
    var result : Set[Set[String]] = Set()
    result = result ++ candidates_set
    for(c <- candidates_set) {
      val k_1_subset = c.subsets(k-1)
      val inner = new Breaks

      inner.breakable {
        for(s <- k_1_subset) {
          if(!prev_frequent_set.contains(s)) {
            result = result - s
            inner.break
          }
        }
      }
    }
    return result
  }
  def a_priori(baskets: List[Iterable[Set[String]]], threshold: Double): Map[Int, Set[Set[String]]] = {
    val C1 = get_singleitem(baskets.flatten)
    val L1 = filter_non_frequent(baskets, C1, threshold)

    var res : Map[Int, Set[Set[String]]] = Map()
    res += (1 -> L1)

    var L_curr = L1
    var k = 2

    while(!L_curr.isEmpty) {
      val construct_set = construct_candidates(L_curr, k)
      val candidate_after_prune = prune_non_frequent(construct_set, L_curr, k)
      val candidate_after_filter = filter_non_frequent(baskets, candidate_after_prune , threshold)
      L_curr = candidate_after_filter
      res += (k -> L_curr)
      k += 1
    }
    return res
  }

  def sorted_candidates(itemsets: Array[Set[String]]): Map[Int, Seq[Iterable[String]]] = {
    var itemsets_dict : Map[Int, List[Set[String]]] = Map()
    itemsets.toSeq.sortBy(k => k.size)
    for(i <- itemsets) {
      var nn = i.size
      if(itemsets_dict.contains(nn)) {
        itemsets_dict(nn) = itemsets_dict(nn).:::(List(i))
      } else
        itemsets_dict += (nn -> List(i))
    }

    var sorted_dict : Map[Int, Seq[Iterable[String]]] = Map()
    itemsets_dict.keys.foreach({
      k =>
        var sets : List[List[String]] = List()
        itemsets_dict(k).foreach({
          kv =>
            sets = sets.:::(List(kv.toList.sortWith(_<_)))
        })
        def sort[A: Ordering](coll: Seq[Iterable[A]]) = coll.sorted
        sorted_dict += (k -> sort(sets))
    })

    return sorted_dict

  }

  def main(args: Array[String]): Unit = {
    val startTime: Long = System.currentTimeMillis

    val case_number = args(0)
    val support = args(1)
    val input_filepath = args(2)
    val output_filepath = args(3)

    val conf = new SparkConf
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    var idRDD = sc.textFile(input_filepath).filter(line => !line.startsWith("user_id"))
      .map(kv => kv.split(",")).map(kv => (kv(0), kv(1)))

    if(case_number == "2") {
      def mapfunc(id: (String, String)): (String, String) = {
          val a = Tuple2(id._2, id._1)
          return a
      }
      idRDD = idRDD.map(kv => mapfunc(kv))
    }

    def mapfunc1(item_set: (String, String)): (String, Set[String]) = {
      val res: Set[String] = Set()
      val group_id = item_set._1
      val set_id = item_set._2
      res.add(set_id)
      return (group_id, res)
    }

    val group_idRDD = idRDD.map(kv => mapfunc1(kv)).groupByKey().map(kv => kv._2)
    val baskets_num = group_idRDD.count().toDouble

    // SON Pass 1
    def SON_PASS1(iterator: Iterator[Iterable[Set[String]]]) : Iterator[Set[Set[String]]] = {
      var candidate_result : Set[Set[String]] = Set()

      var itemsets_list = iterator.toList

      val sub_basket_num = itemsets_list.length

      val sub_threshold = scala.math.ceil((sub_basket_num / baskets_num) * support.toInt)

      val sub_result = a_priori(itemsets_list, sub_threshold)

      sub_result.keys.foreach{
        sets =>
          for(i <- sub_result(sets)) {
            candidate_result.add(i)
          }
      }
      return Iterator(candidate_result)
    }
    val candidates = group_idRDD.mapPartitions(SON_PASS1).collect().flatten.distinct

    /* SON Pass 2*/
    def SON_PASS2(iterator: Iterator[Iterable[Set[String]]]) : Iterator[List[(Set[String], Int)]] = {
      var itemsets_list = iterator.toList
      var counts : Map[Set[String], Int] = Map()

      for(b <- itemsets_list){
        val new_b = b.flatten.toSet
        for(c <- candidates) {
          if(c.subsetOf(new_b)) {
            if(counts.contains(c))
              counts(c) += 1
            else
              counts += (c -> 1)
          }
        }
      }
      var res : List[(Set[String], Int)] = List()
      counts.keys.foreach({
        kv =>
          res = res :+ (kv, counts(kv))
      })
      return Iterator(res)
    }

    var merge_list : List[(Set[String], Int)] = List()
    val l = group_idRDD.mapPartitions(SON_PASS2).collect().flatten
    val frequent_items = sc.parallelize(l).reduceByKey(_+_).filter(kv => kv._2 >= support.toInt).map(kv => kv._1).distinct().collect()


    /* print candidates */
    val print_candidates = sorted_candidates(candidates)
    val file = new File(output_filepath)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write("Candidates:\n")
    var i = 1
    while(i <= print_candidates.size) {
      val sets = print_candidates(i)
      if(i == 1) {
        val s = sets.flatten.mkString("('", "'),('", "')")
        bw.write(s+"\n\n")
      }
      else {
        var str_seq : List[String] = List()
        sets.foreach({
          s =>
            str_seq = str_seq :+ s.mkString("('", "','", "')")
        })
        val str = str_seq.mkString(",")
        bw.write(str+"\n\n")
      }
      i += 1
    }

    /* print frequent_items */
    val print_frequent_items = sorted_candidates(frequent_items)
    bw.write("Frequent Itemsets:\n")
    var j = 1
    while(j <= print_frequent_items.size) {
      val sets = print_frequent_items(j)
      if(j == 1) {
        val s = sets.flatten.mkString("('", "'),('", "')")
        bw.write(s+"\n\n")
      }
      else {
        var str_seq : List[String] = List()
        sets.foreach({
          s =>
            str_seq = str_seq :+ s.mkString("('", "','", "')")
        })
        val str = str_seq.mkString(",")
        bw.write(str+"\n\n")
      }
      j += 1
    }
    bw.close()

    val endTime: Long = System.currentTimeMillis
    println("Duration:"+((endTime - startTime) / 1000.0).toString)
  }
}
