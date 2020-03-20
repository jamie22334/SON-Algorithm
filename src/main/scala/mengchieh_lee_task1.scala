import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import java.io.{PrintWriter, _}

import org.apache.spark.rdd.RDD

import collection.mutable.ListBuffer
import scala.collection.mutable
import util.control.Breaks._

object mengchieh_lee_task1 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.INFO)
    val caseNumber = args(0).toInt
    val support = args(1).toInt

    val ss = SparkSession.builder()
      .appName("task1")
      .config("spark.master", "local[*]")
      .getOrCreate()

    val filePath = args(2)
    val sc = ss.sparkContext

    val start = System.nanoTime

    val csvRDD = sc.textFile(filePath)
    val header = csvRDD.first()
    val textRDD = csvRDD.filter(line => line != header).map(line => line.split(","))

    var userRDD: RDD[Set[String]] = sc.emptyRDD[Set[String]]
    if(caseNumber == 1){
      // (set(business_id))
      userRDD = textRDD.map(arr => (arr(0), arr(1))).groupByKey().map(t => t._2.toSet)
    }else if(caseNumber == 2){
      // (set(user_id))
      userRDD = textRDD.map(arr => (arr(1), arr(0))).groupByKey().map(t => t._2.toSet)
    }

    var partialSupport = support.toDouble / userRDD.getNumPartitions
    if (partialSupport == 0)
      partialSupport = 1
//    println("partialSupport: " + partialSupport)


    def aprioriItemset(iter: Iterator[Set[String]]): Iterator[Set[String]] = {
      var finalItemSet = Set.empty[Set[String]]

      var singleItemSet = Set.empty[String]
      var sizeKItemSet = Set.empty[Set[String]]
      val basketList = iter.toList
      val counterMap1 = mutable.HashMap.empty[Set[String],Int]

      for (basket <- basketList) {
        for (elem <- basket) {
          val singleSet = Set(elem)
          if(counterMap1.contains(singleSet)){
            counterMap1.put(singleSet, counterMap1(singleSet) + 1)
          }else{
            counterMap1.put(singleSet, 1)
          }
        }
      }
      //      print(counterMap1)

      // single itemset
      for (key <- counterMap1.keys) {
        if(counterMap1(key) >= partialSupport){
          singleItemSet = singleItemSet ++ key
        }
      }
      for (elem <- singleItemSet) {
        finalItemSet = finalItemSet + Set(elem)
      }
      //      println(finalItemSet)

      val start2 = System.nanoTime

      if(singleItemSet.nonEmpty){

        val candidateIter = singleItemSet.subsets(2)
        for (candidateItemset <- candidateIter) {
          breakable{
            var count = 0
            for (basket <- basketList) {
              if(candidateItemset.subsetOf(basket)){
                count += 1
                if(count >= partialSupport){
                  sizeKItemSet = sizeKItemSet + candidateItemset
                  singleItemSet = singleItemSet ++ candidateItemset
                  break
                }
              }
            }
          }
        }

        // size = 2 itemset
        finalItemSet = finalItemSet ++ sizeKItemSet
        //        println(sizeKItemSet)
        //        println(finalItemSet)

        val pairTime = (System.nanoTime - start2) / 1e9d
//        println("Pair Duration: " + pairTime)

        val start3 = System.nanoTime
        var sizeK = 3
        while(sizeKItemSet.nonEmpty){
          sizeKItemSet = sizeKItemSet.empty

          val candidateIter = singleItemSet.subsets(sizeK)
          for (candidateItemset <- candidateIter) {
            breakable{
              var count = 0
              for (basket <- basketList) {
                if(candidateItemset.subsetOf(basket)){
                  count += 1
                  if(count >= partialSupport){
                    sizeKItemSet = sizeKItemSet + candidateItemset
                    singleItemSet = singleItemSet ++ candidateItemset
                    break
                  }
                }
              }
            }
          }

          sizeK += 1
          // size = k itemset
          finalItemSet = finalItemSet ++ sizeKItemSet
          //          println(sizeKItemSet)
          //          println(finalItemSet)
        }

        val tripletTime = (System.nanoTime - start3) / 1e9d
//        println("Triplet Duration: " + tripletTime)
      }

      finalItemSet.toIterator
    }


    val candidates = userRDD.mapPartitions(aprioriItemset).map(t => (t,1)).reduceByKey((a,b) => a+b)
      .map(t => t._1.toList).sortBy(itemset => itemset.size)
    val candidateList = candidates.collect()
//    println(candidateList)


    def countAllItemset(iter: Iterator[Set[String]]): Iterator[(Set[String], Int)] = {
      val counterList = new ListBuffer[(Set[String], Int)]()

      val basketList = iter.toList

      for (candidateSet <- candidateList) {
        var count = 0
        for(basket <- basketList){
          if(candidateSet.toSet.subsetOf(basket)){
            count += 1
          }
        }
        if(count > 0){
          counterList.append((candidateSet.toSet, count))
        }
      }
      //      println(counterList)

      counterList.toIterator
    }


    def printResult(resultList: Array[List[String]], pw: PrintWriter): Unit = {
      var curSize = 1
      val subList = new ListBuffer[String]()

      for (itemset <- resultList) {
        val sortedItemset = itemset.sorted
        val sortedSubList = subList.sorted

        if(sortedItemset.size != curSize){
          if(curSize != 1)
            pw.write("\n")
          curSize += 1
          pw.write("\n")

          for(eachItemset <- sortedSubList){
            pw.write(eachItemset)
            if(eachItemset != sortedSubList.last){
              pw.write(",")
            }
          }
          subList.clear()
        }

        val sb = new StringBuilder
        sb.append("(")
        for (item <- sortedItemset) {
          sb.append("\'" + item + "\'")
          if(item != sortedItemset.last){
            sb.append(", ")
          }
        }
        sb.append(")")
        subList.append(sb.toString())
      }

      if(subList.nonEmpty){
        pw.write("\n\n")
        for(eachItemset <- subList){
          pw.write(eachItemset)
          if(eachItemset != subList.last){
            pw.write(",")
          }
        }
      }
    }


    val freqItemSet = userRDD.mapPartitions(countAllItemset).reduceByKey((a,b) => a+b)
      .filter(pair => pair._2 >= support).map(t => t._1.toList).sortBy(itemset => itemset.size)
    val finalResultList = freqItemSet.collect()
//    println(finalResultList)


    val pw = new PrintWriter(new File(args(3)))

    // print immediate result
    pw.write("Candidates:")
    printResult(candidateList, pw)

    // print final result
    pw.write("\n\nFrequent Itemsets:")
    printResult(finalResultList, pw)

    pw.close()

    val executionTime = (System.nanoTime - start) / 1e9d
    println("Duration: " + executionTime)

  }

}