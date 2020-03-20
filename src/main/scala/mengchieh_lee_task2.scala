import java.io.{File, PrintWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}

object mengchieh_lee_task2 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.INFO)
    val filterThreshold = args(0).toInt
    val support = args(1).toInt

    val ss = SparkSession.builder()
      .appName("task2")
      .config("spark.master", "local[*]")
      .getOrCreate()

    val filePath = args(2)
    val sc = ss.sparkContext

    val start = System.nanoTime

    val csvRDD = sc.textFile(filePath)
    val header = csvRDD.first()
    val textRDD = csvRDD.filter(line => line != header).map(line => line.split(","))

    val userRDD = textRDD.map(arr => (arr(0), arr(1))).groupByKey().map(t => t._2.toSet)
      .filter(t => t.size > filterThreshold)

    var partialSupport = support.toDouble / userRDD.getNumPartitions
    if (partialSupport == 0)
      partialSupport = 1
    println("partialSupport: " + partialSupport)


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
        singleItemSet = singleItemSet.empty

        for (candidateItemset <- candidateIter) {
          breakable{
            var count = 0
            for (basket <- basketList) {
              if(candidateItemset.subsetOf(basket)){
                count += 1
                if(count >= partialSupport){
                  sizeKItemSet = sizeKItemSet + candidateItemset
//                  finalItemSet = finalItemSet + candidateItemset
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
        println("Pair Duration: " + pairTime)

        val start3 = System.nanoTime
        var sizeK = 3
        var curTotalSize = finalItemSet.size

        breakable{
          while(sizeKItemSet.nonEmpty){
            if(sizeK > 3 && finalItemSet.size == curTotalSize)
              break

            val candidateIter = singleItemSet.subsets(sizeK)
            singleItemSet = singleItemSet.empty
            sizeKItemSet = sizeKItemSet.empty
            curTotalSize = finalItemSet.size

            for (candidateItemset <- candidateIter) {
              breakable{
                var count = 0
                for (basket <- basketList) {
                  if(candidateItemset.subsetOf(basket)){
                    count += 1
                    if(count >= partialSupport){
                      sizeKItemSet = sizeKItemSet + candidateItemset
//                      finalItemSet = finalItemSet + candidateItemset
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
        }

        val tripletTime = (System.nanoTime - start3) / 1e9d
        println("Triplet Duration: " + tripletTime)
      }

      finalItemSet.toIterator
    }


    val candidateOriginal = userRDD.mapPartitions(aprioriItemset).map(t => (t,1)).reduceByKey((a,b) => a+b)
    val candidates = candidateOriginal.map(t => t._1.toList).sortBy(itemset => itemset.size)
    val candidateList = candidates.collect()
//    val candidatePrint = candidateOriginal.map(t => t._1.toList).sortBy(itemset => itemset.size)
//    val candidateListToPrint = candidatePrint.collect()
//    println(candidateListToPrint)


    def countAllItemset(iter: Iterator[Set[String]]): Iterator[(Set[String], Int)] = {
      val counterList = new ListBuffer[(Set[String], Int)]()

      val start4 = System.nanoTime
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

      val countTime = (System.nanoTime - start4) / 1e9d
      println("Count Duration: " + countTime)
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
