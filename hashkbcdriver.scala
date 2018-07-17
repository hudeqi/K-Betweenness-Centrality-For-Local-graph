package KBC
import java.util.Calendar

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.{HashMap, ListBuffer, Stack}
import scala.reflect.ClassTag
/**
  * Created by hudeqi on 2017/2/16.
  */
object hashkbcdriver {
  //var medKBC = new HashMap[VertexId, Double]()
  def main(args: Array[String]) {
    // Create spark context
    val appName = "hashkBCDriver"
    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)

    // Graph partition params
    //val DEFAULT_K = 2
    //val DEFAULT_EDGE_PARTITIONS=60
    val DEFAULT_CANONICAL_ORIENTATION = true
    val k = args(0).toInt
    println("k : " + k)
    val canonicalOrientation = DEFAULT_CANONICAL_ORIENTATION
    val numEdgePartitions = args(1).toInt

    // Input params
    //val DEFAULT_INPUT_DIR="/tmp/input/"
    //val DEFAULT_INPUT_FILE_NAME="edge_list.txt"
    val inputDir = args(2)
    val inputFileName = args(4)
    val kk = args(5).toInt
    val inputPath = inputDir + inputFileName
    println("inputPath : " + inputPath)

    // Output params
    //val DEFAULT_OUTPUT_DIR="/tmp/output/"
    val DEFAULT_V_OUTPUT_FILE = List(inputFileName, "hashkbc", k, "vertices").mkString("_") + ".txt"
    //val DEFAULT_E_OUTPUT_FILE=List(inputFileName,"realkbc",k,"edges").mkString("_")+".txt"
    val outputDir = args(3)
    val outputVerticesFileName = sc.hadoopConfiguration.get("outputVerticesFileName", DEFAULT_V_OUTPUT_FILE)
    //val outputEdgesFileName = sc.hadoopConfiguration.get("outputEdgesFileName", DEFAULT_E_OUTPUT_FILE)
    val outputVerticesPath = sc.hadoopConfiguration.get("outputVerticesPath", outputDir + outputVerticesFileName)
    //val outputEdgesPath = sc.hadoopConfiguration.get("outputEdgesPath", outputDir+outputEdgesFileName)
    println("outputVerticesPath : " + outputVerticesPath)
    //println("outputEdgesPath : " + outputEdgesPath)

    // Read graph
    val graph = GraphLoader.edgeListFile(sc, inputPath, canonicalOrientation, numEdgePartitions).partitionBy(PartitionStrategy.EdgePartition2D).cache()
    val med = new HashMap[VertexId, Double]()
    var elist = new ListBuffer[(VertexId, VertexId)]()
    for (t <- graph.vertices.collect()) {
      med.put(t._1, 0.0)
    }
    for (edge <- graph.edges.collect())
      elist += ((edge.srcId, edge.dstId))
    val medKBC = sc.accumulator(med, "medaccum")(myaccum)
    // val graphboard = sc.broadcast(graph)
    val elistboard = sc.broadcast(elist)
    // Run kBC
    println(Calendar.getInstance().getTime().toString + ": start kBC")
    graph.mapVertices((id, attr) => (getKNeighbourGraph(id, kk, elistboard.value)))
      .mapVertices((id,attr) => (id,realcomputeVertexBetweenessCentrality(id, k, attr.head._1, attr.head._2, medKBC))).vertices.count()

    val dd = medKBC.value.toSeq
    sc.parallelize(dd).map { case (k, v) => (k, 0.5 * v) }.map { a => (a._2, a._1) }.sortByKey(false).map { b => (b._2, b._1) }.coalesce(1).saveAsTextFile(outputVerticesPath + "hash3gu")

  }

  object myaccum extends AccumulatorParam[HashMap[VertexId, Double]] {
    override def addInPlace(r1: HashMap[VertexId, Double], r2: HashMap[VertexId, Double]): HashMap[VertexId, Double] = {
      (r1 /: r2) { case (hashmap, (k, v)) => hashmap.+=(k -> (v + hashmap.getOrElse(k, 0.0))) }
    }

    def zero(initialValue: HashMap[VertexId, Double]): HashMap[VertexId, Double] = {
      new HashMap[VertexId, Double]()
    }
  }

  def realcomputeVertexBetweenessCentrality(id: VertexId, k: Int, vlist: ListBuffer[VertexId],elist: ListBuffer[(VertexId, VertexId)], medKBC: Accumulator[HashMap[graphx.VertexId, Double]]): Unit = {
    //println(id)
    val dist = new HashMap[VertexId, Double]()
    val succ = new HashMap[(Double, VertexId), ListBuffer[VertexId]]()
    val sigma = new HashMap[(Double, VertexId), Double]()
    val delta = new HashMap[(Double, VertexId), Double]()
    val s = new HashMap[Double, Stack[VertexId]]()
    val zan = new HashMap[VertexId, Double]()
    val neighbourMap: HashMap[VertexId, ListBuffer[VertexId]] = getNeighbourMap(vlist, elist)
    //    println(id)
    //neighbourMap.foreach(println)
    //    val medKBC = new HashMap[VertexId, Double]()
    //
    //    for (t <- vlist) {
    //      medKBC.put(t, 0.0)
    //    }

    for (t <- vlist) {
      if (t == id)
        dist.put(t, 0.0)
      else
        dist.put(t, -1.0)
      for (i <- 0 to k) {
        if ((t == id) && (i == 0)) {
          sigma.put((i, t), 1.0)
          delta.put((i, t), 0.0)
          succ.put((i, t), ListBuffer[VertexId]())
        }
        else {
          succ.put((i, t), ListBuffer[VertexId]())
          sigma.put((i, t), 0.0)
          delta.put((i, t), 0.0)
        }
      }
    }
    //System.out.println(sigma)
    for (i <- 0 to vlist.size)
      s.put(i, Stack[VertexId]())
    var phase = 0
    //s.put(phase,Stack[VertexId]())
    s(phase).push(id)

    var count = 1
    while (count > 0) {
      count = 0
      for (v <- s(phase)) {
        for (w <- neighbourMap(v)) {

          if (dist(w) < 0) {
            val fuphase = phase + 1
            s(fuphase).push(w)
            count = count + 1
            dist(w) = dist(v) + 1
          }
          val deltaD = dist(v) - dist(w) + 1.0
          if (deltaD <= Math.min(k, 1))
            sigma((deltaD, w)) = sigma((deltaD, w)) + sigma((0.0, v))
          if (deltaD <= k)
            succ((deltaD, v)).+=(w)
          //          println(succ)

        }
      }
      phase = phase + 1
    }

    for (i <- 1 to k) {
      for (p <- 0 to (phase - 1)) {
        for (v <- s(p)) {
          for (w <- succ((0, v))) {
            sigma((i, w)) = sigma((i, w)) + sigma((i, v))
          }
          if (i < k) {
            for (j <- 1 to (i + 1)) {
              for (w <- succ((j, v)))
                sigma(((i + 1), w)) = sigma(((i + 1), w)) + sigma(((i + 1 - j), v))
            }
          }
        }
      }
    }

    //    println("test2"+ sigma)
    phase = phase - 1
    for (kk <- 0 to k) {
      var p = phase
      while (p > 0) {
        for (v <- s(p)) {
          for (d <- 0 to kk) {
            for (w <- succ((d, v))) {
              for (i <- 0 to (kk - d)) {
                var sum = 0.0
                var e = kk - d - i
                for (j <- 0 to e) {
                  sum = sum + (dabuliu((e - j), e, w, sigma) * sigma((j, v)))
                }
                delta((kk, v)) = delta((kk, v)) + sum * (delta((i, w)) / Math.pow(sigma((0.0, w)), e + 1))
              }
              var sumsigma = 0.0
              for (ii <- 0 to k)
                sumsigma += sigma((ii, w))
              delta((kk, v)) = delta((kk, v)) + (sigma(((kk - d), v)) / sumsigma)
            }
          }
          zan.put(v, delta((kk, v)))
          medKBC += zan
          zan.remove(v)
        }
        p -= 1
      }
    }
  }

  def dabuliu(k: Int, d: Int, w: VertexId, sigma: HashMap[(Double, VertexId), Double]): Double = {
    if (k == 0)
      return Math.pow(sigma((0, w)), d)
    else {
      var sum = 0.0
      for (i <- 1 to k)
        sum = sum - (sigma((i, w)) * dabuliu((k - i), (d - 1), w, sigma))
      return sum
    }
  }

  def getNeighbourMap(vlist: ListBuffer[VertexId], elist: ListBuffer[(VertexId, VertexId)]): HashMap[VertexId, ListBuffer[VertexId]] = {
    val neighbourList = new HashMap[VertexId, ListBuffer[VertexId]]()

    vlist.foreach { case (v) => {
      val nlist = (elist.filter { case (e) => ((e._1 == v) || (e._2 == v)) })
        .map { case (e) => {
          if (e._1 == v) e._2 else e._1
        }
        }
      neighbourList.+=((v, nlist.distinct))
    }
    }

    neighbourList
  }

  def getKNeighbourGraph[VD: ClassTag, ED: ClassTag](id: VertexId, k: Int, elist: ListBuffer[(VertexId, VertexId)]): List[(ListBuffer[VertexId], ListBuffer[(VertexId, VertexId)])] = {
    var edglist = new ListBuffer[(VertexId, VertexId)]()
    var list = new ListBuffer[VertexId]()
    list.append(id)
    var st = new ListBuffer[VertexId]()
    st.append(id)
    for (i <- 1 to k) {
      var l = new ListBuffer[VertexId]()
      st.foreach(v => {
        //        var l = new ListBuffer[VertexId]()
        l = l ++ getNeighbour(v, elist)
        // st = l
      })
      st = l
      for(s <- st){
        if(!list.exists(_ == s))
          list += s
      }
    }
//    list.foreach { case (v) => {
//      val temp = (elist.filter { case (e) => ((e._1 == v) || (e._2 == v)) })
//      for(t <- temp){
//        if((!edglist.exists(_ == t)))
//          if((list.exists(_ == t._1))&&(list.exists(_ == t._2)))
//            edglist += t
//      }
//    }
//    }
    edglist = elist.filter{case (e) => list.exists(_ == e._1) && list.exists(_ == e._2)}
    //   println(id)
    //    list.foreach(println)
    //    edglist.foreach(println)
    List((list,edglist))
  }

  def getNeighbour(id: VertexId, elist: ListBuffer[(VertexId, VertexId)]): ListBuffer[VertexId] = {
    val neighbourList = {
      (elist.filter { case (e) => ((e._1 == id) || (e._2 == id)) })
        .map { case (e) => {
          if (e._1 == id) e._2 else e._1
        }
        }
    }
    neighbourList
  }
}
