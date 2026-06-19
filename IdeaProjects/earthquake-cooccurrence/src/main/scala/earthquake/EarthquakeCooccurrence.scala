package earthquake

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable

object EarthquakeCooccurrence {

  case class Cell(latTenths: Int, lonTenths: Int) {
    def latitude:  Double = latTenths / 10.0
    def longitude: Double = lonTenths / 10.0
    override def toString: String = s"($latitude, $longitude)"
  }

  implicit val cellOrdering: Ordering[Cell] =
    Ordering.by((c: Cell) => (c.latTenths, c.lonTenths))

  def parseRow(row: org.apache.spark.sql.Row): Option[(Int, Cell)] = {
    try {
      val lat = row.getAs[String]("latitude")
      val lon = row.getAs[String]("longitude")
      val dateStr = row.getAs[String]("date")

      val latInt = new java.math.BigDecimal(lat)
        .movePointRight(1)
        .setScale(0, java.math.RoundingMode.HALF_UP)
        .intValueExact()
      val lonInt = new java.math.BigDecimal(lon)
        .movePointRight(1)
        .setScale(0, java.math.RoundingMode.HALF_UP)
        .intValueExact()

      val y = dateStr.substring(0, 4).toInt
      val m = dateStr.substring(5, 7).toInt
      val d = dateStr.substring(8, 10).toInt
      val dateKey = y * 10000 + m * 100 + d

      Some((dateKey, Cell(latInt, lonInt)))
    } catch {
      case _: Exception => None
    }
  }

  def formatDate(d: Int): String = {
    val s = d.toString
    s"${s.substring(0, 4)}-${s.substring(4, 6)}-${s.substring(6, 8)}"
  }

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      println("Usage: EarthquakeCooccurrence <inputPath> <outputPath> [numPartitions]")
      sys.exit(1)
    }

    val inputPath  = args(0)
    val outputPath = args(1)
    val numPartitions = if (args.length > 2) args(2).toInt else 8

    val sparkConf = new SparkConf().setAppName("Earthquake Co-occurrence")
    if (!sparkConf.contains("spark.master")) sparkConf.setMaster("local[*]")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    println(s"[INFO] Partizioni: $numPartitions")
    println(s"[INFO] Default parallelism: ${sc.defaultParallelism}")

    val startTime = System.currentTimeMillis()

    // STEP 1 - Lettura e parsing
    val rawRdd = spark.read
      .option("header", "true")
      .csv(inputPath)
      .repartition(numPartitions)
      .rdd

    val parsed: RDD[(Int, Cell)] = rawRdd.flatMap(parseRow)

    // STEP 2 - Aggregazione per data con deduplicazione locale
    val groupedByDate: RDD[(Int, Array[Cell])] = parsed
      .aggregateByKey(mutable.HashSet.empty[Cell], numPartitions)(
        (set, c) => { set += c; set },
        (a, b)   => { a ++= b; a }
      )
      .filter { case (_, set) => set.size >= 2 }
      .mapValues(_.toArray.sorted)
      .persist(StorageLevel.MEMORY_AND_DISK)

    // STEP 3 - Generazione coppie e conteggio
    val pairCounts: RDD[((Cell, Cell), Int)] = groupedByDate
      .flatMap { case (_, cells) =>
        val n = cells.length
        val buf = new mutable.ArrayBuffer[((Cell, Cell), Int)](n * (n - 1) / 2)
        var i = 0
        while (i < n) {
          var j = i + 1
          while (j < n) {
            buf += (((cells(i), cells(j)), 1))
            j += 1
          }
          i += 1
        }
        buf
      }
      .reduceByKey(_ + _, numPartitions)

    // STEP 4 - Coppia con frequenza massima
    val (bestPair, maxCount) = pairCounts.reduce { (x, y) =>
      if (x._2 >= y._2) x else y
    }

    val (cellA, cellB) = bestPair

    // STEP 5 - Recupero date in cui la coppia vincente co-occorre
    val winningDates = groupedByDate
      .filter { case (_, cells) => cells.contains(cellA) && cells.contains(cellB) }
      .keys
      .collect()
      .sorted

    groupedByDate.unpersist()

    val elapsed = System.currentTimeMillis() - startTime
    println(s"[INFO] Tempo di esecuzione: ${elapsed}ms (${elapsed / 1000.0}s)")
    println(s"[INFO] Co-occorrenze trovate: $maxCount")

    val out = new StringBuilder
    out.append(s"($cellA, $cellB)\n")
    winningDates.foreach(d => out.append(formatDate(d)).append('\n'))

    println(out.toString())

    if (!outputPath.startsWith("gs://")) {
      import java.io.File
      def deleteRecursively(f: File): Unit = {
        if (f.exists()) {
          if (f.isDirectory) f.listFiles().foreach(deleteRecursively)
          f.delete()
        }
      }
      deleteRecursively(new File(outputPath))
    }

    sc.parallelize(Seq(out.toString()), 1).saveAsTextFile(outputPath)

    spark.stop()
  }
}