package WordClass

import org.apache.spark._

object WordCount {
  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","D:\\UMKC\\PB\\Project\\spark-2.4.5-bin-hadoop2.7" )
    //val inputFile = args(0)
    //val outputFile = args(1)
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[*]")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    // Load our input data.
    //val input =  sc.textFile(inputFile)
    val input = sc.textFile("input")
    // Split up into words.
    val words = input.flatMap(line => line.split(" "))
    // Transform into word and count.
    val counts = words.map(word => (word, 1)) .reduceByKey{case (x, y) => x + y}
    // Save the word count back out to a text file, causing evaluation.
    counts.saveAsTextFile("outputfinal")

    val file = input.flatMap(l=>l.split(" ")).filter(value=>value=="new")
    println("count of new",file.count())
    counts.top(2) .foreach(println)

  }
}
