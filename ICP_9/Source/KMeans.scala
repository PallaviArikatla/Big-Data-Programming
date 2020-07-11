package WordClass

import org.apache.spark._
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors

object Kmeans {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir","D:\\UMKC\\PB\\Project\\spark-2.4.5-bin-hadoop2.7" )
    val conf = new SparkConf().setAppName("k-means application").setMaster("local[*]")
    val sc = new SparkContext(conf)


    val n_clusters = 3
    val n_max_iteration = 20

    /* Load and parse the data */
    val r_csv = sc.textFile("input");

    /* Find the headers */
    val c_header = r_csv.first;

    /* Remove the header */
    val r_data = r_csv.filter(_(0) != c_header(0));
    val r_parsedData = r_data.map(s => Vectors.dense(s.split(",").map(_.toDouble))).cache()

    /* Cluster the data into two classes using KMeans */
    val x_clusters = KMeans.train(r_parsedData, n_clusters, n_max_iteration)

    /* Evaluate clustering by computing Within Sum of Squared Errors */
    val n_wsse = x_clusters.computeCost(r_parsedData)

    println("Within Sum of Squared Errors = " + n_wsse)
    println("Cluster centers = ")
    x_clusters.clusterCenters.foreach(println)
  }
}