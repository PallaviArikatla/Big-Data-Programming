package WordClass

import org.apache.spark._

object DFS {
  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","D:\\UMKC\\PB\\Project\\spark-2.4.5-bin-hadoop2.7")
    val conf = new SparkConf().setAppName("merge sort").setMaster("local[*]")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    type Vertex = Int
    type Graph = Map[Vertex, List[Vertex]]
    //      val g: Graph=Map(1 -> List(2,4,6), 2 -> List(4,7))

    val g: Graph=Map(1 -> List(7,9), 7 -> List(1,8),8 -> List(7,9), 9 -> List(1,8))

    def DFS(start: Vertex, g: Graph): List[Vertex] = {
      def DFS0(vertex: Vertex,visited: List[Vertex]): List[Vertex] = {
        println(vertex)

        println(visited)
        if(visited.contains(vertex)) {
          visited
        }
        else {
          val newNeighbor = g(vertex).filterNot(visited.contains)
          println(newNeighbor)
          newNeighbor.foldLeft(vertex :: visited)((b, a) => DFS0(a, b))
        }
      }

      DFS0(start, List()).reverse
    }
    val dfsresult=DFS(1,g)

    println("DFS Output",dfsresult.mkString(","))
  }
}