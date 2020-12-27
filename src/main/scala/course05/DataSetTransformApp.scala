package course05


import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._

import scala.collection.mutable.ListBuffer

object DataSetTransformApp {

  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

//    env.fromCollection( List(1,2,3,45,123,1))
////    env.fromElements(1,2,3,4,5,6,7,8)
//      .map( (x:Int) => x * x)
//      .map( _ + 1)
//      .filter(x => x > 5)
//      .filter( _ > 10)
//      .print()

//    mapPartitionFunctionDemo(env)

//    env.fromElements("Hello, world")
//      .flatMap( x => x.split(","))
////        .groupBy(0)
////        .sum(1)
//        .print()

//    firstFunctionDemo(env)

    flatMapDemo(env)
  }


  def mapPartitionFunctionDemo(env: ExecutionEnvironment): Unit ={
    // 100 个元素 把结果存储到数据库
    val students = new ListBuffer[String]
    for(i <- 1 to 100){
      students.append(s"student: $i")
    }

    val data = env.fromCollection(students).setParallelism(4)
      data.mapPartition( x => {
        val connection = DBUtils.getConnection()
        println(connection + "....")
        DBUtils.returnConnection(connection)
        x
      }

      )

      .print()

  }



  def firstFunctionDemo(env: ExecutionEnvironment): Unit = {
    val info = ListBuffer[(Int, String)]()
    info.append((1, "Hadoop"))
    info.append((1, "Spark"))
    info.append((1, "Flink"))
    info.append((2, "Java"))
    info.append((2, "Python"))
    info.append((3, "Linux"))
    info.append((4, "VUE"))

    val data = env.fromCollection(info)

//    data.first(3).print()

//    data.groupBy(0).first(2).print()
    data.groupBy(0)
//      .sortGroup(1, Order.ASCENDING)
      .sortGroup(1, Order.DESCENDING)
      .first(2).print()
  }


  def flatMapDemo(env: ExecutionEnvironment): Unit ={
    val info = ListBuffer[String]()
    info.append( "Hadoop,11")
    info.append( "Spark,22")
    info.append( "Flink,33")
    env.fromCollection(info)
        .map( x=> (x.toLowerCase))
        .flatMap(_.split(",").toList)
      .map( x => (x, 1))
      .groupBy(0)
      .sum(1)
      .print()
  }
}
