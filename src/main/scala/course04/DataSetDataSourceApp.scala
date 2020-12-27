package course04

import org.apache.flink.api.java.{DataSet, ExecutionEnvironment}
import org.apache.flink.configuration.Configuration

object DataSetDataSourceApp {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

//    fromLocalCollection(env)
//    fromLocalFile(env)
//
//    fromLocalCSV(env)
//
//    fromLocalCSV2(env)

//    readRecursiveFiles(env)

    readCompressedFiles(env)
  }

  def fromLocalCollection(env: ExecutionEnvironment): Unit ={

    val data = 1 to 10
    env.fromElements(data).print()
  }


  def fromLocalFile(env: ExecutionEnvironment): Unit = {
//    val fileName = "/Users/staticor/FlinkProject/data/test.data"
    val fileName = "/Users/staticor/FlinkProject/data01/"
    env.readTextFile(fileName).print()
  }


  def fromLocalCSV(env: ExecutionEnvironment): Unit ={
    val fileName = "/Users/staticor/FlinkProject/csv/test.data"

    // read a CSV file with three fields// read a CSV file with three fields
    val csvInput = env.readCsvFile(fileName)
      .ignoreFirstLine()
      .types(classOf[String], classOf[Integer], classOf[String])
//      .groupBy(0).sum(1)

//      .ignoreFirstLine()
      csvInput.print()
  }

  def fromLocalCSV2(env: ExecutionEnvironment): Unit ={
    val fileName = "file:///Users/staticor/FlinkProject/csv/test.data"
    case class Mycase(name: String, age: Int)
    env.readCsvFile(fileName).ignoreFirstLine()
      .types(classOf[String], classOf[Int])
      .print()
  }




  def readRecursiveFiles(env: ExecutionEnvironment): Unit ={
    val fileName = "file:///Users/staticor/FlinkProject/data03"
    val parameters = new Configuration

    parameters.setBoolean("recursive.file.enumeration", true)

    env.readTextFile(fileName)
      .withParameters(parameters)
      .print()
  }


  def readCompressedFiles(env: ExecutionEnvironment): Unit ={
    val fileName = "file:///Users/staticor/FlinkProject/data04"
    env.readTextFile(fileName)
//      .com
      .print()
  }


}
