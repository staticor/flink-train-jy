package course0203

import org.apache.flink.api.scala.{ExecutionEnvironment, createTypeInformation}
// 引入隐式转换
import org.apache.flink.api.scala._

object BatchWCApp {

// TODO ... 1) 参考Scala课程   2) API

  def main(args: Array[String]): Unit = {
    val inputPath = "/Users/staticor/FlinkProject/inputData"

    val env = ExecutionEnvironment.getExecutionEnvironment

    val text = env.readTextFile(inputPath)

    text.flatMap(_.toLowerCase.split(","))
      .filter(_.nonEmpty)
      .map((_, 1))
      .groupBy(0)
      .sum(1)
      .print()
  }
}
