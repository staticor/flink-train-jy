package  course04

import com.imooc.flink.course04.Person
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.typeutils.RowTypeInfo
// 导入scala的隐式转换
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.Configuration

/**
 * Created On Date: 2019/11/21 0021 13:08
 * Flink 文件读取及DataSet常用方式
 */
object FlinkReadFileTest {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //用于递归读取文件配置参数，默认是False
    val parameters = new Configuration
    parameters.setBoolean("recursive.file.enumeration", true)

    // 从本地读取文件
    val localLines = env.readTextFile("file:///path/to/my/textfile")

    // 从hdfs读取文件，需要写上hdfs的nameService（高可用集群），或者namenode ip及端口号
    val hdfsLines = env.readTextFile("hdfs://nnHost:nnPort/path/to/my/textfile")


    // 从CSV中读取文件，可以指定读取数据范围
    val csvInput2 = env.readCsvFile[(String, Double)](
      "hdfs:///the/CSV/file",
      includedFields = Array(0, 3)) // 读下标为0,4字段

    // 读取CSV可以用样例类作为泛型
    case class MyCaseClass(str: String, dbl: Double)
    val csvInput3 = env.readCsvFile[MyCaseClass](
      "hdfs:///the/CSV/file",
      includedFields = Array(0, 3)) // 读下标为0,4字段


    // 读取CSV同样可以使用POJO作为泛型
    val csvInput = env.readCsvFile[Person](
      "hdfs:///the/CSV/file", pojoFields = Array("name", "age", "zipcode"))


    // 从CSV中递归读取文件
    val csvInput1 = env.readCsvFile[(String, String, String)]("d://tmp/t/")
      // 使用递归方式读取数据
      .withParameters(parameters).print()

    /**
     * 读取CSV还有以下常用参数，便于我们灵活配置
     * fieldDelimiter: String指定分隔记录字段的定界符。默认的字段分隔符是逗号','
     * lenient: Boolean启用宽大的解析，即忽略无法正确解析的行。默认情况下，宽松的分析是禁用的，无效行会引发异常。
     * includeFields: Array[Int]定义要从输入文件读取的字段（以及忽略的字段）。
     * 默认情况下，将解析前n个字段（由types()调用中的类型数定义） Array(0, 3)
     * lineDelimiter: String指定单个记录的分隔符。默认的行定界符是换行符'\n'
     * pojoFields: Array[String]指定映射到CSV字段的POJO的字段。
     * CSV字段的解析器会根据POJO字段的类型和顺序自动初始化。 pojoFields = Array("name", "age", "zipcode")
     * parseQuotedStrings: Character启用带引号的字符串解析。如果字符串字段的第一个字符是引号字符（不修剪前导或尾部空格）
     * ，则将字符串解析为带引号的字符串。带引号的字符串中的字段定界符将被忽略。
     * 如果带引号的字符串字段的最后一个字符不是引号字符，则带引号的字符串解析将失败。
     * 如果启用了带引号的字符串解析，并且该字段的第一个字符不是带引号的字符串，则该字符串将解析为未带引号的字符串。
     * 默认情况下，带引号的字符串分析是禁用的。
     * ignoreComments: String指定注释前缀。以指定的注释前缀开头的所有行都不会被解析和忽略。默认情况下，不忽略任何行。
     * ignoreFirstLine: Boolean将InputFormat配置为忽略输入文件的第一行。默认情况下，不忽略任何行。
     */


    // 使用元素创建DataSet
    val values = env.fromElements("Foo", "bar", "foobar", "fubar")

    // 生成一个数据序列作为DataSet
    val numbers = env.generateSequence(1, 10000000)

//    // 使用JDBC输入格式从关系数据库读取数据
//    val inputMysql = env.createInput(JDBCInputFormat.buildJDBCInputFormat()
//      // 数据库连接驱动名称
//      .setDrivername("com.mysql.jdbc.driver")
//      // 数据库连接驱动名称
//      .setDBUrl("jdbc:mysql://")
//      // 数据库连接用户名
//      .setUsername("root")
//      // 数据库连接密码
//      .setPassword("password")
//      // 数据库连接查询SQL
//      .setQuery("select name,age,class from test")
//      // 字段类型,顺序个个数必须与SQL保持一致
//      .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO,
//        BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO))
//      .finish()



  }
}

