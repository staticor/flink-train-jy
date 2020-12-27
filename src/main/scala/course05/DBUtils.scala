package course05

import scala.util.Random

object DBUtils {

  /**
   *
   */
  def getConnection():String ={
    new Random().nextInt(13210) + ""
  }

  def returnConnection(connection:String): Unit ={
    println(s"归还connect: $connection")
  }

}
