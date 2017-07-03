package redbee

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}

import scala.util.Properties

object Settings {

  object jdbc {

    def user = Properties.envOrElse("JDBC_USER", jdbc.getString("user"))

    def password = Properties.envOrElse("JDBC_PASSWORD", jdbc.getString("password"))

    def host = Properties.envOrElse("JDBC_HOST", jdbc.getString("host"))

    def port = Properties.envOrElse("JDBC_PORT", jdbc.getString("port")).toInt

    def database = Properties.envOrElse("JDBC_DATABASE", jdbc.getString("database"))

    private val jdbc = config.getConfig("jdbc")
  }

  def numPartitions = config.getInt("num_partitions")

  private val config: Config = ConfigFactory.parseFile(new File("application.conf"))
}
