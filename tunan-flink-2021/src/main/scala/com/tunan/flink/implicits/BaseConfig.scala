package com.tunan.flink.implicits

import com.typesafe.config.{Config, ConfigFactory}

import scala.util.{Failure, Success, Try}

trait BaseConfig {

	var configFileName: String = ""

	 lazy val config: ConnectionConfig = getConfig(configFileName) match {
		case Success(s) => getConnectionConfig(s)
		case Failure(f) => throw f
	}

	private def getConfig(filePath: String): Try[Config] = {
		Try {
			ConfigFactory.load(filePath)
		}
	}

	private def getConnectionConfig(conf: Config): ConnectionConfig = {
		val DRIVER = conf.getString("driver")
		val USERNAME = conf.getString("username")
		val PASSWORD = conf.getString("password")
		val DB = conf.getString("db")
		val HOST = conf.getString("host")
		val PORT = conf.getInt("port")
		ConnectionConfig(DRIVER, USERNAME, PASSWORD, DB, HOST, PORT)
	}
}
