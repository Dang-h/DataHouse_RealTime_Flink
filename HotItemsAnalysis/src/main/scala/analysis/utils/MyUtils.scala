package analysis.utils

import java.util.Properties

import analysis.bean.UserBehavior
import org.apache.flink.streaming.api.scala._

object MyUtils {

	def getKafkaProperties: Properties = {
		val properties = new Properties()

		properties.setProperty("bootstrap.servers", "sql:9092")
		properties.setProperty("group.id", "consumer-group")
		properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
		properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
		properties.setProperty("auto.offset.reset", "latest")
		properties
	}

	def dataMapOp(inputData:DataStream[String]):DataStream[UserBehavior]={
		inputData.map(data => {
			val dataArry: Array[String] = data.split(",")
			UserBehavior(dataArry(0).trim.toLong,dataArry(1).trim.toLong, dataArry(2).trim.toInt, dataArry(3).trim, dataArry(4).trim.toLong)
		})
	}




}


