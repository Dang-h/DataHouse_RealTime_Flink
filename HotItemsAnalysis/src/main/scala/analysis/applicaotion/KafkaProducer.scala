package analysis.applicaotion

import analysis.utils.MyUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.BufferedSource

object KafkaProducer {

	//从文件读取数据存入kafka，供消费者消费
	def main(args: Array[String]): Unit = {
		writeFileToKafka("hotItems")
	}

	def writeFileToKafka(topic: String): Unit = {
		val producer = new KafkaProducer[String, String](MyUtils.getKafkaProperties)

//		val inputBS: BufferedSource = io.Source.fromFile("F:\\workSpace\\DataHouse_RealTime_Flink\\HotItemsAnalysis\\src\\main\\resources\\testKafka.csv")
		val inputBS: BufferedSource = io.Source.fromFile("F:\\workSpace\\DataHouse_RealTime_Flink\\HotItemsAnalysis\\src\\main\\resources\\testKafka.csv")

		for (line <- inputBS.getLines()) {
			val record = new ProducerRecord[String, String](topic, line)
			producer.send(record)
		}

		producer.close()
	}
}
