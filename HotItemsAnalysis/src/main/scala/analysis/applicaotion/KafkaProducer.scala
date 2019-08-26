package analysis.applicaotion

object KafkaProducer {

	def writeToKafka(topic: String): Unit = {

	}


	//从文件读取数据存入kafka，供消费者消费
	def main(args: Array[String]): Unit = {
		writeToKafka("hotItems")
	}

}
