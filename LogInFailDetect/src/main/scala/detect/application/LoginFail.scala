package detect.application

import java.util

import detect.bean.{LoginEvent, Warning}
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object LoginFail {

	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(1)
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

		//		 1. 定义输入数据流
		val loginEventStream: DataStream[LoginEvent] = env.fromCollection(List(
			LoginEvent(1, "192.168.0.1", "fail", 1558430842),
			LoginEvent(1, "192.168.0.3", "success", 1558430846),
			LoginEvent(1, "192.168.0.2", "fail", 1558430843),
			LoginEvent(1, "192.168.0.3", "fail", 1558430844),
			LoginEvent(2, "192.168.10.10", "success", 1558430845)
		))

//		val logMapDS: DataStream[LoginEvent] = loginEventStream.map(data => {
//			val dataArray: Array[String] = data.toString.split(",")
//			LoginEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
//		})
		val logKeyByKS: KeyedStream[LoginEvent, Long] = loginEventStream.assignTimestampsAndWatermarks(
			new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(1L)) {
				override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000
			}).keyBy(_.userId)

		//定义匹配模式
		val logFailPattern: Pattern[LoginEvent, LoginEvent] = Pattern.begin[LoginEvent]("begin")
		  .where((_: LoginEvent).eventType == "fail")
		  .next("next")
		  .where(_.eventType == "fail")
		  .within(Time.seconds(10))

		//得到pattern Stream
		val patternS: PatternStream[LoginEvent] = CEP.pattern(logKeyByKS, logFailPattern)

		//用select检出符合模式的事件
		val loginFailDS: DataStream[Warning] = patternS.select(new LoginFailMatch())

		loginFailDS.print("warning")
		loginEventStream.print("input")

		env.execute("LoginFail Alert for CEP")
	}

}

//输出报警信息
class LoginFailMatch() extends PatternSelectFunction[LoginEvent, Warning]{

	override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
		//按照模式名称拉去对应登录失败事件
		val firstFail: LoginEvent = map.get("begin").iterator().next()
		val secondFail: LoginEvent = map.get("next").iterator().next()
		Warning(firstFail.userId, firstFail.eventTime, secondFail.eventTime, "2s内连续登录2次失败")
	}
}
