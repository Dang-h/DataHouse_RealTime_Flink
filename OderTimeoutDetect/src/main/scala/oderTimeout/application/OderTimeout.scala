package oderTimeout.application

import java.util

import akka.event.Logging.LogEvent
import oderTimeout.bean.{OrderEvent, OrderResult}
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


//订单支付实时监控
object OderTimeout {

	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
		//可选
		env.setParallelism(1)

		val inputData: DataStream[OrderEvent] = env.fromCollection(List(
			OrderEvent(1, "create", 1558430842),
			OrderEvent(2, "create", 1558430843),
			OrderEvent(2, "other", 1558430845),
			OrderEvent(2, "pay", 1558430850),
			OrderEvent(1, "pay", 1558431920)
		))

		val oderEventDS: DataStream[OrderEvent] = inputData.assignAscendingTimestamps(_.eventTime * 1000).keyBy(_.orderId)

		//定义一个带时间窗口的模式
		//CEP组合模式，以”初始模块“开始
		val orderPattern: Pattern[OrderEvent, OrderEvent] = Pattern.begin[OrderEvent]("start")
		  //where的直接组合为AND
		  .where(_.eventType == "create")
		  //宽松近邻，允许中间出现不匹配事件
		  .followedBy("follow")
		  .where(_.eventType == "pay")
		  //within定义检测窗口
		  .within(Time.minutes(15))

		//定义一个输出表签
		val orderTimeoutOutputTap: OutputTag[OrderResult] = OutputTag[OrderResult]("orderResult")

		//将定义好的模式应用到输出流
		val orderPatternStream: PatternStream[OrderEvent] = CEP.pattern(oderEventDS,orderPattern)

		//匹配事件提取
		val resultDS: DataStream[OrderResult] = orderPatternStream.select(orderTimeoutOutputTap, new OrderTimeoutSelect(), new OrderPaySelect())

		//分别从主流和测输出流中打印数据
		resultDS.print("payed Order")
		resultDS.getSideOutput(orderTimeoutOutputTap).print("timeout Order")


		env.execute("Order Timeout Job")
	}

}

//处理标签检测出的超时序列
class OrderTimeoutSelect() extends  PatternTimeoutFunction[OrderEvent, OrderResult]{
	override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
		val timeoutOrderId: Long = map.get("start").iterator().next().orderId
		OrderResult(timeoutOrderId, "timeout")
	}
}

class OrderPaySelect() extends PatternSelectFunction[OrderEvent,OrderResult]{
	override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
		val payOrderId: Long = map.get("follow").iterator().next().orderId
		OrderResult(payOrderId, "pay successfully")
	}
}
