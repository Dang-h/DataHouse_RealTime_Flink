package txMatch.application

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import txMatch.bean.{OrderEvent, ReceiptEvent}

//订单支付实时监控
//两条流的订单交易合并

object TxMatch {

	//定义不匹配的测输出流
	val unmatchedPay = new OutputTag[OrderEvent]("unMatched payment")
	val unmatchedReceipt = new OutputTag[ReceiptEvent]("unMatched receipt")

	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
		env.setParallelism(1)

		//输入流1
		val orderEventStream: DataStream[OrderEvent] = env.fromCollection(
			List(
				OrderEvent(1, "create", "", 1558430842),
				OrderEvent(2, "create", "", 1558430843),
				OrderEvent(1, "pay", "111", 1558430844),
				OrderEvent(2, "pay", "222", 1558430845),
				OrderEvent(3, "create", "", 1558430849),
				OrderEvent(3, "pay", "333", 1558430849)
			)
		)

		val dataDS: DataStream[OrderEvent] = orderEventStream.assignTimestampsAndWatermarks(
			new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(1)) {
				override def extractTimestamp(element: OrderEvent): Long = element.eventTime * 1000L
			}
		)

		//筛选txId不为空
		val orderInputKS: KeyedStream[OrderEvent, String] = dataDS.filter(_.txId != "").keyBy(_.txId)

		//输入流2
		val receiptEventStream: DataStream[ReceiptEvent] = env.fromCollection(
			List(
				ReceiptEvent("111", "wechat", 1558430847),
				ReceiptEvent("222", "alipay", 1558430848),
				ReceiptEvent("444", "alipay", 1558430850)
			)
		)

		val dataDS2: DataStream[ReceiptEvent] = receiptEventStream.assignTimestampsAndWatermarks(
			new BoundedOutOfOrdernessTimestampExtractor[ReceiptEvent](Time.seconds(1)) {
				override def extractTimestamp(element: ReceiptEvent): Long = element.eventTime * 1000L
			}
		)

		val receiveInputKS: KeyedStream[ReceiptEvent, String] = dataDS2.keyBy(_.txId)

		//合并两条流，匹配
		val resultDS1 = orderInputKS.connect(receiveInputKS)
		val resultDS: DataStream[(OrderEvent, ReceiptEvent)] = resultDS1.process(new TxMatchDetection())

		resultDS.print("TxMatch")
		resultDS.getSideOutput(unmatchedPay).print("unMatchedPay")
		resultDS.getSideOutput(unmatchedReceipt).print("unMatchedReceipt")

		env.execute("TxMatch Detect")

	}


	//不同的状态输出到不同的流
	class TxMatchDetection() extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {

		//定义状态，表示pay事件和receive事件
		lazy val payState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay-state", classOf[OrderEvent]))
		lazy val receiptState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receive-state", classOf[ReceiptEvent]))


		override def processElement1(pay: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {

			//获取状态内容
			val receipt: ReceiptEvent = receiptState.value()

			//pay事件到来
			//判断是否有对应的receive事件
			if (receipt != null) {
				//正常匹配，输出到主流
				out.collect((pay, receipt))
				receiptState.clear()
			} else {
				payState.update(pay)
				ctx.timerService().registerEventTimeTimer(pay.eventTime * 1000L + 3000L)
			}
		}

		override def processElement2(receipt: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
			val pay: OrderEvent = payState.value()

			if (pay != null) {
				out.collect((pay, receipt))
				payState.clear()
			} else {
				receiptState.update(receipt)
				ctx.timerService().registerEventTimeTimer(receipt.eventTime * 1000L + 3000L)
			}

		}

		override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {

			val pay: OrderEvent = payState.value()
			val receipt: ReceiptEvent = receiptState.value()

			//分两种情况触发定时器，pay和receipt
			//如果两个状态都有值，正常匹
			//要么pay没有要么receipt没有
			if (payState.value() != null) {
				//不为空，说明receipt没有到
				ctx.output(unmatchedPay, pay)
			}
			if (receipt != null) {
				ctx.output(unmatchedReceipt, receipt)
			}

			payState.clear()
			receiptState.clear()
		}
	}

}
