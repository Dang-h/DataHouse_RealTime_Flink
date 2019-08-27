package analysis.application

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util

import analysis.bean.{ApacheLogEvent, UrlViewCount}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object NetworkFlow {
	/*
	基本思路：
	1、从日志中获取数据，并转换时间为时间戳
	2、按照Url分区
	3、构建滑窗，size为10 min，slide为5 s
	4、聚合
	5、按照访问页面数降序排列取前五
 	*/

	def main(args: Array[String]): Unit = {

		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(1)
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

		// 1、从日志中获取数据，并转换时间为时间戳
		//83.149.9.216 - - 17/05/2015:10:05:56 +0000 GET /favicon.ico
		val inputDS: DataStream[String] = env.readTextFile("F:\\workSpace\\DataHouse_RealTime_Flink\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log")
		val dataDS: DataStream[ApacheLogEvent] = inputDS.map(data => {

			//切分数据，提取时间并转为时间戳
			val dataArray: Array[String] = data.split(" ")
			val timeFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
			val timestamp: Long = timeFormat.parse(dataArray(3).trim).getTime
			//转换成ApacheLogEvent样例类
			ApacheLogEvent(dataArray(0).trim, dataArray(1).trim, timestamp, dataArray(5).trim, dataArray(6).trim)
		}).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(60)) {
			override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime
		})

		//利用正则只留下后缀为html和xhtml的数据

		// 2、按照Url分区
		val urlKeyByKS: KeyedStream[ApacheLogEvent, String] = dataDS.keyBy(_.url)

		// 3、构建滑窗，size为10 min，slide为5 s
		val slideWindow: WindowedStream[ApacheLogEvent, String, TimeWindow] = urlKeyByKS.timeWindow(Time.minutes(10), Time.seconds(5))

		// 4、 聚合，一个窗口关闭获得一组数据
		val aggDS: DataStream[UrlViewCount] = slideWindow.aggregate(new CountAgg(), new WindowResult())

		// 5、按照窗口聚合并对访问页面数降序排列取前五
		val resultDS: DataStream[String] = aggDS.keyBy(_.windowEnd).process(new TopNHotUrl(5))

		resultDS.print("Network Flow")


		env.execute("Network Flow")

	}

}

//[输入数据,中间状态，输出count]
class CountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {

	override def createAccumulator(): Long = 0L

	override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1L

	override def getResult(accumulator: Long): Long = accumulator

	override def merge(a: Long, b: Long): Long = a + b
}

//window要关闭，总体汇总操作
//传入的类型[Long为countAgg的输出, UrlViewCount为包装好的样例类, String为Url, TimeWindow为滑动时间窗口]
class WindowResult() extends WindowFunction[Long, UrlViewCount, String, TimeWindow] {

	override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
		out.collect(UrlViewCount(key, window.getEnd, input.iterator.next()))
	}
}

//自定义排序规则
//[key为windowEnd类型，为aggDS的输出，输出]
class TopNHotUrl(n: Int) extends KeyedProcessFunction[Long, UrlViewCount, String]{

	//状态变成
	//TODO ？？定义状态
	lazy val urlState:ListState[UrlViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("urlState", classOf[UrlViewCount]))

	//可输出0个或多个元素克更新内部状态或者设置定时器
	override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
		urlState.add(value)
		//注册定时器
		ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
	}

	//定时器回调函数
	override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
		//把状态中的数全拿出来
		val allUrlViews = new ListBuffer[UrlViewCount]()

		val iter: util.Iterator[UrlViewCount] = urlState.get().iterator()

		while (iter.hasNext){
			allUrlViews += iter.next()
		}
		//添加完毕，清除状态
		urlState.clear()

		//排序
		val sortedUrl: ListBuffer[UrlViewCount] = allUrlViews.sortWith(_.count > _.count).take(n)

		//格式化输出
		val result = new StringBuilder()

		result.append("=======================\n")
		result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")

		for (i <- sortedUrl.indices) {
			val currentUrlView: UrlViewCount = sortedUrl(i)
			// e.g.  No1：  URL=/blog/tags/firefox?flav=rss20  流量=55
			result.append("No").append(i+1).append(":")
			  .append("  URL=").append(currentUrlView.url)
			  .append("  流量=").append(currentUrlView.count).append("\n")
		}
		result.append("====================================\n")

		Thread.sleep(1000)

		out.collect( result.toString() )
	}
}

