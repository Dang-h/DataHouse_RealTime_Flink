package analysis.applicaotion

import java.lang
import java.sql.Timestamp
import java.util.Properties

import analysis.bean.{ItemViewCount, UserBehavior}
import analysis.utils.MyUtils
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


object HotItem {
	/*
	基本思路：
		1、 从源数据中获取数据，过滤出用户行为“pv”的数据
		2、按照商品ID分区
		3、构建滑窗，size为60 min，slide为5 min
		4、聚合
		5、商品数降序排列取前三
	 */

	def main(args: Array[String]): Unit = {
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(1)
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

		//从文件中获取数据
		//		val inputDS: DataStream[String] = env.readTextFile("F:\\workSpace\\DataHouse_RealTime_Flink\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")

		//从kafka中获取数据
		val inputDS: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("hotItems", new SimpleStringSchema(), MyUtils.getKafkaProperties))

		val dataDS: DataStream[UserBehavior] = MyUtils.dataMapOp(inputDS).assignTimestampsAndWatermarks(
			new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.seconds(1)) {
				override def extractTimestamp(element: UserBehavior): Long = element.timestamp * 1000
			}
		)

		// 1 从源数据中获取数据，过滤出用户行为“pv”的数据
		val filterDS: DataStream[UserBehavior] = dataDS.filter(_.behavior == "pv")

		// 2 按照商品ID分区,得到KeyedStream
		val keyByKS: KeyedStream[UserBehavior, Long] = filterDS.keyBy(_.itemId)

		// 3 构建滑窗，size为60 min，slide为5 min 得到windowedStream
		val slideWindowWS: WindowedStream[UserBehavior, Long, TimeWindow] = keyByKS.timeWindow(Time.hours(1), Time.minutes(5))

		//4 聚合; preAggregator预聚合操作, windowFunction等窗口要关闭时要做的操作（包装成ItemViewCount输出）
		val aggregateDS: DataStream[ItemViewCount] = slideWindowWS.aggregate(new CountAgg(), new WindowResult())

		//5 相同时间的窗口聚合，然后对其按商品数降序排列取前三
		val resultDS: DataStream[String] = aggregateDS.keyBy(_.windowEnd).process(new TopNtoItems(3))

		resultDS.print("hotItems")

		env.execute("Hot Items")

	}

}

//窗口聚合；自定义语句和函数，来一个数据加一个
//继承AggregateFunction，【需聚合的输入类型，聚合过程中保持的状态类型，聚合完的输出类型】
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {

	//初始值累加器，相当于中间状态
	override def createAccumulator(): Long = 0L

	//定义累加规则，来一个数就+1
	override def add(in: UserBehavior, acc: Long): Long = acc + 1

	//聚合后输出结果
	override def getResult(acc: Long): Long = acc

	//不同分区如何merge
	override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

//窗口关闭最后要做输出，【IN：聚合的result，OUT:ItemViewCount,KEY:ItemId,Window:类型为TimeWindow】
class WindowResult() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {

	override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
		//包装陈ItemViewCount输出
		val itemId = key
		//获得窗口的结束时间戳
		val windowEnd = window.getEnd
		val count = input.iterator.next()
		out.collect(ItemViewCount(itemId, windowEnd, count))
	}

}

//【Key，Input，Output】
//来一个数据存一个
class TopNtoItems(n: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {

	//定义状态，用来保存所有ItemViewCount
	private var itemState: ListState[ItemViewCount] = _

	//生命周期方法
	override def open(parameters: Configuration): Unit = {
		itemState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemViewState", classOf[ItemViewCount]))
	}

	override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
		//每一条数据都存入state中
		itemState.add(value)

		// 注册定时器，延迟？？触发,水位现在已经涨到windowEnd
		ctx.timerService().registerEventTimeTimer(value.windowEnd + 100)
	}

	//timeStamp为注册的定时器时间戳
	override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
		//数据存入List
		val allItem: ListBuffer[ItemViewCount] = new ListBuffer()

		import scala.collection.JavaConversions._
		//从itemState中取出数据，存入allItem
		for (item <- itemState.get()) {
			allItem += item
		}
		//清除状态释放空间
		itemState.clear()

		//按照count降序排列
		//函数柯里化
		val sortedItems: ListBuffer[ItemViewCount] = allItem.sortBy(_.count)(Ordering.Long.reverse).take(n)

		//格式化数据
		val result = new StringBuilder()

		result.append("====================\n")
		result.append("时间: ").append(new Timestamp(timestamp - 100)).append("\n")

		//输出商品信息
		for (elem <- sortedItems.indices) {
			val currentItem: ItemViewCount = sortedItems(elem)

			result.append("No.").append(elem + 1).append(": ")
			  .append("商品ID=").append(currentItem.itemId)
			  .append("浏览量=").append(currentItem.count).append("\n")
		}
		result.append("==============================\n")

		//控制输出频率
		Thread.sleep(500)

		//输出result
		out.collect(result.toString())

	}
}

