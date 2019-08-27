package analysis.bean

//输出结果
//商品ID itemId
//窗口关闭时间 windowEnd
//商品浏览量 count
case class ItemViewCount(itemId: Long,
						 windowEnd: Long,
						 count: Long)
