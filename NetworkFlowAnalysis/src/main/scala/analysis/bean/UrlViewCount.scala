package analysis.bean

//统计结果：页面访问日志信息
case class UrlViewCount(url:String,
						windowEnd:Long, //窗口关闭时间
						count:Long)
