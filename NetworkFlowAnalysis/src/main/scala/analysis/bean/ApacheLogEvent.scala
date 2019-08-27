package analysis.bean

//输入数据：数据日志信息
//83.149.9.216 - - 17/05/2015:10:05:56 +0000 GET /favicon.ico
case class ApacheLogEvent(ip:String,
						  usrId:String,
						  eventTime:Long,
						  vistMethod:String,
						  url:String)
