package oderTimeout.bean

//输入数据
case class OrderEvent(orderId:Long,
					  eventType:String,
					  eventTime:Long)
