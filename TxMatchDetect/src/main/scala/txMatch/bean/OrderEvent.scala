package txMatch.bean

//输入订单事件样例类
case class OrderEvent(orderId:Long,
					  eventType:String,
					  txId:String,		//交易ID
					  eventTime:Long)
