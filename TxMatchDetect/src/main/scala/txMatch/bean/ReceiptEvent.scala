package txMatch.bean

//输入到账样例类
case class ReceiptEvent(txId:String,
						payChannel:String,
						eventTime:Long)
