package detect.bean

//登录信息
case class LoginEvent(userId:Long,
					  ip:String,
					  eventType:String,
					  eventTime:Long)
