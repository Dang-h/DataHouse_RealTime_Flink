package detect.bean

//报警信息
case class Warning(userId:Long,
				   firstFailTime:Long,
				   lastFailTime:Long,
				   warningMsg:String)
