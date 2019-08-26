package analysis.bean

case class UserBehavior(
						 userId: Long,
						 itemId: Long,
						 categoryId: Int,
						 behavior: String,
						 timestamp: Long)
