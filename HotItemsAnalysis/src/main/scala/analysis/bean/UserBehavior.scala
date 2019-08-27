package analysis.bean

// 输入数据：源数据分析：
// 561558,加密用户ID userId
// 3611281,加密商品ID itemId
// 965809,加密商品类别categoryId
// pv, 用户行为 behavior
// 1511658000, 时间戳 timestamp
case class UserBehavior(
						 userId: Long,
						 itemId: Long,
						 categoryId: Int,
						 behavior: String,
						 timestamp: Long)
