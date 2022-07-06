package com.hitsz.gmall.realtime.bean

/*
日志数据解析而来
 */
case class PageLog(
                  mid:String,
                  user_id: String,
                  province_id: String,
                  channer: String,
                  is_new: String,
                  model: String,
                  operate_system:String,
                  version_code: String,
                  brand: String,
                  page_id: String,
                  last_page_id: String,
                  page_item: String,
                  page_item_type: String,
                  during_time: Long,
                  sourceType: String,
                  ts:Long
                  ) {

}
