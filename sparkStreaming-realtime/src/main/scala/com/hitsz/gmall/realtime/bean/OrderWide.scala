package com.hitsz.gmall.realtime.bean

import com.hitsz.gmall.realtime.util.MyBeanUtils

case class OrderWide(
                    //orderInfo
                      var  detail_id: Long =0L,
                      var  order_id:Long=0L,
                      var  sku_id: Long=0L,
                      var  order_price: Double=0D,
                      var  sku_num:Long=0L,
                      var  sku_name: String=null,
                      var  split_total_amount:Double=0D,
                      var  split_activity_amount:Double=0D,
                      var  split_coupon_amount:Double=0D,
                //orderDetail
                      var  province_id: Long=0L,
                      var  order_status: String=null,
                      var  user_id: Long=0L,
                      var  total_amount:  Double=0D,
                      var  activity_reduce_amount: Double=0D,
                      var  coupon_reduce_amount: Double=0D,
                      var  original_total_amount: Double=0D,
                      var  feight_fee: Double=0D,
                      var  feight_fee_reduce: Double=0D,
                      var  expire_time: String =null,
                      var  refundable_time:String =null,
                      var  create_time: String=null,
                      var operate_time: String=null,
                      var create_date: String=null,
                      var create_hour: String=null,
                //地区维度数据
                      var province_name:String=null,
                      var province_area_code:String=null,
                      var province_3166_2_code:String=null,
                      var province_iso_code:String=null,
                //信息维度数据
                      var user_age :Int=0,
                      var user_gender:String=null

                    ){
  def this(orderInfo : OrderInfo ,orderDetail: OrderDetail){
    this
    mergeOrderInfo(orderInfo)
    mergeOrderDetail(orderDetail)
  }

  def mergeOrderInfo(orderInfo: OrderInfo): Unit ={
    if(orderInfo != null ){
      MyBeanUtils.copyProperties(orderInfo,this)
      this.order_id = orderInfo.id
    }
  }

  def mergeOrderDetail (orderDetail: OrderDetail): Unit ={
    if(orderDetail != null ){
      MyBeanUtils.copyProperties(orderDetail,this)
      this.detail_id = orderDetail.id
    }
  }

}
