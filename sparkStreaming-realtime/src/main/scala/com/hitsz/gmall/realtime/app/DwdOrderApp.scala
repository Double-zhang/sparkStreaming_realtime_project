package com.hitsz.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig

import com.hitsz.gmall.realtime.bean.{OrderDetail, OrderInfo, OrderWide}
import com.hitsz.gmall.realtime.util.{MyEsUtils, MyKafkaUtils, MyOffsetsUtils, MyRedisUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.Second
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.text.SimpleDateFormat
import java.time.{LocalDate, Period}
import java.util
import scala.collection.mutable.ListBuffer

/*

订单宽表任务
1.准备实时环境
2.从redis中读取offset
3.从kafka中消费数据
4.提取offset
5.数据处理
  转换结构
  1.维度关联
  2.双流join
6.写入ES
7.提交offset
 */
object DwdOrderApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("dwd_order_info").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    //读取offset
    val orderInfoTopicName = "DWD_ORDER_INFO_I"
    val orderInfoGroup = "DWD_ORDER_INFO:GROUP"

    val orderInfoOffsets:Map[TopicPartition, Long] = MyOffsetsUtils.readOffset(orderInfoTopicName, orderInfoGroup)

    val orderDetailTopicName = "DWD_ORDER_DETAIL_I"
    val orderDetailGroup = "DWD_ORDER_DETAIL:GROUP"
    val orderDetailOffsets = MyOffsetsUtils.readOffset(orderDetailTopicName, orderDetailGroup)

    //3.从kafka中消费数据
    //order_info
    var orderInfoKafkaDStream:InputDStream[ConsumerRecord[String,String]] = null
    if (orderInfoOffsets != null && orderDetailOffsets.nonEmpty) {
       orderInfoKafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, orderInfoTopicName, orderInfoGroup, orderInfoOffsets)
    }else {
      orderInfoKafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, orderInfoTopicName, orderInfoGroup, orderInfoOffsets)
    }

    //order_detail
    var orderDetailKafkaDStream:InputDStream[ConsumerRecord[String,String]] = null
    if (orderDetailOffsets != null && orderDetailOffsets.nonEmpty) {
      orderDetailKafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, orderDetailTopicName, orderDetailGroup, orderDetailOffsets)
    }else {
      orderDetailKafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, orderDetailTopicName, orderDetailGroup, orderDetailOffsets)
    }

    //4.提取offset
    //order_info
    var orderInfoOffsetRanges: Array[OffsetRange] = null
    val orderInfoOffsetDStream = orderInfoKafkaDStream.transform(
      rdd => {
        orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )
    //order_detail
    var orderDetailOffsetRanges: Array[OffsetRange] = null
    val orderDetailOffsetDStream = orderDetailKafkaDStream.transform(
      rdd => {
        orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )
    //5.处理数据
    //转换结构
    val orderInfoDStream = orderInfoOffsetDStream.map(
      consumerRecord => {
        val value: String = consumerRecord.value()
        val orderInfo = JSON.parseObject(value, classOf[OrderInfo])
        orderInfo
      }
    )
    //orderInfoDStream.print(100)

    val orderDetailDStream = orderDetailOffsetDStream.map(
      consumerRecord => {
        val value = consumerRecord.value()
        val orderDetail = JSON.parseObject(value, classOf[OrderDetail])
        orderDetail
      }
    )
    //orderDetailDStream.print(100)

    //5.2维度管理
    //order_info
    val orderInfoDimDStream = orderInfoDStream.mapPartitions( //mapPartition方法传进来就是一个orderInfoIter 迭代器
      orderInfoIter => {
        //val orderInfos = ListBuffer[OrderInfo]()
        val orderInfos = orderInfoIter.toList
        val jedis = MyRedisUtils.getJedisFromPool()
        for (orderInfo <- orderInfos) {
          //关联用户维度
          val uid = orderInfo.user_id
          val redisUserKey = s"DIM:USER_INFO:$uid"
          val userInfoJson = jedis.get(redisUserKey)
          val userInfoJsonObj = JSON.parseObject(userInfoJson)
          //提取性别
          val gender = userInfoJsonObj.getString("gender")
          //提取生日
          val birthday = userInfoJsonObj.getString("birthday")
          //换算年龄
          val birthdayLd = LocalDate.parse(birthday)
          val nowId = LocalDate.now();
          val period = Period.between(birthdayLd, nowId)
          val age = period.getYears

          //补充到对象中
          orderInfo.user_gender = gender
          orderInfo.user_age = age

          //关联地区维度
          val provinceId = orderInfo.province_id
          val redisProvinceKey = s"DIM:BASE_PROVINCE:$provinceId"
          val provinceJson = jedis.get(redisProvinceKey)
          val provinceJsonObj = JSON.parseObject(provinceJson)
          //提取字段
          val provinceName = provinceJsonObj.getString("name")
          val provinceAreaCode = provinceJsonObj.getString("area_code")
          val province3166 = provinceJsonObj.getString("iso_3166_2")
          val provinceIsoCode = provinceJsonObj.getString("iso_code")

          //补充到对象中
          orderInfo.province_name = provinceName
          orderInfo.province_area_code = provinceAreaCode
          orderInfo.province_3166_2_code = province3166
          orderInfo.province_area_code = provinceAreaCode

          //处理日期字段
          val createTime = orderInfo.create_time
          val createDtHr: Array[String] = createTime.split(" ")
          val createDate = createDtHr(0)
          val createHr = createDtHr(1).split(":")(0)
          //补充到对象中
          orderInfo.create_date = createDate
          orderInfo.create_hour = createHr

          //orderInfos.append(orderInfo)  //将补充维度数据之后的orderInfo添加到orderInfos中
        }
        jedis.close()
        orderInfos.iterator
      }
    )

    //5.3双流join
    //内连接 join  结果集取交集
    //外连接
      //左外连 leftOuterJoin  ： 左表所有+右表的匹配
      //右外连 rightOuterJoin ： 左表的匹配 + 右表所有
      //全外连 fullOuterJoin ： 取并集
    val orderInfoKVDStream = orderInfoDimDStream.map(orderInfo => (orderInfo.id, orderInfo))  //转换成KV格式的才能进行join
    val orderDetailKVDStream = orderDetailDStream.map(orderDetail => (orderDetail.order_id, orderDetail))

    //val orderJoinDStream = orderInfoKVVDStream.join(orderDetailKVDStream)
    val orderJoinDStream: DStream[(Long, (Option[OrderInfo], Option[OrderDetail]))] =
      orderInfoKVDStream.fullOuterJoin(orderDetailKVDStream)

    val orderWideDStream = orderJoinDStream.mapPartitions(
      orderJoinIter => {
        val orderWides: ListBuffer[OrderWide] = ListBuffer[OrderWide]()
        val jedis = MyRedisUtils.getJedisFromPool()
        for ((key, (orderInfoOp, orderDetailOp)) <- orderJoinIter) {
          //orderInfo有，orderDetail有
          if (orderInfoOp.isDefined) {
            //取出orderInfo
            val orderInfo: OrderInfo = orderInfoOp.get
            if (orderDetailOp.isDefined) {
              //取出orderDetail
              val orderDetail = orderDetailOp.get
              //组装成orderWide
              val orderWide = new OrderWide(orderInfo, orderDetail)
              //放入到结果集中
              orderWides.append(orderWide)
            }
            //orderInfo有，orderDetail没有

            //orderInfo写缓存，redis作为缓存组件
            /*
            类型：String
            key：ORDERJOIN:ORDERINFO:ID  //ID是订单ID
            value：json
            写入API：set
            读取API:get
            过期：24小时
             */
            val redisOrderInfo: String = s"ORDERJOIN:ORDER_INFO:${orderInfo.id}"
            //            jedis.set(redisOrderInfo,JSON.toJSONString(orderInfo,new SerializeConfig(true) ))
            //            jedis.expire(redisOrderInfo,24 * 3600)
            //写到缓存中
            jedis.setex(redisOrderInfo, 24 * 3600, JSON.toJSONString(orderInfo, new SerializeConfig(true)))
            //orderInfo读缓存  读取redis中缓存的不同批次的orderDetail
            val redisOrderDetailKey = s"ORDERJOIN:ORDER_DETAIL:${orderInfo.id}"
            val orderDetails: util.Set[String] = jedis.smembers(redisOrderDetailKey) //写orderDetail的时候，是按照同一个orderInfo的orderDetail作为一个set集合写入到redis中
            if (orderDetails != null && orderDetails.size() > 0) {
              import scala.collection.JavaConverters._
              for (orderDetailJson <- orderDetails.asScala) {
                val orderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
                //组装成orderWide
                val orderWide = new OrderWide(orderInfo, orderDetail)
                //放入到结果集中
                orderWides.append(orderWide)
              }
            }

          } else {
            //orderInfo没有，orderDetail有
            val orderDetail = orderDetailOp.get
            //orderDetail读缓存
            val redisOrderInfoKey = s"ORDERJOIN:ORDER_INFO:${orderDetail.order_id}"
            val orderInfoJson = jedis.get(redisOrderInfoKey)
            if (orderInfoJson != null && orderInfoJson.size > 0) {
              val orderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
              //组装成orderWide
              val orderWide = new OrderWide(orderInfo, orderDetail)
              //加入到结果集中
              orderWides.append(orderWide)
            } else {
              //orderDetail写缓存
              /*
              类型：set
              key ：ORDERJOIN:ORDER_DETAIL:ORDER_ID
              value: json , json
              写入API sadd
                读取API： smembers
               */
              val redisOrderDetailKey = s"ORDERJOIN:ORDER_DETAIL:${orderDetail.order_id}"
              jedis.sadd(redisOrderDetailKey, JSON.toJSONString(orderDetail, new SerializeConfig(true)))
              jedis.expire(redisOrderDetailKey, 24 * 3600)
            }
          }
        }
        jedis.close()
        orderWides.iterator
      }
    )
    orderWideDStream.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          orderWideIter => {
            val orderWides = orderWideIter.map(   //因为要传入ES的结构为   docs: List[(String, AnyRef)]
              orderWide => (orderWide.detail_id.toString,orderWide)
            ).toList
            if(orderWides.nonEmpty) {
              val head = orderWides.head
              val date = head._2.create_time
              //需要一个索引名
              val indexName = s"gmall_order_wide_$date"
              //写入到ES中
              MyEsUtils.bulkSave(indexName,orderWides)
            }
          }
        )
        MyOffsetsUtils.saveOffset(orderInfoTopicName,orderInfoGroup,orderInfoOffsetRanges)
        MyOffsetsUtils.saveOffset(orderDetailTopicName,orderDetailGroup,orderDetailOffsetRanges)
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }
}
