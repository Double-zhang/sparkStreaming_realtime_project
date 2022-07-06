package com.hitsz.gmall.realtime.app

import com.alibaba.fastjson.{JSON, JSONObject}

import com.hitsz.gmall.realtime.bean.{DauInfo, PageLog}
import com.hitsz.gmall.realtime.util.{MyBeanUtils, MyEsUtils, MyKafkaUtils, MyOffsetsUtils, MyRedisUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.{Jedis, Pipeline}

import java.text.SimpleDateFormat
import java.time.{LocalDate, Period}
import java.util.Date
import scala.collection.mutable.ListBuffer

/*
日活宽表
  1.准备实时环境
  2.从redis中读取偏移量
  3.从kafka中消费数据
  4.提取偏移量结束点
  5.处理数据
    5.1转换数据结构
    5.2去重
    5.3维度关联
  6.写入到es
  7.提交offset
 */
object DwdDauApp {
  def main(args: Array[String]): Unit = {
    //1.准备实时环境
    val sparkConf = new SparkConf().setAppName("dwd_dau_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    //2.从redis中读取offset
    val topicName: String = "DWD_PAGE_LOG_TOPIC"
    val groupId: String = "DWD_DAU_GROUP"

    val offsets = MyOffsetsUtils.readOffset(topicName, groupId)

    //3.从kafka中消费数据
    var kafkaDStream: InputDStream[ConsumerRecord[String,String]] = null
    if (offsets != null && offsets.nonEmpty) {
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId, offsets)
    }else {
      kafkaDStream = MyKafkaUtils.getKafkaDStream(ssc,topicName,groupId)
    }

    //4.提取offset结束点
    var offsetRanges:Array[OffsetRange] = null
    val offsetRangesDStream = kafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    //5.处理数据
    //5.1转换结构
    val pageLogDStream = offsetRangesDStream.map(
      consumerRecord => {
        val value = consumerRecord.value()
        val pageLog: PageLog = JSON.parseObject(value, classOf[PageLog])
        pageLog
      }
    )
    pageLogDStream.cache()
    pageLogDStream.foreachRDD(
      rdd =>
        println("自我审查前" + rdd.count())
    )

    //5.2 去重操作
    //自我审查：蒋页面访问数据中last_page_id不为空的数据过滤掉
    val filterDStream:DStream[PageLog] = pageLogDStream.filter(
      pageLog => pageLog.last_page_id == null
    )
    filterDStream.cache()
    filterDStream.foreachRDD(
      rdd =>
        println("自我审查后" + rdd.count())
    )

    //第三方审查：通过redis将当日活跃的mid维护起来，自我审查后的每条数据需要到redis中进行比对去重
    //redis中如何维护日活状态
    //类型：set    String：一对一  hash：可以一对一， 也可以一对多  set，list，zset(维护一个值，使zset中元素有序): 一对多
    //key：DAU:DATE
    //value: mid的集合
    //写入API： sadd
    //读取API:
    //过期:24小时
    //filterDStream.filter() //每条数据执行一次，redis连接太频繁
    val redisFilrerDStream = filterDStream.mapPartitions( //分区为单位进行处理
      pageLogIter => {
        val pageLogs = ListBuffer[PageLog]()
        //println("第三方审查前: " + pageLogs.size)
        val jedis = MyRedisUtils.getJedisFromPool()
        val sdf = new SimpleDateFormat("yy-MM-dd")
        for (pageLog <- pageLogIter) {
          //提取每条数据中的mid
          val mid: String = pageLog.mid
          //获取日期
          val ts = pageLog.ts
          val date = new Date(ts)
          val dateStr = sdf.format(date)
          //redis判断是否包含操作
          val redisDauKey: String = s"DAU:$dateStr"
          /*
          下面代码在分布式环境中，存在并发问题， 可能多个并行度同时进入到if中,导致最终保留多条同一个mid的数据.
          // list
          val mids: util.List[String] = jedis.lrange(redisDauKey, 0 ,-1)
          if(!mids.contains(mid)){
            jedis.lpush(redisDauKey , mid )
            pageLogs.append(pageLog)
          }
          // set
          val setMids: util.Set[String] = jedis.smembers(redisDauKey)
          if(!setMids.contains(mid)){
            jedis.sadd(redisDauKey,mid)
            pageLogs.append(pageLog)
          }
           */
          val isNew = jedis.sadd(redisDauKey, mid) //判断包含和写入实现了原子操作
          if (isNew == 1L) {
            pageLogs.append(pageLog)
          }
        }
        jedis.close()
        //println("第三方审查后: " + pageLogs.size)
        pageLogs.iterator
      }
    )

    //5.3维度关联
    val dauInfoDStream: DStream[DauInfo] = redisFilrerDStream.mapPartitions( //mapPartitions给了一个Iterator，要求也返回一个Iterater
      pageLogIter => {
        val dauInfos: ListBuffer[DauInfo] = ListBuffer[DauInfo]()
        val jedis: Jedis = MyRedisUtils.getJedisFromPool()
        val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        for (pageLog <- pageLogIter) {
          val dauInfo = new DauInfo()
          //通过拷贝对象来完成pageLog中以后的字段拷贝到DauInfo中
          MyBeanUtils.copyProperties(pageLog, dauInfo)

          //2.补充维度
          //2.1用户信息维度
          val uid: String = pageLog.user_id
          val redisUidKey: String = s"DIM:USER_INFO:$uid"
          val userInfoJson: String = jedis.get(redisUidKey)
          val userInfoJsonObj = JSON.parseObject(userInfoJson)
          //提取性别
          val gender = userInfoJsonObj.getString("gender")
          //提取生日
          val birthday = userInfoJsonObj.getString("birthday")
          //换算年龄
          val birthdayLd = LocalDate.parse(birthday)
          val nowLd = LocalDate.now()
          val period = Period.between(birthdayLd, nowLd)
          val age = period.getYears
          //补充到对象中
          dauInfo.user_gender = gender
          dauInfo.user_age = age.toString

          //2.2地区信息维度  地区字段，
          // redis中:
          // 现在: DIM:BASE_PROVINCE:1
          // 之前: DIM:BASE_PROVINCE:110000
          val provinceID: String = dauInfo.province_id
          val redisProvinceKey: String = s"DIM:BASE_PROVINCE:$provinceID"
          val provinceJson: String = jedis.get(redisProvinceKey)
          val provinceJsonObj: JSONObject = JSON.parseObject(provinceJson)

          val provinceName: String = provinceJsonObj.getString("name")
          val provinceIsoCode: String = provinceJsonObj.getString("iso_code")
          val province3166: String = provinceJsonObj.getString("iso_3166_2")
          val provinceAreaCode: String = provinceJsonObj.getString("area_code")

          //补充到对象中
          dauInfo.province_name = provinceName
          dauInfo.province_iso_code = provinceIsoCode
          dauInfo.province_3166_2 = province3166
          dauInfo.province_area_code = provinceAreaCode

          //2.3日期字段处理
          val date: Date = new Date(pageLog.ts)
          val dtHr: String = sdf.format(date)
          val dtHrArr: Array[String] = dtHr.split(" ")
          val dt: String = dtHrArr(0)
          val hr: String = dtHrArr(1).split(":")(0)
          //补充到对象中
          dauInfo.dt = dt
          dauInfo.hr = hr

          dauInfos.append(dauInfo)
        }
        jedis.close()
        dauInfos.iterator
      }
    )
    //dauInfoDStream.print(100)    //写入到OLAP中
    //写入到OLAP中
    //TODO 按照天分割索引，通过索引模板控制mapping，settings，aliases等
    dauInfoDStream.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          dauInfoIter => {
            val docs = dauInfoIter.map(   //因为要传入ES的结构为   docs: List[(String, AnyRef)]
               dauInfo => (dauInfo.mid,dauInfo)
            ).toList
            if(docs.nonEmpty) {
              //需要一个索引名，
              val head = docs.head
              val ts = head._2.ts
              val sdf = new SimpleDateFormat("yyyy-MM-dd")
              val dateStr = sdf.format(new Date(ts))
              val indexName = s"gmall_dau_info_$dateStr"
              //写入到ES中
              MyEsUtils.bulkSave(indexName,docs)
            }
          }
        )
        //提交Offsets
        MyOffsetsUtils.saveOffset(topicName,groupId,offsetRanges)
      }
    )
    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * 状态还原
   *
   * 在每次启动实时任务时，进行一次状态还原。以ES为准，将所有的mid提出，覆盖到Redis中
   */
  def revertState():Unit = {
    //从ES中查询所有的mid
    val date: LocalDate=  LocalDate.now();
    val indexName = s"gmall_dau_info_$date"
    val fieldName = "mid"
    val mids: List[String] = MyEsUtils.searchField(indexName,fieldName)

    //删除redis中记录的状态
    val jedis = MyRedisUtils.getJedisFromPool()
    val redisDauKey = s"DAU:$date"
    jedis.del(redisDauKey)
    //将从ES中查询到的mid覆盖到redis中
    if(mids != null && mids.nonEmpty) {

      //为了实现批处理，这里使用pipeline
      val pipeline: Pipeline = jedis.pipelined()
      for(mid <- mids) {
        pipeline.sadd(mid)  //不会直接到redis中执行
      }
      pipeline.sync()  //到redis中执行
    }
    jedis.close()
  }
}
