package com.hitsz.gmall.realtime.util

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.util
import scala.collection.mutable
/**
 * Kafka工具类，用于生产数据和消费
 */
object MyKafkaUtils {
  /**
   * 消费者配置
   */
  private val consumerConfigs: mutable.Map[String, Object] = mutable.Map[String, Object](
    //Kafka集群位置
    //ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
    //ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> MyPropsUtils("kafka.bootstrap-servers"),
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> MyPropsUtils(MyConfig.KAFKA_BOOTSTRAP_SERVERS),
    //kv反序列化器
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    //groupId
    //offset的提交
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
    //ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG  时间间隔
    //offset重置  ,  默认是重置到尾
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest"
  )

  /**
   * 基于SparkStreaming消费,获取KafkaDStream,使用默认的offset
   */
    def getKafkaDStream(ssc: StreamingContext, topic: String, groupId: String)={
      consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG,groupId)
      val kafkaDStream: InputDStream[ConsumerRecord[String,String]] = KafkaUtils.createDirectStream(ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConfigs))
      kafkaDStream
    }

  /**
   * 基于SparkStreaming消费,获取KafkaDStream,使用指定的offset
   */
  def getKafkaDStream(ssc: StreamingContext, topic: String, groupId: String,offsets:Map[TopicPartition,Long])={
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG,groupId)
    val kafkaDStream: InputDStream[ConsumerRecord[String,String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConfigs,offsets))
    kafkaDStream
  }


  /**
   * 生产者对象
   */
   val producer: KafkaProducer[String, String] = createProducer()

  /**
   * 创建生产者对象
   */
  def createProducer(): KafkaProducer[String,String] = {
    val producerConfigs:util.HashMap[String,AnyRef] = new util.HashMap[String, AnyRef]
    //生产者配置类
    //kafka集群配置
    //producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092,hadoop104:9092")
    //producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,MyPropsUtils("kafka.bootstrap-servers"))
    producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,MyPropsUtils(MyConfig.KAFKA_BOOTSTRAP_SERVERS))
    //kv反序列化器
    producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    //acks
    producerConfigs.put(ProducerConfig.ACKS_CONFIG,"all")
    //batch.size  缓冲区大小  默认16kb
    //linger.ms    默认0
    //retries  默认0
    //幂等配置  默认是false
    producerConfigs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true")

    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](producerConfigs)
    producer
  }

  /**
   * 生产（默认按照黏性分区策略）
   */
  def send( topic: String, msg: String ) = {
    producer.send(new ProducerRecord[String,String](topic,msg))
  }

  /**
   * 生产（默认key进行分区）
   */
  def send( topic: String,key:String, msg: String ) = {
    producer.send(new ProducerRecord[String,String](topic,key,msg))
  }

  /**
   * 关闭生产者对象
   */
  def close(): Unit = {
    if(producer != null) producer.close()
  }

  /**
   * 刷写，将缓冲区的数据刷写到磁盘
   */
  def flush()= {
    producer.flush()
  }
}
