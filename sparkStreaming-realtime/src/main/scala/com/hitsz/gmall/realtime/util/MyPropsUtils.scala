package com.hitsz.gmall.realtime.util

import java.util.ResourceBundle

/**
 * 配置文件解析类
 */
object MyPropsUtils {
  private val bundle: ResourceBundle = ResourceBundle.getBundle("config")

  def apply(propsKey:String):String ={
    bundle.getString(propsKey);
  }

  def main(args: Array[String]): Unit = {
    print(MyPropsUtils.apply("kafka.bootstrap-servers"))
  }
}
