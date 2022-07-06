package com.hitsz.gmall.realtime.util

import com.atguigu.gmall.realtime.bean.DauInfo
import com.hitsz.gmall.realtime.bean.{DauInfo, PageLog}

import java.lang.reflect.{Field, Method, Modifier}
import scala.util.control.Breaks

object MyBeanUtils {

  def main(args: Array[String]): Unit = {
    val pageLog: PageLog =
      PageLog("mid1001" , "uid101" , "prov101" , null ,null ,null ,null ,null ,null ,null ,null ,null ,null ,0L ,null ,123456)

    val dauInfo: DauInfo = new DauInfo()
    println("拷贝前: " + dauInfo)

    copyProperties(pageLog,dauInfo)

    println("拷贝后: " + dauInfo)

  }
  /*
    将srcObj中属性的值拷贝到destObj对应的属性上
   */
  def copyProperties(srcObj:AnyRef,destObj: AnyRef): Unit = {
    if (srcObj == null || destObj == null) {return }
    //用反射的方式得到srcObj中所有的属性
    val srcFields: Array[Field] = srcObj.getClass.getDeclaredFields
    //处理每个属性的拷贝
    for (srcField <- srcFields) {

      Breaks.breakable{
        //scala会自动为类中的属性提供get和set方法
        //get:fieldname()
        //set:fieldname_$eq(参数类型)

        //getMethodName
        var getMethodName = srcField.getName
        var setMethodName = srcField.getName+"_$eq"

        //从srcObj中获取get方法对象
        val getMethod:Method = srcObj.getClass.getDeclaredMethod(getMethodName)
        //从destObj中获取set方法对象
        val setMethod:Method =
        try {
          destObj.getClass.getDeclaredMethod(setMethodName, srcField.getType)
        }catch {
          case ex: Exception => Breaks.break()
        }

        //忽略val属性
        val destField = destObj.getClass.getDeclaredField(srcField.getName)
        if (destField.getModifiers.equals((Modifier.FINAL))) {
          Breaks.break()
        }

        //调用set方法获取到srcObj属性的值，再调用set方法将获取到的属性值赋值给到destObj中
        setMethod.invoke(destObj,getMethod.invoke(srcObj))
      }
    }
  }
}
