package com.moonchain.rulemk.engine.utils;

import org.apache.commons.lang3.time.DateUtils;

import java.util.Calendar;
import java.util.Date;

/**
 * 时间分割点工具类
 *
 * @author: Moon-Chain 2022-03-05 15:56
 **/
public class SegmentQueryUtil {

    /**
     * 给定时间向上取整,倒退2小时
     * @param timestamp
     * @return
     */
  public static long getSegmentPoint(long timestamp){
      Date ceiling = DateUtils.ceiling(new Date(timestamp - 2 * 60 * 60 * 1000L), Calendar.HOUR);
      return ceiling.getTime();
  }
}
