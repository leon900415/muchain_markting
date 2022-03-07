package com.moonchain.rulemk.engine.utils;

import com.moonchain.rulemk.engine.beans.EventBean;
import com.moonchain.rulemk.engine.beans.EventParam;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;

/**
 * 规则时间比较器
 *
 * @author: Moon-Chain 2022-02-28 16:14
 */
@Slf4j
public class EventParamComparator {

  public static boolean compare(EventParam param1, EventParam target) {
    if (param1.getEventId().equals(target.getEventId())) {
      Set<String> keys = param1.getEventProperties().keySet();
      for (String key : keys) {
        String targetValue = target.getEventProperties().get(key);
        if (!param1.getEventProperties().get(key).equals(targetValue)) {
          return false;
        }
        return true;
      }
    }
    return false;
  }

  public static boolean compare(EventParam param1, EventBean target) {
    log.debug("接收到的事件对象 {}", target);
    if (param1.getEventId().equals(target.getEventId())) {

      Set<String> keys = param1.getEventProperties().keySet();
      for (String key : keys) {

        String targetValue = target.getProperties().get(key);
        if (!param1.getEventProperties().get(key).equals(targetValue)) {
          return false;
        }
        return true;
      }
    }
    return false;
  }
}
