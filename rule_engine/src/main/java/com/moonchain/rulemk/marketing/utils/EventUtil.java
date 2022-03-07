package com.moonchain.rulemk.marketing.utils;

import com.moonchain.rulemk.marketing.beans.EventBean;
import com.moonchain.rulemk.marketing.beans.EventCondition;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;

/**
 * 事件比较工具
 *
 * @author: Moon-Chain 2022-03-07 19:26
 **/
@Slf4j
public class EventUtil {

    /**
     * 事件匹配条件
     * @param eventBean
     * @param eventCondition
     * @return
     */
    public static boolean eventMatchCondition(EventBean eventBean, EventCondition eventCondition) {
        log.debug("接收到的事件对象 {}", eventBean);
        if (eventCondition.getEventId().equals(eventBean.getEventId())) {
            Set<String> keys = eventCondition.getEventProperties().keySet();
            for (String key : keys) {
                String targetValue = eventBean.getProperties().get(key);
                return eventCondition.getEventProperties().get(key).equals(targetValue);
            }
        }
        return false;
    }
}
