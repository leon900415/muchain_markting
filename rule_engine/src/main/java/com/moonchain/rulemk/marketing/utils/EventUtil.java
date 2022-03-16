package com.moonchain.rulemk.marketing.utils;

import com.moonchain.rulemk.marketing.beans.EventBean;
import com.moonchain.rulemk.marketing.beans.EventCondition;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 事件比较工具
 *
 * @author: Moon-Chain 2022-03-07 19:26
 **/
@Slf4j
public class EventUtil {

    /**
     * 事件匹配条件
     *
     * @param eventBean
     * @param eventCondition
     * @return
     */
    public static boolean eventMatchCondition(EventBean eventBean, EventCondition eventCondition) {
        log.debug("接收到的事件对象 {}", eventBean);
        if (eventCondition.getEventId().equals(eventBean.getEventId())) {
            Set<String> keys = eventCondition.getEventProperties().keySet();
            for (String key : keys) {
                String conditionValue = eventCondition.getEventProperties().get(key);
                return conditionValue.equals(eventBean.getProperties().get(key));
            }
        }
        return false;
    }

    /**
     * 正则校验规则字符串与组合事件
     *
     * @param eventStr     组合事件字符串
     * @param matchPattern 正则表达式
     * @return 匹配个数
     */
    public static int eventStrMatchGroupCount(String eventStr, String matchPattern) {
        Pattern r = Pattern.compile(matchPattern);
        Matcher matcher = r.matcher(eventStr);
        int count = 0;
        while (matcher.find()) {
            count++;
        }
        return count;
    }
}
