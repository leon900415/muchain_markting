package com.moonchain.rulemk.marketing.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.List;
import java.util.Map;

/**
 * 规则封装
 *
 * @author: Moon-Chain 2022-03-07 15:29
 **/

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class MarketingRule {

    /**
     * 规则id
     */
    private String ruleId;

    /**
     * 触发事件
     */
    private EventCondition triggerEventCondition;

    /**
     * 规则推送的次数限制
     */
    private int matchLimit;

    //画像属性条件
    private Map<String, String> userProfileConditions;

    //行为条件
    private List<EventCombinationCondition> eventCombinationConditionList;

    // 是否要往册timer
    private boolean isOnTimer;

    // 定时条件时间
    private List<TimerCondition> timerConditionList;

}
