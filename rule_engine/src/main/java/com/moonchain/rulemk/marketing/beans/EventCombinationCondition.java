package com.moonchain.rulemk.marketing.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.List;

/**
 * 事件组合体
 *
 * @author: Moon-Chain 2022-03-07 13:54
 **/
@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class EventCombinationCondition {

    /**
     * 组合条件中的开始时间
     */
    private long timeRangeStart;

    /**
     * 组合条件中的结束时间
     */
    private long timeRangeEnd;

    /**
     * 组合条件中包含的事件
     */
    private List<EventCondition> eventConditionList;

    /**
     * 组合条件中时间的最大次数
     */
    private int maxLimit;

    /**
     * 组合条件中时间的最小次数
     */
    private int minLimit;

    /**
     * 正则表达式
     */
    private String matchPattern;

    /**
     * sql类型
     */
    private String sqlType;

    /**
     * 查询sql
     */
    private String querySql;

    /**
     * 条件缓存id
     */
    private String cacheId;
}
