package com.moonchain.rulemk.marketing.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.Map;

/**
 * 规则事件中最原子的一个封装
 *
 * @author: Moon-Chain 2022-03-07 11:40
 **/
@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class EventCondition {

    /*
    * 事件id
    */
    private String eventId;

    /*
     * 规则条件中的一个事件的属性约束
     */
    private Map<String,String> eventProperties;

    /*
    * 事件的开始时间
    * */
    private long timeRangeStart;

    /*
    * 事件的结束时间
    * */
    private long timeRangeEnd;

    /*
    * 规则事件中要求的最低次数
    * */
    private int minLimit;

    /*
    * 规则事件中要求的最高次数
    * */
    private int maxLimit;
}
