package com.moonchain.rulemk.engine.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * 事件序列参数
 *
 * @author: Moon-Chain 2022-03-02 17:54
 **/
@Data
@NoArgsConstructor
@AllArgsConstructor
public class EventSequenceParam {

    private String ruleId;

    private long timeRangeStart;

    private long timeRangeEnd;

    private List<EventParam> eventSequence;

    private String sql;


}
