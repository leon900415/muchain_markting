package com.moonchain.rulemk.engine.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * 规则参数
 *
 * @author: Moon-Chain 2022-02-28 14:02
 **/

@Data
@NoArgsConstructor
@AllArgsConstructor
public class EventParam {

    private String eventId;

    private Map<String,String> eventProperties;

    private int countThreshold;

    private long timeRangeStart;

    private long timeRangeEnd;

    private String sql;
}
