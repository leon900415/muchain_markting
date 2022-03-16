package com.moonchain.rulemk.marketing.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.List;

/**
 * @author: Moon-Chain 2022-03-10 18:52
 **/

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class TimerCondition {

    private long timeLate;

    private List<EventCombinationCondition> eventCombinationConditionList;

}
