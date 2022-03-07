package com.moonchain.rulemk.marketing.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * 规则处理后的实体pojo
 *
 * @author: Moon-Chain 2022-02-28 10:27
 **/

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class RuleMatchResult {

    String keyByValue;
    String ruleId;
    long trigEventTimestamp;
    long matchTimestamp;

}
