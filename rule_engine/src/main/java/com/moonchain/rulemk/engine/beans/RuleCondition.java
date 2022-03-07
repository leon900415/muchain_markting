package com.moonchain.rulemk.engine.beans;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * 规则实体
 *
 * @author: Moon-Chain 2022-02-28 13:51
 */

@Data
public class RuleCondition {

  // 规则id
  private String ruleId;

  // 触发事件
  private EventParam tiggerEvent;

  // 画像属性条件
  private Map<String, String> userProfileConditions;

  // 行为次数条件
  private List<EventParam> actionCountConditionsList;

  // 行为序列条件
  private List<EventSequenceParam> actionSequenceConditionList;
}
