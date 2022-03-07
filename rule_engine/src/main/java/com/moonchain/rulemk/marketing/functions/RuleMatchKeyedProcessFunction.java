package com.moonchain.rulemk.marketing.functions;


import com.moonchain.rulemk.marketing.beans.EventBean;
import com.moonchain.rulemk.marketing.beans.MarketingRule;
import com.moonchain.rulemk.marketing.beans.RuleMatchResult;
import com.moonchain.rulemk.marketing.utils.EventUtil;
import com.moonchain.rulemk.marketing.utils.RuleSimulation2;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 规则匹配处理函数
 *
 * @author: Moon-Chain 2022-02-28 10:19
 */
@Slf4j
public class RuleMatchKeyedProcessFunction
    extends KeyedProcessFunction<String, EventBean, RuleMatchResult> {



  @Override
  public void open(Configuration parameters) throws Exception {

  }

  @Override
  public void processElement(
      EventBean eventBean,
      KeyedProcessFunction<String, EventBean, RuleMatchResult>.Context context,
      Collector<RuleMatchResult> collector)
      throws Exception {

    // 生成模拟规则
    MarketingRule rule = RuleSimulation2.getRule();

    // 1. 比较触发事件
    boolean isTriggerMatch = EventUtil.eventMatchCondition(eventBean, rule.getTriggerEvent());
    if (!isTriggerMatch) {
      return ;
    }

    //比较用户画像



    //查行为组合

  }
}
