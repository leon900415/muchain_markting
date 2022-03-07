package com.moonchain.rulemk.engine.function;

import com.moonchain.rulemk.engine.beans.EventBean;
import com.moonchain.rulemk.engine.beans.RuleCondition;
import com.moonchain.rulemk.engine.beans.RuleMatchResult;
import com.moonchain.rulemk.engine.router.SimpleQueryRouter;
import com.moonchain.rulemk.engine.utils.RuleSimulation;
import com.moonchain.rulemk.engine.utils.StateDescContainer;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ListState;
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

  SimpleQueryRouter simpleQueryRouter;
  ListState<EventBean> listState;

  @Override
  public void open(Configuration parameters) throws Exception {
    simpleQueryRouter = new SimpleQueryRouter();
    listState = getRuntimeContext().getListState(StateDescContainer.getState());
  }

  @Override
  public void processElement(
      EventBean eventBean,
      KeyedProcessFunction<String, EventBean, RuleMatchResult>.Context context,
      Collector<RuleMatchResult> collector)
      throws Exception {

    // 生成模拟规则
    RuleCondition rule = RuleSimulation.getRule();
    boolean match = simpleQueryRouter.isMatch(rule, eventBean);
    if (!match) {
      return;
    }
    collector.collect(
        new RuleMatchResult(
            eventBean.getDeviceId(),
            rule.getRuleId(),
            eventBean.getTimeStamp(),
            System.currentTimeMillis()));
  }
}
