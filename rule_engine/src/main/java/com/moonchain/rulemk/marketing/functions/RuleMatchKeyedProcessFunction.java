package com.moonchain.rulemk.marketing.functions;


import com.moonchain.rulemk.marketing.beans.EventBean;
import com.moonchain.rulemk.marketing.beans.MarketingRule;
import com.moonchain.rulemk.marketing.beans.RuleMatchResult;
import com.moonchain.rulemk.marketing.beans.TimerCondition;
import com.moonchain.rulemk.marketing.controller.TriggerModelMatchController;
import com.moonchain.rulemk.marketing.utils.EventUtil;
import com.moonchain.rulemk.marketing.utils.RuleSimulation2;
import com.moonchain.rulemk.marketing.utils.StateDescContainer;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 规则匹配处理函数
 *
 * @author: Moon-Chain 2022-02-28 10:19
 */
@Slf4j
public class RuleMatchKeyedProcessFunction extends KeyedProcessFunction<String, EventBean, RuleMatchResult> {

    ListState<EventBean> eventBeanListState;
    TriggerModelMatchController triggerModelMatchController;
    List<MarketingRule> marketingRules;
    ListState<Tuple2<MarketingRule, Long>> runTimerState;

    @Override
    public void open(Configuration parameters) throws Exception {
        eventBeanListState = getRuntimeContext().getListState(StateDescContainer.getStateDescriptor());
        triggerModelMatchController = new TriggerModelMatchController(eventBeanListState);
        // 生成模拟规则
        marketingRules = Arrays.asList(RuleSimulation2.getRule());
        //mapstate
        runTimerState = getRuntimeContext().getListState(StateDescContainer.RUN_TIMER_STATE_DESC);
    }

    @Override
    public void processElement(EventBean eventBean,
                               KeyedProcessFunction<String, EventBean, RuleMatchResult>.Context context,
                               Collector<RuleMatchResult> collector) throws Exception {

        // 将数据流放到state缓存里面
        eventBeanListState.add(eventBean);
        log.debug("接收到数据流事件,事件id : {},用户id : {}", eventBean.getEventId(), eventBean.getDeviceId());
        for (MarketingRule marketingRule : marketingRules) {
            log.debug("遍历到一个规则,规则id : {},开始计算", marketingRule.getRuleId());
            boolean triggerIsMatch = EventUtil.eventMatchCondition(eventBean, marketingRule.getTriggerEventCondition());
            boolean b = triggerModelMatchController.ruleIsMatch(eventBean, marketingRule);
            if (!triggerIsMatch) {
                continue;
            }
            if (b) {
                if (marketingRule.isOnTimer()) {
                    //从规则里面去除定时规则集合
                    List<TimerCondition> timerConditionList = marketingRule.getTimerConditionList();
                    //目前只限定一个
                    TimerCondition timerCondition = timerConditionList.get(0);
                    //触发时间
                    long triggerTime = eventBean.getTimeStamp() + timerCondition.getTimeLate();
                    //定时器
                    context.timerService().registerEventTimeTimer(triggerTime);
                    //添加到flink状态里面
                    runTimerState.add(Tuple2.of(marketingRule, triggerTime));
                } else {
                    collector.collect(new RuleMatchResult(eventBean.getDeviceId(), marketingRule.getRuleId(),
                            eventBean.getTimeStamp(), System.currentTimeMillis()));
                }
            }
        }
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, EventBean, RuleMatchResult>.OnTimerContext ctx,
                        Collector<RuleMatchResult> out) throws Exception {
        Iterator<Tuple2<MarketingRule, Long>> iterator = runTimerState.get().iterator();
        while (iterator.hasNext()) {
            Tuple2<MarketingRule, Long> marketingRuleLongTuple2 = iterator.next();
            if (marketingRuleLongTuple2.f1 == timestamp) {
                MarketingRule marketingRule = marketingRuleLongTuple2.f0;
                TimerCondition timerCondition = marketingRule.getTimerConditionList().get(0);
                boolean matchCondition = triggerModelMatchController.isMatchCondition(ctx.getCurrentKey(),
                        timerCondition, timestamp - timerCondition.getTimeLate(), timestamp);
                //匹配之后 删掉state里面的数据
                iterator.remove();
                if (matchCondition) {
                    out.collect(new RuleMatchResult(ctx.getCurrentKey(), marketingRule.getRuleId(), timestamp,
                            System.currentTimeMillis()));
                }
                
                if ((marketingRuleLongTuple2.f1 < timestamp)) {
                    iterator.remove();
                }
            }
        }
    }
}
