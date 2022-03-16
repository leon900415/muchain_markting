package com.moonchain.rulemk.marketing.controller;

import com.moonchain.rulemk.marketing.beans.*;
import com.moonchain.rulemk.marketing.service.TriggerModeMatchServiceImpl;
import com.moonchain.rulemk.marketing.utils.EventUtil;
import org.apache.flink.api.common.state.ListState;

import java.util.List;
import java.util.Map;

/**
 * tirgger的controller
 *
 * @author: Moon-Chain 2022-03-08 22:24
 **/
public class TriggerModelMatchController {

    private final TriggerModeMatchServiceImpl triggerModeMatchService;

    /**
     * 构造方法
     *
     * @param eventBeanListState flink的状态句柄
     * @throws Exception 异常
     */
    public TriggerModelMatchController(ListState<EventBean> eventBeanListState) throws Exception {
        this.triggerModeMatchService = new TriggerModeMatchServiceImpl(eventBeanListState);
    }

    /**
     * 根据规则匹配触发器
     *
     * @param eventBean 事件
     * @param rule      规则
     * @return 是否匹配
     * @throws Exception 异常
     */
    public boolean ruleIsMatch(EventBean eventBean, MarketingRule rule) throws Exception {

        //触发事件
        EventCondition triggerEventCondition = rule.getTriggerEventCondition();
        boolean triggerEventIsMatch = EventUtil.eventMatchCondition(eventBean, triggerEventCondition);
        if (!triggerEventIsMatch) {
            return false;
        }

        //用户画像
        Map<String, String> userProfileConditions = rule.getUserProfileConditions();
        boolean profileIsMatch = triggerModeMatchService.matchProfileCondition(userProfileConditions,
                eventBean.getDeviceId());
        if (!profileIsMatch) {
            return false;
        }

        //组合规则匹配
        List<EventCombinationCondition> eventCombinationConditionList = rule.getEventCombinationConditionList();
        for (EventCombinationCondition eventCombinationCondition : eventCombinationConditionList) {
            boolean combinationIsMatch =
                    triggerModeMatchService.matchEventCombinationCondition(eventCombinationCondition, eventBean);
            //多个组合规则 且的关系
            if (!combinationIsMatch) {
                return false;
            }
        }
        return true;
    }

    public boolean isMatchCondition(String deviceId, TimerCondition timerCondition, long queryStartTime,
                                    long queryEndTime) throws Exception {
        for (EventCombinationCondition eventCombinationCondition : timerCondition.getEventCombinationConditionList()) {

            eventCombinationCondition.setTimeRangeStart(queryStartTime);
            eventCombinationCondition.setTimeRangeEnd(queryEndTime);

           
            EventBean eventBean = new EventBean();
            eventBean.setDeviceId(deviceId);
            eventBean.setTimeStamp(queryEndTime);

            boolean combinationIsMatch =
                    triggerModeMatchService.matchEventCombinationCondition(eventCombinationCondition, eventBean);
            if (!combinationIsMatch) {
                return false;
            }
        }
        return true;
    }

}
