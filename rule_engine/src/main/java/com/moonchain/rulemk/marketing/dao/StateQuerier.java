package com.moonchain.rulemk.marketing.dao;

import com.moonchain.rulemk.marketing.beans.EventBean;
import com.moonchain.rulemk.marketing.beans.EventCombinationCondition;
import com.moonchain.rulemk.marketing.beans.EventCondition;
import com.moonchain.rulemk.marketing.utils.EventUtil;
import org.apache.flink.api.common.state.ListState;

import java.util.List;

/**
 * flink状态的查询服务类
 *
 * @author: Moon-Chain 2022-03-07 19:40
 **/
public class StateQuerier {

    private final ListState<EventBean> eventBeanStateList;

    public StateQuerier(ListState<EventBean> eventBeanStateList) {
        this.eventBeanStateList = eventBeanStateList;
    }

    /**
     * 在clickhouse中，根据组合条伴及查询的时问范園，得到返回结果的1213形式字符串序列
     *
     * @param eventCombinationCondition 行为组合条件
     * @param queryRangeStart           查询时间范園开始
     * @return 用户做过的组合条件中的字符串序列
     * @panam queryRangeEnd 查询时间范围结來
     */
    public String getEventCombineConditionStr(String deviceId, EventCombinationCondition eventCombinationCondition, long queryRangeStart, long queryRangeEnd) throws Exception {
        List<EventCondition> eventConditionList = eventCombinationCondition.getEventConditionList();
        StringBuilder sb = new StringBuilder();
        for (EventBean eventBean : eventBeanStateList.get()) {
            if ((eventBean.getTimeStamp() >= queryRangeStart && eventBean.getTimeStamp() <= queryRangeEnd)) {
                for (int i = 1; i < eventConditionList.size(); i++) {
                    if (EventUtil.eventMatchCondition(eventBean, eventConditionList.get(i - 1))) {
                        sb.append(i);
                        break;
                    }
                }
            }
        }
        return sb.toString();
    }


    /**
     * @param deviceId                  事件id
     * @param eventCombinationCondition 行为组合条件
     * @param queryRangeStart           查询时间范園开始
     * @param queryRangeEnd             查询时间范围结来
     * @return 出现的次数
     */
    public int queryEventCombinationConditionCount(String deviceId, EventCombinationCondition eventCombinationCondition, long queryRangeStart, long queryRangeEnd) throws Exception {
        String eventCombineConditionStr = getEventCombineConditionStr(deviceId, eventCombinationCondition, queryRangeStart, queryRangeEnd);
        String matchPattern = eventCombinationCondition.getMatchPattern();
        return EventUtil.eventStrMatchGroupCount(eventCombineConditionStr, matchPattern);
    }

}
