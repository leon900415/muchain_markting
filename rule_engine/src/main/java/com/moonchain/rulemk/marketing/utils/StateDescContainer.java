package com.moonchain.rulemk.marketing.utils;

import com.moonchain.rulemk.marketing.beans.EventBean;
import com.moonchain.rulemk.marketing.beans.MarketingRule;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * flink的state
 *
 * @author: Moon-Chain 2022-03-04 16:59
 **/
public class StateDescContainer {

    public static final ListStateDescriptor<Tuple2<MarketingRule, Long>> RUN_TIMER_STATE_DESC = new ListStateDescriptor<Tuple2<MarketingRule, Long>>(
            "run_timer_state",
            Types.TUPLE(Types.POJO(MarketingRule.class), Types.LONG));


    /*
     * 近期行为事件存储描述器
     * */
    public static ListStateDescriptor<EventBean> getStateDescriptor() {
        ListStateDescriptor<EventBean> eventBeanDesc = new ListStateDescriptor<>("event_beans", EventBean.class);
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(2)).build();
        eventBeanDesc.enableTimeToLive(ttlConfig);
        return eventBeanDesc;
    }
}
