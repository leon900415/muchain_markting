package com.moonchain.rulemk.marketing.utils;

import com.moonchain.rulemk.engine.beans.EventBean;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;

/**
 * flink的state
 *
 * @author: Moon-Chain 2022-03-04 16:59
 **/
public class StateDescContainer {

    /*
    * 近期行为事件存储描述器
    * */
    public static ListStateDescriptor<EventBean> getState(){
        ListStateDescriptor<EventBean> eventBeanDesc = new ListStateDescriptor<>("event_beans", EventBean.class);
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(2)).build();
        eventBeanDesc.enableTimeToLive(ttlConfig);
        return eventBeanDesc;
    }
}
