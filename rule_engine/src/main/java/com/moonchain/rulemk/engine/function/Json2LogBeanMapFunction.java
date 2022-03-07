package com.moonchain.rulemk.engine.function;

import com.alibaba.fastjson.JSON;
import com.moonchain.rulemk.engine.beans.EventBean;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * json转logbean的fucntion
 *
 * @author: Moon-Chain 2022-02-28 09:35
 **/
public class Json2LogBeanMapFunction implements MapFunction<String, EventBean>    {

    @Override
    public EventBean map(String s) throws Exception {
        EventBean eventBean = null;
        try {
            eventBean = JSON.parseObject(s, EventBean.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return eventBean;
    }
}
