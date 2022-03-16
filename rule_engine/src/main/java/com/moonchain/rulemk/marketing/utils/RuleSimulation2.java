package com.moonchain.rulemk.marketing.utils;

import com.moonchain.rulemk.marketing.beans.EventCombinationCondition;
import com.moonchain.rulemk.marketing.beans.EventCondition;
import com.moonchain.rulemk.marketing.beans.MarketingRule;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

/**
 * 规则模拟器
 *
 * @author: Moon-Chain 2022-03-07 16:30
 */
public class RuleSimulation2 {

    public static MarketingRule getRule() {

        MarketingRule marketingRule = new MarketingRule();
        marketingRule.setRuleId("rule_001");

    /*
     触发事件
    */
        HashMap<String, String> map1 = new HashMap<>();
        //        eventParams.put("p2", "v1");
        EventCondition triggerEvent = new EventCondition("K", map1, -1, Long.MAX_VALUE, 1, 999);
        marketingRule.setTriggerEventCondition(triggerEvent);

    /*
     用户画像
    */
        HashMap<String, String> map2 = new HashMap<>();
        map2.put("tag1", "v1");
        //    userProfile.put("tag26", "v1");
        marketingRule.setUserProfileConditions(map2);

        /*
         * 单个行为次数条件列表
         */
        String eventId = "C";
        HashMap<String, String> map3 = new HashMap<>();
        map3.put("p6", "v8");
        map3.put("p12", "v5");
        long startTime = -1L;
        long endTime = Long.MAX_VALUE;
        String sql =
                "select count(1) as cnt\n"
                        + "from zenniu_detail\n"
                        + "where eventId = 'C'\n"
                        + "  and deviceId = ?\n"
                        + "  and timeStamp between ? and ?";
        String rPattern = "(1)";
        EventCondition eventCondition = new EventCondition(eventId, map3, startTime, endTime, 1, 999);
        EventCombinationCondition eventGroupParam =
                new EventCombinationCondition(
                        startTime,
                        endTime,
                        Collections.singletonList(eventCondition),
                        1,
                        999,
                        rPattern,
                        "ck",
                        sql,
                        "001");

        long st = -1L;
        long ed = Long.MAX_VALUE;
        String eventId1 = "A";
        HashMap<String, String> m1 = new HashMap<>();
        //    m1.put("p3", "v2");
        EventCondition e1 = new EventCondition(eventId1, m1, st, ed, 1, 999);

        String eventId2 = "C";
        HashMap<String, String> m2 = new HashMap<>();
        //    m2.put("p1", "v1");
        EventCondition e2 = new EventCondition(eventId2, m2, st, ed, 1, 999);

        String eventId3 = "F";
        HashMap<String, String> m3 = new HashMap<>();
        //    m3.put("p1", "v1");
        EventCondition e3 = new EventCondition(eventId3, m3, st, ed, 1, 999);

        String querySql =
                "select eventId\n"
                        + "from zenniu_detail\n"
                        + "where deviceId = ?\n"
                        + "  and timeStamp between ? and ?\n"
                        + "  and ((eventId = 'A') or (eventId = 'C') or (eventId = 'F'))";

        String rPattern2 = "(1.*2.*3)";
        EventCombinationCondition eventGroupParam2 =
                new EventCombinationCondition(
                        st, ed, Arrays.asList(e1, e2, e3), 1, 999, rPattern2, "ck", querySql, "002");

        marketingRule.setEventCombinationConditionList(
                Arrays.asList(eventGroupParam, eventGroupParam2));
        return marketingRule;
    }
}
