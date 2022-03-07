package com.moonchain.rulemk.engine.utils;

import com.moonchain.rulemk.engine.beans.EventParam;
import com.moonchain.rulemk.engine.beans.EventSequenceParam;
import com.moonchain.rulemk.engine.beans.RuleCondition;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

/**
 * 规则模拟器
 *
 * @author: Moon-Chain 2022-02-28 14:32 触发事伴，K事件，事件属性（p2=v1 画像属性条件：tag87=v2, tag26=v1
 *     行为次数条件：2021-06-18 当前，事件 C p6=v8, p12=V5] 做过>=2次
 */
public class RuleSimulation {

  public static RuleCondition getRule() {

    RuleCondition ruleCondition = new RuleCondition();
    ruleCondition.setRuleId("rule_001");
    HashMap<String, String> eventParams = new HashMap<>();
    //        eventParams.put("p2", "v1");
    EventParam triggerEvent = new EventParam("k", eventParams, 0, -1, -1, null);
    ruleCondition.setTiggerEvent(triggerEvent);

    /*
     * 用户画像
     * */
    HashMap<String, String> userProfile = new HashMap<>();
    userProfile.put("tag87", "v2");
    userProfile.put("tag26", "v1");
    ruleCondition.setUserProfileConditions(userProfile);

    /*
     * 行为次数
     * */
    HashMap<String, String> actionMaps = new HashMap<>();
    //    actionMaps.put("p6", "v8");
    //    actionMaps.put("p12", "v5");
    String sql =
        "select count(1) as cnt\n"
            + "from zenniu_detail\n"
            + "where eventId = 'C'\n"
            + "  and deviceId = ?\n"
            + "  and timeStamp between ? and ?";
    EventParam actionCountConditions =
        new EventParam("c", actionMaps, 2, 1623945600000L, Long.MAX_VALUE, sql);
    ruleCondition.setActionCountConditionsList(Collections.singletonList(actionCountConditions));

    long st = 1623945600000L;
    long ed = Long.MAX_VALUE;
    String eventId1 = "A";
    HashMap<String, String> m1 = new HashMap<>();
    m1.put("p3", "v2");
    EventParam e1 = new EventParam(eventId1, m1, -1, st, ed, null);

    String eventId2 = "C";
    HashMap<String, String> m2 = new HashMap<>();
    m2.put("p1", "v1");
    EventParam e2 = new EventParam(eventId2, m2, -1, st, ed, null);

    String eventId3 = "F";
    HashMap<String, String> m3 = new HashMap<>();
    m3.put("p1", "v1");
    EventParam e3 = new EventParam(eventId3, m3, -1, st, ed, null);
    String seqSql =
        "SELECT\n"
            + "    deviceId,\n"
            + "    sequenceMatch('.*(?1).*(?2).*(?3)')(\n"
            + "    toDateTime(`timeStamp`),\n"
            + "    eventId = 'A',\n"
            + "    eventId = 'C',\n"
            + "    eventId = 'F'\n"
            + "  ) as is_match3,\n"
            + "    sequenceMatch('.*(?1).*(?2)')(\n"
            + "    toDateTime(`timeStamp`),\n"
            + "    eventId = 'A',\n"
            + "    eventId = 'C',\n"
            + "    eventId = 'F'\n"
            + "  ) as is_match2,\n"
            + "    sequenceMatch('.*(?1).*')(\n"
            + "    eventId = 'A',\n"
            + "    eventId = 'C',\n"
            + "    eventId = 'F'\n"
            + "  ) as is_match1\n"
            + "from zenniu_detail\n"
            + "where deviceId=? and  `timeStamp` between ? and ?\n"
            + "  and (\n"
            + "        (eventId='A' and properties['p3']='v2')\n"
            + "        or\n"
            + "        (eventId = 'C' and properties['p1']='v1')\n"
            + "        or\n"
            + "        (eventId = 'F' and properties['p1']='v1')\n"
            + "    )\n"
            + "group by deviceId\n"
            + ";\n";

    EventSequenceParam eventSequence =
        new EventSequenceParam("rule_001", st, ed, Arrays.asList(e1, e2, e3), seqSql);

    ruleCondition.setActionSequenceConditionList(Collections.singletonList(eventSequence));

    return ruleCondition;
  }
}
