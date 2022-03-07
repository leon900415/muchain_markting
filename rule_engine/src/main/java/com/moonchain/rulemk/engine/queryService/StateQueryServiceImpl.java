package com.moonchain.rulemk.engine.queryService;

import com.moonchain.rulemk.engine.beans.EventBean;
import com.moonchain.rulemk.engine.beans.EventParam;
import com.moonchain.rulemk.engine.utils.EventParamComparator;
import org.apache.flink.api.common.state.ListState;

import java.util.List;

/**
 * 行为序列的状态查询接口实现
 *
 * @author: Moon-Chain 2022-03-06 17:19
 */
public class StateQueryServiceImpl implements QueryService {

  private final ListState<EventBean> beanListState;

  public StateQueryServiceImpl(ListState<EventBean> beanListState) {
    this.beanListState = beanListState;
  }

  public int queryEventSequence(List<EventParam> eventSequence, long startTime, long endTime)
      throws Exception {
    int i = 0;
    int count = 0;
    for (EventBean eventBean : beanListState.get()) {
      if (eventBean.getTimeStamp() >= startTime
          && eventBean.getTimeStamp() <= endTime
          && EventParamComparator.compare(eventSequence.get(i), eventBean)) {
        count++;
        i++;
        if (i == eventSequence.size()) {
          return count;
        }
      }
    }
    return count;
  }
}
