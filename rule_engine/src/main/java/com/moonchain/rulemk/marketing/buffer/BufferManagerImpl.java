package com.moonchain.rulemk.marketing.buffer;

import com.moonchain.rulemk.marketing.beans.BufferData;
import com.moonchain.rulemk.marketing.utils.ConfigNames;
import com.moonchain.rulemk.marketing.utils.ConnectionUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import redis.clients.jedis.Jedis;

import java.util.Map;

/**
 * @author: Moon-Chain 2022-03-13 20:06
 */
public class BufferManagerImpl implements BufferManager {

  Jedis jedis;
  long bufferPeriod;

  public BufferManagerImpl() {
    jedis = ConnectionUtils.getjedisConnection();
    Config config = ConfigFactory.load();
    bufferPeriod = config.getLong(ConfigNames.REDIS_BUFFER_TTL);
  }

  @Override
  public BufferData getDataFromBuffer(String bufferKey) {
    String[] fields = bufferKey.split(":");
    Map<String, String> valueMap = jedis.hgetAll(bufferKey);
    return new BufferData(fields[0], fields[1], valueMap);
  }

  @Override
  public boolean putDataToBuffer(BufferData bufferData) {
    String bufferKey = bufferData.getDeviceId() + ":" + bufferData.getCacheId();
    String result = jedis.hmset(bufferKey, bufferData.getValueMap());
    return "OK".equals(result);
  }

  @Override
  public boolean putDataToBuffer(String bufferKey, Map<String, String> valueMap) {
    String result = jedis.hmset(bufferKey, valueMap);
    return "OK".equals(result);
  }

  public void delBufferEntry(String bufferKey, String[] hsetKey) {
    jedis.hdel(bufferKey, hsetKey);
  }
}
