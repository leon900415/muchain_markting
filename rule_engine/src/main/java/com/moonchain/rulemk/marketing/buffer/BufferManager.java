package com.moonchain.rulemk.marketing.buffer;

import com.moonchain.rulemk.marketing.beans.BufferData;

import java.util.Map;

/**
 * 缓存管理类
 *
 * @author: Moon-Chain 2022-03-07 19:41
 */
public interface BufferManager {

  BufferData getDataFromBuffer(String bufferKey);

  boolean putDataToBuffer(BufferData bufferData);

  boolean putDataToBuffer(String bufferKey, Map<String, String> valueMap);
}
