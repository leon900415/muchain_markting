package com.moonchain.rulemk.marketing.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * 缓存实体类
 *
 * @author: Moon-Chain 2022-03-14 16:09
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class BufferData {

  private String deviceId;

  private String cacheId;

  private Map<String, String> valueMap;
}
