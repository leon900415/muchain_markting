package com.moonchain.rulemk.datagen;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * hbase的用户画像数据
 *
 * @author: Moon-Chain 2022-02-23 15:07
 */
public class UserProfileDataGen {

  private static Admin admin;

  public static final String COLUMNS_FAMILY_1 = "f";
  public static final String TABLE_NAME = "zenniu_profile";

  public static void main(String[] args) throws IOException {
    TableName tableName = TableName.valueOf(TABLE_NAME);
//    createTable(tableName, COLUMNS_FAMILY_1);
    insertData(initHbase().getTable(tableName));
  }

  /*
   * 初始化hbase连接
   * */
  public static Connection initHbase() throws IOException {
    Configuration configuration = HBaseConfiguration.create();
    configuration.set("hbase.zookeeper.quorum", "192.168.1.93");
    configuration.set("hbase.zookeeper.property.clientPort", "2181");
    return ConnectionFactory.createConnection(configuration);
  }

  /*
   * 创建表
   * */
  public static void createTable(TableName tableName, String columnFamily) throws IOException {
    admin = initHbase().getAdmin();
    if (admin.tableExists(tableName)) {
      System.out.println("Table Already Exists！");
    } else {
      TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
      ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder =
          ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily));
      ColumnFamilyDescriptor columnFamilyDescriptor = columnFamilyDescriptorBuilder.build();
      // 列族   列族描述器
      builder.setColumnFamily(columnFamilyDescriptor);
      admin.createTable(builder.build());
      System.out.println("Table Create Successful");
    }
  }

  public static void insertData(Table table) throws IOException {
    ArrayList<Put> puts = new ArrayList<>();
    for (int i = 0; i < 10000; i++) {

      // 生成一个用户的画像标签数据
      String deviceId = StringUtils.leftPad(i + "", 6, "0");
      Put put = new Put(Bytes.toBytes(deviceId));
      for (int k = 1; k <= 100; k++) {
        String key = "tag" + k;
        String value = "v" + RandomUtils.nextInt(1, 3);
        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes(key), Bytes.toBytes(value));
      }

      // 将这一条画像数据，添加到list中
      puts.add(put);

      // 攒满100条一批
      if(puts.size()==100) {
        table.put(puts);
        puts.clear();
      }
    }
    // 提交最后一批
    if(puts.size()>0) {
      table.put(puts);
    }
  }

  // scan数据
  public static List<Student> allScan(TableName tableName) throws IOException {
    ResultScanner results =
        initHbase().getTable(tableName).getScanner(new Scan().addFamily(Bytes.toBytes("cf1")));
    List<String> list = new ArrayList<>();
    for (Result result : results) {
      Student student = new Student();
      for (Cell cell : result.rawCells()) {
        String colName =
            Bytes.toString(
                cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
        String value =
            Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
        System.out.println("colName:" + colName + "," + "value:" + value);
      }
    }
    return null;
  }
}
