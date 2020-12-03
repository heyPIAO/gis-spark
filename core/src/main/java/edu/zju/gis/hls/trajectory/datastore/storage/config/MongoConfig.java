package edu.zju.gis.hls.trajectory.datastore.storage.config;


import lombok.Getter;

/**
 * @author Hu
 * @date 2019/9/18
 * mongoDB 数据库连接配置
 **/
@Getter
public class MongoConfig extends DataSourceConfig {

  private String[] ip;
  private int[] port;

  // TODO 配置放到配置文件里去
  public MongoConfig(){
    this(new String[]{"localhost"}, new int[]{27017});
  }

  public MongoConfig(String[] ips){
    this(ips, 27017);
  }

  public MongoConfig(String[] ips, int port) {
    int size = ips.length;
    int[] m = new int[ips.length];
    for (int i=0; i<size; i++) {
      m[i] = port;
    }
    this.port = m;
  }

  public MongoConfig(String[] ips, int[] ports) {
    this.ip = ips;
    this.port = ports;
  }

//  private static class MongoConfigInstance {
//    private static final MongoConfig instance = new MongoConfig();
//  }

//  public static MongoConfig getInstance() {
//    return MongoConfigInstance.instance;
//  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("mongodb://");
    for (int i=0; i<ip.length; i++){
      sb.append(String.format("%s:%d,", ip[i], port[i]));
    }
    sb.deleteCharAt(sb.length()-1);
    return sb.toString();
  }

}
