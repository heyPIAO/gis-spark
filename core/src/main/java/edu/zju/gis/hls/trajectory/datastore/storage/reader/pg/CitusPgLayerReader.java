package edu.zju.gis.hls.trajectory.datastore.storage.reader.pg;

import edu.zju.gis.hls.trajectory.analysis.rddLayer.Layer;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import edu.zju.gis.hls.trajectory.datastore.storage.config.PgConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.helper.ListStringSQLResultHandler;
import edu.zju.gis.hls.trajectory.datastore.storage.helper.PgHelper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.json.JSONObject;
import scala.Tuple2;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Hu
 * @date 2020/9/9
 * Citus 分区的 Pg集群图层读取
 * Tips：
 * （1）在CN节点上查询分区表在各子节点的分布式情况
 * （2）直连各子节点读取需要的数据
 * 目的：减少CN节点的网络带宽压力，提高集群的可用性
 **/
public class CitusPgLayerReader<T extends Layer> extends PgLayerReader<T> {

  public CitusPgLayerReader(SparkSession ss, PgLayerReaderConfig config) {
    super(ss, config);
  }

  public CitusPgLayerReader(SparkSession ss, LayerType layerType) {
    super(ss, layerType);
  }

  @Override
  protected Dataset<Row> readFromSource() {
    // 获取数据表分布信息
    PgHelper helper = this.getPgHelper();
    // select * from pg_dist_shard_placement where shardid in (select shardid from pg_dist_shard where logicalrelid='example.tb_points' ::regclass)
    ListStringSQLResultHandler handler = new ListStringSQLResultHandler();
    String sql = String.format("select * from pg_dist_shard_placement where shardid " +
      "in (select shardid from pg_dist_shard where logicalrelid='%s.%s' ::regclass)", this.readerConfig.getSchema(), this.readerConfig.getDbtable());
    helper.runSQL(sql, handler);
    // key 为 tablename，value 为目标数据库的 url
    List<Tuple2<String, String>> sources = handler.getResult().stream().map(JSONObject::new).map(x->{
      String tablename = String.format("%s.%s_%s", this.readerConfig.getSchema(), this.readerConfig.getDbtable(), x.getString("shardid"));
      String node = x.getString("nodename");
      Integer port = x.getInt("nodeport");
      String url = String.format("jdbc:postgresql://%s:%d/%s", node, port, this.readerConfig.getDatabase());
      return new Tuple2<>(tablename, url);
    }).collect(Collectors.toList());
    helper.close();
    // 从各个分表中读取数据并union到一个DataSet中
    Dataset<Row> df = this.ss.emptyDataFrame();
    for (Tuple2<String, String> table: sources) {
      df.union(this.readFromDistTables(table._1, table._2));
    }
    return df;
  }

  private Dataset<Row> readFromDistTables(String tableName, String url) {
    String dbtableSql = String.format("(%s) as %s_t", this.getReaderConfig().getFilterSql(tableName), tableName);
    return this.ss.read().format("jdbc")
      .option("url", url)
      .option("dbtable", dbtableSql)
      .option("user", this.readerConfig.getUsername())
      .option("password", this.readerConfig.getPassword())
      .option("continueBatchOnError",true)
      .option("pushDownPredicate", true) // 默认请求下推
      .load();
  }

  private PgHelper getPgHelper() {
    PgConfig config = new PgConfig();
    config.setUrl(config.getUrl());
    config.setPort(config.getPort());
    config.setDatabase(config.getDatabase());
    config.setUsername(config.getUsername());
    config.setPassword(config.getPassword());
    config.setSchema(config.getSchema());
    return new PgHelper(config);
  }


}
