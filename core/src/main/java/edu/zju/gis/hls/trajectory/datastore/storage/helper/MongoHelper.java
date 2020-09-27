package edu.zju.gis.hls.trajectory.datastore.storage.helper;

import com.mongodb.client.*;
import edu.zju.gis.hls.trajectory.datastore.exception.GISSparkException;
import edu.zju.gis.hls.trajectory.datastore.storage.config.MongoConfig;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * @author Hu
 * @date 2019/9/18
 * MongoDB 数据库操作
 * 使用方法：获取单例 MongoHelper，选择数据库，操作数据库
 * TODO 改写到现有框架
 **/
@Slf4j
public class MongoHelper implements JDBCHelper {

  private static MongoHelper instance = null;

  @Getter
  private MongoClient client;

  @Getter
  private MongoDatabase database;

  @Getter
  private MongoConfig config;

  private MongoCursor<Document> reader;

  private MongoHelper(MongoConfig config) {
    this.config = config;
    this.client = MongoClients.create(config.toString());
    log.info("init MongoDB client with url: " + config.toString());
  }

  private MongoHelper() {
    this(new MongoConfig());
  }

  public static MongoHelper getHelper() {
    return MongoHelper.getHelper(new MongoConfig());
  }

  public static MongoHelper getHelper(MongoConfig config) {
    if (instance != null) {
      log.warn("MongoHelper has already inited, configuration abort");
    } else {
      instance = new MongoHelper(config);
    }
    return instance;
  }

  @Override
  public void useDB(String databaseName) {
    this.database = this.client.getDatabase(databaseName);
  }

  @Override
  public long insert(String table, Map<String, Object> data) {
    this.checkConfig();
    MongoCollection<Document> collection = this.database.getCollection(table);
    collection.insertOne(this.mapToDocument(data));
    return 1;
  }

  @Override
  public long insert(String table, List<Map<String, Object>> data) {
    this.checkConfig();
    MongoCollection<Document> collection = this.database.getCollection(table);
    collection.insertMany(data.stream().map(this::mapToDocument).collect(Collectors.toList()));
    return data.size();
  }

  @Override
  public long getSize(String table) {
    this.checkConfig();
    return this.database.getCollection(table).countDocuments();
  }

  @Override
  public void initReader(String table) {
    this.checkConfig();
    this.reader = this.database.getCollection(table).find().cursor();
  }

  @Override
  public String next() {
    this.checkConfig();
    if (this.reader == null){
      log.error("please verify the target table by function read(String)");
      return null;
    }
    if (this.hasNext()) {
      log.warn("the reader has come to the end of the table");
      return this.reader.next().toJson();
    } else {
      return null;
    }
  }

//  @Override
  public boolean hasNext() {
    this.checkConfig();
    if (this.reader == null){
      log.error("please verify the target table by function read(String)");
      return false;
    }
    return this.reader.hasNext();
  }

  @Override
  public void closeReader() {
    if (this.reader != null){
      this.reader.close();
      this.reader = null;
    }
  }

  @Override
  public void close() {
    if (this.client != null) {
      this.client.close();
    }
  }

  private void checkConfig() {
    // 默认使用local数据库
    if (this.database == null){
      this.database = client.getDatabase("local");
    }
  }

  private Document mapToDocument(Map<String, Object> data){
    Document document = new Document();
    for(String key: data.keySet()) {
      document.append(key, data.get(key));
    }
    return document;
  }

  @Override
  public <T> void runSQL(String sql, SQLResultHandler<T> callBack) {
    throw new GISSparkException("Under developing");
  }

  @Override
  public boolean runSQL(String sql) {
    throw new GISSparkException("Under developing");
  }

//  @Override
//  public boolean runSQL(String sql, Object... params){throw new GISSparkException("Under developing");}

}
