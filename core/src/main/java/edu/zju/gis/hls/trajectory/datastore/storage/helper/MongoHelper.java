package edu.zju.gis.hls.trajectory.datastore.storage.helper;

import com.mongodb.client.*;
import edu.zju.gis.hls.trajectory.datastore.storage.config.MongoConfig;
import lombok.Getter;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * @author Hu
 * @date 2019/9/18
 * MongoDB 数据库操作
 * 使用方法：获取单例MongoHelper，选择数据库，操作数据库
 **/
public class MongoHelper extends StorageHelper {

  private final Logger logger = LoggerFactory.getLogger(MongoHelper.class);

  private static MongoHelper instance = new MongoHelper();

  private MongoClient client;

  @Getter
  private MongoDatabase database;

  @Getter
  private MongoConfig config;

  private MongoCursor<Document> reader;

  private MongoHelper() {
    this.config = MongoConfig.getInstance();
    this.client = MongoClients.create(config.toString());
    logger.info("init MongoDB client with url: " + config.toString());
  }

  public static MongoHelper getClient() {
    return instance;
  }

  @Override
  public void useDB(String databaseName) {
    this.database = this.client.getDatabase(databaseName);
  }

  @Override
  public long insert(String table, Map<String, ?> data) {
    this.checkConfig();
    MongoCollection<Document> collection = this.database.getCollection(table);
    collection.insertOne(this.mapToDocument(data));
    return 1;
  }

  @Override
  public long insert(String table, List<Map<String, ?>> data) {
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
      logger.error("please verify the target table by function read(String)");
      return null;
    }
    return this.reader.next().toJson();
  }

  @Override
  public boolean hasNext() {
    this.checkConfig();
    if (this.reader == null){
      logger.error("please verify the target table by function read(String)");
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

  private void checkConfig() {
    // 默认使用local数据库
    if (this.database == null){
      this.database = client.getDatabase("local");
    }
  }

  private Document mapToDocument(Map<String, ?> data){
    Document document = new Document();
    for(String key: data.keySet()) {
      document.append(key, data.get(key));
    }
    return document;
  }
}
