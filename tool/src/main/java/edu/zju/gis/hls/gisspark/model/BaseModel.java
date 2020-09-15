package edu.zju.gis.hls.gisspark.model;

import edu.zju.gis.hls.gisspark.model.args.BaseArgs;
import edu.zju.gis.hls.gisspark.model.util.SparkSessionType;
import edu.zju.gis.hls.gisspark.model.util.SparkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.lang.reflect.ParameterizedType;

/**
 * @author Hu
 * @date 2020/1/2
 **/
@Slf4j
public abstract class BaseModel<T extends BaseArgs> implements Serializable {

  protected T arg;

  transient protected SparkSession ss;

  transient protected JavaSparkContext jsc;

  public BaseModel(String args[]) {
    this.init(SparkSessionType.LOCAL, this.getClass().getSimpleName(), new SparkConf(), args);
  }

  public BaseModel(SparkSessionType type, String args[]) {
    this.init(type, this.getClass().getName(), new SparkConf(), args);
  }

  public BaseModel(String appName, String args[]) {
    this.init(SparkSessionType.LOCAL, appName, new SparkConf(), args);
  }

  public BaseModel(SparkSessionType type, String appName, String args[]) {
    this.init(type, appName, new SparkConf(), args);
  }

  public BaseModel(SparkSessionType type, String appName, SparkConf sc, String args[]) {
    this.init(type, appName, sc, args);
  }

  public void exec() throws Exception {
    this.prepare();
    log.info("JOB START");
    this.run();
    this.finish();
  }

  /**
   * 模型核心业务逻辑
   */
  protected abstract void run() throws Exception;

  protected void init(SparkSessionType type, String appName, SparkConf sc, String args[]) {
    initArg(args);
    initSparkSession(type, appName, sc);
  }

  // TODO 对于Spark的参数注入，如需要Kryo序列化的配置，es 的配置等，可在这里执行
  protected void initSparkSession(SparkSessionType type, String appName, SparkConf conf) {
    this.ss = SparkUtil.getSparkSession(type, appName, conf);
    this.jsc = new JavaSparkContext(this.ss.sparkContext());
  }

  protected void initArg(String args[]) {
    this.arg = (T) BaseArgs.initArgs(args, this.getTClass());
  }

  private Class<T> getTClass() {
    ParameterizedType type = (ParameterizedType) this.getClass()
      .getGenericSuperclass();
    return (Class<T>) type.getActualTypeArguments()[0];
  }

  protected void prepare() {
    log.info("JOB PREPARE");
  }

  protected void finish() {
    this.ss.stop();
    this.ss.close();
    log.info("JOB FINISH");
  }

}
