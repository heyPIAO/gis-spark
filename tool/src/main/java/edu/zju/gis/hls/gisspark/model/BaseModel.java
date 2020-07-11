package edu.zju.gis.hls.gisspark.model;

import edu.zju.gis.hls.gisspark.model.args.BaseArgs;
import edu.zju.gis.hls.gisspark.model.exception.ModelFailedException;
import edu.zju.gis.hls.gisspark.model.util.SparkSessionType;
import edu.zju.gis.hls.gisspark.model.util.SparkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;

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
    this.init(SparkSessionType.LOCAL, this.getClass().getName(), new SparkConf(), args);
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
    String argClassName = this.getClass().getName() + "Args";
    Class c = null;
    try {
      c = Class.forName(argClassName);
      if (c.asSubclass(BaseArgs.class) != null) {
        this.arg = (T) BaseArgs.initArgs(args, c);
      } else {
        throw new ModelFailedException(BaseArgs.class, "initArg()", "is not a sub class of BaseArgs", argClassName);
      }
    } catch (ClassNotFoundException e) {
      throw new ModelFailedException(BaseArgs.class, "initArg()", "cannot find argument class named " + argClassName, this.getClass().getName());
    }

  }

  protected void finish() {
    this.ss.stop();
    this.ss.close();
  }

}
