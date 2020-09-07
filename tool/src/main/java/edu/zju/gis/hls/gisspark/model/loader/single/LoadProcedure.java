package edu.zju.gis.hls.gisspark.model.loader.single;


import edu.zju.gis.hls.trajectory.datastore.base.BaseEntity;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * 定义数据入库过程
 * 0. 参数校验
 * 1. 数据源装载
 * 2. Loader 初始化
 * 3. 数据预处理
 * 4. 数据从Model类转到Map
 * 4. 数据入库
 * @param <T>
 */
public interface LoadProcedure<T extends BaseEntity> {

  /**
   * 初始化
   */
  void init();

  /**
   * 参数校验
   * @return
   */
  void check();

  /**
   * 数据装载
   */
  void addSource();

  /**
   * 数据转化: if need
   * @param values
   * @return
   */
  List<Map<String, Object>> transformAll(List<T> values);

  Map<String, Object> transform(T values);

  /**
   * 数据清洗
   * @param lines
   * @return
   */
  List<T> preprocess(List<T> lines);

  /**
   * 数据插入
   * @return
   */
  long insert(List<Map<String, Object>> data);

  /**
   * 数据入库任务启动
   * @throws IOException
   */
  void exec() throws IOException;

}
