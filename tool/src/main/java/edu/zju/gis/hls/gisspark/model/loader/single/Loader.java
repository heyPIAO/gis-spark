package edu.zju.gis.hls.gisspark.model.loader.single;

import com.google.gson.Gson;
import edu.zju.gis.hls.trajectory.datastore.base.BaseEntity;
import edu.zju.gis.hls.trajectory.datastore.exception.LoaderException;
import edu.zju.gis.hls.trajectory.datastore.storage.helper.JDBCHelper;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 数据装载类的基类
 * 使用方法：
 * 1. 构建Model类
 * 2. 构建Model类对应的Loader
 *    （1）定义init初始化必备参数
 *    （2）定义preprocess，数据清洗
 *    （3）定义transform将数据从读取对象转到Map
 *    （4）执行exec
 */
@Getter
@Setter
public abstract class Loader<T extends BaseEntity> implements LoadProcedure<T>, Serializable {

    private static final Logger logger = LoggerFactory.getLogger(Loader.class);

    protected String database; // database name

    protected String table; // table name

    protected JDBCHelper reader;

    protected JDBCHelper writer;

    @Override
    public void check() {
        if(database == null){
          throw new LoaderException("database needs to be defined");
        }
        if(table == null) {
          throw new LoaderException("data table needs to be defined");
        }
        if(this.reader == null) {
          throw new LoaderException("need to initial a reader");
        }

        if(this.writer == null){
          throw new LoaderException("need to initial a writer");
        }
    }

    @Override
    public List<Map<String, Object>> transformAll(List<T> values) {
        List<Map<String, Object>> result = new ArrayList<>();
        for(T value: values){
            Map<String, Object> t = this.transform(value);
            result.add(t);
        }
        return result;
    }

    @Override
    public long insert(List<Map<String, Object>> data) {
      this.writer.insert(table, data);
      return data.size();
    }

  /**
   * TODO 待测试
   * @throws IOException
   */
  @Override
  public void exec() throws IOException {
    this.check();
    int count = 0;
    this.check();
    this.addSource();
    this.init();
    List<T> data = new ArrayList<>();
    Gson gson = new Gson();
    String line = reader.next();
    while(line != null) {
      data.add(gson.fromJson(line, this.getTClass()));
      if (data.size() == 1000 || (!reader.hasNext())) {
        count = count + data.size();
        data = this.preprocess(data);
        List<Map<String, Object>> tuple = this.transformAll(data);
        this.insert(tuple);
        data = new ArrayList<>();
        logger.info(String.format("load %d tuple successful", data.size()));
      }
      line = reader.next();
    }
    logger.info(String.format("total count %d", count));
  }


  protected Class<T> getTClass() {
    return (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
  }


}
