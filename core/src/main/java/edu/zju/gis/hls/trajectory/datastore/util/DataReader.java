package edu.zju.gis.hls.trajectory.datastore.util;

import edu.zju.gis.hls.trajectory.analysis.model.Term;
import edu.zju.gis.hls.trajectory.datastore.exception.DataReaderException;
import lombok.Getter;
import lombok.Setter;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Hu
 * @date 2019/6/19
 * 文件类型数据读取基类
 **/
@Getter
@Setter
public abstract class DataReader implements Closeable, Serializable {

  protected String filename;

  protected String[] headers;

  protected String crs = Term.DEFAULT_CRS.toWKT();

  public DataReader filename(String filename){
    this.filename = filename;
    return this;
  }

  protected void check(){
    if(this.filename == null) throw new DataReaderException("none input data file set yet");
  }

  public DataReader reset() {
    try {
      this.close();
      this.init();
      return this;
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  public void init(){
    this.check();
  }

  public abstract String next();

  public List<String> next(int size){
    if(size < 0) return null;
    List<String> result = new ArrayList<>();
    String feature = this.next();
    while(feature != null){
      result.add(feature);
      if(result.size() == size) break;
      feature = this.next();
    }
    return result;
  }

  public List<String> read(){
    List<String> result = new ArrayList<>();
    String feature = this.next();
    while(feature != null){
      result.add(feature);
      feature = this.next();
    }
    return result;
  }

  protected void readCRS() {
    this.crs = Term.DEFAULT_CRS.toWKT();
  }

  protected abstract String[] readHeader();

}
