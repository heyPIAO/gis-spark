package edu.zju.gis.hls.trajectory.datastore.storage.writer.file;

import edu.zju.gis.hls.trajectory.datastore.storage.reader.ReaderConfigTerm;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

/**
   * 重写 MultipleTextOutputFormat方法，自定义生成文件的文件名
   */
  public class KeyFileOutputFormat extends MultipleTextOutputFormat {

    @Override
    protected String generateFileNameForKeyValue(Object key, Object value, String name) {
      return String.valueOf(key);
    }

  @Override
  protected Object generateActualValue(Object key, Object value) {
    return key + ReaderConfigTerm.DEFAULT_FILE_SEPARATOR + value;
  }
}