package edu.zju.gis.hls.trajectory.analysis.rddLayer;

import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import scala.reflect.ClassTag;

import java.io.Serializable;
import java.util.Map;

/**
 * @author Hu
 * @date 2019/9/19
 **/
public abstract class Layer<K,V extends Feature> extends JavaPairRDD<K, V> implements Serializable {

  @Getter
  @Setter
  protected boolean hasIndexed;

  /**
   * key 为字段名
   * value 为字段中文名
   */
  @Getter
  protected Map<String, String> attributes;

  @Getter
  protected Map<String, String> attributeTypes;

  /**
   * 图层元数据信息
   */
  @Getter
  @Setter
  protected LayerMetadata metadata;

  private Layer(RDD<Tuple2<K, V>> rdd, ClassTag<K> kClassTag, ClassTag<V> vClassTag) {
    this(rdd, kClassTag, vClassTag, false);
  }

  public Layer(RDD<Tuple2<K, V>> rdd, ClassTag<K> kClassTag, ClassTag<V> vClassTag, boolean hasIndexed) {
    super(rdd, kClassTag, vClassTag);
    this.hasIndexed = hasIndexed;
  }

  public void setAttributes(Map<String, String> attributes) {
    this.attributes = attributes;
    this.attributes.put("fid", "FID");
    this.attributes.put("shape", "SHAPE");
  }

  public void setAttributeTypes(Map<String, String> attributeTypes) {
    this.attributeTypes = attributeTypes;
    this.attributeTypes.put("fid", String.class.getName());
    this.attributeTypes.put("shape", String.class.getName());
  }

}
