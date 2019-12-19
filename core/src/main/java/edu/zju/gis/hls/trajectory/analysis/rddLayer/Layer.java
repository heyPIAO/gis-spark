package edu.zju.gis.hls.trajectory.analysis.rddLayer;

import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.model.Field;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import scala.reflect.ClassTag;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Hu
 * @date 2019/9/19
 **/
public abstract class Layer<K,V extends Feature> extends JavaPairRDD<K, V> implements Serializable {

  /**
   * key 为字段名
   * value 为字段中文名
   */
  @Getter
  protected Map<String, String> attributes;

  /**
   * key 为字段名
   * value 为字段类型
   */
  @Getter
  protected Map<String, String> attributeTypes;


  // TODO 用 field 代替 attributes
  @Getter
  protected Map<Integer, Field> fields;

  /**
   * 图层元数据信息
   */
  @Getter
  @Setter
  protected LayerMetadata metadata;

  protected Layer(){
    this(null, null, null);
  }

  protected Layer(RDD<Tuple2<K, V>> rdd, ClassTag<K> kClassTag, ClassTag<V> vClassTag) {
    super(rdd, kClassTag, vClassTag);
    this.metadata = new LayerMetadata();
    this.attributeTypes = new HashMap<>();
    this.attributes = new HashMap<>();
  }

  private <T extends Layer<K,V>> T initialize(T layer, RDD<Tuple2<K, V>> rdd) {
    try {
      Constructor c = layer.getConstructor(RDD.class);
      T t = (T) c.newInstance(rdd);
      return copy(layer, t);
    } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * only copy meta info of the layer
   * @return
   */
  protected <T extends Layer<K,V>> T copy(T from, T to) {
    to.metadata = from.metadata;
    to.attributes = from.attributes;
    to.attributeTypes = from.attributeTypes;
    to.fields = from.fields;
    return to;
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

  /**
   * 图层偏移
   * @param deltaX
   * @param deltaY
   * @return
   */
  public <T extends Layer<K,V>> T shift(double deltaX, double deltaY) {

    if (this.metadata.getExtent() != null) {
      this.metadata.shift(deltaX, deltaY);
    }

    Function shiftFunction = new Function<Tuple2<K, V>, Tuple2<K, V>>() {
      @Override
      public Tuple2<K, V> call(Tuple2<K, V> t) throws Exception {
        Feature f = t._2;
        f.shift(deltaX, deltaY);
        return new Tuple2<>(t._1, (V)f);
      }
    };

    T result = (T) this.mapToLayer(shiftFunction);

    return result;
  }

  public Constructor getConstructor(Class<?>... parameterTypes) throws NoSuchMethodException {
    return this.getClass().getConstructor(parameterTypes);
  }

  /**
   * 重写 RDD 的 map/flatMap/mapToPair/filter 等方法，继承图层的元数据信息
   * 通过强制类型转换，保留了图层的类型信息
   * @param f
   * @return
   */
  public <L extends Layer<K,V>> L flatMapToLayer(PairFlatMapFunction<Tuple2<K, V>, K, V> f) {
    return (L) this.initialize(this, this.flatMapToPair(f).rdd());
  }

  public <L extends Layer<K,V>> L mapToLayer(Function<Tuple2<K, V>, Tuple2<K, V>> f) {
    return (L) this.initialize(this, this.map(f).rdd());
  }

  public <L extends Layer<K,V>> L flatMapToLayer(FlatMapFunction<Tuple2<K, V>, Tuple2<K, V>> f) {
    return (L) this.initialize(this, this.flatMap(f).rdd());
  }

  public <L extends Layer<K,V>> L filterToLayer(Function<Tuple2<K, V>, Boolean> f) {
    return (L) this.initialize(this, this.filter(f).rdd());
  }

  public <L extends Layer<K,V>> L repartitionToLayer(int num) {
    return (L) this.initialize(this, this.repartition(num).rdd());
  }

}
