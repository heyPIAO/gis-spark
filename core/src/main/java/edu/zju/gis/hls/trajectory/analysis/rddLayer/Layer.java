package edu.zju.gis.hls.trajectory.analysis.rddLayer;

import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.model.Field;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.reflect.ClassTag;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Hu
 * @date 2019/9/19
 **/
public class Layer<K,V extends Feature> extends JavaPairRDD<K, V> implements Serializable {

  /**
   * 图层元数据信息
   */
  @Getter
  @Setter
  protected LayerMetadata metadata;

  protected Layer(RDD<Tuple2<K, V>> rdd, ClassTag<K> kClassTag, ClassTag<V> vClassTag) {
    super(rdd, kClassTag, vClassTag);
    this.metadata = new LayerMetadata();
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
    return to;
  }

  /**
   * @param from
   * @param <L>
   */
  public <L extends Layer> void copy(L from) {
    this.metadata = from.metadata;
  }

  public Field findAttribute(String name) {
    for (Field f: this.metadata.getAttributes().keySet()) {
      if (f.getName().equals(name)) {
        return f;
      }
    }
    return null;
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

    return (T) this.mapToLayer(shiftFunction);
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

  /**
   * 如果图层未被缓存，则缓存
   * TODO 换个名字.. 太丑了..  @HLS
   * @return
   */
  public void makeSureCached() {
    if (!isCached()) {
      this.cache();
    }
  }

  public void release() {
    if (isCached()) {
      this.unpersist();
    }
  }

  public boolean isCached() {
    return !this.getStorageLevel().equals(StorageLevel.NONE());
  }

  // TODO 默认的 distinct 效率太低了，先偷懒重写一下
  public List<String> distinctKeys() {
    // 分区内部先去重
    JavaRDD<String> keys = this.keys().map(x->(String) x).mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
      @Override
      public Iterator<String> call(Iterator<String> it) throws Exception {
        List<String> m = IteratorUtils.toList(it);
        List<String> r = m.stream().distinct().collect(Collectors.toList());
        return r.iterator();
      }
    });

    // collect 之后再去重
    return keys.collect().stream().distinct().collect(Collectors.toList());
  }

}
