package edu.zju.gis.hls.trajectory.doc.util;

import com.esotericsoftware.kryo.Kryo;
import com.github.davidmoten.rtreemulti.RTree;
import com.github.davidmoten.rtreemulti.geometry.internal.RectangleDouble;
import org.apache.spark.serializer.KryoRegistrator;

/**
 * @author Hu
 * @date 2021/1/12
 **/
public class MyKryoRegister implements KryoRegistrator {

  @Override
  public void registerClasses(Kryo kryo) {
    kryo.register(RectangleDouble.class);
    kryo.register(RTree.class);
  }

}
