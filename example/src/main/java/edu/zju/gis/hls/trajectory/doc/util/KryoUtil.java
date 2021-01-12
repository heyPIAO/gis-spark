//package edu.zju.gis.hls.trajectory.doc.util;
//
///**
// * @author Hu
// * @date 2021/1/10
// **/
//import com.esotericsoftware.kryo.Kryo;
//import com.esotericsoftware.kryo.io.Input;
//import com.esotericsoftware.kryo.io.Output;
//import com.esotericsoftware.kryo.util.Pool;
//
///****
// * Kryo 序列化工具
// * @author dgmislrh
// */
//public class KryoUtil {
//
//  private static int BUFFER_SIZE = 1024;
//
//  private static Pool<Kryo> mKryoPool = new Pool<Kryo>(true, false, 8) {
//    protected Kryo create() {
//      Kryo kryo = new Kryo();
//      kryo.setRegistrationRequired(false);
//      kryo.setReferences(false);
//      // Configure the Kryo instance.
//      return kryo;
//    }
//  };
//  private static Pool<Output> mOutputPool = new Pool<Output>(true, false, 16) {
//    protected Output create() {
//      return new Output(BUFFER_SIZE, -1);
//    }
//  };
//  private static Pool<Input> mInputPool = new Pool<Input>(true, false, 16) {
//    protected Input create() {
//      return new Input(BUFFER_SIZE);
//    }
//  };
//
//  public KryoUtil() {
//  }
//
//  public static byte[] serialize(Object object) {
//    Kryo lvKryo = mKryoPool.obtain();
//    Output lvOutput = mOutputPool.obtain();
//    try {
//      lvOutput.clear();
//      lvKryo.writeObject(lvOutput, object);
//      return lvOutput.getBuffer();
//    } finally {
//      mKryoPool.free(lvKryo);
//      mOutputPool.free(lvOutput);
//    }
//  }
//  public static <T> T unserialize(byte[] pvBytes,Class<T> pvClass) {
//    Kryo lvKryo = mKryoPool.obtain();
//    Input lvInput= mInputPool.obtain();
//    try {
//      lvInput.setBuffer(pvBytes);
//      return lvKryo.readObject(lvInput, pvClass);
//    } finally {
//      mKryoPool.free(lvKryo);
//      mInputPool.free(lvInput);
//    }
//  }
//
//}
