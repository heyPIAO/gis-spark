package edu.zju.gis.hls.trajectory.analysis.util;

import edu.zju.gis.hls.trajectory.datastore.exception.GISSparkException;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Hu
 * @date 2020/7/13
 **/
public class ClassUtil {

  public static Class getTClass(Class clazz, int index) {
    ParameterizedType type = (ParameterizedType) clazz
      .getGenericSuperclass();
    return (Class)type.getActualTypeArguments()[index];
  }

  /**
   * 获得当前类及其父类中声明的所有字段中名为name的字段
   * @return
   */
  public static Field getFieldFromAllFields(Class clazz, String name) {
    Field[] fields = ClassUtil.getAllDeclaredFields(clazz);
    for (Field f: fields) {
      if (f.getName().equals(name)) return f;
    }
    throw new GISSparkException(String.format("Unvalid field name[%s] for class[%s]", name, clazz.getName()));
  }


  /**
   * 获得当前类及其父类中声明的所有字段
   * @param clazz
   * @return
   */
  public static Field[] getAllDeclaredFields(Class clazz) {

    List<Field[]> fieldArrayList = new ArrayList<Field[]>();
    while (clazz != null) {
      fieldArrayList.add(clazz.getDeclaredFields());
      clazz = clazz.getSuperclass();
    }
    int fieldCount = 0;
    int fieldIndex = 0;
    for (Field[] fieldArray : fieldArrayList) {
      fieldCount = fieldCount + fieldArray.length;
    }
    Field[] allFields = new Field[fieldCount];
    for (Field[] fieldArray : fieldArrayList) {
      for (Field field : fieldArray) {
        allFields[fieldIndex++] = field;
      }
    }
    return allFields;
  }

}
