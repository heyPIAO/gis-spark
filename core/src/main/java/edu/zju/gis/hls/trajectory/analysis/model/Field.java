package edu.zju.gis.hls.trajectory.analysis.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

/**
 * @author Hu
 * @date 2019/12/17
 * 图层字段
 **/
@Getter
@ToString
public class Field implements Serializable, Cloneable {

  @Setter
  private String name;

  @Setter
  private String alias;

  @Setter
  private String type;

  @Setter
  private int length;

  private int index;

  private boolean exist;

  @Setter
  private FieldType fieldType;

  public Field(String name, Integer index, FieldType fieldType) {
    this(name, name, index, fieldType);
  }

  public Field(String name, String alias, Integer index, FieldType fieldType) {
    this(name, alias, String.class.getName(), 0, index, fieldType);
  }

  public Field(String name, String alias, String type, int length, int index, FieldType fieldType) {
    this.name = name;
    this.alias = alias;
    this.type = type;
    this.length = length;
    this.index = index;
    this.fieldType = fieldType;
    this.exist = (index >= 0);
  }

  public boolean isExist() {
    return this.exist;
  }

  public void setIndex(int index) {
    this.index = index;
    this.exist = (index >= 0);
  }

  public void setType(Class c) {
    this.type = c.getName();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Field) {
      Field of = (Field) obj;
      return of.getIndex() == this.index
        && of.getName().equals(this.name)
        && of.getAlias().equals(this.getAlias())
        && of.getType().equals(this.type)
        && of.getLength() == this.length;
    }
    return false;
  }

  @Override
  protected Object clone() throws CloneNotSupportedException {
    return super.clone();
  }

}
