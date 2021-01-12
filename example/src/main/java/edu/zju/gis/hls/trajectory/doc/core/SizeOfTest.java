package edu.zju.gis.hls.trajectory.doc.core;

import sizeof.agent.SizeOfAgent;

/**
 * @author Hu
 * @date 2021/1/11
 **/
public class SizeOfTest {

  public static void main(String[] args) {

    Integer rTree = 0;
    long size = getSize(rTree);
    System.out.println(size);

  }

  public static long getSize(Object o) {
    return SizeOfAgent.fullSizeOf(o);
  }


}
