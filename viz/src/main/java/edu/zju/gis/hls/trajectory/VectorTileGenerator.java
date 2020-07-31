package edu.zju.gis.hls.trajectory;

import edu.zju.gis.hls.trajectory.analysis.rddLayer.KeyIndexedLayer;

/**
 * @author Hu
 * @date 2020/7/31
 * TODO 金字塔矢量瓦片构建
 **/
public class VectorTileGenerator {

  private PyramidConfig pc;

  public VectorTileGenerator(PyramidConfig pc) {
    this.pc = pc;
  }

  public <T extends KeyIndexedLayer> void generate(T layer) {
    // TODO 矢量瓦片生成
  }


}
