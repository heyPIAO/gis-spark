package edu.zju.gis.hls.trajectory;

import edu.zju.gis.hls.trajectory.analysis.index.unifromGrid.PyramidConfig;
import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.KeyIndexedLayer;
import edu.zju.gis.hls.trajectory.model.Pipeline;
import edu.zju.gis.hls.trajectory.model.TileID;
import edu.zju.gis.hls.trajectory.model.TileJob;
import edu.zju.gis.hls.trajectory.model.TileUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * @author Hu
 * @date 2020/7/31
 * TODO 金字塔矢量瓦片构建
 **/
@Slf4j
public class VectorTileGenerator implements Serializable {

  public static int SCREEN_TILE_SIZE = 256;
  public static int SCREEN_TILE_BUFFER = 1;

  private PyramidConfig pConfig;

  public VectorTileGenerator(PyramidConfig pConfig) {
    this.pConfig = pConfig;
  }

  public <T extends KeyIndexedLayer> void generate(T layer, String outDir) throws Exception {
    String layerName = layer.getMetadata().getLayerName();
    JavaPairRDD<String, Feature> keyLayer = layer.getLayer();
    JavaPairRDD<TileID, Feature> allFeatureRDD = keyLayer.mapToPair(x -> {
      String[] id = x._1.split("_");
      TileID tileID= new TileID();
      tileID.setzLevel(Integer.parseInt(id[0]));
      tileID.setX(Integer.parseInt(id[1]));
      tileID.setY(Integer.parseInt(id[2]));
      return new Tuple2<>(tileID, x._2);
    });

    for(int i = pConfig.getZMax(); i >= pConfig.getZMin(); i--){
      allFeatureRDD = allFeatureRDD.groupByKey().flatMapToPair(new PairFlatMapFunction<Tuple2<TileID, Iterable<Feature>>, TileID, Feature>() {
        @Override
        public Iterator<Tuple2<TileID, Feature>> call(Tuple2<TileID, Iterable<Feature>> in) throws Exception {
          Iterator<Feature> featureIterator = in._2.iterator();

          TileID tileID = in._1;
          Envelope tileEnvelope = TileUtil.createTileBox(tileID, pConfig);
          TileJob tileJob = new TileJob();
          tileJob.setZMax(pConfig.getZMax());

          Map<String, Feature> unionFeature = new HashMap<>();
          while(featureIterator.hasNext()){
            Feature feature = featureIterator.next();
            if (!unionFeature.containsKey(feature.getFid())){
              unionFeature.put(feature.getFid(), feature);
            }
            else{
              Geometry g = tileJob.onlyPolygon(feature.getGeometry());
              Geometry og = tileJob.onlyPolygon(unionFeature.get(feature.getFid()).getGeometry());
              if(og == null) continue;
              feature.setGeometry(tileJob.union(g, og, Math.max(tileEnvelope.getWidth()/(16*SCREEN_TILE_SIZE), tileEnvelope.getHeight()/(16*SCREEN_TILE_SIZE)), 0, 2));
              unionFeature.put(feature.getFid(), feature);
            }
          }

          tileJob.buildTile(new ArrayList<Feature>(unionFeature.values()), tileID, pConfig, layerName, outDir);

          TileID upperTileID = new TileID();
          upperTileID.setzLevel(tileID.getzLevel() - 1);
          upperTileID.setX(tileID.getX() / 2);
          upperTileID.setY(tileID.getY() / 2);

          List<Tuple2<TileID, Feature>> result = new ArrayList<>();

          if(tileID.getzLevel() > pConfig.getZMin()){
            Envelope upperLevelEnvelope = TileUtil.createTileBox(upperTileID, pConfig);
            Pipeline upperLevelSimplifyPipeline = tileJob.getPipeline(pConfig.getCrs(), SCREEN_TILE_BUFFER, upperLevelEnvelope, false, false, true);
            Iterator<String> keys = unionFeature.keySet().iterator();
            while(keys.hasNext()){
              String key = keys.next();
              Feature f = unionFeature.get(key);
              Geometry g = upperLevelSimplifyPipeline.execute(f.getGeometry());
              if(g.isEmpty()) continue;
              g = tileJob.onlyPolygon(g);
              if(g == null) continue;
              f.setGeometry(g);
              result.add(new Tuple2<>(upperTileID, f));
            }
          }
          else{
            result.add(new Tuple2<>(null, null));
          }
          return result.iterator();
        }
      });
      System.out.println("level" + i + ": " + allFeatureRDD.count());
    }

  }

}
