package edu.zju.gis.hls.trajectory;

import edu.zju.gis.hls.trajectory.analysis.index.unifromGrid.GridID;
import edu.zju.gis.hls.trajectory.analysis.index.unifromGrid.GridUtil;
import edu.zju.gis.hls.trajectory.analysis.index.unifromGrid.PyramidConfig;
import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.model.Term;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.KeyIndexedLayer;
import edu.zju.gis.hls.trajectory.model.Pipeline;
import edu.zju.gis.hls.trajectory.model.TileJob;
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

  private int zMin;
  private int zMax;
  private String fileType;
  private PyramidConfig pConfig;

  public VectorTileGenerator(int zMin, int zMax) {
    this(zMin, zMax, "geojson");
  }

  public VectorTileGenerator(int zMin, int zMax, String fileType) {
    this.zMin = zMin;
    this.zMax = zMax;
    this.fileType = fileType.equals("mvt")? fileType: "geojson";
  }

  public <T extends KeyIndexedLayer> void generate(T layer, String outDir) throws Exception {
    pConfig = new PyramidConfig.PyramidConfigBuilder()
            .setCrs(layer.getMetadata().getCrs())
            .setBaseMapEnv(-180, 180, -90, 90)
            .setzLevelRange(Term.QUADTREE_MIN_Z, Term.QUADTREE_MAX_Z)
            .build();

    String layerName = layer.getMetadata().getLayerName();
    JavaPairRDD<String, Feature> keyLayer = layer.getLayer();
    JavaPairRDD<GridID, Feature> allFeatureRDD = keyLayer.mapToPair(x -> {
      String[] id = x._1.split("_");
      GridID gridID= new GridID();
      gridID.setzLevel(Integer.parseInt(id[0]));
      gridID.setX(Integer.parseInt(id[1]));
      gridID.setY(Integer.parseInt(id[2]));
      return new Tuple2<>(gridID, x._2);
    });

    for(int i = zMax; i >= zMin; i--){
      allFeatureRDD = allFeatureRDD.groupByKey().flatMapToPair(new PairFlatMapFunction<Tuple2<GridID, Iterable<Feature>>, GridID, Feature>() {
        @Override
        public Iterator<Tuple2<GridID, Feature>> call(Tuple2<GridID, Iterable<Feature>> in) throws Exception {
          Iterator<Feature> featureIterator = in._2.iterator();

          GridID gridID = in._1;
          Envelope tileEnvelope = GridUtil.createTileBox(gridID, pConfig);
          TileJob tileJob = new TileJob();

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
              feature.setGeometry(tileJob.union(g, og, Math.max(tileEnvelope.getWidth()/(16*Term.SCREEN_TILE_SIZE), tileEnvelope.getHeight()/(16*Term.SCREEN_TILE_SIZE)), 0, 2));
              unionFeature.put(feature.getFid(), feature);
            }
          }

          tileJob.buildTile(new ArrayList<>(unionFeature.values()), gridID, pConfig, layerName, fileType, outDir);

          GridID upperTileID = new GridID();
          upperTileID.setzLevel(gridID.getzLevel() - 1);
          upperTileID.setX(gridID.getX() / 2);
          upperTileID.setY(gridID.getY() / 2);

          List<Tuple2<GridID, Feature>> result = new ArrayList<>();

          if(gridID.getzLevel() > zMin){
            Envelope upperLevelEnvelope = GridUtil.createTileBox(upperTileID, pConfig);
            Pipeline upperLevelSimplifyPipeline = tileJob.getPipeline(pConfig.getCrs(), Term.SCREEN_TILE_BUFFER, upperLevelEnvelope, false, false, true);
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
      allFeatureRDD.count();
    }

  }

}
