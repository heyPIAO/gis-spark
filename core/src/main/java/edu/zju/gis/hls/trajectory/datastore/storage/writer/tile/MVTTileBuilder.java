package edu.zju.gis.hls.trajectory.datastore.storage.writer.tile;

import com.wdtinc.mapbox_vector_tile.VectorTile;
import com.wdtinc.mapbox_vector_tile.adapt.jts.*;
import com.wdtinc.mapbox_vector_tile.build.MvtLayerBuild;
import com.wdtinc.mapbox_vector_tile.build.MvtLayerParams;
import com.wdtinc.mapbox_vector_tile.build.MvtLayerProps;
import edu.zju.gis.hls.trajectory.analysis.index.unifromGrid.GridID;
import edu.zju.gis.hls.trajectory.analysis.model.Field;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Hu
 * @date 2019/10/7
 **/
public class MVTTileBuilder implements VectorTileWriter {

  private static final Logger logger = LoggerFactory.getLogger(MVTTileBuilder.class);

  private List<Geometry> geom;
  private String layerName;
  private String dir;
  private GridID gridID;
  private Envelope tileEnvelope;
  private GeometryFactory gf;
  private IGeometryFilter igf;
  private MvtLayerParams layerParams;

  public MVTTileBuilder(String dir, GridID gridID, Envelope envelope, String layerName) {
    this.dir = dir;
    this.layerName = layerName;
    this.gridID = gridID;
    this.tileEnvelope = envelope;
    this.geom = new ArrayList<>();
    this.gf = new GeometryFactory();
    this.igf = geo -> true;
    this.layerParams = new MvtLayerParams(256, 4096);
  }

  @Override
  public boolean addFeature(String layerName, String featureId, String geometryName, Geometry geometry, Map<Field, Object> properties) {
    this.geom.add(geometry);
    return true;
  }

  @Override
  public String build() throws IOException {
    TileGeomResult tileGeom = JtsAdapter.createTileGeom(
            this.geom,
            this.tileEnvelope,
            this.gf,
            this.layerParams,
            this.igf);
    VectorTile.Tile.Builder tileBuilder = VectorTile.Tile.newBuilder();
    VectorTile.Tile.Layer.Builder layerBuilder = MvtLayerBuild.newLayerBuilder(this.layerName, this.layerParams);
    MvtLayerProps layerProps = new MvtLayerProps();
    IUserDataConverter userDataConverter = new UserDataKeyValueMapConverter();
    List<VectorTile.Tile.Feature> features = JtsAdapter.toFeatures(tileGeom.mvtGeoms, layerProps, userDataConverter);
    layerBuilder.addAllFeatures(features);
    MvtLayerBuild.writeProps(layerBuilder, layerProps);
    tileBuilder.addLayers(layerBuilder.build());
    VectorTile.Tile mvt = tileBuilder.build();
    String tilePath = String.format("%s%s%d%s%d_%d.mvt", this.dir, File.separator, this.gridID.getzLevel(), File.separator, this.gridID.getX(), this.gridID.getY());
    File outDir = new File(this.dir);
    if (!outDir.exists()) {
      outDir.mkdirs();
    }
    File dir = new File(outDir, String.valueOf(this.gridID.getzLevel()));
    if (!dir.exists()) {
      dir.mkdirs();
    }
    File t = new File(tilePath);
    if (t.exists()) {
      t.delete();
    }
    Path p = Paths.get(tilePath);
    try {
      Files.write(p, mvt.toByteArray());
    } catch (IOException e) {
        return e.getMessage();
    }
    return "success";
  }

}
