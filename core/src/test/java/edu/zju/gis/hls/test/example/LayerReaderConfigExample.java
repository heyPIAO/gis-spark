package edu.zju.gis.hls.test.example;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import edu.zju.gis.hls.trajectory.analysis.model.Term;
import edu.zju.gis.hls.trajectory.analysis.rddLayer.LayerType;
import edu.zju.gis.hls.trajectory.datastore.serializer.CRSJsonSerializer;
import edu.zju.gis.hls.trajectory.datastore.storage.reader.file.FileLayerReaderConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.file.FileLayerWriterConfig;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.pg.PgLayerWriterConfig;
import lombok.extern.slf4j.Slf4j;
import org.geotools.referencing.CRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

/**
 * @author Hu
 * @date 2020/7/12
 **/
@Slf4j
public class LayerReaderConfigExample {

  public static void main(String[] args) throws FactoryException {
    readerConfigTest();
    writerConfigTest();
    pgLayerWriterConfigTest();
  }

  public static void readerConfigTest() throws FactoryException {
    FileLayerReaderConfig config = new FileLayerReaderConfig("temp", "file:///", LayerType.POINT_LAYER);
    config.setCrs(CRS.parseWKT(Term.WKT_4528));
    Gson gson = new GsonBuilder().registerTypeAdapter(CoordinateReferenceSystem.class, new CRSJsonSerializer())
      .registerTypeAdapter(CoordinateReferenceSystem.class, new CRSJsonSerializer.CRSJsonDeserializer()).create();
    System.out.println(gson.toJson(config));
  }

  public static void writerConfigTest() {
    FileLayerWriterConfig config = new FileLayerWriterConfig("file:///", false);
    Gson gson = new Gson();
    System.out.println(gson.toJson(config));
  }

  public static void pgLayerWriterConfigTest() {
    String source = "jdbc:postgresql://localhost:5432/postgres";
    String user = "postgres";
    String password = "root";
    String schema = "public";
    String dbtable = "test";
    PgLayerWriterConfig wconfig = new PgLayerWriterConfig(source, "public", "test2", user, password);
    Gson gson = new Gson();
    System.out.println(gson.toJson(wconfig));
  }

}
