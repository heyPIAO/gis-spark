import edu.zju.gis.hls.trajectory.analysis.model.PointFeature;
import org.geotools.geojson.geom.GeometryJSON;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Hu
 * @date 2019/9/25
 **/
public class GeoJsonTest {

  public static void main(String[] args) throws IOException {

    // geojson 写出
    Map<String, Object> props = new HashMap<>();
    props.put("test", "test");
    PointFeature pointFeature = new PointFeature("123", new GeometryFactory().createPoint(new Coordinate(120.00, 30.00)), props);
    String json = pointFeature.toJson();
    System.out.println(json);

    // geojson 读取
    GeometryJSON gjson = new GeometryJSON();
    Reader reader = new StringReader(json);
    Geometry geometry = gjson.read(reader);
    System.out.println(geometry.toText());
  }

}
