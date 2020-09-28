package edu.zju.gis.hls.trajectory.datastore.storage.writer.tile;

import edu.zju.gis.hls.trajectory.analysis.index.unifromGrid.GridID;
import edu.zju.gis.hls.trajectory.analysis.index.unifromGrid.PyramidConfig;
import edu.zju.gis.hls.trajectory.analysis.model.Field;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.geojson.GeoJSONWriter;
import lombok.Getter;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.precision.CoordinatePrecisionReducerFilter;
import si.uom.SI;

import javax.measure.Unit;
import java.io.*;
import java.nio.charset.Charset;
import java.util.Map;

/**
 * create a geojson tile
 * Created by lan yu on 2017/6/23.
 * Updated by Hu on 2019/09/03.
 */
public class GeoJsonTileWriter implements VectorTileWriter {
    private BufferedOutputStream out;
    private Writer writer;
    private GeoJSONWriter jsonWriter;
    private CoordinatePrecisionReducerFilter precisionReducerFilter;

    @Getter
    private int count = 0;

    public GeoJsonTileWriter(String dir, GridID gridID, PyramidConfig pyramidConfig) throws IOException {
        int memoryBufferThreshold = 8096;
        File dirFolder = new File(dir, String.valueOf(gridID.getzLevel()));
        if (!dirFolder.exists()){
            dirFolder.mkdir();
        }
        // tile_key = tileId.toString();
        out = new BufferedOutputStream(new FileOutputStream( new File(dirFolder, gridID.getX()+"_" + gridID.getY() + ".geojson")),memoryBufferThreshold);
        writer = new OutputStreamWriter(out, Charset.forName("UTF-8"));
        jsonWriter = new GeoJSONWriter(writer);
        jsonWriter.object(); // start root object
        jsonWriter.key("type").value("FeatureCollection");
        jsonWriter.key("totalFeatures").value("unknown");
        jsonWriter.key("features");
        jsonWriter.array();
//        jsonWriter.setAxisOrder(CRS.getAxisOrder(pyramidConfig.getCrs()));

        Unit<?> unit = pyramidConfig.getCrs().getCoordinateSystem().getAxis(0).getUnit();
        Unit<?> standardUnit = unit.getSystemUnit();

        PrecisionModel pm = null;
        if (SI.RADIAN.equals(standardUnit)) {
            pm = new PrecisionModel(1e6); // truncate coords at 6 decimals
        } else if (SI.METRE.equals(standardUnit)) {
            pm = new PrecisionModel(100); // truncate coords at 2 decimals
        }
        if (pm != null) {
            precisionReducerFilter = new CoordinatePrecisionReducerFilter(pm);
        }
    }

    public boolean addFeature(String layerName, String featureId, String geometryName, Geometry aGeom, Map<Field, Object> properties){
        if (precisionReducerFilter != null) {
            aGeom.apply(precisionReducerFilter);
        }

        jsonWriter.object();
        jsonWriter.key("type").value("Feature");

        jsonWriter.key("id").value(featureId);

        jsonWriter.key("geometry");

        // Write the geometry, whether it is a null or not
        jsonWriter.writeGeom(aGeom);
        jsonWriter.key("geometry_name").value(geometryName);

        jsonWriter.key("properties");
        jsonWriter.object();

        for (Map.Entry<Field, Object> e : properties.entrySet()) {
            String attributeName = e.getKey().getName();
            Object value = e.getValue();

            jsonWriter.key(attributeName);
            if (value == null) {
                jsonWriter.value(null);
            } else {
                jsonWriter.value(value);
            }
        }

        jsonWriter.endObject(); // end the properties
        jsonWriter.endObject(); // end the feature

        count ++;
        return true;
    }

    public String build(){
        try{
            jsonWriter.endArray()
                    .endObject();
            writer.flush();
            out.flush();
            writer.close();
            out.close();
        }catch (IOException ex){
            ex.printStackTrace();
        }
        return null;
    }

}
