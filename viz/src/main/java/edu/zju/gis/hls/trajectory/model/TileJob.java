package edu.zju.gis.hls.trajectory.model;

import edu.zju.gis.hls.trajectory.analysis.index.unifromGrid.GridID;
import edu.zju.gis.hls.trajectory.analysis.index.unifromGrid.GridUtil;
import edu.zju.gis.hls.trajectory.analysis.index.unifromGrid.PyramidConfig;
import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import edu.zju.gis.hls.trajectory.analysis.model.Term;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.tile.GeoJsonTileWriter;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.tile.MVTTileBuilder;
import edu.zju.gis.hls.trajectory.datastore.storage.writer.tile.VectorTileWriter;
import lombok.Getter;
import lombok.Setter;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.Polygon;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.*;
import java.io.Serializable;
import java.util.List;

@Getter
@Setter
public class TileJob implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(TileJob.class);

    /**
     * 构建瓦片中每个像素处理的Pipeline
     * 默认目标crs与源crs一致
     */
    public Pipeline getPipeline(CoordinateReferenceSystem sourceCrs, int buffer, Envelope referenceEnvelop, boolean preprocess, boolean isClip, boolean isSimplify) {
        CoordinateReferenceSystem targetCrs = sourceCrs;
        int mapWidth = Term.SCREEN_TILE_SIZE;
        int mapHeight = Term.SCREEN_TILE_SIZE;
        int overSampleFactor = Term.OVERSAMPLE_FACTOR;
        // rectangle of the image: width and height
        Rectangle paintArea = new Rectangle(mapWidth, mapHeight);

        // bounding box - in final map (target) CRS
        ReferencedEnvelope renderingArea = new ReferencedEnvelope(referenceEnvelop.getMinX(), referenceEnvelop.getMaxX(), referenceEnvelop.getMinY(), referenceEnvelop.getMaxY(), targetCrs);
        Pipeline pipeline = null;

        try {
            final PipelineBuilder builder = PipelineBuilder.newBuilder(renderingArea, paintArea, sourceCrs, overSampleFactor, buffer);
            if (preprocess){
                pipeline = builder
                        .preprocess()
                        .collapseCollections()
                        .build();
            } else  {
                pipeline = builder
                        .clip(isClip, false)
                        .simplify(isSimplify)
                        .collapseCollections()
                        .build();
            }
        } catch (FactoryException e) {
            e.printStackTrace();
        }

        return pipeline;
    }

    public Geometry onlyPolygon(Geometry result) {
        if ((result instanceof Polygon) || (result instanceof MultiPolygon)) {
            return result;
        }
        List polys = org.locationtech.jts.geom.util.PolygonExtracter.getPolygons(result);
        if (polys.size() == 0) {
            return null;
        }
        if (polys.size() == 1) {
            return (Polygon) polys.get(0);
        }
        return new MultiPolygon(
                (Polygon[]) polys.toArray(new Polygon[polys.size()]), result.getFactory());
    }

    public Geometry union(Geometry g1, Geometry g2, double buffer, int depth, int maxDepth) {

        Geometry bg1;
        Geometry bg2;

        if (depth == 0) {
            bg1 = g1;
            bg2 = g2;
        } else {
            bg1 = g1.buffer(buffer);
            bg2 = g2.buffer(buffer);
        }

        try {
            return bg1.union(bg2);
        } catch (TopologyException e) {
            depth ++;
            if (depth > maxDepth) {
                return null;
            }
            return union(g1, g2, buffer*2, depth, maxDepth);
        }
    }

    public void buildTile(List<Feature> features, GridID gridID, PyramidConfig pyramidConfig, String layerName, String fileType, String outDir) throws Exception {
        Envelope tileEnvelope = GridUtil.createTileBox(gridID, pyramidConfig);

        VectorTileWriter vectorTileWriter;
        Pipeline prePipeline = getPipeline(pyramidConfig.getCrs(), Term.SCREEN_TILE_BUFFER, tileEnvelope, true, false, false);
        if(fileType.equals("geojson")) {
            vectorTileWriter = new GeoJsonTileWriter(outDir, gridID, pyramidConfig);
        }
        else{
            vectorTileWriter = new MVTTileBuilder(outDir, gridID, tileEnvelope, layerName);
        }

        for (Feature feature: features) {
            Geometry tileGeometry = prePipeline.execute(feature.getGeometry());
            if (tileGeometry.isEmpty()) {
                continue;
            }
            tileGeometry = onlyPolygon(tileGeometry);
            if (tileGeometry == null) {
                continue;
            }
            vectorTileWriter.addFeature(layerName, feature.getFid(), feature.getFid(), tileGeometry, feature.getAttributes());
        }

        vectorTileWriter.build();
    }

}
