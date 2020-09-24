package edu.zju.gis.hls.trajectory.model;

import edu.zju.gis.hls.trajectory.analysis.index.unifromGrid.PyramidConfig;
import edu.zju.gis.hls.trajectory.analysis.model.Feature;
import lombok.Getter;
import lombok.Setter;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.simplify.DouglasPeuckerSimplifier;
import org.locationtech.jts.simplify.TopologyPreservingSimplifier;
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

    private final static Integer BUFFER = 1;
    public final static Integer SCREEN_TILE_SIZE = 256;
    public final static Integer OVERSAMPLE_FACTOR = 1;
    private static final Logger logger = LoggerFactory.getLogger(TileJob.class);
    private int partition;
    private int zMax;
    private int zMin;

    public TileJob() {
        this.partition = 8;
        this.zMax = 12;
        this.zMax = 6;
    }

    /**
     * 简化图像
     * @param geo
     * @param distanceTolerance
     * @return
     */
    public static Geometry simplify(Geometry geo, double distanceTolerance) {
        switch (geo.getDimension()) {
            case 2:
                return TopologyPreservingSimplifier.simplify(geo, distanceTolerance);
            case 1:
                return DouglasPeuckerSimplifier.simplify(geo, distanceTolerance);
            default:
                logger.warn("Unvalid geometry type for geometry simplify");
                return geo;
        }
    }

    /**
     * 根据阈值过滤小图斑
     * @param geo
     * @param span
     * @return
     */
    public static boolean smallGeomeryFilter(Geometry geo, double span) {
        double area = span * span;
        if (geo.getDimension() > 0) {
            Envelope e = geo.getEnvelopeInternal();
            if (Math.max(e.getWidth(), e.getHeight()) < span) {
                return false;
            }
            if (e.getArea() < area) {
                return false;
            }
            return true;
        } else {
            logger.error("Unvalid geometry type for small geometry filter");
            return false;
        }
    }

    /**
     * 构建瓦片中每个像素处理的Pipeline
     * 默认目标crs与源crs一致
     */
    public Pipeline getPipeline(CoordinateReferenceSystem sourceCrs, int buffer, Envelope referenceEnvelop, boolean preprocess, boolean isClip, boolean isSimplify) {
        CoordinateReferenceSystem targetCrs = sourceCrs;
        int mapWidth = SCREEN_TILE_SIZE;
        int mapHeight = SCREEN_TILE_SIZE;
        int overSampleFactor = OVERSAMPLE_FACTOR;
        // rectangle of the image: width and height
        Rectangle paintArea = new Rectangle(mapWidth, mapHeight);

        // bounding box - in final map (target) CRS
        ReferencedEnvelope renderingArea = new ReferencedEnvelope(referenceEnvelop.getMinX(), referenceEnvelop.getMaxX(), referenceEnvelop.getMinY(), referenceEnvelop.getMaxY(), targetCrs);
        Pipeline pipeline = null;

        try {
            final PipelineBuilder builder =
                    PipelineBuilder.newBuilder(
                            renderingArea, paintArea, sourceCrs, overSampleFactor, buffer);

            if (preprocess){
                pipeline =
                        builder.preprocess()
                                .collapseCollections()
//                                .precision(targetCrs)
                                .build();
            } else  {
                pipeline = builder
                        .clip(isClip, false)
                        .simplify(isSimplify)
                        .collapseCollections()
//                        .precision(targetCrs)
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

    public void buildTile(List<Feature> features, TileID tileID, PyramidConfig pyramidConfig, String layerName, String outDir) throws Exception {
        Envelope tileEnvelope = TileUtil.createTileBox(tileID, pyramidConfig);
        VectorTileBuilder vectorTileBuilder;
        Pipeline prePipeline = getPipeline(pyramidConfig.getCrs(), BUFFER, tileEnvelope, true, false, false);
        vectorTileBuilder = new GeoJsonTileBuilder(outDir, tileID, pyramidConfig);
        for (Feature feature: features) {
            Geometry tileGeometry = prePipeline.execute(feature.getGeometry());
            if (tileGeometry.isEmpty())
                continue;

            tileGeometry = onlyPolygon(tileGeometry);

            if (tileGeometry == null)
                continue;

            vectorTileBuilder.addFeature(layerName, feature.getFid(), feature.getFid(),
                    tileGeometry, feature.getAttributes());
        }

        vectorTileBuilder.build();
    }

}
