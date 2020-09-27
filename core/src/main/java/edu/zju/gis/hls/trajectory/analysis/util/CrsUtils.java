package edu.zju.gis.hls.trajectory.analysis.util;

import edu.zju.gis.hls.trajectory.analysis.model.Term;
import lombok.Getter;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Envelope;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.ReferenceIdentifier;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.io.Serializable;
import java.util.Set;

/**
 * @author Hu
 * @date 2019/12/18
 **/
public class CrsUtils implements Serializable {


    public static ReferencedEnvelope getCrsEnvelope(CoordinateReferenceSystem crs) {
        return SupportedCRS.getExtent(crs);
    }

    @Getter
    public enum SupportedCRS {

        WGS84("4326", -180, -90, 180, 90),
        WGS84_Pseudo_Mercator("3857", -20026376.39, -20048966.10, 20026376.39, 20048966.10),
        CGCS2000_3_40("4528", 40347872.25, 2703739.74, 40599933.05, 5912395.20); // CGCS2000 3-degree Gauss-Kruger zone 40

        private String epsg;
        private double xmin;
        private double ymin;
        private double xmax;
        private double ymax;

        SupportedCRS(String epsg, double xmin, double ymin, double xmax, double ymax) {
            this.epsg = epsg;
            this.xmin = xmin;
            this.ymin = ymin;
            this.xmax = xmax;
            this.ymax = ymax;
        }

        public static CoordinateReferenceSystem getCRS(SupportedCRS scrs) {
            try {
                //todo find out why 4528 not found
                if (scrs.epsg.equals("4528"))
                    return Term.DEFAULT_CRS;
                return CRS.decode("epsg:" + scrs.epsg);
            } catch (FactoryException e) {
                e.printStackTrace();
            }
            return null;
        }

        public static ReferencedEnvelope getExtent(SupportedCRS scrs) {
            CoordinateReferenceSystem crs = SupportedCRS.getCRS(scrs);
            Envelope e = new Envelope(scrs.xmin, scrs.xmax, scrs.ymin, scrs.ymax);
            return new ReferencedEnvelope(e, crs);
        }

        public static ReferencedEnvelope getExtent(CoordinateReferenceSystem crs) {
            String epsg = crs.getIdentifiers().iterator().next().getCode();
            for (SupportedCRS scrs : SupportedCRS.values()) {
                if (scrs.epsg.equals(epsg)) {
                    return SupportedCRS.getExtent(scrs);
                }
            }
            throw new UnknownError("Unvalid Coordinate Reference System for finding it's extent: " + crs.getName().toString());
        }

    }

}
