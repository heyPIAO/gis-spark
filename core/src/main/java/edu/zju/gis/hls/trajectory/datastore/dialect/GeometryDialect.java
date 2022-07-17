package edu.zju.gis.hls.trajectory.datastore.dialect;

import org.apache.spark.sql.hls.udt.MultiPolygonUDT$;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcType;
import org.apache.spark.sql.types.DataType;
import scala.Option;

import java.util.Locale;

import static org.apache.spark.sql.types.DataTypes.BinaryType;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class GeometryDialect extends JdbcDialect {

    public static final DataType MultiPolylineType = MultiPolygonUDT$.MODULE$;

    @Override
    public boolean canHandle(String url) {
        return url.toLowerCase(Locale.ROOT).startsWith("jdbc:postgresql");
    }

    @Override
    public Option<JdbcType> getJDBCType(DataType dt) {
        if (StringType.equals(dt)) {
            System.out.println();
        } else if (BinaryType.equals(dt)) {
            System.out.println();
        } else if (MultiPolylineType.equals(dt)) {

        }
        return null;
    }
}
