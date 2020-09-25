package edu.zju.gis.hls.trajectory.datastore.storage.helper;

import edu.zju.gis.hls.trajectory.analysis.model.Term;
import edu.zju.gis.hls.trajectory.analysis.util.ClassUtil;
import edu.zju.gis.hls.trajectory.datastore.exception.GISSparkException;
import edu.zju.gis.hls.trajectory.datastore.storage.config.JDBCHelperConfig;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Hu
 * @date 2020/7/14
 * JDBC 常用操作类
 * 流式查询实现：https://www.jianshu.com/p/c7c5dbe63019
 * TODO 加c3p0连接池
 **/
@Slf4j
public abstract class JDBCHelperImpl<T extends JDBCHelperConfig> implements JDBCHelper {


    protected Connection conn;

    @Getter
    protected T config;

    protected PreparedStatement statement;
    protected ResultSet resultSet;

    public JDBCHelperImpl(T config) {
        this.config = config;
        this.initConn();
    }

    @Override
    public void useDB(String databaseName) {
        this.config.setDatabase(databaseName);
        this.close();
        this.initConn();
    }

    @Override
    public long insert(String tableName, Map<String, Object> data) {
        tableName = this.transformTableName(tableName);
        String sql = this.mapToSql(tableName, data);
        PreparedStatement stat = null;
        try {
            stat = this.conn.prepareStatement(sql);
            return stat.executeUpdate();
        } catch (SQLException e) {
            throw new GISSparkException("JDBCHelper insert data failed：" + e.getMessage());
        } finally {
            try {
                if (stat != null && (!stat.isClosed())) stat.close();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new GISSparkException("JDBCHelper close preparedstatement failed：" + e.getMessage());
            }
        }
    }

    @Override
    public long insert(String tableName, List<Map<String, Object>> data) {
        tableName = this.transformTableName(tableName);
        PreparedStatement pstm = null;
        try {
            conn.setAutoCommit(false);
            for (int i = 1; i <= data.size(); i++) {
                String sql = mapToSql(tableName, data.get(i));
                pstm = this.conn.prepareStatement(sql);
                pstm.addBatch();
            }
            pstm.executeBatch();
            conn.commit();
            return data.size();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new GISSparkException("JDBCHelper insert batch data failed：" + e.getMessage());
        } finally {
            try {
                if (pstm != null && (pstm.isClosed())) {
                    pstm.close();
                }
                conn.setAutoCommit(true);
            } catch (SQLException e) {
                throw new GISSparkException("JDBCHelper close preparedstatement failed：" + e.getMessage());
            }
        }
    }

    @Override
    public long getSize(String tableName) {
        tableName = this.transformTableName(tableName);
        String sql = String.format("select count(*) as total from %s", tableName);
        try {
            PreparedStatement stat = this.conn.prepareStatement(sql);
            ResultSet rs = stat.executeQuery();
            rs.next();
            long result = rs.getLong(1);
            rs.close();
            stat.close();
            return result;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new GISSparkException("JDBCHelper get table size failed：" + e.getMessage());
        }
    }

    @Override
    public void initReader(String tableName) {
        tableName = this.transformTableName(tableName);
        String sql = String.format("select * from %s", tableName);
        try {
            this.statement = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            this.statement.setFetchSize(Integer.MIN_VALUE); // streaming read
            this.resultSet = this.statement.executeQuery();
        } catch (SQLException e) {
            throw new GISSparkException("JDBCHelper init table reader failed");
        }
    }

    private void initConn() {
        try {
            this.conn = DriverManager.getConnection(this.initUrl(), this.config.getUsername(), this.config.getPassword());
        } catch (SQLException e) {
            log.error(e.getMessage());
            throw new RuntimeException("JDBCHelper init connection failed");
        }
    }

    protected String initUrl() {
        StringBuilder sb = new StringBuilder(this.dbUrl());
        if (sb.lastIndexOf("?") == -1) {
            sb.append("?");
        } else {
            sb.append("&");
        }
        sb.append(this.initConnConfig());
        return sb.toString();
    }

    protected abstract String dbUrl();

    /**
     * 覆盖一些全局的数据库连接参数
     *
     * @return
     */
    protected String initConnConfig() {
        StringBuilder sb = new StringBuilder();
        sb.append("rewriteBatchedStatements=true");
        return sb.toString();
    }

    /**
     * 返回体转为json字符串
     *
     * @return
     */
    @Override
    public String next() {
        return this.mapToObject(this.resultSet, String.class);
    }

    /**
     * 返回体转为对应指定类
     *
     * @return
     */
    public <T> T next(Class<T> clz) {
        try {
            if (this.resultSet.next()) {
                return this.mapToObject(resultSet, clz);
            } else {
                log.warn("the reader has come to the end of the table");
                return null;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new GISSparkException("JDBCHelper read next element failed: " + e.getMessage());
        }
    }

    @Override
    public void closeReader() {
        try {
            if (resultSet != null && (!resultSet.isClosed())) {
                resultSet.close();
            }
            if (statement != null && (!statement.isClosed())) {
                statement.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new GISSparkException("JDBCHelper close reader failed: " + e.getMessage());
        }
    }

    @Override
    public void close() {
        this.closeReader();
        this.closeConn();
    }

    /**
     * @param sql
     * @param callBack
     * @param <T>
     */
    @Override
    public <T> void runSQL(String sql, SQLResultHandler<T> callBack) {
        log.info("SQL: " + sql);
        try {
            PreparedStatement ps = this.conn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            List<T> r = new ArrayList<>();
            while (rs.next()) {
                r.add(this.mapToObject(rs, (Class<T>) ClassUtil.getTClass(callBack.getClass(), 0)));
            }
            callBack.handle(r);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new GISSparkException("JDBCHelper execute sql failed: " + sql);
        }
    }

    @Override
    public boolean runSQL(String sql, Object... params) {
        log.info("SQL: " + sql);
        PreparedStatement ps = null;
        try {
            ps = this.conn.prepareStatement(sql);
            for (int i = 1; i < params.length + 1; i++) {
                ps.setObject(i, params[i - 1]);
            }
            return ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new GISSparkException("JDBCHelper execute sql failed: " + sql);
        }
    }

    @Override
    public boolean runSQL(String sql) {
        log.info("SQL: " + sql);
        PreparedStatement ps = null;
        try {
            ps = this.conn.prepareStatement(sql);
            return ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new GISSparkException("JDBCHelper execute sql failed: " + sql);
        }
    }

    private void closeConn() {
        try {
            if (this.conn != null && (!this.conn.isClosed()))
                this.conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new GISSparkException("JDBCHelper sql exception: " + e.getMessage());
        }
    }

    private <T> T mapToObject(ResultSet set, Class<T> clz) {
        if (clz.equals(String.class)) return (T) this.mapToJson(set).toString();
        return Term.GSON_CONTEXT.fromJson(this.mapToJson(set).toString(), clz);
    }

    private JSONObject mapToJson(ResultSet rs) {
        JSONObject jsonObj = new JSONObject();
        try {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnLabel(i);
                String value = rs.getString(columnName);
                jsonObj.put(columnName, value);
            }
            return jsonObj;
        } catch (SQLException e) {
            throw new GISSparkException("JDBCHelper map result element to json string failed: " + e.getMessage());
        }
    }

    private String mapToSql(String tableName, Map<String, Object> data) {
        tableName = this.transformTableName(tableName);
        String[] keys = data.keySet().toArray(new String[]{});
        StringBuilder sql = new StringBuilder("insert into " + tableName
                + "(");
        for (String key : keys) {
            sql.append(key).append(",");
        }
        sql.deleteCharAt(sql.length() - 1);
        sql.append(") values (");
        for (String key : keys) {
            Object o = data.get(key);
            if (o instanceof String || o instanceof Character) {
                sql.append("\'").append(String.valueOf(o)).append("\',");
            } else {
                sql.append(String.valueOf(o)).append(",");
            }
        }
        sql.deleteCharAt(sql.length() - 1);
        sql.append(");");

        log.debug("Insert SQL: " + sql.toString());
        return sql.toString();
    }

    public abstract String transformTableName(String tableName);

}
