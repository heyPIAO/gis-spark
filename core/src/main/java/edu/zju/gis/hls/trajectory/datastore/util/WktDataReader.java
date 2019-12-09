package edu.zju.gis.hls.trajectory.datastore.util;


import edu.zju.gis.hls.trajectory.datastore.base.Seperator;
import edu.zju.gis.hls.trajectory.datastore.exception.DataReaderException;
import edu.zju.gis.hls.trajectory.datastore.exception.DataReaderExceptionEnum;
import edu.zju.gis.hls.trajectory.datastore.exception.LoaderException;
import edu.zju.gis.hls.trajectory.datastore.exception.LoaderExceptionEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

import static edu.zju.gis.hls.trajectory.datastore.base.Seperator.TAB;

/**
 * @author Hu
 * @date 2019/6/19
 * Linux本地wkt文件读取
 **/
public class WktDataReader extends DataReader {

    private static final Logger logger = LoggerFactory.getLogger(WktDataReader.class);

    private BufferedReader reader;

    private Seperator seprator;

    private boolean hasHeader;

    public WktDataReader() {
        this(TAB, true);
    }

    public WktDataReader(Seperator seprator) {
        this(seprator, true);
    }

    public WktDataReader(Seperator seprator, boolean hasHeader) {
        super();
        this.seprator = seprator;
        this.hasHeader = hasHeader;
    }

    @Override
    protected void check() {
        super.check();
        if (this.seprator == null)
            throw new DataReaderException(DataReaderExceptionEnum.SYSTEM_READ_ERROR, "Wkt File seprator needs to be set");
    }

    @Override
    public void init() {
        super.init();
        this.initReader();
    }

    private void initReader() {
        File file = new File(this.filename);
        if (file.isDirectory())
            throw new LoaderException(LoaderExceptionEnum.SYSTEM_LOAD_ERROR, "Not support directory reader yet: " + this.filename);
        try {
            this.reader = new BufferedReader(new FileReader(file));
            if (hasHeader) this.headers = this.readHeader();
        } catch (FileNotFoundException e) {
            throw new LoaderException(LoaderExceptionEnum.SYSTEM_LOAD_ERROR, "File not exist: " + this.filename);
        }
    }

    public WktDataReader seprator(Seperator seprator) {
        this.seprator = seprator;
        return this;
    }

    public WktDataReader headers(String headers) {
        this.headers = headers.split(this.seprator.getValue());
        return this;
    }

    @Override
    public String next() {
        try {
            return reader.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    protected String[] readHeader() {
        if (this.hasHeader && this.headers == null) {
            try {
                String fl = this.reader.readLine();
                this.headers = fl.split(this.seprator.getValue());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return this.headers;
    }

    @Override
    public void close() throws IOException {
        if (reader != null) reader.close();
    }

}
