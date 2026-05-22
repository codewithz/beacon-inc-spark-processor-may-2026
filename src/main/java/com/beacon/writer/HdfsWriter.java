package com.beacon.writer;



import com.beacon.config.HdfsConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.URI;

/**
 * Writes raw files (PDFs, images, documents) to HDFS.
 *
 * This is the Object Storage path in the Corelate platform:
 *   S3 Bucket / local files → Adapter → HDFS /files/
 *
 * The HDFS path is stored in PropertyTransaction.titleDeedUri
 * so downstream systems know where to find the raw file.
 */
public class HdfsWriter {

    private static final Logger log = LoggerFactory.getLogger(HdfsWriter.class);

    private final FileSystem fs;

    public HdfsWriter() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", HdfsConfig.NAMENODE_URI);
        conf.set("dfs.client.use.datanode.hostname", "true");
        this.fs = FileSystem.get(new URI(HdfsConfig.NAMENODE_URI), conf);
        log.info("Connected to HDFS: {}", HdfsConfig.NAMENODE_URI);
    }

    /**
     * Copy a local file to HDFS.
     * Returns the full HDFS URI to store in titleDeedUri.
     *
     * Example:
     *   local:  C:/temp/deed.pdf
     *   HDFS:   hdfs://namenode:8020/files/pdfs/titles/PCL-MM-001/deed.pdf
     */
    public String copyToHdfs(String localPath, String parcelId, String fileName) throws Exception {
        Path src  = new Path(localPath);
        Path dest = new Path(HdfsConfig.PDF_PATH + "/" + parcelId + "/" + fileName);

        fs.mkdirs(dest.getParent());
        fs.copyFromLocalFile(false, true, src, dest);

        String uri = dest.toString();
        log.info("File copied to HDFS | parcel={} path={}", parcelId, uri);
        return uri;
    }

    /**
     * Write bytes from an InputStream directly to HDFS.
     * Used when files come from an HTTP upload or Kafka byte payload.
     */
    public String writeStreamToHdfs(InputStream data, String parcelId,
                                    String fileName) throws Exception {
        Path dest = new Path(HdfsConfig.PDF_PATH + "/" + parcelId + "/" + fileName);
        fs.mkdirs(dest.getParent());

        try (FSDataOutputStream out = fs.create(dest, true)) {
            data.transferTo(out);
        }

        String uri = dest.toString();
        log.info("Stream written to HDFS | parcel={} path={}", parcelId, uri);
        return uri;
    }

    /**
     * Check if a file already exists on HDFS.
     * Useful for idempotent file processing.
     */
    public boolean exists(String parcelId, String fileName) throws Exception {
        Path path = new Path(HdfsConfig.PDF_PATH + "/" + parcelId + "/" + fileName);
        return fs.exists(path);
    }

    /**
     * List all files for a given parcel.
     */
    public void listFiles(String parcelId) throws Exception {
        Path dir = new Path(HdfsConfig.PDF_PATH + "/" + parcelId);
        if (!fs.exists(dir)) {
            log.info("No files found for parcel: {}", parcelId);
            return;
        }
        log.info("Files for parcel {}:", parcelId);
        for (var status : fs.listStatus(dir)) {
            log.info("  {} ({} bytes)", status.getPath().getName(), status.getLen());
        }
    }

    public void close() throws Exception {
        if (fs != null) fs.close();
    }
}
