package com.example.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;

import java.io.IOException;

/**
 * A simple Parquet file reader that reads and displays the contents of a Parquet file.
 */
public class ParquetReader {

    /**
     * Reads and displays the contents of a Parquet file.
     *
     * @param filePath the path to the Parquet file
     * @throws IOException if an error occurs while reading the file
     */
    public static void readParquetFile(String filePath) throws IOException {
        Configuration conf = new Configuration();
        Path path = new Path(filePath);

        try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))) {
            MessageType schema = reader.getFooter().getFileMetaData().getSchema();
            
            System.out.println("=== Parquet File Schema ===");
            System.out.println(schema);
            System.out.println();
            
            System.out.println("=== Parquet File Contents ===");
            
            PageReadStore pages;
            long recordCount = 0;
            
            while ((pages = reader.readNextRowGroup()) != null) {
                long rows = pages.getRowCount();
                MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
                
                for (int i = 0; i < rows; i++) {
                    Group group = recordReader.read();
                    recordCount++;
                    System.out.println("Record " + recordCount + ": " + group);
                }
            }
            
            System.out.println();
            System.out.println("Total records read: " + recordCount);
        }
    }

    /**
     * Main method to run the Parquet reader.
     *
     * @param args command line arguments - expects the path to a Parquet file
     */
    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("Usage: java -jar parquet-read.jar <path-to-parquet-file>");
            System.err.println("Example: java -jar parquet-read.jar /path/to/file.parquet");
            System.exit(1);
        }

        String filePath = args[0];
        
        try {
            System.out.println("Reading Parquet file: " + filePath);
            System.out.println();
            readParquetFile(filePath);
        } catch (IOException e) {
            System.err.println("Error reading Parquet file: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
