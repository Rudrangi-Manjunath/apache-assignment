package assignment;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

public class CSVProcessor {
    private final String filePath;
    private final CSVFormat csvFormat;

    public CSVProcessor(String filePath, CSVFormat csvFormat) {
        this.filePath = filePath;
        this.csvFormat = csvFormat;
    }

    public List<CSVRecord> parse() throws IOException {
        List<CSVRecord> records = new ArrayList<>();
        try (Reader reader = new FileReader(filePath)) {
            Iterable<CSVRecord> csvRecords = csvFormat.parse(reader);
            for (CSVRecord csvRecord : csvRecords) {
                System.out.println(csvRecord);
                records.add(csvRecord);
            }
        }
        return records;
    }

}
