package assignment;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class CSVProcessor {

    public static CSVRecord process(String input, CSVFormat csvFormat) {
        CSVRecord record = null;
        try {
            CSVParser csvParser = CSVParser.parse(input, csvFormat);
            record = csvParser.getRecords().get(0);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return record;
    }
}
