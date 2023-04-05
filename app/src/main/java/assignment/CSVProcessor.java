package assignment;

import java.io.Reader;
import java.io.StringReader;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class CSVProcessor {

    public static CSVRecord process(String input, CSVFormat csvFormat) {
        CSVRecord record = null;
        try {
            Reader stringReader = new StringReader(input);
            CSVParser parser = CSVParser.parse(stringReader, csvFormat);
            record = parser.getRecords().get(0);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return record;
    }
}
