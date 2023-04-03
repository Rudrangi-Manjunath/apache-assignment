package assignment;

import java.io.IOException;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

public class Practise {

    static String schemaString = "{\n" +
            "  \"type\" : \"record\",\n" +
            "  \"namespace\" : \"Podium\",\n" +
            "  \"name\" : \"CardAccessConnector\",\n" +
            "  \"fields\" : [\n" +
            "    { \"name\" : \"building_code\" , \"type\" : \"string\" },\n" +
            "    { \"name\" : \"equipment_code\" , \"type\" : \"string\" },\n" +
            "    { \"name\" : \"datetime\" , \"type\" : \"string\" },\n" +
            "    { \"name\" : \"timezone\" , \"type\" : \"string\" },\n" +
            "    { \"name\" : \"event_type\" , \"type\" : \"string\" },\n" +
            "    { \"name\" : \"direction\" , \"type\" : [\"null\",\"string\"], \"default\": \"null\" },\n" +
            "    { \"name\" : \"card_id\" , \"type\" : \"string\" },\n" +
            "    { \"name\" : \"person_type\" , \"type\" : [\"null\",\"string\"], \"default\": \"null\" },\n" +
            "    { \"name\" : \"team\" , \"type\" : [\"null\",\"string\"], \"default\": \"null\" },\n" +
            "    { \"name\" : \"event_id\" , \"type\" : [\"null\",\"string\"], \"default\": \"null\" },\n" +
            "    { \"name\" : \"pdm_job_id\" , \"type\" : [\"null\",\"string\"] , \"default\": \"null\" },\n" +
            "    { \"name\" : \"filename\" , \"type\" : [\"null\",\"string\"] , \"default\": \"null\" },\n" +
            "    { \"name\" : \"business_unit_code\" , \"type\" : [\"null\",\"string\"], \"default\": \"null\" },\n" +
            "    { \"name\" : \"allocated_space_code\" , \"type\" : [\"null\",\"string\"] , \"default\": \"null\" },\n"
            +
            "    { \"name\" : \"career_level_code\" , \"type\" : [\"null\",\"string\"] , \"default\": \"null\" }\n" +
            "  ]\n" +
            "}";

    private static boolean isValidRecord(GenericRecord record) {
        if (record.get("building_code") == null || record.get("building_code").toString().isEmpty()) {
            return false;
        } else if (record.get("equipment_code") == null || record.get("equipment_code").toString().isEmpty()) {
            return false;
        } else if (record.get("datetime") == null || record.get("datetime").toString().isEmpty()) {
            return false;
        } else if (record.get("timezone") == null || record.get("timezone").toString().isEmpty()) {
            return false;
        } else if (record.get("event_type") == null || record.get("event_type").toString().isEmpty()) {
            return false;
        } else if (record.get("card_id") == null || record.get("card_id").toString().isEmpty()) {
            return false;
        } else {
            return true;
        }

    }

    public static void main(String[] args) throws IOException {
        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        // options.setRunner(DirectRunner.class);

        Pipeline pipeline = Pipeline.create(options);
        Schema schema = new Schema.Parser().parse(schemaString);

        PCollection<String> lines = pipeline.apply("ReadLines", TextIO.read().from(options.getCSVFilePath()));

        PCollection<CSVRecord> records = lines.apply(ParDo.of(new DoFn<String, CSVRecord>() {
            @ProcessElement
            public void processElement(ProcessContext c) throws IOException {
                String line = c.element();
                CSVFormat format = CSVFormat.DEFAULT.withHeader().withDelimiter(',');
                CSVRecord record = CSVProcessor.process(line, format);
                c.output(record);
            }
        }));

        List<GenericRecord> genericRecords = new ArrayList<>();
        records.apply(ParDo.of(new DoFn<CSVRecord, GenericRecord>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                CSVRecord record = c.element();
                GenericRecord genericRecord = new GenericData.Record(schema);
                genericRecord.put("building_code", record.get("building_code"));
                genericRecord.put("equipment_code", record.get("equipment_code"));
                genericRecord.put("datetime", record.get("datetime"));
                genericRecord.put("timezone", record.get("timezone"));
                genericRecord.put("event_type", record.get("event_type"));
                genericRecord.put("direction", record.get("direction"));
                genericRecord.put("card_id", record.get("card_id"));
                genericRecord.put("person_type", record.get("person_type"));
                genericRecord.put("team", record.get("team"));
                genericRecord.put("event_id", record.get("event_id"));
                genericRecord.put("pdm_job_id", record.get("pdm_job_id"));
                genericRecord.put("filename", record.get("filename"));
                genericRecord.put("business_unit_code", record.get("business_unit_code"));
                genericRecord.put("allocated_space_code", record.get("allocated_space_code"));
                genericRecord.put("career_level_code", record.get("career_level_code"));
                c.output(genericRecord);
            }
        }));

        TupleTag<GenericRecord> goodRecords = new TupleTag<GenericRecord>() {
        };
        TupleTag<GenericRecord> badRecords = new TupleTag<GenericRecord>() {
        };

        PCollection<GenericRecord> recordPCollection = pipeline.apply(Create.of(genericRecords));

        PCollectionTuple categorizedRecords = recordPCollection
                .apply(ParDo.of(new DoFn<GenericRecord, GenericRecord>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        GenericRecord record = c.element();
                        if (isValidRecord(record)) {
                            c.output(record);
                        } else {
                            c.output(badRecords, record);
                        }
                    }
                }).withOutputTags(goodRecords, TupleTagList.of(badRecords)));

        PCollection<GenericRecord> goodCollection = categorizedRecords.get(goodRecords);
        goodCollection.apply(JdbcIO.<GenericRecord>write()
                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                        .create("com.postgresql.jdbc.Driver", options.getDB_url())
                        .withUsername(options.getDB_user())
                        .withPassword(options.getDB_password()))
                .withStatement(
                        "INSERT INTO" + options.gettable_name()
                                + "(building_code, equipment_code, datetime, timezone, event_type, direction, card_id, person_type, team, event_id, pdm_job_id, filename, business_unit_code, allocated_space_code, career_level) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
                .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<GenericRecord>() {
                    @Override
                    public void setParameters(GenericRecord record, PreparedStatement statement) throws Exception {
                        statement.setString(1, record.get("building_code").toString());
                        statement.setString(2, record.get("equipment_code").toString());
                        statement.setString(3, record.get("datetime").toString());
                        statement.setString(4, record.get("timezone").toString());
                        statement.setString(5, record.get("event_type").toString());
                        statement.setString(6, record.get("direction").toString());
                        statement.setString(7, record.get("card_id").toString());
                        statement.setString(8, record.get("person_type").toString());
                        statement.setString(9, record.get("team").toString());
                        statement.setString(10, record.get("event_id").toString());
                        statement.setString(11, record.get("pdm_job_id").toString());
                        statement.setString(12, record.get("filename").toString());
                        statement.setString(13, record.get("business_unit_code").toString());
                        statement.setString(14, record.get("allocated_space_code").toString());
                        statement.setString(15, record.get("career_level_code").toString());
                    }
                }));

        PCollection<Long> good_records_count = goodCollection.apply(Count.globally());
        PCollection<GenericRecord> badCollection = categorizedRecords.get(badRecords);
        PCollection<Long> bad_records_count = badCollection.apply(Count.globally());

        pipeline.run();
    }
}
