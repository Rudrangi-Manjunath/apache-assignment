package assignment;

import java.io.IOException;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

public class Practise {

    private static boolean isValidRecord(GenericRecord record) {
        if (record == null) {
            System.out.println("Record is null");
            return false;
        }

        String[] requiredFields = { "building_code", "equipment_code", "datetime", "timezone", "event_type",
                "card_id" };
        for (String field : requiredFields) {
            Object value = record.get(field);
            if (value.toString().isEmpty() || value.toString().equals("null")) {
                return false;
            }
        }
        return true;

    }

    public static void main(String[] args) throws IOException {
        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        Schema schema = new Schema.Parser().parse(Practise.class.getResourceAsStream("/Avroschema.avsc"));

        PCollection<String> lines = pipeline.apply("ReadLines", TextIO.read().from(options.getCSVFilePath()));

        String[] headers = { "building_code", "equipment_code", "datetime", "timezone", "event_type", "direction",
                "card_id", "person_type", "team", "event_id", "pdm_job_id", "filename", "business_unit_code",
                "allocated_space_code", "career_level_code" };

        CSVFormat csvFormat = CSVFormat.DEFAULT.builder().setHeader(headers).setDelimiter(',').build();

        PCollection<GenericRecord> records = lines.apply(ParDo.of(new DoFn<String, GenericRecord>() {
            @ProcessElement
            public void processElement(ProcessContext c) throws IOException {
                String line = c.element();
                CSVRecord record = CSVProcessor.process(line, csvFormat);
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
        })).setCoder(AvroCoder.of(schema));

        TupleTag<GenericRecord> goodRecords = new TupleTag<GenericRecord>() {
        };
        TupleTag<GenericRecord> badRecords = new TupleTag<GenericRecord>() {
        };

        PCollectionTuple categorizedRecords = records
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
                        .create("org.postgresql.Driver", options.getdbUrl())
                        .withUsername(options.getdbUserName())
                        .withPassword(options.getdbPassword()))
                .withStatement(
                        "INSERT INTO " + options.gettableName()
                                + "(building_code, equipment_code, datetime, timezone, event_type, direction, card_id, person_type, team, event_id, pdm_job_id, filename, business_unit_code, allocated_space_code, career_level_code) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
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

        PCollection<GenericRecord> badCollection = categorizedRecords.get(badRecords);
        PCollection<Long> good_records_count = goodCollection.apply(Count.globally());
        PCollection<Long> bad_records_count = badCollection.apply(Count.globally());

        PCollection<KV<String, Long>> good_records_count_kv = good_records_count
                .apply(ParDo.of(new DoFn<Long, KV<String, Long>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        c.output(KV.of("good_records_count", c.element()));
                    }
                }));

        PCollection<KV<String, Long>> bad_records_count_kv = bad_records_count
                .apply(ParDo.of(new DoFn<Long, KV<String, Long>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        c.output(KV.of("bad_records_count", c.element()));
                    }
                }));

        PCollectionView<Map<String, Long>> good_records_count_view = good_records_count_kv.apply(View.asMap());
        PCollectionView<Map<String, Long>> bad_records_count_view = bad_records_count_kv.apply(View.asMap());

        PCollection<String> result = pipeline.apply(Create.of("dummy")).apply(ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                Map<String, Long> good_records_count_map = c.sideInput(good_records_count_view);
                Map<String, Long> bad_records_count_map = c.sideInput(bad_records_count_view);
                String result = "good_records_count: " + good_records_count_map.get("good_records_count")
                        + ", bad_records_count: " + bad_records_count_map.get("bad_records_count");
                c.output(result);
            }
        }).withSideInputs(good_records_count_view, bad_records_count_view));

        result.apply(JdbcIO.<String>write()
                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                        .create("org.postgresql.Driver", options.getdbUrl())
                        .withUsername(options.getdbUserName())
                        .withPassword(options.getdbPassword()))
                .withStatement(
                        "INSERT INTO job(job_date,good_record_count,error_record_count) VALUES (?,?,?)")
                .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<String>() {
                    public void setParameters(String element, PreparedStatement query) throws SQLException {
                        String[] result = element.split(",");
                        long goodRecordCount = Long.parseLong(result[0].split(":")[1].trim());
                        long badRecordCount = Long.parseLong(result[1].split(":")[1].trim());
                        query.setDate(1, new java.sql.Date(System.currentTimeMillis()));
                        query.setLong(2, goodRecordCount);
                        query.setLong(3, badRecordCount);
                    }
                }));

        pipeline.run();
    }
}
