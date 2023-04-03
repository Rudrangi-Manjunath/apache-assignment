package assignment;

import org.apache.beam.sdk.options.PipelineOptions;

public interface MyOptions extends PipelineOptions {

    void setCSVFilePath(String filePath);

    String getCSVFilePath();

    void setSchemaPath(String schemaPath);

    String getSchemaPath();

    void setDB_url(String db_url);

    String getDB_url();

    void setDB_user(String db_user);

    String getDB_user();

    void setDB_password(String db_password);

    String getDB_password();

    void settable_name(String table_name);

    String gettable_name();

    // @Default.Class(DirectRunner.class)
    // Class<? extends PipelineRunner<?>> getRunner();

    // @Default.Class(DirectRunner.class)
    // void setRunner(Class<DirectRunner> class1);

}
