package assignment;

import org.apache.beam.sdk.options.PipelineOptions;

public interface MyOptions extends PipelineOptions {

    void setCSVFilePath(String filePath);

    String getCSVFilePath();

    void setschemaPath(String schemaPath);

    String getschemaPath();

    void setdbUrl(String db_url);

    String getdbUrl();

    void setdbUserName(String db_user);

    String getdbUserName();

    void setdbPassword(String db_password);

    String getdbPassword();

    void settableName(String table_name);

    String gettableName();

    // @Default.Class(DirectRunner.class)
    // Class<? extends PipelineRunner<?>> getRunner();

    // @Default.Class(DirectRunner.class)
    // void setRunner(Class<DirectRunner> class1);

}
