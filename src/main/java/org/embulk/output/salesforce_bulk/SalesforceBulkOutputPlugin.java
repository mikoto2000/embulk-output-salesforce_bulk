package org.embulk.output.salesforce_bulk;

import com.google.common.base.Optional;
import com.sforce.async.AsyncApiException;
import com.sforce.async.BatchInfo;
import com.sforce.async.BatchStateEnum;
import com.sforce.bulk.CsvWriter;
import com.sforce.ws.ConnectionException;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.InterruptedException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampParseException;
import org.embulk.spi.util.Timestamps;
import org.slf4j.Logger;

public class SalesforceBulkOutputPlugin
        implements OutputPlugin
{
    public interface PluginTask
            extends Task
    {
        // 認証用エンドポイントURL
        @Config("authEndpointUrl")
        @ConfigDefault("http://login.salesforce.com/")
        public String getAuthEndpointUrl();

        // ユーザー名
        @Config("userName")
        public String getUserName();

        // パスワード
        @Config("password")
        public String getPassword();

        // オブジェクトタイプ
        @Config("objectType")
        public String getObjectType();

        // 外部Id
        @Config("externalId")
        public String getExternalId();

        // 作業ディレクトリ
        @Config("workingDirectory")
        public String getWorkingDirectory();

        // 圧縮設定
        @Config("isCompression")
        @ConfigDefault("true")
        public Boolean getCompression();

        // ポーリング間隔(ミリ秒)
        @Config("pollingIntervalMillisecond")
        @ConfigDefault("30000")
        public int getPollingIntervalMillisecond();

        // バッファアロケーターの実装を定義
        @ConfigInject
        public BufferAllocator getBufferAllocator();
    }

    private Logger log = Exec.getLogger(SalesforceBulkOutputPlugin.class);
    private Map<Integer, List<String>> tempFilePaths = Collections.synchronizedMap(new HashMap<>());

    // TODO: static でなくしたい。
    private static SalesforceBulkWrapper sfbw;

    @Override
    public ConfigDiff transaction(ConfigSource config,
            Schema schema, int taskCount,
            OutputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        return resume(task.dump(), schema, taskCount, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            Schema schema, int taskCount,
            OutputPlugin.Control control)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        try {
            sfbw = new SalesforceBulkWrapper(
                    task.getUserName(),
                    task.getPassword(),
                    task.getAuthEndpointUrl(),
                    task.getCompression(),
                    task.getPollingIntervalMillisecond());
        } catch (ConnectionException|AsyncApiException e) {
            log.error("sfbw connection error: {}", e);
            return Exec.newConfigDiff();
        }

        List<TaskReport> taskReportList =
                control.run(task.dump());

        ConfigDiff configDiff = Exec.newConfigDiff();
        return configDiff;
    }

    @Override
    public void cleanup(TaskSource taskSource,
            Schema schema, int taskCount,
            List<TaskReport> successTaskReports)
    {
        try {
            sfbw.close();
        } catch (ConnectionException e) {
            log.error("sfbw close faild: {}.", e);
        }
    }

    @Override
    public TransactionalPageOutput open(
            TaskSource taskSource,
            Schema schema, int taskIndex)
    {
        tempFilePaths.put(taskIndex, new ArrayList<>());
        PluginTask task = taskSource.loadTask(PluginTask.class);
        final PageReader reader = new PageReader(schema);

        return new TransactionalPageOutput() {
            Path workingDirectory = Paths.get(task.getWorkingDirectory());

            @Override
            public void add(Page page) {
                reader.setPage(page);

                List<Column> columns = schema.getColumns();

                try {
                    Path tempFile = Files.createTempFile(workingDirectory, "task_" + taskIndex + "_data_", ".csv");
                    try (BufferedWriter bw =
                            new BufferedWriter(
                            new OutputStreamWriter(
                            Files.newOutputStream(tempFile)))) {

                        while (reader.nextRecord()) {
                            // スキーマを基に要求を組み立てる
                            // スキーマ解析
                            ColumnVisitorImpl visitor = new ColumnVisitorImpl(reader);
                            schema.visitColumns(visitor);

                            // 列数取得
                            int columnCount = schema.getColumnCount();
                            // 列名配列作成
                            String[] columnNames = new String[columnCount];
                            for (int i = 0; i < columnCount; i++) {
                                columnNames[i] = schema.getColumnName(i);
                            }
                            // 列をファイルに書き出す
                            bw.write(createCsvRecord(visitor.getValues(columnNames)) + "\n");
                        }
                        bw.flush();
                    }
                    Path absolutePath = tempFile.toAbsolutePath();
                    tempFilePaths.get(taskIndex).add(absolutePath.toString());
                } catch (IOException e) {
                    log.error("add error: {}.", e);
                    return;
                }
            }

            @Override
            public void finish() {
                // スキーマを基にヘッダーを組み立てる
                try {
                    Path tempFile = Files.createTempFile(workingDirectory, "task_" + taskIndex + "_header_", ".csv");
                    // 列数取得
                    int columnCount = schema.getColumnCount();
                    // 列名配列作成
                    String[] columnNames = new String[columnCount];
                    for (int i = 0; i < columnCount; i++) {
                        columnNames[i] = schema.getColumnName(i);
                    }
                    try (BufferedWriter bw = Files.newBufferedWriter(tempFile, StandardOpenOption.WRITE)) {
                        bw.write(createCsvRecord(columnNames) + "\n");
                        bw.flush();
                    }
                    tempFilePaths.get(taskIndex).add(0, tempFile.toAbsolutePath().toString());
                } catch(IOException e) {
                    log.error("{}", e);
                    return;
                }
                try (MultiFileInputStream mfis
                        = new MultiFileInputStream(tempFilePaths.get(taskIndex))) {
                    List<BatchInfoWithFilePath> bis = sfbw.syncUpsert(
                            task.getObjectType()
                            , task.getExternalId()
                            , mfis);

                    // エラーフラグ
                    boolean isError = false;

                    for (BatchInfoWithFilePath biwfp : bis) {
                        BatchInfo bi = biwfp.getBatchInfo();
                        if (bi.getState() == BatchStateEnum.NotProcessed) {
                            // バッチが実行されていないのであればその旨を表示。
                            log.error("Batch not processed, job id:{}, batch id:{}, state message:{}.", bi.getJobId(), bi.getId(), bi.getStateMessage());
                            isError = true;
                        } else {
                            // バッチが実行されているならば、実行結果を確認する。
                            List<BatchResult> batchResults = sfbw.checkBatchResult(bi);
                            for (BatchResult result : batchResults) {
                                Path path = Paths.get(biwfp.getFilePath());
                                BufferedReader br = Files.newBufferedReader(path);
                                int oldLineNumber = 0;
                                if (!result.isSuccess()) {
                                    int lineNumber = result.getLineNumber();

                                    String lineStr = "";
                                    for (int i = 0; i < lineNumber - oldLineNumber; i++) {
                                        lineStr = br.readLine();
                                    }
                                    log.error("error line number: {}", result.getLineNumber());
                                    log.error("error line : {}", lineStr);
                                    log.error("error message : {}", result.getError());
                                    log.error("error wsc temp file : {}", biwfp.getFilePath());
                                    log.error("error salesforce_bulk plugin temp file : {}",
                                            tempFilePaths.get(taskIndex));
                                    isError = true;
                                }
                            }
                        }
                    }

                    // エラーがなければファイルを削除する
                    if (!isError) {
                        List<String> tfp = tempFilePaths.get(taskIndex);
                        for (String pathStr : tfp) {
                            Files.delete(Paths.get(pathStr));
                        }
                    }
                } catch (
                        AsyncApiException
                        |InterruptedException
                        |InvalidPathException
                        |IOException e) {
                    log.error("{}", e);
                }

                System.out.flush();
            }

            @Override
            public void close() {
                reader.close();
            }

            @Override
            public void abort() {
            }

            @Override
            public TaskReport commit() {
                return Exec.newTaskReport();
            }
        };
    }

    private String createCsvRecord(String[] values) {
        // CSV エスケープしたうえで、カンマ区切り結合する。
        return Arrays.stream(values).map(
                x->{return x.replaceAll("\"", "\"\"");})
                .collect(Collectors.joining("\",\"", "\"", "\""));
    }

    class ColumnVisitorImpl implements ColumnVisitor {
        private final PageReader reader;
        private final Map<String, String> values;

        ColumnVisitorImpl(PageReader reader) {
            this.reader = reader;
            values = new HashMap<>();
        }

        public String getValue(String name) {
            return values.get(name);
        }

        public String[] getValues(String... names) {
            ArrayList<String> values = new ArrayList<>();
            for (String name : names) {
                values.add(getValue(name));
            }
            return values.toArray(new String[values.size()]);
        }

        @Override
        public void booleanColumn(Column column) {
            Boolean value = reader.getBoolean(column);
            if (value == null) {
                values.put(column.getName(), null);
            } else {
                values.put(column.getName(), value.toString());
            }
        }

        @Override
        public void longColumn(Column column) {
            Long value = reader.getLong(column);
            if (value == null) {
                values.put(column.getName(), null);
            } else {
                values.put(column.getName(), value.toString());
            }
        }

        @Override
        public void doubleColumn(Column column) {
            Double value = reader.getDouble(column);
            if (value == null) {
                values.put(column.getName(), null);
            } else {
                values.put(column.getName(), value.toString());
            }
        }

        @Override
        public void stringColumn(Column column) {
            String value = reader.getString(column);
            if (value == null) {
                values.put(column.getName(), null);
            } else {
                values.put(column.getName(), value);
            }
        }

        @Override

    public void jsonColumn(Column column) {

        throw new UnsupportedOperationException("This plugin doesn't support json type. Please try to upgrade version of the plugin using 'embulk gem update' command. If the latest version still doesn't support json type, please contact plugin developers, or change configuration of input plugin not to use json type.");

    }


    @Override
        public void timestampColumn(Column column) {
            Timestamp value = reader.getTimestamp(column);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
            sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
            String time = sdf.format(new Date(value.toEpochMilli()));
            values.put(column.getName(), time);
        }
    }
}
