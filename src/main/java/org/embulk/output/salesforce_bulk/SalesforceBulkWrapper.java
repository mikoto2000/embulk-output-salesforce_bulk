package org.embulk.output.salesforce_bulk;

import com.sforce.async.AsyncApiException;
import com.sforce.async.AsyncExceptionCode;
import com.sforce.async.BatchInfo;
import com.sforce.async.BatchStateEnum;
import com.sforce.async.BulkConnection;
import com.sforce.async.CSVReader;
import com.sforce.async.ContentType;
import com.sforce.async.JobInfo;
import com.sforce.async.JobStateEnum;
import com.sforce.async.OperationEnum;
import com.sforce.async.QueryResultList;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SalesforceBulkWrapper.
 *
 * -- example:
 * <pre>
 * {@code
 * SalesforceBulkWrapper sfbw = new SalesforceBulkWrapper(
 *         USER_NAME,
 *         PASSWORD,
 *         AUTH_ENDPOINT_URL,
 *         IS_COMPRESSION,
 *         POLLING_INTERVAL_MILLISECOND);
 * List<BatchInfoWithFilePath> bis = sfbw.syncUpsert(
 *         OBJECT_TYPE
 *         , EXTERNAL_ID
 *         , BULK_CSV_INPUT_STREAM);
 * sfbw.close();
 * }
 * </pre>
 */
public class SalesforceBulkWrapper implements AutoCloseable {

    // コネクション
    private PartnerConnection partnerConnection;
    private BulkConnection bulkConnection;

    // Bulk 接続設定
    private int pollingIntervalMillisecond;

    private static final int MAX_BYTES_PER_BATCH = 10000000; // 10 million bytes per batch
    private static final int MAX_ROWS_PER_BATCH = 10000; // 10 thousand rows per batch

    private static final String API_VERSION = "34.0";
    private static final String AUTH_ENDPOINT_URL_DEFAULT =
            "https://login.salesforce.com/services/Soap/u/" + API_VERSION;

    private static final boolean IS_COMPRESSION_DEFAULT = true;
    private static final int POLLING_INTERVAL_MILLISECOND_DEFAULT = 30000;

    /**
     * Constructor
     */
    public SalesforceBulkWrapper(String userName, String password)
            throws AsyncApiException, ConnectionException {
        this(userName,
                password,
                AUTH_ENDPOINT_URL_DEFAULT,
                IS_COMPRESSION_DEFAULT,
                POLLING_INTERVAL_MILLISECOND_DEFAULT);
    }

    /**
     * Constructor
     */
    public SalesforceBulkWrapper(
            String userName,
            String password,
            String authEndpointUrl,
            boolean isCompression,
            int pollingIntervalMillisecond)
            throws AsyncApiException, ConnectionException {

        partnerConnection = createPartnerConnection(
                authEndpointUrl,
                userName,
                password,
                isCompression);
        bulkConnection = createBulkConnection(partnerConnection.getConfig());

        this.pollingIntervalMillisecond = pollingIntervalMillisecond;
    }

    public void syncUpsert(
            String objectType,
            String externalId,
            String dataPath)
            throws InterruptedException, AsyncApiException, IOException {

        InputStream data = new FileInputStream(dataPath);

        syncUpsert(objectType, externalId, data);
    }

    public List<BatchInfoWithFilePath> syncUpsert(
            String objectType,
            String externalId,
            InputStream dataStream)
            throws InterruptedException, AsyncApiException, IOException {

        // ジョブ作成
        JobInfo jobInfo = new JobInfo();
        jobInfo.setObject(objectType);
        jobInfo.setOperation(OperationEnum.upsert);
        jobInfo.setExternalIdFieldName(externalId);
        jobInfo.setContentType(ContentType.CSV);
        jobInfo = bulkConnection.createJob(jobInfo);

        // バッチ作成
        List<BatchInfoWithFilePath> batchInfos = createBatches(bulkConnection, jobInfo, dataStream);

        // ジョブクローズ
        JobInfo closeJob = new JobInfo();
        closeJob.setId(jobInfo.getId());
        closeJob.setState(JobStateEnum.Closed);
        bulkConnection.updateJob(closeJob);

        // 実行状況取得
        batchInfos = waitBatchs(batchInfos);

        return batchInfos;
    }

    private List<BatchInfoWithFilePath> createBatches(BulkConnection connection,
            JobInfo jobInfo, InputStream inputStream)
        throws IOException, AsyncApiException {
        List<BatchInfoWithFilePath> batchInfos = new ArrayList<>();
        BufferedReader rdr = new BufferedReader(
                new InputStreamReader(inputStream));

        // read the CSV header row
        byte[] headerBytes = (rdr.readLine() + "\n").getBytes("UTF-8");
        int headerBytesLength = headerBytes.length;
        File tmpFile = File.createTempFile("temp", ".csv");

        // Split the CSV file into multiple batches
        FileOutputStream tmpOut = new FileOutputStream(tmpFile);
        int currentBytes = 0;
        int currentLines = 0;
        String nextLine;
        while ((nextLine = rdr.readLine()) != null) {
            byte[] bytes = (nextLine + "\n").getBytes("UTF-8");
            // Create a new batch when our batch size limit is reached
            if (currentBytes + bytes.length > MAX_BYTES_PER_BATCH
                    || currentLines > MAX_ROWS_PER_BATCH) {
                tmpOut.flush();
                tmpOut.close();
                try (FileInputStream fis = new FileInputStream(tmpFile)) {
                    BatchInfo batchInfo =
                        bulkConnection.createBatchFromStream(jobInfo, fis);
                    batchInfos.add(new BatchInfoWithFilePath(tmpFile.getAbsolutePath(), batchInfo));
                }
                currentBytes = 0;
                currentLines = 0;
            }
            if (currentBytes == 0) {
                tmpFile = File.createTempFile("temp", ".csv");
                tmpOut = new FileOutputStream(tmpFile);
                tmpOut.write(headerBytes);
                currentBytes = headerBytesLength;
                currentLines = 1;
            }
            tmpOut.write(bytes);
            currentBytes += bytes.length;
            currentLines++;
        }
        // Finished processing all rows
        // Create a final batch for any remaining data
        if (currentLines > 1) {
            tmpOut.flush();
            tmpOut.close();
            try (FileInputStream fis = new FileInputStream(tmpFile)) {
                BatchInfo batchInfo =
                    bulkConnection.createBatchFromStream(jobInfo, fis);
                batchInfos.add(new BatchInfoWithFilePath(tmpFile.getAbsolutePath(), batchInfo));
            }
        }

        return batchInfos;
    }

    public List<List<BatchResult>> checkBatchResults(List<BatchInfo> batchInfos)
            throws AsyncApiException, IOException {
        List<List<BatchResult>> batchResultsList = new ArrayList<>();
        for (BatchInfo batchInfo : batchInfos) {
            List<BatchResult> batchResults = checkBatchResult(batchInfo);
            batchResultsList.add(batchResults);
        }
        return batchResultsList;
    }

    public List<BatchResult> checkBatchResult(BatchInfo batchInfo)
            throws AsyncApiException, IOException {
        CSVReader rdr = new CSVReader(bulkConnection.getBatchResultStream(
                batchInfo.getJobId(), batchInfo.getId()));
        List<String> resultHeader = rdr.nextRecord();
        int resultCols = resultHeader.size();

        int lineNumber = 1;
        List<String> row;
        List<BatchResult> results = new ArrayList<>();
        while ((row = rdr.nextRecord()) != null) {
            Map<String, String> resultInfo = new HashMap<String, String>();
            for (int i = 0; i < resultCols; i++) {
                resultInfo.put(resultHeader.get(i), row.get(i));
            }

            // Salesforce id 取得
            String id = resultInfo.get("Id");

            // 成否判定
            boolean success = Boolean.valueOf(resultInfo.get("Success"));

            // 挿入更新判定
            boolean created = Boolean.valueOf(resultInfo.get("Created"));

            // エラー内容を取得
            String error = resultInfo.get("Error");

            results.add(new BatchResult(++lineNumber, id, success, created, error));
        }

        return results;
    }

    public void close() throws ConnectionException {
        partnerConnection.logout();
    }

    private PartnerConnection createPartnerConnection(
            String endpointUrl,
            String userName,
            String password,
            boolean isCompression)
            throws ConnectionException {

        ConnectorConfig partnerConfig = new ConnectorConfig();
        partnerConfig.setUsername(userName);
        partnerConfig.setPassword(password);
        partnerConfig.setAuthEndpoint(endpointUrl);
        partnerConfig.setCompression(isCompression);

        return new PartnerConnection(partnerConfig);
    }

    private BulkConnection createBulkConnection(ConnectorConfig partnerConfig)
            throws AsyncApiException {

        ConnectorConfig config = new ConnectorConfig();
        config.setSessionId(partnerConfig.getSessionId());

        String soapEndpoint = partnerConfig.getServiceEndpoint();
        String restEndpoint = soapEndpoint.substring(
                0, soapEndpoint.indexOf("Soap/")) + "async/" + API_VERSION;
        config.setRestEndpoint(restEndpoint);
        config.setCompression(partnerConfig.isCompression());

        return new BulkConnection(config);
    }

    private List<BatchInfoWithFilePath> waitBatchs(List<BatchInfoWithFilePath> batchInfos)
            throws InterruptedException, AsyncApiException {
        List<BatchInfoWithFilePath> newBatchInfos = new ArrayList<>();
        for (BatchInfoWithFilePath batchInfo : batchInfos) {
            BatchInfo newBatchInfo = waitBatch(batchInfo.getBatchInfo());
            newBatchInfos.add(new BatchInfoWithFilePath(
                        batchInfo.getFilePath(), newBatchInfo));
        }
        return newBatchInfos;
    }

    private BatchInfo waitBatch(BatchInfo batchInfo)
            throws InterruptedException, AsyncApiException {
        while(true) {
            batchInfo = bulkConnection.getBatchInfo(
                    batchInfo.getJobId(),
                    batchInfo.getId());
            BatchStateEnum state = batchInfo.getState();
            if (state == BatchStateEnum.Completed ||
                    state == BatchStateEnum.Failed ||
                    state == BatchStateEnum.NotProcessed) {
                return batchInfo;
            }
            Thread.sleep(pollingIntervalMillisecond);
        }
    }
}

