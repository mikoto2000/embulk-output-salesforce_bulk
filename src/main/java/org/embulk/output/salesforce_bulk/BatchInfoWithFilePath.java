package org.embulk.output.salesforce_bulk;

import com.sforce.async.BatchInfo;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class BatchInfoWithFilePath {
    private String filePath;
    private BatchInfo batchInfo;
}

