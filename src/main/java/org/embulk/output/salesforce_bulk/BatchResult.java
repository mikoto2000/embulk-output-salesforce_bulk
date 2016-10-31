package org.embulk.output.salesforce_bulk;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class BatchResult {
    private int lineNumber;
    private String id;
    private boolean isSuccess;
    private boolean isCreate;
    private String error;
}

