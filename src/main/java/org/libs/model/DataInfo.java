package org.libs.model;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@Data
public class DataInfo {
    private String caseTableName;
    private String filePath;
    private String idField;
    private String dateField;
    private String dateFormat;
    private boolean useRecursive;
}
