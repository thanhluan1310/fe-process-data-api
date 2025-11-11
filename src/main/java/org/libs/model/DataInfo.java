package org.libs.model;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import static org.libs.contanst.AppCont.FILE_PATH_DEFAULT;

@Component
@Data
public class DataInfo {
    private String caseTableName;
    private String filePath = FILE_PATH_DEFAULT;
    private String idField;
    private String dateField;
    private String dateFormat;
    private boolean useRecursive;
}
