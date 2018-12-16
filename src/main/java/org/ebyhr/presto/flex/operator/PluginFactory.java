package org.ebyhr.presto.flex.operator;

import org.ebyhr.presto.flex.FileType;

import static org.ebyhr.presto.flex.FileType.CSV;
import static org.ebyhr.presto.flex.FileType.TSV;
import static org.ebyhr.presto.flex.FileType.TXT;

public class PluginFactory {
    public static FilePlugin create(String typeName)
    {
        FileType fileType = FileType.valueOf(typeName.toUpperCase());

        if (fileType == CSV) {
            return new CsvPlugin();
        } else if (fileType == TSV) {
            return new TsvPlugin();
        } else if (fileType == TXT) {
            return new TextPlugin();
        }
        throw new IllegalArgumentException("The file type is not supported " + typeName);
    }
}
