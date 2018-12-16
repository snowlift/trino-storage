package org.ebyhr.presto.flex.operator;

import org.ebyhr.presto.flex.FlexColumn;

import java.util.List;

public interface FilePlugin {
    List<FlexColumn> getFields(String schema, String table);

    List<String> splitToList(String line);

    boolean skipFirstLine();
}
