package org.ebyhr.presto.flex.operator;

import java.util.List;

public interface FilePlugin {
    List<String> splitToList(String line);

    boolean skipFirstLine();
}
