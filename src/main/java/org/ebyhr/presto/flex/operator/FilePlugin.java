package org.ebyhr.presto.flex.operator;

import com.google.common.io.ByteSource;
import org.ebyhr.presto.flex.FlexColumn;

import java.util.Iterator;
import java.util.List;

public interface FilePlugin {
    List<FlexColumn> getFields(String schema, String table);

    List<String> splitToList(Iterator lines);

    Iterator getIterator(ByteSource byteSource);

    boolean skipFirstLine();
}
