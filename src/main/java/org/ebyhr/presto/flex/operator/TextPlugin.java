package org.ebyhr.presto.flex.operator;

import com.google.common.collect.ImmutableList;
import org.ebyhr.presto.flex.FlexColumn;

import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class TextPlugin implements FilePlugin {
    @Override
    public List<FlexColumn> getFields(String schema, String table)
    {
        return ImmutableList.of(new FlexColumn("value", VARCHAR));
    }

    @Override
    public List<String> splitToList(String line)
    {
        return Arrays.asList(line);
    }

    @Override
    public boolean skipFirstLine()
    {
        return false;
    }
}
