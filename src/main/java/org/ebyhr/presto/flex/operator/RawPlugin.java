package org.ebyhr.presto.flex.operator;

import com.google.common.collect.ImmutableList;
import org.ebyhr.presto.flex.FlexColumn;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class RawPlugin implements FilePlugin {
    @Override
    public List<FlexColumn> getFields(String schema, String table)
    {
        return ImmutableList.of(new FlexColumn("data", VARCHAR));
    }

    @Override
    public List<String> splitToList(Iterator<String> lines)
    {
        Iterable<String> iterable = () -> lines;
        String line = StreamSupport.stream(iterable.spliterator(), false).map(Object::toString).collect(Collectors.joining("\n"));
        return Arrays.asList(line);
    }

    @Override
    public boolean skipFirstLine()
    {
        return false;
    }
}
