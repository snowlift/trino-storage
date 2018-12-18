package org.ebyhr.presto.flex.operator;

import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSource;
import org.ebyhr.presto.flex.FlexColumn;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_8;

public class RawPlugin implements FilePlugin {
    @Override
    public List<FlexColumn> getFields(String schema, String table)
    {
        return ImmutableList.of(new FlexColumn("data", VARCHAR));
    }

    @Override
    public List<String> splitToList(Iterator lines)
    {
        Iterable<String> iterable = () -> lines;
        String line = StreamSupport.stream(iterable.spliterator(), false).map(Object::toString).collect(Collectors.joining("\n"));
        return Arrays.asList(line);
    }

    @Override
    public Iterator<String> getIterator(ByteSource byteSource)
    {
        try {
            return byteSource.asCharSource(UTF_8).readLines().iterator();
        } catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to get iterator");
        }
    }

    @Override
    public boolean skipFirstLine()
    {
        return false;
    }
}
