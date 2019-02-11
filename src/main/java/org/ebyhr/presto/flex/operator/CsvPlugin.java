package org.ebyhr.presto.flex.operator;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSource;
import com.google.common.io.Resources;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.type.Type;
import org.ebyhr.presto.flex.FlexColumn;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.ebyhr.presto.flex.FileType.TXT;

public class CsvPlugin implements FilePlugin {
    private static final String DELIMITER = ",";

    @Override
    public List<FlexColumn> getFields(String schema, String table)
    {
        Splitter splitter = Splitter.on(DELIMITER).trimResults();

        ByteSource byteSource;
        try {
            byteSource = Resources.asByteSource(URI.create(table).toURL());
        }
        catch (IllegalArgumentException | MalformedURLException e) {
            throw new TableNotFoundException(new SchemaTableName(schema, table));
        }

        if (schema.equalsIgnoreCase(TXT.toString())) {
            return ImmutableList.of(new FlexColumn("value", VARCHAR));
        }

        List<FlexColumn> columnTypes = new LinkedList<>();
        try {
            ImmutableList<String> lines = byteSource.asCharSource(UTF_8).readLines();
            List<String> fields = splitter.splitToList(lines.get(0));
//            List<String> data = splitter.splitToList(lines.get(1));

            for (int i = 0; i < fields.size(); i++) {
                Type type = VARCHAR;
                columnTypes.add(new FlexColumn(fields.get(i), type));
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return columnTypes;

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
    public List<String> splitToList(Iterator lines)
    {
        String line = (String) lines.next();
        Splitter splitter = Splitter.on(DELIMITER).trimResults();
        return splitter.splitToList(line);
    }

    @Override
    public boolean skipFirstLine()
    {
        return true;
    }
}
