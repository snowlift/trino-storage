package org.ebyhr.presto.flex.operator;

import java.util.Arrays;
import java.util.List;

public class TextPlugin implements FilePlugin {
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
