package org.ebyhr.presto.flex.operator;

import com.google.common.base.Splitter;

import java.util.List;

public class CsvPlugin implements FilePlugin {
    @Override
    public List<String> splitToList(String line)
    {
        Splitter splitter = Splitter.on(",").trimResults();
        return splitter.splitToList(line);
    }

    @Override
    public boolean skipFirstLine()
    {
        return true;
    }
}
