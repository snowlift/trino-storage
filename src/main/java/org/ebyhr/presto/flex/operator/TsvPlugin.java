package org.ebyhr.presto.flex.operator;

import com.google.common.base.Splitter;

import java.util.List;

public class TsvPlugin implements FilePlugin {
    @Override
    public List<String> splitToList(String line)
    {
        Splitter splitter = Splitter.on("\t").trimResults();
        return splitter.splitToList(line);
    }

    @Override
    public boolean skipFirstLine()
    {
        return true;
    }
}
