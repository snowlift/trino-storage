package org.ebyhr.presto.flex;

public enum FileType {
    CSV, TSV, TXT, RAW, EXCEL;

    @Override
    public String toString()
    {
        return name().toLowerCase();
    }
}
