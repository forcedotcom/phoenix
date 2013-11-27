package com.salesforce.phoenix.cache;

import java.io.Closeable;
import java.util.List;

import com.salesforce.phoenix.index.IndexMaintainer;

public interface IndexMetaDataCache extends Closeable {
    public List<IndexMaintainer> getIndexMaintainers();
}
