package com.salesforce.phoenix.cache;

import java.io.Closeable;

import com.salesforce.phoenix.index.IndexMaintainer;

public interface IndexMetaDataCache extends Closeable, Iterable<IndexMaintainer> {
}
