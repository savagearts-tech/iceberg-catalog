package com.lakehouse.catalog.migration;

/**
 * Optional contract for table implementations that can expose a metadata file location directly.
 */
interface MetadataLocationSupplier {

    String metadataFileLocation();
}
