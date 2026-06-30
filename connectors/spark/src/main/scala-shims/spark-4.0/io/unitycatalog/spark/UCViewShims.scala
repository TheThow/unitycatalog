package io.unitycatalog.spark

// Spark 4.0 does not route view DDL (SHOW/CREATE/DROP VIEW) to a v2 ViewCatalog, so the
// connector does not implement ViewCatalog on this line. View reads still work via the
// V1Table path in UCProxy.loadTable. These shims are intentionally empty markers; the real
// ViewCatalog implementation lives in the spark-4.2 shim.
private[spark] trait UCSingleCatalogViewShim { self: UCSingleCatalog => }

private[spark] trait UCProxyViewShim { self: UCProxy => }
