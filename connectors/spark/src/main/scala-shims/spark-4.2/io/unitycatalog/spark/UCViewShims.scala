package io.unitycatalog.spark

import io.unitycatalog.client.ApiException
import io.unitycatalog.client.model.{CreateTable, Dependency, DependencyList, TableInfo, TableType}
import org.apache.spark.sql.catalyst.analysis.NoSuchViewException
import org.apache.spark.sql.connector.catalog.{Identifier, MetadataTable, Table, TableCatalog, TableViewCatalog, ViewCatalog, ViewInfo}
import org.apache.spark.sql.types.{DataType, StructField, StructType}

import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

// Spark 4.2 routes view DDL (SHOW/CREATE/DROP VIEW) and view resolution to a v2 ViewCatalog.
// A catalog that exposes both tables and views must implement TableViewCatalog (not TableCatalog
// and ViewCatalog separately); Spark rejects the latter at catalog initialization. View ops are
// delegated to the UC proxy; loadTableOrView is the single-RPC read entry point.
private[spark] trait UCSingleCatalogViewShim extends TableViewCatalog { self: UCSingleCatalog =>

  override def loadTableOrView(ident: Identifier): Table = {
    try {
      new MetadataTable(self.viewDelegate.loadView(ident), ident.toString)
    } catch {
      case _: NoSuchViewException => self.loadTable(ident)
    }
  }

  override def listViews(namespace: Array[String]): Array[Identifier] =
    self.viewDelegate.listViews(namespace)

  override def loadView(ident: Identifier): ViewInfo = self.viewDelegate.loadView(ident)

  override def createView(ident: Identifier, info: ViewInfo): ViewInfo =
    self.viewDelegate.createView(ident, info)

  override def replaceView(ident: Identifier, info: ViewInfo): ViewInfo =
    self.viewDelegate.replaceView(ident, info)

  override def dropView(ident: Identifier): Boolean = self.viewDelegate.dropView(ident)

  override def viewExists(ident: Identifier): Boolean = self.viewDelegate.viewExists(ident)

  override def invalidateView(ident: Identifier): Unit = self.viewDelegate.invalidateView(ident)

  override def renameView(oldIdent: Identifier, newIdent: Identifier): Unit =
    self.viewDelegate.renameView(oldIdent, newIdent)
}

private[spark] trait UCProxyViewShim extends ViewCatalog { self: UCProxy =>

  override def listViews(namespace: Array[String]): Array[Identifier] = {
    UCSingleCatalog.checkUnsupportedNestedNamespace(namespace)
    val schemaName = namespace.head
    val views = ArrayBuffer.empty[Identifier]
    var pageToken: String = null
    do {
      val response = self.viewTablesApi.listTables(self.name(), schemaName, /* limit */ 0, pageToken)
      views ++= response.getTables.asScala
        .filter(_.getTableType == TableType.VIEW)
        .map(t => Identifier.of(namespace, t.getName))
      pageToken = response.getNextPageToken
    } while (pageToken != null && pageToken.nonEmpty)
    views.toArray
  }

  override def loadView(ident: Identifier): ViewInfo = {
    val t = try {
      self.viewTablesApi.getTable(
        UCSingleCatalog.fullTableNameForApi(self.name(), ident),
        /* readStreamingTableAsManaged = */ true,
        /* readMaterializedViewAsManaged = */ true)
    } catch {
      case e: ApiException if e.getCode == 404 => throw new NoSuchViewException(ident)
    }
    if (t.getTableType != TableType.VIEW) {
      throw new NoSuchViewException(ident)
    }
    buildViewInfo(t)
  }

  override def createView(ident: Identifier, info: ViewInfo): ViewInfo = {
    UCSingleCatalog.checkUnsupportedNestedNamespace(ident.namespace())
    if (ident.namespace().isEmpty) {
      throw new ApiException(
        "Cannot create view '" + ident.name() + "' without a schema; qualify it as " +
          "<schema>." + ident.name() + " or set the current schema with USE.")
    }
    val createTable = new CreateTable()
    createTable.setName(ident.name())
    createTable.setSchemaName(ident.namespace().head)
    createTable.setCatalogName(self.name())
    createTable.setTableType(TableType.VIEW)
    createTable.setColumns(self.buildViewColumns(info.schema).asJava)
    createTable.setViewDefinition(info.queryText())
    // UC requires a non-null dependency list; we do not derive UC-style dependencies from the
    // view text yet, so we send an empty list. Dependency derivation is a follow-up.
    createTable.setViewDependencies(
      new DependencyList().dependencies(new util.ArrayList[Dependency]()))
    Option(info.properties.get(TableCatalog.PROP_COMMENT)).foreach(createTable.setComment(_))
    createTable.setProperties(info.properties)
    self.viewTablesApi.createTable(createTable)
    loadView(ident)
  }

  override def replaceView(ident: Identifier, info: ViewInfo): ViewInfo =
    throw new UnsupportedOperationException("Replacing a view is not supported yet")

  override def dropView(ident: Identifier): Boolean = {
    try {
      self.viewTablesApi.deleteTable(UCSingleCatalog.fullTableNameForApi(self.name(), ident)) == 200
    } catch {
      case e: ApiException if e.getCode == 404 => false
    }
  }

  override def renameView(oldIdent: Identifier, newIdent: Identifier): Unit =
    throw new UnsupportedOperationException("Renaming a view is not supported yet")

  private def buildViewInfo(t: TableInfo): ViewInfo = {
    val fields = t.getColumns.asScala.map { col =>
      StructField(col.getName, DataType.fromDDL(col.getTypeText), col.getNullable)
        .withComment(col.getComment)
    }.toArray
    new ViewInfo.Builder()
      .withSchema(StructType(fields))
      .withQueryText(t.getViewDefinition)
      .withCurrentCatalog(t.getCatalogName)
      .withCurrentNamespace(Array(t.getSchemaName))
      .withQueryColumnNames(t.getColumns.asScala.map(_.getName).toArray)
      .withProperties(t.getProperties)
      .build()
  }
}
