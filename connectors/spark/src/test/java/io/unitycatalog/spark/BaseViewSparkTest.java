package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static io.unitycatalog.server.utils.TestUtils.createApiClient;

import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.Dependency;
import io.unitycatalog.client.model.DependencyList;
import io.unitycatalog.client.model.TableDependency;
import io.unitycatalog.client.model.TableType;
import java.io.File;
import java.util.List;
import lombok.SneakyThrows;
import org.junit.jupiter.api.io.TempDir;

/** Shared source-table and view fixtures for Spark view read/DDL integration tests. */
public abstract class BaseViewSparkTest extends BaseSparkIntegrationTest {

  @TempDir protected File dataDir;

  protected static final String SOURCE_TABLE = "src_events";
  protected static final String VIEW_NAME = "v_events";

  protected String sourceFullName() {
    return CATALOG_NAME + "." + SCHEMA_NAME + "." + SOURCE_TABLE;
  }

  protected String viewFullName() {
    return CATALOG_NAME + "." + SCHEMA_NAME + "." + VIEW_NAME;
  }

  @SneakyThrows
  protected String createSourceTable() {
    String fullName = sourceFullName();
    String location = new File(dataDir, SOURCE_TABLE).getCanonicalPath();
    sql("CREATE TABLE %s (i INT, s STRING) USING PARQUET LOCATION '%s'", fullName, location);
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", fullName);
    return fullName;
  }

  protected void createViewViaSdk(String sourceFullName) throws Exception {
    TablesApi tablesApi = new TablesApi(createApiClient(serverConfig));
    List<ColumnInfo> columns =
        List.of(
            new ColumnInfo()
                .name("i")
                .typeText("int")
                .typeJson("{\"name\":\"i\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}")
                .typeName(ColumnTypeName.INT)
                .position(0)
                .nullable(true),
            new ColumnInfo()
                .name("s")
                .typeText("string")
                .typeJson("{\"name\":\"s\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}")
                .typeName(ColumnTypeName.STRING)
                .position(1)
                .nullable(true));
    tablesApi.createTable(
        new CreateTable()
            .name(VIEW_NAME)
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .tableType(TableType.VIEW)
            .columns(columns)
            .viewDefinition("SELECT i, s FROM " + sourceFullName)
            .viewDependencies(
                new DependencyList()
                    .dependencies(
                        List.of(
                            new Dependency()
                                .table(new TableDependency().tableFullName(sourceFullName))))));
  }
}
