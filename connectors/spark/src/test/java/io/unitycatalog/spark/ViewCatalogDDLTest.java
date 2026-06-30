package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

public class ViewCatalogDDLTest extends BaseViewSparkTest {

  @Test
  public void testShowViews() throws Exception {
    session = createSparkSessionWithCatalogs(CATALOG_NAME);
    createViewViaSdk(createSourceTable());

    List<Row> views = sql("SHOW VIEWS IN %s.%s", CATALOG_NAME, SCHEMA_NAME);
    assertThat(views.stream().anyMatch(r -> VIEW_NAME.equals(r.getAs("viewName")))).isTrue();
  }

  @Test
  public void testCreateViewThenSelect() throws Exception {
    session = createSparkSessionWithCatalogs(CATALOG_NAME);
    String sourceFullName = createSourceTable();

    sql("CREATE VIEW %s AS SELECT i, s FROM %s", viewFullName(), sourceFullName);

    List<Row> rows = sql("SELECT * FROM %s ORDER BY i", viewFullName());
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0).getInt(0)).isEqualTo(1);
    assertThat(rows.get(0).getString(1)).isEqualTo("a");
    assertThat(rows.get(1).getInt(0)).isEqualTo(2);
    assertThat(rows.get(1).getString(1)).isEqualTo("b");
  }

  @Test
  public void testDropView() throws Exception {
    session = createSparkSessionWithCatalogs(CATALOG_NAME);
    createViewViaSdk(createSourceTable());
    String viewFullName = viewFullName();

    assertThat(sql("SELECT * FROM %s", viewFullName)).hasSize(2);

    sql("DROP VIEW %s", viewFullName);

    assertThatThrownBy(() -> sql("SELECT * FROM %s", viewFullName)).isInstanceOf(Exception.class);
  }
}
