package io.unitycatalog.spark;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

public class ViewReadTest extends BaseViewSparkTest {

  @Test
  public void testReadView() throws Exception {
    session = createSparkSessionWithCatalogs(CATALOG_NAME);
    createViewViaSdk(createSourceTable());

    List<Row> rows = sql("SELECT * FROM %s ORDER BY i", viewFullName());
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0).getInt(0)).isEqualTo(1);
    assertThat(rows.get(0).getString(1)).isEqualTo("a");
    assertThat(rows.get(1).getInt(0)).isEqualTo(2);
    assertThat(rows.get(1).getString(1)).isEqualTo("b");
  }

  @Test
  public void testReadViewWithProjectionAndFilter() throws Exception {
    session = createSparkSessionWithCatalogs(CATALOG_NAME);
    createViewViaSdk(createSourceTable());

    List<Row> rows = sql("SELECT s FROM %s WHERE i = 2", viewFullName());
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).getString(0)).isEqualTo("b");
  }
}
