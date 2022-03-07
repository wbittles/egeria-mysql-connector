package properties;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.odpi.openmetadata.adapters.connectors.integration.mysql.properties.MysqlForeignKeyLinks;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MysqlForeginKeyLinksTest {


    @DisplayName("Test getImportedColumnQualifiedName()")
    @Test
    void getImportedColumnQualifiedName() {

        MysqlForeignKeyLinks link = new MysqlForeignKeyLinks("table_name",
                                                                        "column_name",
                                                                        "constraint_name",
                                                                        "referenced_table_name");


        assertEquals( link.getImportedColumnQualifiedName(), "referenced_table_name" + "." + "column_name");
    }

    @DisplayName("Test getExportedColumnQualifiedName()")
    @Test
    void getExportedColumnQualifiedName() {

        MysqlForeignKeyLinks link = new MysqlForeignKeyLinks("table_name",
                "column_name",
                "constraint_name",
                "referenced_table_name");

        assertEquals( link.getExportedColumnQualifiedName(), "table_name" + "." + "column_name");

    }
}