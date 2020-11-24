/*
 * Copyright (c) 2020 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static org.junit.Assert.*;

import java.sql.*;
import java.util.logging.Logger;
import net.snowflake.client.AbstractDriverIT;
import net.snowflake.client.category.TestCategoryOthers;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(TestCategoryOthers.class)
public final class GSRetryIT extends AbstractDriverIT {
  private static Logger logger = Logger.getLogger(BaseJDBCTest.class.getName());

  /** Test for SNOW-225928: GS Retry was leading to double-inserts */
  @Test
  public void testGSRetryForDMLWithoutBinds() throws Throwable {
    Connection connection = null;
    Statement statement = null;
    try {
      connection = getConnection();
      statement = connection.createStatement();
      // Create the table
      statement.execute("create or replace table testGSRetry(col1 number);");

      // Enable the fix
      statement.executeQuery("alter session set ENABLE_FIX_225928 = true");
      PreparedStatement prepStatement =
          connection.prepareStatement("insert into testGSRetry values (100);");
      // Execute with GS retry
      int rowCount = testGSRetryHelper(connection, prepStatement, false);
      assertEquals("update count", 1, rowCount);

      // Validate there is only 1 entry
      ResultSet resultSet = statement.executeQuery("select * from testGSRetry");
      assertTrue(resultSet.next());
      assertEquals(100, resultSet.getInt(1));
      assertFalse(resultSet.next());

      // SNOW-227539: Reproducible test-case
      // Re-create the table
      statement.execute("create or replace table testGSRetry(col1 number);");
      // Execute with GS retry and combined_describe
      prepStatement = connection.prepareStatement("insert into testGSRetry values (100);");
      rowCount = testGSRetryHelper(connection, prepStatement, true);
      assertEquals("update count", 1, rowCount);

      // Validate there is only 1 entry
      resultSet = statement.executeQuery("select * from testGSRetry");
      assertTrue(resultSet.next());
      assertEquals(100, resultSet.getInt(1));
      // TODO: This assert will fail until SNOW-227539 is fixed
      // assertFalse(resultSet.next());
    } finally {
      if (statement != null) {
        statement.execute("DROP TABLE testGSRetry");
      }
      closeSQLObjects(statement, connection);
    }
  }

  /** Test for SNOW-226106: GS Retry was leading to exceptions with binds */
  @Test
  public void testGSRetryForDMLWithBinds() throws Throwable {
    Connection connection = null;
    Statement statement = null;
    try {
      connection = getConnection();
      statement = connection.createStatement();
      // Create the table
      statement.execute("create or replace table testGSRetry(col1 number);");

      PreparedStatement preparedStatement =
          connection.prepareStatement("insert into testGSRetry(col1) values(?)");

      preparedStatement.setInt(1, 100);

      // Enable the fix
      statement.executeQuery("alter session set ENABLE_FIX_225928 = true");
      // Execute with GS retry
      int rowCount = testGSRetryHelper(connection, preparedStatement, false);
      assertEquals("update count", 1, rowCount);

      // Validate there is only 1 entry
      ResultSet resultSet = statement.executeQuery("select * from testGSRetry");
      assertTrue(resultSet.next());
      assertEquals(100, resultSet.getInt(1));
      assertFalse(resultSet.next());

      // SNOW-227539: Reproducible test-case
      // Re-create the table
      statement.execute("create or replace table testGSRetry(col1 number);");

      preparedStatement = connection.prepareStatement("insert into testGSRetry(col1) values(?)");
      preparedStatement.setInt(1, 100);

      // Execute with GS retry and combined_describe
      // TODO: This test can't be executed until SNOW-227539 is fixed. It will throw an exception
      /*
      rowCount = testGSRetryHelper(connection, preparedStatement, true);
      assertEquals("update count", 1, rowCount);

      // Validate there is only 1 entry
      resultSet = statement.executeQuery("select * from testGSRetry");
      assertTrue(resultSet.next());
      assertEquals(100, resultSet.getInt(1));

      assertFalse(resultSet.next());
      */
    } finally {
      if (statement != null) {
        statement.execute("DROP TABLE testGSRetry");
      }
      closeSQLObjects(statement, connection);
    }
  }

  private int testGSRetryHelper(
      Connection connection, PreparedStatement prepStatement, boolean useCombinedDescribe)
      throws SQLException {
    Statement statement = connection.createStatement();
    setGSRetryParams(statement, useCombinedDescribe);

    // Try an insert using executeBatch(). This will guarantee 2-phase execution: Prepare and
    // Execute
    try {
      prepStatement.addBatch();
      int[] insertCounts;

      insertCounts = prepStatement.executeBatch();
      return insertCounts[0];
    } finally {
      statement.executeQuery("alter session unset GS_FAULT_INJECTION;");
    }
  }

  private void setGSRetryParams(Statement statement, boolean useCombinedDescribe)
      throws SQLException {
    statement.executeQuery("alter session set ENABLE_QUERY_RETRIES = true;");
    if (!useCombinedDescribe) {
      statement.executeQuery("alter session set ENABLE_COMBINED_DESCRIBE=false;");
      statement.executeQuery("alter session set JDBC_ENABLE_COMBINED_DESCRIBE=false;");
    } else {
      statement.executeQuery("alter session set ENABLE_COMBINED_DESCRIBE=true;");
      statement.executeQuery("alter session set JDBC_ENABLE_COMBINED_DESCRIBE=true;");
    }
    statement.executeQuery("alter session set QUERY_RETRY_ALLOW_SNOWFLAKE_QUERIES = true;");
    statement.executeQuery("alter session set QUERY_RETRY_MAX_ATTEMPTS = 1;");
    statement.executeQuery(
        "alter session set GS_FAULT_INJECTION = '\n"
            + "[{\n"
            + "\"location\": \"JOB_AFTER_COMPILATION\",\n"
            + "\"action\": \"GS_RETRY\",\n"
            + "\"id\": null,\n"
            + "\"value\": null\n"
            + "}]\n"
            + "';");
  }
}
