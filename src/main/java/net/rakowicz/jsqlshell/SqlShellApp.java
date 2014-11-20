package net.rakowicz.jsqlshell;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SqlShellApp {

    private static PrintStream out = System.out;
    
    private String dbName;
    private DbConfig dbConfig;
    private Connection connection;
    private Statement statement;
    
    public SqlShellApp(String dbName) {
        this.dbName = dbName;
    }
    
    public void run() {
        try {
            dbConfig = new DbConfig(dbName).loadDriver();
            connection = dbConfig.connect();
            DatabaseMetaData dbmd = connection.getMetaData();
            out.print("Connected to: " + dbmd.getDatabaseProductName() + " " + dbmd.getDatabaseProductVersion());

            statement = connection.createStatement();
            statement.setMaxRows(1000);
            out.println(" (autocommit=" + connection.getAutoCommit() + ", readonly=" + connection.isReadOnly() + ", maxrows=" + statement.getMaxRows() + ")");
            out.println("Type 'help' for more options");

            while (true) {
                String command = CommandReader.readLine("jss> ");
                try {
                    checkConnection();
                    if (CommandHelper.isCommand(command, out, connection, statement)) {
                        continue;
                    }

                    String sql = command;
                    if (!sql.trim().toLowerCase().startsWith("select ")) {
                        int changed = statement.executeUpdate(sql);
                        out.println("info: affected " + changed + " rows");
                    } else {
                        long started = System.currentTimeMillis();
                        ResultSet rset = statement.executeQuery(sql);
                        String[] selectClause = sql.substring(
                                sql.indexOf("select ") + "select ".length(),
                                sql.indexOf(" from ")
                            ).trim().split(",");
                        new DataFormatter(rset).columns(selectClause).format().printResults(out, started);
                    }
                    if (connection.getWarnings() != null) {
                        out.println("warning: " + connection.getWarnings().getErrorCode() + ":" + connection.getWarnings().getMessage());
                    }
                } catch (SQLException e) {
                    out.println("error: " + e.getMessage());
                }
                connection.clearWarnings();
            }
        } catch (Exception e) {
            if (e instanceof RuntimeException && "exit".equals(e.getMessage())) {
                // exit
            } else {
                e.printStackTrace();
                out.println("Exited...");
            }
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (Exception ignore) {
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception ignore) {
                }
            }
        }
    }
    
    private void checkConnection() throws SQLException {
        if (connection == null
                || connection.isClosed()) {
            if (statement != null) {
                try {
                    connection.close();
                } catch (Exception ignored) {
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception ignored) {
                }
            }
            connection = dbConfig.connect();
            out.println("info: Connection expired, re-connected...");
        }
        if (statement == null
                || statement.isClosed()) {
            if (statement != null) {
                try {
                    connection.close();
                } catch (Exception ignored) {
                }
            }
            statement = connection.createStatement();
            out.println("info: Statement was closed, re-created...");
        }
    }
}
