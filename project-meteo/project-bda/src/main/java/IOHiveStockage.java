import java.sql.*;
import java.util.ArrayList;
import java.util.List;


public class IOHiveStockage {

    private static final String JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
    private static final String HIVE_URL = "jdbc:hive2://node-2:10000/mmmm";
    private Connection con = null;
    private Statement stmt;

    public IOHiveStockage() throws ClassNotFoundException {
        Class.forName(JDBC_DRIVER_NAME);
    }

    public boolean initConnection() {
        try {
            con = DriverManager.getConnection(HIVE_URL,"hdfs","");
            this.stmt = con.createStatement();
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    public void createTable(String tableName, List<String> columns) throws SQLException {
       String sqlStatementCreate = "CREATE TABLE IF NOT EXISTS  "+ tableName +" (";

        for (int i = 0 ; i < columns.size() ; i++) {
            String attribute = columns.get(i);
            if (attribute.equals("date")) attribute = "date_";
            sqlStatementCreate = sqlStatementCreate + " " +attribute + " String,";
        }
        sqlStatementCreate = sqlStatementCreate.substring(0, sqlStatementCreate.length()-1);

        sqlStatementCreate += ")";

        Statement stmt = con.createStatement();


        stmt.execute(sqlStatementCreate);

    }

    public void deleteTable(String tableName) throws SQLException {
        String sqlStatementDrop = "DROP TABLE IF EXISTS " + tableName;
        this.stmt.execute(sqlStatementDrop);
    }

   public void insertTable(String tableName, List<List<Object>> values) throws SQLException {
        String sqlStatementInsert = "INSERT INTO " + tableName + " VALUES ";

        for (List<Object> vals : values){
            String valSql = "";
            for (Object val : vals){
                valSql += "'" + val + "',";
            }
            sqlStatementInsert += "(" + valSql.substring(0,valSql.length()-1) + "),";
        }

        sqlStatementInsert = sqlStatementInsert.substring(0, sqlStatementInsert.length() - 1);

       System.out.println(sqlStatementInsert);

       this.stmt.execute(sqlStatementInsert);

    }



    public List<List<Object>> selectAll(String tableName) throws SQLException {
        String sqlStatementSelect = "Select * from " + tableName;
        ResultSet rs = this.stmt.executeQuery(sqlStatementSelect);
        ResultSetMetaData resultSetMetaData = rs.getMetaData();
        int columnCount = resultSetMetaData.getColumnCount();

        List<List<Object>> results = new ArrayList<>();
        while(rs.next()) {
            List<Object> values = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                values.add(rs.getString(i));
            }
            results.add(values);
        }

        return results;
    }


    public boolean isExist(String tableName) throws SQLException {

        DatabaseMetaData meta = con.getMetaData();
        ResultSet res = meta.getTables(null, null, null, new String[] {"TABLE"});

        while (res.next()) {
            if (res.getString("TABLE_NAME").equals(tableName))
                return true;
        }

        res.close();

        return false;
    }


    public void close() throws SQLException {
        con.close();
    }


}
