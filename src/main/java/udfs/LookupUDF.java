package udfs;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.sql.*;
import java.util.HashMap;

/**
 * look up UDF.
 *
 *
 */
@Description(name = "LookupUDF", value = "Returns matched record from the table", extended = "SELECT LookupUDF(customerName);")

class LookupUDF extends GenericUDF {
    private static String driver = "org.apache.hive.jdbc.HiveDriver";
    private static String hiveTableConf = "hiveTable.conf";
    protected HashMap<String, String> tableConfMap = new HashMap<String, String>();
    private Statement state;

    StringObjectInspector nameOrAddress;
    StringObjectInspector Address;

    @Override
    public String getDisplayString(String[] args) {
        return "arrayContainsExample()"; // this should probably be better
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException("arrayContainsExample only takes 2 arguments: List<T>, T");
        }
        // 1. Check we received the right object types.
        ObjectInspector a = arguments[0];
        ObjectInspector b = arguments[1];
        if (!(a instanceof ListObjectInspector) || !(b instanceof StringObjectInspector)) {
            throw new UDFArgumentException("first argument must be a list / array, second argument must be a string");
        }
        this.nameOrAddress = (StringObjectInspector) a;
        //this.Address = (StringObjectInspector) b;

        if (!(nameOrAddress.getCategory() != ObjectInspector.Category.PRIMITIVE && ((PrimitiveObjectInspector) arguments[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING)) {
            throw new UDFArgumentException("argument must be a string");
        }

        HashMap<String, String> tableConf = getHiveTableConf(hiveTableConf);

        try {
            Connection connect = getHiveConn(tableConf.get("URL"), tableConf.get("USER"), tableConf.get("PASSWORD"));
            state = connect.createStatement();


        } catch (SQLException e) {
            e.printStackTrace();
        }


        // the return type of our function is a boolean, so we provide the correct object inspector
        return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) {
        ResultSet result = null;
        try {
            result = state.executeQuery(String.format(tableConfMap.get("QUERY"), nameOrAddress.toString()));
        } catch (SQLException e) {
            e.printStackTrace();
        }


        return result;
    }

    public Connection getHiveConn(String url, String user, String password) throws SQLException {
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        return DriverManager.getConnection(url, user, password);
    }

    public HashMap<String, String> getHiveTableConf(String confFile) {
        try {
            Config tableConf = ConfigFactory.parseResources(confFile).withFallback(ConfigFactory.load());
            HashMap<String, String> tableConfMap = new HashMap<String, String>();
            tableConfMap.put("URL", (tableConf.getString("conf.url")));
            tableConfMap.put("USER", (tableConf.getString("conf.user")));
            tableConfMap.put("PASSWORD", (tableConf.getString("conf.password")));
            tableConfMap.put("DATABASE", (tableConf.getString("conf.database")));
            tableConfMap.put("TABLE", (tableConf.getString("conf.table")));
            tableConfMap.put("QUERY", (tableConf.getString("conf.query")));
            return tableConfMap;

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        return tableConfMap;
    }
}
