package trivial;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

/** Minimal JDBC driver stub: every query returns one row, one INTEGER column "val" = 1 (zero rows for WHERE 1=0 probes). */
public class TrivialDriver implements Driver {
    static { try { DriverManager.registerDriver(new TrivialDriver()); } catch (SQLException e) { throw new RuntimeException(e); } }

    public boolean acceptsURL(String url) { return url != null && url.startsWith("jdbc:trivial:"); }
    public Connection connect(String url, Properties info) {
        if (!acceptsURL(url)) return null;
        return proxy(Connection.class, new ConnH());
    }
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) { return new DriverPropertyInfo[0]; }
    public int getMajorVersion() { return 1; }
    public int getMinorVersion() { return 0; }
    public boolean jdbcCompliant() { return false; }
    public Logger getParentLogger() { return Logger.getLogger("trivial"); }

    @SuppressWarnings("unchecked")
    static <T> T proxy(Class<T> c, InvocationHandler h) { return (T) Proxy.newProxyInstance(TrivialDriver.class.getClassLoader(), new Class<?>[]{c}, h); }

    static Object dflt(Method m) {
        Class<?> r = m.getReturnType();
        if (r == boolean.class) return false;
        if (r == int.class) return 0;
        if (r == long.class) return 0L;
        if (r == short.class) return (short) 0;
        if (r == byte.class) return (byte) 0;
        if (r == float.class) return 0f;
        if (r == double.class) return 0d;
        return null;
    }

    static class ConnH implements InvocationHandler {
        boolean closed;
        public Object invoke(Object p, Method m, Object[] a) {
            switch (m.getName()) {
                case "prepareStatement": case "prepareCall": return proxy(PreparedStatement.class, new StmtH((String) a[0]));
                case "createStatement": return proxy(Statement.class, new StmtH(null));
                case "isClosed": return closed;
                case "close": closed = true; return null;
                case "isValid": return true;
                case "getAutoCommit": return true;
                case "getCatalog": return "trivial";
                case "getMetaData": return proxy(DatabaseMetaData.class, new MetaH());
                case "isReadOnly": return false;
                case "getTransactionIsolation": return Connection.TRANSACTION_NONE;
                case "toString": return "TrivialConnection";
                case "hashCode": return System.identityHashCode(p);
                case "equals": return p == a[0];
                default: return dflt(m);
            }
        }
    }

    static class MetaH implements InvocationHandler {
        public Object invoke(Object p, Method m, Object[] a) {
            switch (m.getName()) {
                case "getDatabaseProductName": return "Trivial";
                case "getDatabaseProductVersion": return "1.0";
                case "getDriverName": return "TrivialDriver";
                case "getDriverVersion": return "1.0";
                case "supportsTransactions": return false;
                case "getURL": return "jdbc:trivial:mem";
                case "getIdentifierQuoteString": return "\"";
                case "toString": return "TrivialMeta";
                case "hashCode": return System.identityHashCode(p);
                case "equals": return p == a[0];
                default: return dflt(m);
            }
        }
    }

    static class StmtH implements InvocationHandler {
        String sql; ResultSet last; boolean closed;
        StmtH(String sql) { this.sql = sql; }
        ResultSet rs(String q) {
            boolean probe = q != null && q.replaceAll("\\s", "").toUpperCase().contains("1=0");
            return proxy(ResultSet.class, new RsH(probe));
        }
        public Object invoke(Object p, Method m, Object[] a) {
            switch (m.getName()) {
                case "executeQuery":
                    last = rs(a != null && a.length > 0 ? (String) a[0] : sql); return last;
                case "execute":
                    last = rs(a != null && a.length > 0 ? (String) a[0] : sql); return true;
                case "getResultSet": return last;
                case "getMetaData": // PreparedStatement.getMetaData
                    return proxy(ResultSetMetaData.class, new RsMetaH());
                case "getUpdateCount": return -1;
                case "getMoreResults": return false;
                case "isClosed": return closed;
                case "close": closed = true; return null;
                case "toString": return "TrivialStatement";
                case "hashCode": return System.identityHashCode(p);
                case "equals": return p == a[0];
                default: return dflt(m);
            }
        }
    }

    static class RsMetaH implements InvocationHandler {
        public Object invoke(Object p, Method m, Object[] a) {
            switch (m.getName()) {
                case "getColumnCount": return 1;
                case "getColumnName": case "getColumnLabel": return "val";
                case "getColumnType": return Types.INTEGER;
                case "getColumnTypeName": return "INTEGER";
                case "getColumnClassName": return "java.lang.Integer";
                case "getPrecision": return 10;
                case "getScale": return 0;
                case "isNullable": return ResultSetMetaData.columnNoNulls;
                case "isSigned": return true;
                case "getTableName": case "getSchemaName": case "getCatalogName": return "";
                case "toString": return "TrivialRsMeta";
                case "hashCode": return System.identityHashCode(p);
                case "equals": return p == a[0];
                default: return dflt(m);
            }
        }
    }

    static class RsH implements InvocationHandler {
        final boolean probe; int pos = 0; boolean closed;
        RsH(boolean probe) { this.probe = probe; }
        public Object invoke(Object p, Method m, Object[] a) {
            switch (m.getName()) {
                case "next": if (probe || pos >= 1) return false; pos++; return true;
                case "getMetaData": return proxy(ResultSetMetaData.class, new RsMetaH());
                case "getInt": case "getShort": case "getByte": return m.getReturnType() == int.class ? (Object) 1 : m.getReturnType() == short.class ? (Object) (short) 1 : (Object) (byte) 1;
                case "getLong": return 1L;
                case "getDouble": return 1d;
                case "getFloat": return 1f;
                case "getObject": return 1;
                case "getString": return "1";
                case "wasNull": return false;
                case "findColumn": return 1;
                case "isClosed": return closed;
                case "close": closed = true; return null;
                case "toString": return "TrivialResultSet";
                case "hashCode": return System.identityHashCode(p);
                case "equals": return p == a[0];
                default: return dflt(m);
            }
        }
    }
}
