package net.elementj.jdbchelper.logging;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;

/**
 * Wrapper for prepared statements that does logging of statements and bound values.
 */
public abstract class LoggedPreparedStatement implements PreparedStatement {
	protected static final int DEFAULT_SIZE=128;
	protected static final Object[] BOUND_NONE=new Object[]{};
	protected static final int NONE_BOUND=0;
	protected static final Null NULL_VALUE=new Null();
	protected Object[] bound=new Object[DEFAULT_SIZE];
	protected final PreparedStatement stm;
	protected int maxBound=NONE_BOUND;
	protected final String sql;
	LoggedPreparedStatement(PreparedStatement stm,String sql) {
		this.stm=stm;
		this.sql=sql;
	}
	/**
	 * Sets a bound value for debugging
	 * @param idx The JDBC index
	 * @param value The value
	 */
	protected void setBound(int idx, Object value) {
		if (bound.length<=idx) {
			Object[] nbound=new Object[bound.length+DEFAULT_SIZE];
			System.arraycopy(bound, 0, nbound, 0, bound.length);
			bound=nbound;
		}
		bound[idx]=value;
		if (idx>maxBound) { maxBound=idx; }
	}
	/**
	 * Clears bound values and resets the maximum bound index to NONE_BOUND.
	 */
	protected void clearBound() {
		bound=new Object[DEFAULT_SIZE];
		maxBound=NONE_BOUND;
	}
	protected boolean isBound() {
		return maxBound!=NONE_BOUND;
	}
	/**
	 * Retrieves an object array containing all of the bound values.
	 * @return
	 */
	protected Object[] getBound() {
		if (!isBound()) {
			return BOUND_NONE;
		}
		Object[] ret=new Object[maxBound];
		System.arraycopy(bound, 1, ret, 0, maxBound);
		return ret;
	}
	/**
	 * Log a call from a method.
	 * @param method The method name and parameters, if applicable
	 */
	protected abstract void log(String method);
	public final ResultSet executeQuery(String sql) throws SQLException {
		log(String.format("executeQuery(%s)",sql));
		return stm.executeQuery(sql);
	}
	public final int executeUpdate(String sql) throws SQLException {
		log(String.format("executeUpdate(%s)",sql)); 
		return stm.executeUpdate(sql);
	}
	public final void close() throws SQLException {
		stm.close();
	}
	public final int getMaxFieldSize() throws SQLException {
		return stm.getMaxFieldSize();
	}
	public final void setMaxFieldSize(int max) throws SQLException {
		stm.setMaxFieldSize(max);
	}
	public final int getMaxRows() throws SQLException {
		return stm.getMaxRows();
	}
	public final <T> T unwrap(Class<T> iface) throws SQLException {
		return stm.unwrap(iface);
	}
	public final ResultSet executeQuery() throws SQLException {
		log("executeQuery()"); 
		return stm.executeQuery();
	}
	public final boolean isWrapperFor(Class<?> iface) throws SQLException {
		return stm.isWrapperFor(iface);
	}
	public final int executeUpdate() throws SQLException {
		log("executeUpdate()");
		return stm.executeUpdate();
	}
	public final void setNull(int parameterIndex, int sqlType) throws SQLException {
		setBound(parameterIndex,NULL_VALUE);
		stm.setNull(parameterIndex, sqlType);
	}
	public final void setBoolean(int parameterIndex, boolean x) throws SQLException {
		setBound(parameterIndex,x);
		stm.setBoolean(parameterIndex, x);
	}
	public final void setByte(int parameterIndex, byte x) throws SQLException {
		setBound(parameterIndex,x);
		stm.setByte(parameterIndex, x);
	}
	public final void setShort(int parameterIndex, short x) throws SQLException {
		setBound(parameterIndex,x);
		stm.setShort(parameterIndex, x);
	}
	public final void setInt(int parameterIndex, int x) throws SQLException {
		setBound(parameterIndex,x);
		stm.setInt(parameterIndex, x);
	}
	public final void setMaxRows(int max) throws SQLException {
		stm.setMaxRows(max);
	}
	public final void setLong(int parameterIndex, long x) throws SQLException {
		setBound(parameterIndex,x);
		stm.setLong(parameterIndex, x);
	}
	public final void setEscapeProcessing(boolean enable) throws SQLException {
		stm.setEscapeProcessing(enable);
	}
	public final void setFloat(int parameterIndex, float x) throws SQLException {
		setBound(parameterIndex,x);
		stm.setFloat(parameterIndex, x);
	}
	public final int getQueryTimeout() throws SQLException {
		return stm.getQueryTimeout();
	}
	public final void setDouble(int parameterIndex, double x) throws SQLException {
		setBound(parameterIndex,x);
		stm.setDouble(parameterIndex, x);
	}
	public final void setQueryTimeout(int seconds) throws SQLException {
		stm.setQueryTimeout(seconds);
	}
	public final void setBigDecimal(int parameterIndex, BigDecimal x)
			throws SQLException {
		setBound(parameterIndex,x);
		stm.setBigDecimal(parameterIndex, x);
	}
	public final void cancel() throws SQLException {
		stm.cancel();
	}
	public final void setString(int parameterIndex, String x) throws SQLException {
		setBound(parameterIndex,x);
		stm.setString(parameterIndex, x);
	}
	public final SQLWarning getWarnings() throws SQLException {
		return stm.getWarnings();
	}
	public final void setBytes(int parameterIndex, byte[] x) throws SQLException {
		setBound(parameterIndex,x);
		stm.setBytes(parameterIndex, x);
	}
	public final void clearWarnings() throws SQLException {
		stm.clearWarnings();
	}
	public final void setDate(int parameterIndex, Date x) throws SQLException {
		setBound(parameterIndex,x);
		stm.setDate(parameterIndex, x);
	}
	public final void setCursorName(String name) throws SQLException {
		stm.setCursorName(name);
	}
	public final void setTime(int parameterIndex, Time x) throws SQLException {
		setBound(parameterIndex,x);
		stm.setTime(parameterIndex, x);
	}
	public final void setTimestamp(int parameterIndex, Timestamp x)
			throws SQLException {
		setBound(parameterIndex,x);
		stm.setTimestamp(parameterIndex, x);
	}
	public final boolean execute(String sql) throws SQLException {
		log(String.format("execute(%s)",sql)); 
		return stm.execute(sql);
	}
	public final void setAsciiStream(int parameterIndex, InputStream x, int length)
			throws SQLException {
		setBound(parameterIndex,x);
		stm.setAsciiStream(parameterIndex, x, length);
	}
	public final ResultSet getResultSet() throws SQLException {
		return stm.getResultSet();
	}
	@Deprecated
	public final void setUnicodeStream(int parameterIndex, InputStream x, int length)
			throws SQLException {
		setBound(parameterIndex,x);
		stm.setUnicodeStream(parameterIndex, x, length);
	}
	public final int getUpdateCount() throws SQLException {
		return stm.getUpdateCount();
	}
	public final boolean getMoreResults() throws SQLException {
		return stm.getMoreResults();
	}
	public final void setBinaryStream(int parameterIndex, InputStream x, int length)
			throws SQLException {
		setBound(parameterIndex,x);
		stm.setBinaryStream(parameterIndex, x, length);
	}
	public final void setFetchDirection(int direction) throws SQLException {
		stm.setFetchDirection(direction);
	}
	public final void clearParameters() throws SQLException {
		clearBound();
		stm.clearParameters();
	}
	public final int getFetchDirection() throws SQLException {
		return stm.getFetchDirection();
	}
	public final void setObject(int parameterIndex, Object x, int targetSqlType)
			throws SQLException {
		setBound(parameterIndex,x);
		stm.setObject(parameterIndex, x, targetSqlType);
	}
	public final void setFetchSize(int rows) throws SQLException {
		stm.setFetchSize(rows);
	}
	public final int getFetchSize() throws SQLException {
		return stm.getFetchSize();
	}
	public final void setObject(int parameterIndex, Object x) throws SQLException {
		setBound(parameterIndex,x);
		stm.setObject(parameterIndex, x);
	}
	public final int getResultSetConcurrency() throws SQLException {
		return stm.getResultSetConcurrency();
	}
	public final int getResultSetType() throws SQLException {
		return stm.getResultSetType();
	}
	public final void addBatch(String sql) throws SQLException {
		stm.addBatch(sql);
	}
	public final void clearBatch() throws SQLException {
		stm.clearBatch();
	}
	public final boolean execute() throws SQLException {
		log("execute");
		return stm.execute();
	}
	public final int[] executeBatch() throws SQLException {
		log("executeBatch"); 
		return stm.executeBatch();
	}
	public final void addBatch() throws SQLException {
		stm.addBatch();
	}
	public final void setCharacterStream(int parameterIndex, Reader reader, int length)
			throws SQLException {
		setBound(parameterIndex,reader);
		stm.setCharacterStream(parameterIndex, reader, length);
	}
	public final void setRef(int parameterIndex, Ref x) throws SQLException {
		setBound(parameterIndex,x);
		stm.setRef(parameterIndex, x);
	}
	public final Connection getConnection() throws SQLException {
		return stm.getConnection();
	}
	public final void setBlob(int parameterIndex, Blob x) throws SQLException {
		setBound(parameterIndex,x);
		stm.setBlob(parameterIndex, x);
	}
	public final void setClob(int parameterIndex, Clob x) throws SQLException {
		setBound(parameterIndex,x);
		stm.setClob(parameterIndex, x);
	}
	public final boolean getMoreResults(int current) throws SQLException {
		return stm.getMoreResults(current);
	}
	public final void setArray(int parameterIndex, Array x) throws SQLException {
		setBound(parameterIndex,x);
		stm.setArray(parameterIndex, x);
	}
	public final ResultSetMetaData getMetaData() throws SQLException {
		return stm.getMetaData();
	}
	public final ResultSet getGeneratedKeys() throws SQLException {
		return stm.getGeneratedKeys();
	}
	public final void setDate(int parameterIndex, Date x, Calendar cal)
			throws SQLException {
		setBound(parameterIndex,x);
		stm.setDate(parameterIndex, x, cal);
	}
	public final int executeUpdate(String sql, int autoGeneratedKeys)
			throws SQLException {
		log(String.format("executeUpdate(%s,%d)",sql,autoGeneratedKeys)); 
		return stm.executeUpdate(sql, autoGeneratedKeys);
	}
	public final void setTime(int parameterIndex, Time x, Calendar cal)
			throws SQLException {
		setBound(parameterIndex,x);
		stm.setTime(parameterIndex, x, cal);
	}
	public final int executeUpdate(String sql, int[] columnIndexes)
			throws SQLException {
		log(String.format("executeUpdate(%s,%s)",sql,Arrays.asList(columnIndexes)));
		return stm.executeUpdate(sql, columnIndexes);
	}
	public final void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
			throws SQLException {
		setBound(parameterIndex,x);
		stm.setTimestamp(parameterIndex, x, cal);
	}
	public final void setNull(int parameterIndex, int sqlType, String typeName)
			throws SQLException {
		setBound(parameterIndex,NULL_VALUE);
		stm.setNull(parameterIndex, sqlType, typeName);
	}
	public final int executeUpdate(String sql, String[] columnNames)
			throws SQLException {
		log(String.format("execute(%s,%s)",sql,Arrays.asList(columnNames))); 
		return stm.executeUpdate(sql, columnNames);
	}
	public final boolean execute(String sql, int autoGeneratedKeys)
			throws SQLException {
		log(String.format("execute(%s,%d)",sql,autoGeneratedKeys));
		return stm.execute(sql, autoGeneratedKeys);
	}
	public final void setURL(int parameterIndex, URL x) throws SQLException {
		setBound(parameterIndex,x);
		stm.setURL(parameterIndex, x);
	}
	public final ParameterMetaData getParameterMetaData() throws SQLException {
		return stm.getParameterMetaData();
	}
	public final void setRowId(int parameterIndex, RowId x) throws SQLException {
		stm.setRowId(parameterIndex, x);
	}
	public final boolean execute(String sql, int[] columnIndexes) throws SQLException {
		log(String.format("execute(%s,%s)",sql,Arrays.asList(columnIndexes))); 
		return stm.execute(sql, columnIndexes);
	}
	public final void setNString(int parameterIndex, String value)
			throws SQLException {
		setBound(parameterIndex,value);
		stm.setNString(parameterIndex, value);
	}
	public final void setNCharacterStream(int parameterIndex, Reader value,
			long length) throws SQLException {
		setBound(parameterIndex,value);
		stm.setNCharacterStream(parameterIndex, value, length);
	}
	public final boolean execute(String sql, String[] columnNames)
			throws SQLException {
		log(String.format("execute(%s,%s)",sql,Arrays.asList(columnNames)));
		return stm.execute(sql, columnNames);
	}
	public final void setNClob(int parameterIndex, NClob value) throws SQLException {
		setBound(parameterIndex,value);
		stm.setNClob(parameterIndex, value);
	}
	public final void setClob(int parameterIndex, Reader reader, long length)
			throws SQLException {
		setBound(parameterIndex,reader);
		stm.setClob(parameterIndex, reader, length);
	}
	public final int getResultSetHoldability() throws SQLException {
		return stm.getResultSetHoldability();
	}
	public final void setBlob(int parameterIndex, InputStream inputStream, long length)
			throws SQLException {
		setBound(parameterIndex,inputStream);
		stm.setBlob(parameterIndex, inputStream, length);
	}
	public final boolean isClosed() throws SQLException {
		return stm.isClosed();
	}
	public final void setPoolable(boolean poolable) throws SQLException {
		stm.setPoolable(poolable);
	}
	public final void setNClob(int parameterIndex, Reader reader, long length)
			throws SQLException {
		setBound(parameterIndex,reader);
		stm.setNClob(parameterIndex, reader, length);
	}
	public final boolean isPoolable() throws SQLException {
		return stm.isPoolable();
	}
	public final void setSQLXML(int parameterIndex, SQLXML xmlObject)
			throws SQLException {
		setBound(parameterIndex,xmlObject);
		stm.setSQLXML(parameterIndex, xmlObject);
	}
	public final void setObject(int parameterIndex, Object x, int targetSqlType,
			int scaleOrLength) throws SQLException {
		setBound(parameterIndex,x);
		stm.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
	}
	public final void setAsciiStream(int parameterIndex, InputStream x, long length)
			throws SQLException {
		setBound(parameterIndex,x);
		stm.setAsciiStream(parameterIndex, x, length);
	}
	public final void setBinaryStream(int parameterIndex, InputStream x, long length)
			throws SQLException {
		setBound(parameterIndex,x);
		stm.setBinaryStream(parameterIndex, x, length);
	}
	public final void setCharacterStream(int parameterIndex, Reader reader,
			long length) throws SQLException {
		setBound(parameterIndex,reader);
		stm.setCharacterStream(parameterIndex, reader, length);
	}
	public final void setAsciiStream(int parameterIndex, InputStream x)
			throws SQLException {
		setBound(parameterIndex,x);
		stm.setAsciiStream(parameterIndex, x);
	}
	public final void setBinaryStream(int parameterIndex, InputStream x)
			throws SQLException {
		setBound(parameterIndex,x);
		stm.setBinaryStream(parameterIndex, x);
	}
	public final void setCharacterStream(int parameterIndex, Reader reader)
			throws SQLException {
		setBound(parameterIndex,reader);
		stm.setCharacterStream(parameterIndex, reader);
	}
	public final void setNCharacterStream(int parameterIndex, Reader value)
			throws SQLException {
		setBound(parameterIndex,value);
		stm.setNCharacterStream(parameterIndex, value);
	}
	public final void setClob(int parameterIndex, Reader reader) throws SQLException {
		setBound(parameterIndex,reader);
		stm.setClob(parameterIndex, reader);
	}
	public final void setBlob(int parameterIndex, InputStream inputStream)
			throws SQLException {
		setBound(parameterIndex,inputStream);
		stm.setBlob(parameterIndex, inputStream);
	}
	public final void setNClob(int parameterIndex, Reader reader) throws SQLException {
		setBound(parameterIndex,reader);
		stm.setNClob(parameterIndex, reader);
	}
	public static class Null {
		public String toString() { return "null"; }
	}
	public static class OutParameter {
		public String toString() { return "[ out parameter ]"; }
	}
	@Override
	public final void closeOnCompletion() throws SQLException {
		stm.closeOnCompletion();
	}
	@Override
	public final boolean isCloseOnCompletion() throws SQLException {
		return stm.isCloseOnCompletion();
	}
}
