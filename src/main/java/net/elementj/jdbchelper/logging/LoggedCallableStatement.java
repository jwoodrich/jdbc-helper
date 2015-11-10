package net.elementj.jdbchelper.logging;

import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

/**
 * Debugging wrapper for callable statements.
 */
public abstract class LoggedCallableStatement extends LoggedPreparedStatement implements CallableStatement {
	private final CallableStatement cstm;
	private final Map<String,Object> boundmap=new HashMap<String,Object>();
	protected LoggedCallableStatement(CallableStatement stm, String sql) {
		super(stm, sql);
		cstm=stm;
	}
	/**
	 * {@inheritDoc}
	 */
	public final void registerOutParameter(int parameterIndex, int sqlType)
			throws SQLException {
		setBound(parameterIndex, new TypedOutParameter(sqlType));
		cstm.registerOutParameter(parameterIndex, sqlType);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void registerOutParameter(int parameterIndex, int sqlType, int scale)
			throws SQLException {
		setBound(parameterIndex, new TypedOutParameter(sqlType));
		cstm.registerOutParameter(parameterIndex, sqlType, scale);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void setURL(String parameterName, URL val) throws SQLException {
		boundmap.put(parameterName,val);
		cstm.setURL(parameterName, val);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void setNull(String parameterName, int sqlType) throws SQLException {
		boundmap.put(parameterName, NULL_VALUE);
		cstm.setNull(parameterName, sqlType);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void setBoolean(String parameterName, boolean x) throws SQLException {
		boundmap.put(parameterName, x);
		cstm.setBoolean(parameterName, x);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void setByte(String parameterName, byte x) throws SQLException {
		boundmap.put(parameterName, x);
		cstm.setByte(parameterName, x);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void setShort(String parameterName, short x) throws SQLException {
		boundmap.put(parameterName, x);
		cstm.setShort(parameterName, x);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void setInt(String parameterName, int x) throws SQLException {
		boundmap.put(parameterName, x);
		cstm.setInt(parameterName, x);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void setLong(String parameterName, long x) throws SQLException {
		boundmap.put(parameterName, x);
		cstm.setLong(parameterName, x);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void setFloat(String parameterName, float x) throws SQLException {
		boundmap.put(parameterName, x);
		cstm.setFloat(parameterName, x);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void setDouble(String parameterName, double x) throws SQLException {
		boundmap.put(parameterName, x);
		cstm.setDouble(parameterName, x);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void setBigDecimal(String parameterName, BigDecimal x)
			throws SQLException {
		boundmap.put(parameterName, x);
		cstm.setBigDecimal(parameterName, x);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void setString(String parameterName, String x) throws SQLException {
		boundmap.put(parameterName, x);
		cstm.setString(parameterName, x);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void setBytes(String parameterName, byte[] x) throws SQLException {
		boundmap.put(parameterName, x);
		cstm.setBytes(parameterName, x);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void setDate(String parameterName, Date x) throws SQLException {
		boundmap.put(parameterName, x);
		cstm.setDate(parameterName, x);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void setTime(String parameterName, Time x) throws SQLException {
		boundmap.put(parameterName, x);
		cstm.setTime(parameterName, x);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void setTimestamp(String parameterName, Timestamp x)
			throws SQLException {
		boundmap.put(parameterName, x);
		cstm.setTimestamp(parameterName, x);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void setAsciiStream(String parameterName, InputStream x, int length)
			throws SQLException {
		boundmap.put(parameterName, x);
		cstm.setAsciiStream(parameterName, x, length);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void setBinaryStream(String parameterName, InputStream x, int length)
			throws SQLException {
		boundmap.put(parameterName, x);
		cstm.setBinaryStream(parameterName, x, length);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void setObject(String parameterName, Object x, int targetSqlType,
			int scale) throws SQLException {
		boundmap.put(parameterName, x);
		cstm.setObject(parameterName, x, targetSqlType, scale);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void setObject(String parameterName, Object x, int targetSqlType)
			throws SQLException {
		boundmap.put(parameterName, x);
		cstm.setObject(parameterName, x, targetSqlType);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void setObject(String parameterName, Object x) throws SQLException {
		boundmap.put(parameterName, x);
		cstm.setObject(parameterName, x);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void setCharacterStream(String parameterName, Reader reader,
			int length) throws SQLException {
		boundmap.put(parameterName, reader);
		cstm.setCharacterStream(parameterName, reader, length);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void setDate(String parameterName, Date x, Calendar cal)
			throws SQLException {
		boundmap.put(parameterName, x);
		cstm.setDate(parameterName, x, cal);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void setTime(String parameterName, Time x, Calendar cal)
			throws SQLException {
		boundmap.put(parameterName, x);
		cstm.setTime(parameterName, x, cal);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void setTimestamp(String parameterName, Timestamp x, Calendar cal)
			throws SQLException {
		boundmap.put(parameterName, x);
		cstm.setTimestamp(parameterName, x, cal);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void setNull(String parameterName, int sqlType, String typeName)
			throws SQLException {
		boundmap.put(parameterName, NULL_VALUE);
		cstm.setNull(parameterName, sqlType, typeName);
	}
	/**
	 * {@inheritDoc}
	 */
	public final String getString(String parameterName) throws SQLException {
		return cstm.getString(parameterName);
	}
	/**
	 * {@inheritDoc}
	 */
	public final boolean getBoolean(String parameterName) throws SQLException {
		return cstm.getBoolean(parameterName);
	}
	/**
	 * {@inheritDoc}
	 */
	public final byte getByte(String parameterName) throws SQLException {
		return cstm.getByte(parameterName);
	}
	/**
	 * {@inheritDoc}
	 */
	public final short getShort(String parameterName) throws SQLException {
		return cstm.getShort(parameterName);
	}
	/**
	 * {@inheritDoc}
	 */
	public final int getInt(String parameterName) throws SQLException {
		return cstm.getInt(parameterName);
	}
	/**
	 * {@inheritDoc}
	 */
	public final long getLong(String parameterName) throws SQLException {
		return cstm.getLong(parameterName);
	}
	/**
	 * {@inheritDoc}
	 */
	public final float getFloat(String parameterName) throws SQLException {
		return cstm.getFloat(parameterName);
	}
	/**
	 * {@inheritDoc}
	 */
	public final double getDouble(String parameterName) throws SQLException {
		return cstm.getDouble(parameterName);
	}
	/**
	 * {@inheritDoc}
	 */
	public final byte[] getBytes(String parameterName) throws SQLException {
		return cstm.getBytes(parameterName);
	}
	/**
	 * {@inheritDoc}
	 */
	public final Date getDate(String parameterName) throws SQLException {
		return cstm.getDate(parameterName);
	}
	/**
	 * {@inheritDoc}
	 */
	public final Time getTime(String parameterName) throws SQLException {
		return cstm.getTime(parameterName);
	}
	/**
	 * {@inheritDoc}
	 */
	public final Timestamp getTimestamp(String parameterName) throws SQLException {
		return cstm.getTimestamp(parameterName);
	}
	/**
	 * {@inheritDoc}
	 */
	public final Object getObject(String parameterName) throws SQLException {
		return cstm.getObject(parameterName);
	}
	/**
	 * {@inheritDoc}
	 */
	public final BigDecimal getBigDecimal(String parameterName) throws SQLException {
		return cstm.getBigDecimal(parameterName);
	}
	/**
	 * {@inheritDoc}
	 */
	public final Object getObject(String parameterName, Map<String, Class<?>> map)
			throws SQLException {
		return cstm.getObject(parameterName, map);
	}
	/**
	 * {@inheritDoc}
	 */
	public final Ref getRef(String parameterName) throws SQLException {
		return cstm.getRef(parameterName);
	}
	/**
	 * {@inheritDoc}
	 */
	public final Blob getBlob(String parameterName) throws SQLException {
		return cstm.getBlob(parameterName);
	}
	/**
	 * {@inheritDoc}
	 */
	public final Clob getClob(String parameterName) throws SQLException {
		return cstm.getClob(parameterName);
	}
	/**
	 * {@inheritDoc}
	 */
	public final Array getArray(String parameterName) throws SQLException {
		return cstm.getArray(parameterName);
	}
	public final Date getDate(String parameterName, Calendar cal) throws SQLException {
		return cstm.getDate(parameterName, cal);
	}
	/**
	 * {@inheritDoc}
	 */
	public final Time getTime(String parameterName, Calendar cal) throws SQLException {
		return cstm.getTime(parameterName, cal);
	}
	/**
	 * {@inheritDoc}
	 */
	public final Timestamp getTimestamp(String parameterName, Calendar cal)
			throws SQLException {
		return cstm.getTimestamp(parameterName, cal);
	}
	/**
	 * {@inheritDoc}
	 */
	public final URL getURL(String parameterName) throws SQLException {
		return cstm.getURL(parameterName);
	}
	/**
	 * {@inheritDoc}
	 */
	public final RowId getRowId(int parameterIndex) throws SQLException {
		return cstm.getRowId(parameterIndex);
	}
	/**
	 * {@inheritDoc}
	 */
	public final RowId getRowId(String parameterName) throws SQLException {
		return cstm.getRowId(parameterName);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void setRowId(String parameterName, RowId x) throws SQLException {
		cstm.setRowId(parameterName, x);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void setNString(String parameterName, String value)
			throws SQLException {
		boundmap.put(parameterName, value);
		cstm.setNString(parameterName, value);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void setNCharacterStream(String parameterName, Reader value,
			long length) throws SQLException {
		boundmap.put(parameterName, value);
		cstm.setNCharacterStream(parameterName, value, length);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void setNClob(String parameterName, NClob value) throws SQLException {
		boundmap.put(parameterName, value);
		cstm.setNClob(parameterName, value);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void setClob(String parameterName, Reader reader, long length)
			throws SQLException {
		boundmap.put(parameterName, reader);
		cstm.setClob(parameterName, reader, length);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void setBlob(String parameterName, InputStream inputStream,
			long length) throws SQLException {
		boundmap.put(parameterName, inputStream);
		cstm.setBlob(parameterName, inputStream, length);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void setNClob(String parameterName, Reader reader, long length)
			throws SQLException {
		boundmap.put(parameterName, reader);
		cstm.setNClob(parameterName, reader, length);
	}
	/**
	 * {@inheritDoc}
	 */
	public final NClob getNClob(int parameterIndex) throws SQLException {
		return cstm.getNClob(parameterIndex);
	}
	/**
	 * {@inheritDoc}
	 */
	public final NClob getNClob(String parameterName) throws SQLException {
		return cstm.getNClob(parameterName);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void setSQLXML(String parameterName, SQLXML xmlObject)
			throws SQLException {
		boundmap.put(parameterName, xmlObject);
		cstm.setSQLXML(parameterName, xmlObject);
	}
	/**
	 * {@inheritDoc}
	 */
	public final SQLXML getSQLXML(int parameterIndex) throws SQLException {
		return cstm.getSQLXML(parameterIndex);
	}
	/**
	 * {@inheritDoc}
	 */
	public final SQLXML getSQLXML(String parameterName) throws SQLException {
		return cstm.getSQLXML(parameterName);
	}
	/**
	 * {@inheritDoc}
	 */
	public final String getNString(int parameterIndex) throws SQLException {
		return cstm.getNString(parameterIndex);
	}
	/**
	 * {@inheritDoc}
	 */
	public final String getNString(String parameterName) throws SQLException {
		return cstm.getNString(parameterName);
	}
	/**
	 * {@inheritDoc}
	 */
	public final Reader getNCharacterStream(int parameterIndex) throws SQLException {
		return cstm.getNCharacterStream(parameterIndex);
	}
	/**
	 * {@inheritDoc}
	 */
	public final Reader getNCharacterStream(String parameterName) throws SQLException {
		return cstm.getNCharacterStream(parameterName);
	}
	/**
	 * {@inheritDoc}
	 */
	public final Reader getCharacterStream(int parameterIndex) throws SQLException {
		return cstm.getCharacterStream(parameterIndex);
	}
	/**
	 * {@inheritDoc}
	 */
	public final Reader getCharacterStream(String parameterName) throws SQLException {
		return cstm.getCharacterStream(parameterName);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void setBlob(String parameterName, Blob x) throws SQLException {
		boundmap.put(parameterName, x);
		cstm.setBlob(parameterName, x);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void setClob(String parameterName, Clob x) throws SQLException {
		boundmap.put(parameterName, x);
		cstm.setClob(parameterName, x);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void setAsciiStream(String parameterName, InputStream x, long length)
			throws SQLException {
		boundmap.put(parameterName, x);
		cstm.setAsciiStream(parameterName, x, length);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void setBinaryStream(String parameterName, InputStream x, long length)
			throws SQLException {
		boundmap.put(parameterName, x);
		cstm.setBinaryStream(parameterName, x, length);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void setCharacterStream(String parameterName, Reader reader,
			long length) throws SQLException {
		boundmap.put(parameterName, reader);
		cstm.setCharacterStream(parameterName, reader, length);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void setAsciiStream(String parameterName, InputStream x)
			throws SQLException {
		boundmap.put(parameterName, x);
		cstm.setAsciiStream(parameterName, x);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void setBinaryStream(String parameterName, InputStream x)
			throws SQLException {
		boundmap.put(parameterName, x);
		cstm.setBinaryStream(parameterName, x);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void setCharacterStream(String parameterName, Reader reader)
			throws SQLException {
		boundmap.put(parameterName, reader);
		cstm.setCharacterStream(parameterName, reader);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void setNCharacterStream(String parameterName, Reader value)
			throws SQLException {
		boundmap.put(parameterName, value);
		cstm.setNCharacterStream(parameterName, value);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void setClob(String parameterName, Reader reader)
			throws SQLException {
		boundmap.put(parameterName, reader);
		cstm.setClob(parameterName, reader);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void setBlob(String parameterName, InputStream inputStream)
			throws SQLException {
		boundmap.put(parameterName, inputStream);
		cstm.setBlob(parameterName, inputStream);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void setNClob(String parameterName, Reader reader)
			throws SQLException {
	boundmap.put(parameterName, reader);
	cstm.setNClob(parameterName, reader);
	}
	/**
	 * {@inheritDoc}
	 */
	public final boolean wasNull() throws SQLException {
		return cstm.wasNull();
	}
	/**
	 * {@inheritDoc}
	 */
	public final String getString(int parameterIndex) throws SQLException {
		return cstm.getString(parameterIndex);
	}
	/**
	 * {@inheritDoc}
	 */
	public final boolean getBoolean(int parameterIndex) throws SQLException {
		return cstm.getBoolean(parameterIndex);
	}
	/**
	 * {@inheritDoc}
	 */
	public final byte getByte(int parameterIndex) throws SQLException {
		return cstm.getByte(parameterIndex);
	}
	/**
	 * {@inheritDoc}
	 */
	public final short getShort(int parameterIndex) throws SQLException {
		return cstm.getShort(parameterIndex);
	}
	/**
	 * {@inheritDoc}
	 */
	public final int getInt(int parameterIndex) throws SQLException {
		return cstm.getInt(parameterIndex);
	}
	/**
	 * {@inheritDoc}
	 */
	public final long getLong(int parameterIndex) throws SQLException {
		return cstm.getLong(parameterIndex);
	}
	/**
	 * {@inheritDoc}
	 */
	public final float getFloat(int parameterIndex) throws SQLException {
		return cstm.getFloat(parameterIndex);
	}
	/**
	 * {@inheritDoc}
	 */
	@Deprecated
	public final BigDecimal getBigDecimal(int parameterIndex, int scale)
			throws SQLException {
		return cstm.getBigDecimal(parameterIndex, scale);
	}
	/**
	 * {@inheritDoc}
	 */
	public final byte[] getBytes(int parameterIndex) throws SQLException {
		return cstm.getBytes(parameterIndex);
	}
	/**
	 * {@inheritDoc}
	 */
	public final Date getDate(int parameterIndex) throws SQLException {
		return cstm.getDate(parameterIndex);
	}
	/**
	 * {@inheritDoc}
	 */
	public final Time getTime(int parameterIndex) throws SQLException {
		return cstm.getTime(parameterIndex);
	}
	/**
	 * {@inheritDoc}
	 */
	public final Timestamp getTimestamp(int parameterIndex) throws SQLException {
		return cstm.getTimestamp(parameterIndex);
	}
	/**
	 * {@inheritDoc}
	 */
	public final Object getObject(int parameterIndex) throws SQLException {
		return cstm.getObject(parameterIndex);
	}
	/**
	 * {@inheritDoc}
	 */
	public final BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
		return cstm.getBigDecimal(parameterIndex);
	}
	/**
	 * {@inheritDoc}
	 */
	public final Object getObject(int parameterIndex, Map<String, Class<?>> map)
			throws SQLException {
		return cstm.getObject(parameterIndex, map);
	}
	/**
	 * {@inheritDoc}
	 */
	public final Ref getRef(int parameterIndex) throws SQLException {
		return cstm.getRef(parameterIndex);
	}
	/**
	 * {@inheritDoc}
	 */
	public final Blob getBlob(int parameterIndex) throws SQLException {
		return cstm.getBlob(parameterIndex);
	}
	/**
	 * {@inheritDoc}
	 */
	public final Clob getClob(int parameterIndex) throws SQLException {
		return cstm.getClob(parameterIndex);
	}
	/**
	 * {@inheritDoc}
	 */
	public final Array getArray(int parameterIndex) throws SQLException {
		return cstm.getArray(parameterIndex);
	}
	/**
	 * {@inheritDoc}
	 */
	public final Date getDate(int parameterIndex, Calendar cal) throws SQLException {
		return cstm.getDate(parameterIndex, cal);
	}
	/**
	 * {@inheritDoc}
	 */
	public final Time getTime(int parameterIndex, Calendar cal) throws SQLException {
		return cstm.getTime(parameterIndex, cal);
	}
	/**
	 * {@inheritDoc}
	 */
	public final Timestamp getTimestamp(int parameterIndex, Calendar cal)
			throws SQLException {
		return cstm.getTimestamp(parameterIndex, cal);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void registerOutParameter(int parameterIndex, int sqlType,
			String typeName) throws SQLException {
		setBound(parameterIndex, new TypedOutParameter(sqlType));
		cstm.registerOutParameter(parameterIndex, sqlType, typeName);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void registerOutParameter(String parameterName, int sqlType)
			throws SQLException {
		cstm.registerOutParameter(parameterName, sqlType);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void registerOutParameter(String parameterName, int sqlType,
			int scale) throws SQLException {
		cstm.registerOutParameter(parameterName, sqlType, scale);
	}
	/**
	 * {@inheritDoc}
	 */
	public final void registerOutParameter(String parameterName, int sqlType,
			String typeName) throws SQLException {
		cstm.registerOutParameter(parameterName, sqlType, typeName);
	}
	/**
	 * {@inheritDoc}
	 */
	public final URL getURL(int parameterIndex) throws SQLException {
		return cstm.getURL(parameterIndex);
	}
	/**
	 * {@inheritDoc}
	 */
	public final double getDouble(int parameterIndex) throws SQLException {
		return cstm.getDouble(parameterIndex);
	}
	public static class TypedOutParameter extends OutParameter {
		private static final String TYPE_UNKNOWN="UNKNOWN";
		private final String type;
		public TypedOutParameter(String type) {
			this.type = type;
		}
		public TypedOutParameter(int type) {
			Class<java.sql.Types> clazz=java.sql.Types.class;
			String strType=null;
			for (Field field : clazz.getDeclaredFields()) {
				if (field.getType()==int.class&&(field.getModifiers()&(Modifier.PUBLIC|Modifier.STATIC))>0) {
					try {
						if (field.getInt(null)==type) { 
							strType=field.getName(); 
							break; 
						}
					} catch (Exception e) {
						continue;
					}
				}
			}
			this.type=strType!=null?strType:TYPE_UNKNOWN;
		}
		@Override
		public String toString() {
			return "[ ("+type+") out parameter ]";
		}
	}
	@Override
	public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
		return cstm.getObject(parameterIndex, type);
	}
	@Override
	public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
		return cstm.getObject(parameterName, type);
	}
}
