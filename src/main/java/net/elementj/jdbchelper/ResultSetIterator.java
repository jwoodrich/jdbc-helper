package net.elementj.jdbchelper;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * An iterator for retrieving ordered columns from a ResultSet. 
 * @author Jason Woodrich
 */
public class ResultSetIterator {
	private final ResultSet rs;
	private int idx;
	private String defaultTrueValue="Y";
	private String defaultFalseValue="N";
	public ResultSetIterator(ResultSet rs) {
		this(rs,1);
	}
	public ResultSetIterator(ResultSet rs, int idx) {
		super();
		this.rs = rs;
		this.idx = idx;
	}
	public boolean nextBoolean() throws SQLException {
		return rs.getBoolean(idx++);
	}
	public boolean nextBoolean(boolean defaultval) throws SQLException {
		return nextBoolean(defaultTrueValue,defaultFalseValue,defaultval);
	}
	public boolean nextBoolean(String trueval, String falseval, boolean defaultval) throws SQLException {
		String tmp=rs.getString(idx++);
		if (trueval!=null&&trueval.equalsIgnoreCase(tmp)) { return true; }
		if (falseval!=null&&falseval.equalsIgnoreCase(tmp)) { return false; }
		return defaultval;
	}
	public String nextString() throws SQLException {
		return rs.getString(idx++);
	}
	public int nextInt() throws SQLException {
		return rs.getInt(idx++);
	}
	public long nextLong() throws SQLException {
		return rs.getLong(idx++);
	}
	public Timestamp nextTimestamp() throws SQLException {
		return rs.getTimestamp(idx++);
	}
	public Date nextDate() throws SQLException {
		return rs.getDate(idx++);
	}
	public Time nextTime() throws SQLException {
		return rs.getTime(idx++);
	}
	public DateTime nextDateTime() throws SQLException {
		Date date=nextTimestamp();
		return date!=null?new DateTime(date.getTime()):null;
	}
	public DateTime nextDateTime(DateTimeZone dtz) throws SQLException {
		Date date=nextTimestamp();
		return date!=null?new DateTime(date.getTime(),dtz):null;
	}
	public BigDecimal nextBigDecimal() throws SQLException {
		return rs.getBigDecimal(idx++);
	}
	public String getDefaultTrueValue() {
		return defaultTrueValue;
	}
	public void setDefaultTrueValue(String defaultTrueValue) {
		this.defaultTrueValue = defaultTrueValue;
	}
	public String getDefaultFalseValue() {
		return defaultFalseValue;
	}
	public void setDefaultFalseValue(String defaultFalseValue) {
		this.defaultFalseValue = defaultFalseValue;
	}
	public void skip(int count) {
		idx+=count;
	}
	public void skip() {
		idx++;
	}
	public boolean isNull() throws SQLException {
		rs.getObject(idx);
		return rs.wasNull();
	}
	public boolean wasNull() throws SQLException {
		return rs.wasNull();
	}
	public Double nextDoubleWrapper() throws SQLException {
		double ret=rs.getDouble(idx++);
		if (rs.wasNull()) { return null; }
		return ret;
	}
	public Float nextFloatWrapper() throws SQLException {
		float ret=rs.getFloat(idx++);
		if (rs.wasNull()) { return null; }
		return ret;
	}
	public Short nextShortWrapper() throws SQLException {
		short ret=rs.getShort(idx++);
		if (rs.wasNull()) { return null; }
		return ret;
	}
	public Integer nextIntegerWrapper() throws SQLException {
		int ret=rs.getInt(idx++);
		if (rs.wasNull()) { return null; }
		return ret;
	}
	public Byte nextByteWrapper() throws SQLException {
		byte ret=rs.getByte(idx++);
		if (rs.wasNull()) { return null; }
		return ret;
	}
	public Long nextLongWrapper() throws SQLException {
		long ret=rs.getLong(idx++);
		if (rs.wasNull()) { return null; }
		return ret;
	}
	public Boolean nextBooleanWrapper(Boolean defaultval) throws SQLException {
		return nextBooleanWrapper(defaultTrueValue,defaultFalseValue,defaultval);
	}
	public Boolean nextBooleanWrapper(String trueval, String falseval, Boolean defaultval) throws SQLException {
		String tmp=rs.getString(idx++);
		if (rs.wasNull()) { return null; }
		if (trueval!=null&&trueval.equalsIgnoreCase(tmp)) { return true; }
		if (falseval!=null&&falseval.equalsIgnoreCase(tmp)) { return false; }
		return defaultval;
	}
}
