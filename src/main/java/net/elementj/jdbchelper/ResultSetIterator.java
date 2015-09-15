package net.elementj.jdbchelper;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;

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
}
