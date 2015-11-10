package net.elementj.jdbchelper.logging;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBC logging helper that uses SLF4J.
 */
public class SLF4JLoggingHelper implements LoggingHelper<LogLevel> {
	private static final String LOG_PREPARE_STATEMENT="preparing statement: {}";
	private static final String DEFAULT_DATE_FORMAT="yyyy-MM-dd'T'HH:mm:ssZ";
	private final Connection connection;
	private final Logger logger;
	private SimpleDateFormat dateFormat=new SimpleDateFormat(DEFAULT_DATE_FORMAT);
	
	public SLF4JLoggingHelper(Connection connection, Logger logger) {
		super();
		this.connection = connection;
		this.logger = logger;
	}
	private InternalLogger logger(LogLevel level) {
		switch (level) {
		case DEBUG:
			if (logger.isDebugEnabled()) {
				return new InternalLogger() {
					@Override
					public void log(String message, Object... args) {
						logger.debug(message, args);
					}
				};
			} else { return NotLogging.INSTANCE; }
		case ERROR: 
			if (logger.isErrorEnabled()) { 
				return new InternalLogger() {
					@Override
					public void log(String message, Object... args) {
						logger.error(message,args);						
					}
				};
			} else { return NotLogging.INSTANCE; }
		case INFO: 
			if (logger.isInfoEnabled()) {
				return new InternalLogger() {
					@Override
					public void log(String message, Object... args) {
						logger.info(message,args);
					}
				};
			} else { return NotLogging.INSTANCE; }
		case TRACE: 
			if (logger.isTraceEnabled()) {
				return new InternalLogger() {
					@Override
					public void log(String message, Object... args) {
						logger.trace(message, args);
					}
				};
			} else { return NotLogging.INSTANCE; }
		case WARN: 
			if (logger.isWarnEnabled()) {
				return new InternalLogger() {
					@Override
					public void log(String message, Object... args) {
						logger.warn(message, args);
					}
				};
			} else { return NotLogging.INSTANCE; }
		}
		LoggerFactory.getLogger(SLF4JLoggingHelper.class).warn("Unknown logging level {}",level);
		return NotLogging.INSTANCE;
	}
	protected void formatBindings(Object[] bindings) {
		if (bindings==null) { return; }
		for (int i=0;i<bindings.length;i++) {
			if (bindings[i]==null) { bindings[i]="NULL"; continue; }
			Object obj=bindings[i];
			if (obj instanceof String) { bindings[i]="'"+(String)obj+"'"; }
			else if (obj instanceof java.util.Date) { bindings[i]=dateFormat.format((java.util.Date)obj); }
			else { bindings[i]=String.valueOf(obj); }
		}
	}
	
	public PreparedStatement prepareStatement(LogLevel level, String sql) throws SQLException {
		final InternalLogger logger=logger(level);
		logger.log(LOG_PREPARE_STATEMENT, sql);
		PreparedStatement stm=connection.prepareStatement(sql);
		if (logger!=NotLogging.INSTANCE) {
			stm=new LoggedPreparedStatement(stm,sql) {
				@Override
				protected void log(String method) {
					Object[] bound=getBound();
					formatBindings(bound);
					logger.log(method+": "+sql.replace("?", "{}"), bound);
				}
			};
		}
		return stm;
	}
	public CallableStatement prepareCall(LogLevel level, String sql) throws SQLException {
		final InternalLogger logger=logger(level);
		logger.log(LOG_PREPARE_STATEMENT, sql);
		CallableStatement stm=connection.prepareCall(sql);
		if (logger!=NotLogging.INSTANCE) {
			stm=new LoggedCallableStatement(stm,sql) {
				@Override
				protected void log(String method) {
					Object[] bound=getBound();
					formatBindings(bound);
					logger.log(method+": "+sql.replace("?", "{}"), bound);
				}
			};
		}
		return stm;
	}
	
	private interface InternalLogger {
		void log(String message, Object... args);
	}
	
	private static class NotLogging implements InternalLogger {
		public static final NotLogging INSTANCE=new NotLogging();
		@Override
		public void log(String message, Object... args) {
		}
	}
}
