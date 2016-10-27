package com.jianfa.flume.sink.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

public class MysqlSink extends AbstractSink implements Configurable {
	private static final Logger LOG = LoggerFactory.getLogger(MysqlSink.class);
	
	//数据库IP地址
	private String hostname = "localhost";
	//数据库端口号
	private int port = 3306;
	//库名
	private String database = "mysql";
	//表名
	private String table;
	//用户名
	private String user;
	//密码
	private String password;
	//批量提交的数量
	private int batchSize = 300;
	//连接编码方式
	private String encode = "UTF8";
	//写入数据的列名
	private String columnName;

	//数据库连接
	private Connection conn;
	//执行计划
	private PreparedStatement preparedStatement;
	private SinkCounter sinkCounter;
	
	public MysqlSink() {
	}
	
	@Override
	public Status process() throws EventDeliveryException {
		Status result = Status.READY;
		Channel channel = getChannel();
		Transaction transaction = channel.getTransaction();
		Event event;
		String content;
		List<String> actions = Lists.newArrayList();
		transaction.begin();
		try {
			for (int i=0; i<batchSize; i++) {
				event = channel.take();
				if (event == null) {
					result = Status.BACKOFF;
					break;
				}
				content = new String(event.getBody());
				actions.add(content);
			}
			
			if (actions.size() == 0) {
				sinkCounter.incrementBatchEmptyCount();
			} else if (actions.size() == batchSize) {
		        sinkCounter.incrementBatchCompleteCount();
		    } else {
		        sinkCounter.incrementBatchUnderflowCount();
		    }
		    sinkCounter.addToEventDrainAttemptCount(actions.size());
			
			if (actions.size() > 0) {
				preparedStatement.clearBatch();
				for (String msg : actions) {
					if (msg == null || msg.isEmpty()) continue;
					preparedStatement.setString(1, msg);
					preparedStatement.addBatch();
				}
				preparedStatement.executeBatch();
				conn.commit();
			    sinkCounter.addToEventDrainSuccessCount(actions.size());
			}
			transaction.commit();
		} catch (Throwable e) {
			try {
				transaction.rollback();
			} catch (Exception e1) {
				LOG.error("Exception in rollback. Rollback might not hvae been successful.", e1);
			}
			LOG.error("Failed to commit transaction, Transaction rolled back:"+actions.size(), e);
			Throwables.propagate(e);
		} finally {
			transaction.close();
		}
		return result;
	}
	@Override
	public void configure(Context context) {
		hostname = context.getString(Config.MYSQL_HOSTNAME, "localhost");
		Log.info("warning : "+Config.MYSQL_HOSTNAME+" no be set,default value localhost ");
		port = context.getInteger(Config.MYSQL_PORT, 3306);
		Log.info("warning : "+Config.MYSQL_PORT+" no be set,default value 3306 ");
		database = context.getString(Config.MYSQL_DATABASE);
		Preconditions.checkNotNull(database, Config.MYSQL_DATABASE+" must be set!");
		table = context.getString(Config.MYSQL_TABLE);
		Preconditions.checkNotNull(table, Config.MYSQL_TABLE+" must be set!");
		user = context.getString(Config.MYSQL_USER);
		Preconditions.checkNotNull(user, Config.MYSQL_USER+" must be set!");
		password = context.getString(Config.MYSQL_PASSWORD);
		Preconditions.checkNotNull(password, Config.MYSQL_PASSWORD+" must be set!");
		batchSize = context.getInteger(Config.MYSQL_BATCHSIZE, 300);
		Log.info("warning : "+Config.MYSQL_BATCHSIZE+" no be set,default value 300 ");
		encode = context.getString(Config.MYSQL_ENCODE, "UTF8");
		Log.info("warning : "+Config.MYSQL_ENCODE+" no be set,default value UTF8 ");
		columnName = context.getString(Config.MYSQL_COLUMN, "content");
		Log.info("warning : "+Config.MYSQL_COLUMN+" no be set,default value content ");
		
		if (sinkCounter == null) {
			sinkCounter = new SinkCounter(getName());
		}
	}

	@Override
	public synchronized void start() {
		sinkCounter.start();
		super.start();
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (Exception e) {
			e.printStackTrace();
		}
		String url = "jdbc:mysql://"+hostname+":"+port+"/"+database+"?useUnicode=true&characterEncoding="+encode;
		
		try {
			conn = DriverManager.getConnection(url, user, password);
			conn.setAutoCommit(false);
			preparedStatement = conn.prepareStatement("insert into "+table + "("+columnName+") values (?)");
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	@Override
	public synchronized void stop() {
		if (preparedStatement != null) {
			try {
				preparedStatement.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		if (conn != null) {
			try {
				conn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		sinkCounter.stop();
		super.stop();
	}
	
	
}
