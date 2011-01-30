package net.firefang.mysqlmonk.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

import net.firefang.mysqlmonk.EventHandler;
import net.firefang.mysqlmonk.MysqlMonk;
import net.firefang.mysqlmonk.ServerDef;
import net.firefang.mysqlmonk.email.EmailHandler;
import net.firefang.mysqlmonk.email.EmailSender;
import net.firefang.swush.Swush;

/**
 * @author Christopher Donaldson <chrisdonaldson@gmail.com>
 * @date Aug 30, 2010
 */
public class MysqlSlaveStatusHandler implements EventHandler{
	public static Logger logger = Logger.getLogger(MysqlSlaveStatusHandler.class);
	
	MysqlMonk mySqlMonk;
	int restartErrorNums[];
	String monitoredServers[];
	
	@Override
	public void setMysqlMonk(MysqlMonk mySqlMonk){
		this.mySqlMonk = mySqlMonk;
	}
	
	@Override
	public void init(Swush sw) throws Exception {
		Object servers[] = sw.selectFirst("handler.servers").asArray();
		monitoredServers = new String[servers.length];
		for (int i =0; i < servers.length; i++){
			monitoredServers[i] = (String)servers[i];
		}
		
		Object restart[] = sw.selectFirst("handler.restart").asArray();
	
		restartErrorNums = new int[restart.length];
		for (int i =0; i < restart.length; i++){
			restartErrorNums[i] = Integer.parseInt((String) restart[i]);
		}
			
	}

	
	@Override
	public void error(ServerDef server, String message, Exception ex) {
		logger.info("Server error: "+ message);
	}

	@Override
	public void clearError(ServerDef server, String message) {
		checkMysqlSlaveStatus(server, message);
	}
	
	@Override
	public void lagEnded(ServerDef server, String message) {
		checkMysqlSlaveStatus(server, message);
	}

	@Override
	public void lagStarted(ServerDef server, String message) {
		checkAndRestartMysqlSlaveStaus(server, message);
	}

	private synchronized void checkAndRestartMysqlSlaveStaus(ServerDef server,String message){
		try {
			if(!isMonitoredServer(server))
				return;
			
			Connection c = DriverManager.getConnection(server.getConnectionAlias());
			try{
				MysqlSlaveStatus slaveStatus = getMysqlSlaveStatus(c);
				if (slaveStatus == null){
					sendSmtpError(server,"Can't get mysql slave status",null);
					return;
				}
				String replicationStatus = getReplicationStatus(slaveStatus); 
				logger.info(replicationStatus);
				if (isMySqlSlaveOk(slaveStatus)){
					return;
				}
				
				String replicationStopMsg = message+"\n"+"Replication stopped or falied on server: "+server.getHost()+"\n"+replicationStatus+"\n"; 
				sendSmtpMessage(server,replicationStopMsg);
				
				if(isRestartErrorNum(slaveStatus.getLastErrno())){
					logger.info("Trying to restart mysql slave on server: "+ server.getHost());
					if (!restartMysqlSlave(c)){
						logger.info("Failed to restart mysql slave on server: " + server.getHost());
						sendSmtpError(server, replicationStopMsg+"Failed to restart mysql slave on server: "+server.getHost(),null);
						return;
					}{
						sleep(1500);
						logger.info("Mysql slave restarted on server: " + server.getHost());
						slaveStatus = getMysqlSlaveStatus(c);
						replicationStatus = getReplicationStatus(slaveStatus); 
						logger.info("Updated status after restarting slave on server: "+ server.getHost()+" "+replicationStatus);
						sendSmtpMessage(server, "Updated status after restarting slave on server: "+ server.getHost()+" \n"+replicationStatus);
					}
					
				}else{
					sendSmtpMessage(server, replicationStopMsg+"\nError code:  "+ slaveStatus.getLastErrno()+" , not configured for restart.");
				}
			}
			finally{
				c.close();
			}
			
		}catch (SQLException e)
		{
			String error = "SQLException";
			if (e instanceof com.mysql.jdbc.CommunicationsException)
			{
				error = "Can't connect to database";
			}
			if (!server.isInError())
			{
				sendSmtpError(server, error, e);
				server.setInError(true);
			}
			
		}catch (InterruptedException e)
		{
			sendSmtpError(server, "Restart slq slave on server: "+server.getHost()+" interepted \n", e);
		}
	}
	
	private synchronized void checkMysqlSlaveStatus(ServerDef server,String message){
		try {
			if(!isMonitoredServer(server))
				return;
			
			Connection c = DriverManager.getConnection(server.getConnectionAlias());
			try{
				MysqlSlaveStatus slaveStatus = getMysqlSlaveStatus(c);
				if (slaveStatus == null){
					sendSmtpError(server,"Can't get mysql slave status",null);
					return;
				}
				
				String replicationStatus = getReplicationStatus(slaveStatus); 
				logger.info(replicationStatus);
				if (isMySqlSlaveOk(slaveStatus)){
					String statusMsg = message+"\n"+"Replication slave on : "+server.getHost()+"\nIs Running\n "+replicationStatus+"\n"; 
					sendSmtpMessage(server, statusMsg);
				}
				else {
					String statusMsg = message+"\n"+"Replication slave on : "+server.getHost()+"\nIs Not Running\n "+replicationStatus+"\n"; 
					sendSmtpError(server, statusMsg,null);
				}
			}
			finally{
				c.close();
			}
				
		}catch (SQLException e)
		{
			String error = "SQLException";
			if (e instanceof com.mysql.jdbc.CommunicationsException)
			{
				error = "Can't connect to database";
			}
			if (!server.isInError())
			{
				sendSmtpError(server, error, e);
				server.setInError(true);
			}
		}
	}
	
	private void sendSmtpError(ServerDef server, String message, Exception ex){
		logger.info("Error in " + server.niceName()  + " : " + server, ex);
		for(EventHandler handler : mySqlMonk.getEventHandlers())
		{
			if (handler instanceof EmailHandler)
				handler.error(server, message, ex);
		}	
	}
	
	private void sendSmtpMessage(ServerDef server, String message){
		logger.info("Request smtp in " + server.niceName()  + " : " + server);
		for(EventHandler handler : mySqlMonk.getEventHandlers())
		{
			if (handler instanceof EmailHandler)
				handler.clearError(server, message);
		}	
	}

	private  MysqlSlaveStatus getMysqlSlaveStatus(Connection c) throws SQLException
	{
		MysqlSlaveStatus slaveStatus = null;
		String sql = "SHOW SLAVE STATUS";
		
		Statement st = c.createStatement();
		try
		{
			ResultSet res = st.executeQuery(sql);
			if (res.next())
			{	
				slaveStatus = new MysqlSlaveStatus();
				slaveStatus.setLastErrno(res.getInt("Last_Errno"));
				slaveStatus.setLastError(res.getString("Last_Error"));
				String slaveIoRunning = res.getString("Slave_IO_Running");
				String slaveSQLRunning = res.getString("Slave_SQL_Running");
				slaveStatus.setSlaveIoRunning(slaveIoRunning.equals("Yes")?true:false);
				slaveStatus.setSlaveSqlRunning(slaveSQLRunning.equals("Yes")?true:false);
				slaveStatus.setSecondsBehindMaster(res.getLong("Seconds_Behind_Master"));
				return slaveStatus;
			}
		}
		finally
		{
			st.close();
		}
		return null;
	}
	
	private  boolean restartMysqlSlave(Connection c) throws SQLException,InterruptedException
	{
		String sqlStopSlave = "STOP SLAVE";
		String sqlStartlave = "START SLAVE";
		
		Statement st = c.createStatement();
		try
		{
			st.executeQuery(sqlStopSlave);
			Thread.sleep(1000);
			st.executeQuery(sqlStartlave);
			st.close();
			return true;
		}
		finally{
			st.close();
		}
	}
	
	private boolean isMySqlSlaveOk(MysqlSlaveStatus slaveStatus){
		if (slaveStatus.isSlaveIoRunning() && slaveStatus.isSlaveSqlRunning())
			return true;
		else
			return false;
	}
	
	private boolean isRestartErrorNum(int errNum){
		for (int i =0; i < restartErrorNums.length; i++){
			if (errNum ==restartErrorNums[i])
				return true;
		}
		return false;
	}
	
	private boolean isMonitoredServer(ServerDef server){
		for (int i =0; i < monitoredServers.length; i++){
			if (server.getHost().equals(monitoredServers[i]))
				return true;
		}
		return false;
	}
	
	private String getReplicationStatus(MysqlSlaveStatus slaveStatus){
		if (slaveStatus == null)
			return "";
		String slaveIORunning = slaveStatus.isSlaveIoRunning() ? "Yes" : "No";
		String slaveSQLRunning = slaveStatus.isSlaveSqlRunning() ? "Yes" : "No";
		
		String replicationStatus = "MySqlSlavestatus: " +
		"Slave_IO_Running: " + slaveIORunning + ", " +
		"Slave_IO_Running: " + slaveSQLRunning + ", " +
		"Last_Errno: " + slaveStatus.getLastErrno()+ ", Last_Error: "+slaveStatus.getLastError();
		return replicationStatus;
	}
	
	private synchronized void sleep(long sleepMs)
	{
		try
		{
			Thread.sleep(sleepMs);
		}
		catch (InterruptedException e)
		{
		}
	}
}
