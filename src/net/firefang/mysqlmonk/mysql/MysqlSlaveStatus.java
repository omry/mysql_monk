package net.firefang.mysqlmonk.mysql;

/**
 * @author Christopher Donaldson <chrisdonaldson@gmail.com>
 * @date Aug 30, 2010
 */
public class MysqlSlaveStatus {
 
  	int lastErrno;
  	String lastError;
  	boolean slaveIoRunning;
  	boolean slaveSqlRunning;
  	long secondsBehindMaster;
	
  	public MysqlSlaveStatus() {
		this.lastErrno = 0;
		this.lastError = "";
		this.slaveIoRunning = false;
		this.slaveSqlRunning = false;
		this.secondsBehindMaster = 0;
	}
  	public MysqlSlaveStatus(int lastErrno, String lastError,
			boolean slaveIoRunning, boolean slaveSqlRunning,
			long secondsBehindMaster) {
		super();
		this.lastErrno = lastErrno;
		this.lastError = lastError;
		this.slaveIoRunning = slaveIoRunning;
		this.slaveSqlRunning = slaveSqlRunning;
		this.secondsBehindMaster = secondsBehindMaster;
	}
	public int getLastErrno() {
		return lastErrno;
	}
	public void setLastErrno(int lastErrno) {
		this.lastErrno = lastErrno;
	}
	public String getLastError() {
		return lastError;
	}
	public void setLastError(String lastError) {
		this.lastError = lastError;
	}
	public boolean isSlaveIoRunning() {
		return slaveIoRunning;
	}
	public void setSlaveIoRunning(boolean slaveIoRunning) {
		this.slaveIoRunning = slaveIoRunning;
	}
	public boolean isSlaveSqlRunning() {
		return slaveSqlRunning;
	}
	public void setSlaveSqlRunning(boolean slaveSqlRunning) {
		this.slaveSqlRunning = slaveSqlRunning;
	}
	public long getSecondsBehindMaster() {
		return secondsBehindMaster;
	}
	public void setSecondsBehindMaster(long secondsBehindMaster) {
		this.secondsBehindMaster = secondsBehindMaster;
	}
  	
  	
	
	
}
