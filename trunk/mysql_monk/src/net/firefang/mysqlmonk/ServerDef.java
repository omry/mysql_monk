/**
 * 
 */
package net.firefang.mysqlmonk;

import net.firefang.swush.Swush;

/**
 * @author omry
 * @date Aug 10, 2009
 */
public class ServerDef
{
	long m_maxAllowedLag = 0;
	
	boolean isSlave = false;
	boolean isMaster = false;

	String host;

	int port;

	String dbName;

	String user;

	String password;

	String master;
	
	// if master, the last update time
	long updateTime = -1;

	// if this server is a master, what is it's mysql server_id
	int mysqlServerID = -1; 
	
	// if slave, the lag from the server 
	long slaveLag = 0;
	
	// maximum lag seen in this lag session
	long maxLagSeen;
	
	boolean inError = false;

	protected boolean m_installed = false;
	
	
	ServerDef(Swush s, int defaultMaxAllowedLag)
	{
		host = s.selectProperty("server.host");
		port = s.selectIntProperty("server.port", 3306);
		dbName = s.selectProperty("server.db_name");
		user = s.selectProperty("server.username");
		password = s.selectProperty("server.password");
		master = s.selectProperty("server.master");
		m_maxAllowedLag = s.selectLongProperty("server.max_allowed_lag", defaultMaxAllowedLag);
	}
	
	public String getConnectionAlias()
	{
		return "proxool." + user + "_" + password + "_" + host + "_" + port + "_" + dbName;
	}

	public String toString()
	{
		String s = user + ":" + password + "@" + host + ":" + port + "/" + dbName;
		if (isMaster) s += ", master";
		if (isSlave) s += ", slave (max_lag=" + m_maxAllowedLag + "s)";
		return s;
	}

	public String getID()
	{
		return host + "_" + port;
	}
	
	public String niceName()
	{
		return host + (port != 3306 ? ":" + port : "");
	}
	
	
	
	public boolean isInError() {
		return inError;
	}

	public void setInError(boolean inError) {
		this.inError = inError;
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((dbName == null) ? 0 : dbName.hashCode());
		result = prime * result + ((host == null) ? 0 : host.hashCode());
		result = prime * result + (isMaster ? 1231 : 1237);
		result = prime * result + ((master == null) ? 0 : master.hashCode());
		result = prime * result + ((password == null) ? 0 : password.hashCode());
		result = prime * result + port;
		result = prime * result + ((user == null) ? 0 : user.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		ServerDef other = (ServerDef) obj;
		if (dbName == null)
		{
			if (other.dbName != null) return false;
		}
		else if (!dbName.equals(other.dbName)) return false;
		if (host == null)
		{
			if (other.host != null) return false;
		}
		else if (!host.equals(other.host)) return false;
		if (isMaster != other.isMaster) return false;
		if (master == null)
		{
			if (other.master != null) return false;
		}
		else if (!master.equals(other.master)) return false;
		if (password == null)
		{
			if (other.password != null) return false;
		}
		else if (!password.equals(other.password)) return false;
		if (port != other.port) return false;
		if (user == null)
		{
			if (other.user != null) return false;
		}
		else if (!user.equals(other.user)) return false;
		return true;
	}

	public String getHost() {
		return host;
	}

	
	
	
}
