package net.firefang.mysqlmonk;

public class MysqlProc
{
	public int id;
	public String user;
	public String host;
	public String db;
	public String command;
	public int time;
	public String state;
	public String info;
	
	@Override
	public String toString()
	{
		return id + " " + user + " " + host + " " + db + " " + command + " " + time + " " + state + " " + info; 
	}

	public String getHostName()
	{
		int i = host.indexOf(':');
		if (i == -1)
			return host;
		else 
			return host.substring(0, i);
	}
}