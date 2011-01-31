package net.firefang.mysqlmonk;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.firefang.mysqlmonk.email.EmailHandler;

public class Utils
{
	public static String formatTimeLengthSec(long sec)
	{
		return formatTimeLength(sec * 1000);
	}
	
	public static String formatTimeLength(long ms)
	{
		int SECOND = 1000;
		int MINUTE = SECOND * 60;
		int HOUR = MINUTE * 60;
		int DAY = HOUR * 24;
		if (ms < SECOND) return ms + " ms";
		if (ms < MINUTE) return round((ms / (float)SECOND),2) + " secs";
		if (ms < HOUR) return round((ms /  (float)MINUTE),2) + " mins";
		if (ms < DAY) return round((ms / (float)HOUR ),2)+ " hours";
		else
			return round((ms / (float)DAY), 2) + " days";
		
	}
	
	private static String round(float f, int n)
	{
		int d = (int) Math.pow(10, n);
		return ""+ ((int)(f * d))/(float)d;
	}
	
	public static List<String> getSortedKeys(Map<?,?> p)
	{
		List<String> list = new ArrayList<String>(p.size());
		for (Object key : p.keySet())
		{
			list.add("" + key);
		}
		Collections.sort(list);
		return list;
	}
	
	public static List<String> getSorted(Set<String> p)
	{
		List<String> list = new ArrayList<String>(p.size());
		for (Iterator<String> i = p.iterator();i.hasNext();)
		{
			list.add(i.next());
		}
		Collections.sort(list);
		return list;
	}
	
	public static String rpad(String s, int len, char c)
	{
		if (s == null) s = "null";
		if (s.length() < len)
		{
			StringBuilder sb = new StringBuilder(len);
			sb.append(s);
			for(int i=s.length();i<len;i++) 
				sb.append(c);
			return sb.toString();
		}
		else
			return s;
	}
	
	public static List<MysqlProc> getProcList(ServerDef server) throws SQLException
	{
		Connection c = null;
		try
		{
			c = DriverManager.getConnection(server.getConnectionAlias());
			return getProcList(c);
		}
		finally
		{
			if (c != null)
				c.close();
		}
	}

	public static List<MysqlProc> getProcList(Connection c) throws SQLException
	{
		List<MysqlProc> res = new ArrayList<MysqlProc>();
		Statement st = c.createStatement();
		ResultSet rs = null;
		try
		{
			st.execute("SHOW PROCESSLIST");
			rs = st.getResultSet();
			while(rs.next())
			{
				MysqlProc p = new MysqlProc();
				p.id = rs.getInt("id");
				p.user = rs.getString("user");
				p.host = rs.getString("host");
				p.db = rs.getString("db");
				p.command = rs.getString("command");
				p.time = rs.getInt("time");
				p.state = rs.getString("state");
				p.info = rs.getString("info");
				res.add(p);
			}
		}
		finally
		{
			if (rs != null)
				rs.close();
			st.close();
		}
		return res;
	}

	public static String getProcTable(List<MysqlProc> proclist)
	{
		StringBuffer sb = new StringBuffer();
		Set<String> commands = new HashSet<String>();
		Map<String, Map<String, Integer>> host2CommandCount = new HashMap<String, Map<String,Integer>>();
		EmailHandler.crunchProcessList(proclist, commands, host2CommandCount);
		
		sb.append("\n");
		sb.append("Mysql process list when this messages was triggered : \n");
		sb.append(rpad("id", 8, ' '));
		sb.append(rpad("user", 14, ' '));
		sb.append(rpad("host", 30, ' '));
		sb.append(rpad("db", 16, ' '));
		sb.append(rpad("command", 20, ' '));
		sb.append(rpad("time", 9, ' '));
		sb.append(rpad("state", 16, ' '));
		sb.append(rpad("info", 8, ' '));
		sb.append("\n");
		for(MysqlProc p : proclist)
		{
			if (p.command.equals("Sleep")) continue;
			sb.append(rpad(""+p.id, 8, ' '));
			sb.append(rpad(p.user	, 14, ' '));
			sb.append(rpad(p.host	, 30, ' '));
			sb.append(rpad(p.db	, 16, ' '));
			sb.append(rpad(p.command, 20, ' '));
			sb.append(rpad(""+p.time	, 9, ' '));
			sb.append(rpad(p.state	, 16, ' '));
			sb.append(p.info);
			sb.append("\n");
		}
		
		sb.append("\n");
		if (host2CommandCount.size() > 0)
		{
			sb.append(rpad("hostname", 30, ' '));
			for(String command : getSorted(commands))
			{
				sb.append(rpad(command, 16, ' '));
			}
			sb.append("\n");
			for(String hostname : getSortedKeys(host2CommandCount))
			{
				sb.append(rpad(hostname, 30, ' '));
				Map<String, Integer> mm = host2CommandCount.get(hostname);
				for(String command : getSorted(commands))
				{
					Integer ii = mm.get(command);
					sb.append(rpad("" + (ii != null ? ii : " "), 16, ' '));
				}
				sb.append("\n");
			}
		}
		
		String table = sb.toString();
		return table;
	}
}
