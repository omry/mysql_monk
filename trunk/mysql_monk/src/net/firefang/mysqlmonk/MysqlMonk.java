package net.firefang.mysqlmonk;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import net.firefang.swush.Swush;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.logicalcobwebs.proxool.ProxoolException;
import org.logicalcobwebs.proxool.ProxoolFacade;

/**
 * @author omry 
 * @date Aug 10, 2009
 */
public class MysqlMonk 
{
	static Logger logger = Logger.getLogger(MysqlMonk.class);
	
	static int checkInterval;
	static int updateInterval;
	static int maxAllowedLag;
	
	private static Set<String> s_hosts;
	private static Map<String, Server> s_serversMap;
	private static Map<String, Server> s_aliasesMap;
	private static List<EventHandler> eventHandlers;

	public static void main(String[] args) throws Exception
	{
		configureLog4J();
		
		CmdLineParser p = new CmdLineParser();
		p.addStringOption('c', "conf");
		p.addBooleanOption("install");
		p.addBooleanOption("list");
		p.addStringOption("server");
		p.parse(args);
		
		String conf = (String) p.getOptionValue("conf", "monk.conf");
		loadConfiguration(conf);
		
		boolean install = (Boolean)p.getOptionValue("install", Boolean.FALSE);
		boolean list = (Boolean)p.getOptionValue("list", Boolean.FALSE);
		if (install)
		{
			String sid = (String)p.getOptionValue("server", -1);
			if (sid == null) throw new RuntimeException("server not specified");
			
			installInto(sid);
			
			return;
		}
		else
		if (list)
		{
			List<String> servers = new ArrayList<String>(s_serversMap.keySet());
			Collections.sort(servers);
			for(String s : servers)
			{
				Server server = s_serversMap.get(s);
				boolean hasAlias = s_aliasesMap.containsKey(server.host);
				logger.info(s  + (hasAlias ? " (alias = " + server.host + ")" : ""));
			}
		}
		else
		{
			monitor();
		}
	}

	private static boolean isInstalled(String sid) throws SQLException
	{
		Server server = getServer(sid);
		Connection c = DriverManager.getConnection(server.getConnectionAlias());
		try
		{
			String s = getVar(c, "SHOW TABLES LIKE 'myslq_monk'");
			return s != null;
		}
		finally
		{
			c.close();
		}		
	}
	
	private static void installInto(String sid) throws SQLException
	{
		Server server = getServer(sid);
		logger.info("Installing into master server " + server);
		
		Connection c = DriverManager.getConnection(server.getConnectionAlias());
		try
		{
			
			String create = "CREATE TABLE IF NOT EXISTS " + server.dbName + ".mysql_monk ("
					 + "master_id INT NOT NULL ,"
					 + "last_update DATETIME NOT NULL ,"
					 + "PRIMARY KEY ( master_id )"
					 + ") ENGINE = InnoDB";
			query(c, create);
		}
		finally
		{
			c.close();
		}
	}

	private static void monitor() throws SQLException
	{
		ensuredEnstalled();
		startUpdateThread();
		startCheckingThread();
	}

	private static void ensuredEnstalled() throws SQLException
	{
		for (Server s : s_serversMap.values())
		{
			if (!isInstalled(s.getID()))
			{
				installInto(s.getID());
			}
		}
	}

	private static void startCheckingThread()
	{
		new Thread("Slave checker thread")
		{
			@Override
			public void run()
			{
				logger.info("Starting checker thread, max allowed replication lag is " + maxAllowedLag + " seconds, checking every " + checkInterval + " seconds");
				while(true)
				{
					
					long now = System.currentTimeMillis() / 1000;
					for (Server s : s_serversMap.values())
					{
						if (s.isSlave)
						{
							try
							{
								Connection c = DriverManager.getConnection(s.getConnectionAlias());
								try
								{
									Server master = getServer(s.master);
									String lastUpdate = getVar(c, "SELECT UNIX_TIMESTAMP(last_update) FROM " + s.dbName + ".mysql_monk WHERE master_id = " + master.mysqlServerID);
									if (lastUpdate != null)
									{
										long slaveUpdateTime = Long.parseLong(lastUpdate);
										long masterUpdateTime = master.updateTime;
										if (masterUpdateTime != -1) // master was actually updated
										{
											long oldLag = s.slaveLag; 
											s.slaveLag = masterUpdateTime - slaveUpdateTime;
											
											if (oldLag != -1)
											{
												if (oldLag < maxAllowedLag && s.slaveLag >= maxAllowedLag)
												{
													_lagStarted(s, "Server " + s.niceName() + " is lagging behind master " + master.niceName() + " by " + s.slaveLag + " seconds");
												}
												
												if (oldLag >= maxAllowedLag && s.slaveLag < maxAllowedLag)
												{
													_lagEnded(s, "Server " + s.niceName() + " is no longer lagging behind master " + master.niceName());
												}
											}
										}
									}
								}
								finally
								{
									c.close();
								}
								s.updateTime = now;
								
								if (s.inError)
								{
									s.inError = false;
									_clearError(s, "Error cleared");
								}
							}
							catch (SQLException e)
							{
								if (!s.inError)
								{
									_error(s, "SQLException", e);
									s.inError = true;
								}
							}
						}
					}
					
					try
					{
						Thread.sleep(checkInterval * 1000);
					}
					catch (InterruptedException e)
					{
					}
				}
			}
		}.start();
	}

	private static void startUpdateThread()
	{
		new Thread("Master update thread")
		{
			@Override
			public void run()
			{
				logger.info("Updating masters every " + updateInterval + " seconds");
				
				while(true)
				{
					
					long now = System.currentTimeMillis() / 1000;
					for (Server s : s_serversMap.values())
					{
						if (s.isMaster)
						{
							try
							{
								Connection c = DriverManager.getConnection(s.getConnectionAlias());
								try
								{
									String mysqlServerID = getServerID(c);
									if (mysqlServerID == null)
									{
										if (!s.inError)
										{
											_error(s, "server_id not configured", null);
											s.inError = true;
										}
									}
									else
									{
										query(c, "REPLACE INTO " + s.dbName + ".mysql_monk(master_id,last_update) VALUES("+mysqlServerID+", FROM_UNIXTIME("+now+"))");
										s.mysqlServerID = Integer.parseInt(mysqlServerID);
									}
								}
								finally
								{
									c.close();
								}
								s.updateTime = now;
								if (s.inError)
								{
									s.inError = false;
									_clearError(s, "Error cleared");
								}								
							}
							catch (SQLException e)
							{
								if (!s.inError)
								{
									_error(s, "SQLException", e);
									s.inError = true;
								}
								
							}
						}
					}
					
					try
					{
						Thread.sleep(updateInterval * 1000);
					}
					catch (InterruptedException e)
					{
					}
				}
			}
		}.start();
	}

	static String getServerID(Connection c) throws SQLException
	{
		return getVar(c, "SELECT @@server_id");
	}

	private static String getVar(Connection c, String sql) throws SQLException
	{
		Statement st = c.createStatement();
		try
		{
			ResultSet res = st.executeQuery(sql);
			if (res.next())
			{
				return res.getString(1);
			}
		}
		finally
		{
			st.close();
		}
		return null;
	}

	private static void loadConfiguration(String name) throws Exception
	{
		logger.info("Loading " + name);
		
		s_hosts = new HashSet<String>();
		
		Swush conf = new Swush(new File(name));
		
		updateInterval = conf.selectIntProperty("mysql_monk.monitor.update_interval", 10);
		maxAllowedLag =  conf.selectIntProperty("mysql_monk.monitor.max_allowed_lag", 20);
		checkInterval = conf.selectIntProperty("mysql_monk.monitor.check_interval", 5);
		
		List<Swush> servers = conf.select("mysql_monk.server");
		s_serversMap = new HashMap<String, Server>();
		s_aliasesMap = new HashMap<String, Server>();
		
		Set<String> removed = new HashSet<String>(); 
		
		for(Swush sw : servers)
		{
			Server s = new Server(sw);
			String id = s.getID();
			Server old = s_serversMap.put(id, s);
			if (old != null) throw new IllegalArgumentException("Duplicate server with id " + id);
			
			Server ss = s_aliasesMap.get(s.host);
			if (ss != null)
			{
				removed.add(s.host); // host run multiple servers, shoud use fully qualified id (host_port) to access.
			}
			else
			{
				s_aliasesMap.put(s.host, s);
			}
			
			s_hosts.add(s.host);
		}
		
		for(String rem : removed)
		{
			s_aliasesMap.remove(rem);
		}
		
		
		for(Server s : s_serversMap.values())
		{
			String masterID = s.master;
			if (masterID != null)
			{
				if (isServer(masterID))
				{
					Server master = getServer(masterID);
					master.isMaster = true;
					s.isSlave = true;
				}
				else
				{
					if (s_hosts.contains(masterID))
					{
						throw new RuntimeException(masterID + " is not unique because there is more than one server on this host, use fully qualified id (host_port)");
					}
				}
			}
			
			registerConnectionPool(s.getConnectionAlias(), s.host, s.port, s.dbName, s.user, s.password);
		}
		
		eventHandlers = new ArrayList<EventHandler>();
		for(Swush sw : conf.select("mysql_monk.handlers.handler"))
		{
			String clazz = sw.selectProperty("handler.class");
			EventHandler handler = (EventHandler) Class.forName(clazz).newInstance();
			handler.init(sw);
			eventHandlers.add(handler);
		}
	}

	private static Server getServer(String id)
	{
		Server server = s_serversMap.get(id);
		if (server == null)
			server = s_aliasesMap.get(id);
		
		if (server == null) throw new IllegalArgumentException("Server not found, id " + id + ", use --list to view available servers");
		return server;
	}
	
	private static boolean isServer(String id)
	{
		Server server = s_serversMap.get(id);
		if (server == null)
			server = s_aliasesMap.get(id);
		
		if (server == null) 
			return false;
		
		return true;
	}
	
	
	public static void registerConnectionPool(String alias, String server, int port, String dbname, String username, String password) throws ProxoolException
	{
		try
		{
			Class.forName("org.logicalcobwebs.proxool.ProxoolDriver");
		}
		catch (ClassNotFoundException e)
		{
			throw new ProxoolException(e);
		}
		Properties info = new Properties();
		info.setProperty("proxool.maximum-connection-count", "10");
		info.setProperty("proxool.house-keeping-test-sql", "select VERSION()");
		info.setProperty("user", username);
		info.setProperty("password", password);
		info.setProperty("proxool.driver-class", "com.mysql.jdbc.Driver");
		String url = alias + ":" + "com.mysql.jdbc.Driver" + ":jdbc:mysql://" + server + ":"+port+"/" + dbname + "?characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull";
		try
		{
			ProxoolFacade.registerConnectionPool(url, info);
		}
		catch (Exception e)
		{
			logger.error(e);
		}
	}
	
	public static void query(Connection conn, String sql) throws SQLException
	{
		Statement st = null;
		try
		{
			st = conn.createStatement();
			st.execute(sql);
		}
		finally
		{
			if (st != null) st.close();
		}
	}
	
	
	public static void configureLog4J()
	{
	    Logger root = Logger.getRootLogger();
    	root.setLevel(Level.DEBUG);
    	ConsoleAppender consoleAppender = new ConsoleAppender(new PatternLayout("%d{HH:mm:ss.SSS} %c{2} [%t] %p - %m%n"));
    	consoleAppender.setName("CONSOLE");
    	if (root.getAppender("CONSOLE") == null)
    		root.addAppender(consoleAppender);
    	Logger.getLogger("org.logicalcobwebs").setLevel(Level.WARN);
	}
	
	static void _lagStarted(Server server, String message)
	{
		for(EventHandler handler : eventHandlers)
		{
			handler.lagStarted(server, message);
		}
	}
	
	static void _lagEnded(Server server, String message)
	{
		for(EventHandler handler : eventHandlers)
		{
			handler.lagEnded(server, message);
		}
	}
	
	static void _error(Server server, String message, Exception ex)
	{
		for(EventHandler handler : eventHandlers)
		{
			handler.error(server, message, ex);
		}	
	}
	
	static void _clearError(Server server, String message)
	{
		for(EventHandler handler : eventHandlers)
		{
			handler.clearError(server, message);
		}	
	}
}
