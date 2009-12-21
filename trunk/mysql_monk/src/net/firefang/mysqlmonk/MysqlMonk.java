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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import net.firefang.swush.Swush;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.xml.DOMConfigurator;
import org.logicalcobwebs.proxool.ProxoolException;
import org.logicalcobwebs.proxool.ProxoolFacade;

/**
 * @author omry 
 * @date Aug 10, 2009
 */
public class MysqlMonk 
{
	public static Logger logger = Logger.getLogger(MysqlMonk.class);
	
	int checkInterval;
	int updateInterval;
	
	private Set<String> s_hosts;
	private Map<String, ServerDef> s_serversMap;
	private Map<String, ServerDef> s_aliasesMap;
	private List<EventHandler> eventHandlers;

	private Thread m_checkerThread;

	private Thread m_masterUpdater;
	
	private boolean m_running = true;

	public static void main(String[] args) throws Exception
	{
		new MysqlMonk(args);
	}
	
	public MysqlMonk(String args[]) throws Exception
	{
		DOMConfigurator.configure("conf/log4j.xml");
		
		CmdLineParser p = new CmdLineParser();
		p.addStringOption('c', "conf");
		p.addBooleanOption("install");
		p.addBooleanOption("list");
		p.addStringOption("server");
		p.parse(args);
		
		String conf = (String) p.getOptionValue("conf", "conf/monk.conf");
		loadConfiguration(conf);
		
		boolean install = (Boolean)p.getOptionValue("install", Boolean.FALSE);
		boolean list = (Boolean)p.getOptionValue("list", Boolean.FALSE);
		if (install)
		{
			String sid = (String)p.getOptionValue("server", -1);
			if (sid == null) throw new RuntimeException("server not specified");
			
			ensureInstalled(sid);
			
			return;
		}
		else
		if (list)
		{
			List<String> servers = new ArrayList<String>(s_serversMap.keySet());
			Collections.sort(servers);
			for(String s : servers)
			{
				ServerDef server = s_serversMap.get(s);
				boolean hasAlias = s_aliasesMap.containsKey(server.host);
				logger.info(s  + (hasAlias ? " (alias = " + server.host + ")" : ""));
			}
		}
		else
		{
			monitor();
		}

	}

	private boolean isInstalled(String sid) throws SQLException
	{
		ServerDef server = s_serversMap.get(sid);
		Connection c = DriverManager.getConnection(server.getConnectionAlias());
		try
		{
			return isInstalled(c, server.dbName);
		}
		finally
		{
			c.close();
		}				
	}
	
	private boolean isInstalled(Connection c, String dbName) throws SQLException
	{
		return getVar(c, "SHOW TABLES IN "+dbName+" LIKE 'mysql_monk'") != null;
	}
	
	private void ensureInstalled(String sid) throws SQLException
	{
		ServerDef server = getServer(sid);
		Connection c = DriverManager.getConnection(server.getConnectionAlias());
		try
		{
			if (isInstalled(sid))
			{
				String create = "CREATE TABLE IF NOT EXISTS " + server.dbName + ".mysql_monk ("
				+ "master_id INT NOT NULL ,"
				+ "last_update DATETIME NOT NULL ,"
				+ "PRIMARY KEY ( master_id )"
				+ ") ENGINE = InnoDB";
				query(c, create);
			}
		}
		finally
		{
			c.close();
		}
	}

	private void monitor() throws Exception
	{
		startUpdateThread();
		startChecking();
	}

	private void startChecking()
	{
		ExecutorService pool = getThreadPool(s_serversMap.size(), "SlaveChecker_");
		logger.info("Starting checker thread, checking every " + checkInterval + " seconds");
		while(m_running)
		{
			final CountDownLatch latch = new CountDownLatch(s_serversMap.size());
			for (final ServerDef s : s_serversMap.values())
			{
				if (s.isSlave)
				{
					pool.execute(new Runnable()
					{
						public void run() 
						{
							long now = System.currentTimeMillis() / 1000;
							try
							{
								Connection c = DriverManager.getConnection(s.getConnectionAlias());
								
								if (!s.m_installed)
								{
									logger.debug("Ensuring mysql_monk table is installed in " + s.niceName());
									ensureInstalled(s.getID());
									s.m_installed = true;
								}
								
								try
								{
									ServerDef master = getServer(s.master);
									logger.debug("Checking lag in " + s.niceName());
									String lastUpdate = getVar(c, "SELECT UNIX_TIMESTAMP(last_update) FROM " + s.dbName + ".mysql_monk WHERE master_id = " + master.mysqlServerID);
									if (lastUpdate != null)
									{
										long slaveUpdateTime = Long.parseLong(lastUpdate);
										long masterUpdateTime = master.updateTime;
										if (masterUpdateTime != -1) // master was actually updated
										{
											long oldLag = s.slaveLag; 
											s.slaveLag =  masterUpdateTime - slaveUpdateTime;
											s.maxLagSeen = Math.max(s.maxLagSeen, s.slaveLag);
											
											if (oldLag != -1)
											{
												if (oldLag < s.m_maxAllowedLag && s.slaveLag >= s.m_maxAllowedLag)
												{
													_lagStarted(s, s.niceName() + " is lagging behind master " + master.niceName() + " by more than the allowed " + s.m_maxAllowedLag  + " seconds lag for this server");
												}
												
												if (oldLag >= s.m_maxAllowedLag && s.slaveLag < s.m_maxAllowedLag)
												{
													_lagEnded(s, "Server " + s.niceName() + " is no longer lagging behind master " + master.niceName() + ", worse lag seen was " + s.maxLagSeen + " seconds");
													s.maxLagSeen = 0;
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
							finally
							{
								latch.countDown();
							}
						}
					});
				}
			}
			
			try
			{
				latch.await();
			}
			catch (InterruptedException e1)
			{
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

	
	private void startUpdateThread()
	{
		m_masterUpdater = new Thread("Master update thread")
		{
			@Override
			public void run()
			{
				logger.info("Updating masters every " + updateInterval + " seconds");
				
				while(m_running)
				{
					long now = System.currentTimeMillis() / 1000;
					for (ServerDef s : s_serversMap.values())
					{
						if (s.isMaster)
						{
							try
							{
								if (!s.m_installed)
								{
									ensureInstalled(s.getID());
									s.m_installed = true;
								}
								
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
		};
		
		m_masterUpdater.start();
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

	private void loadConfiguration(String name) throws Exception
	{
		logger.info("Loading " + name);
		
		s_hosts = new HashSet<String>();
		
		Swush conf = new Swush(new File(name));
		
		updateInterval = conf.selectIntProperty("mysql_monk.monitor.update_interval", 10);
		int defaultMaxAllowedLag =  conf.selectIntProperty("mysql_monk.monitor.max_allowed_lag", 20);
		checkInterval = conf.selectIntProperty("mysql_monk.monitor.check_interval", 5);
		
		List<Swush> servers = conf.select("mysql_monk.server");
		s_serversMap = new HashMap<String, ServerDef>();
		s_aliasesMap = new HashMap<String, ServerDef>();
		
		Set<String> removed = new HashSet<String>(); 
		
		for(Swush sw : servers)
		{
			ServerDef s = new ServerDef(sw, defaultMaxAllowedLag);
			String id = s.getID();
			ServerDef old = s_serversMap.put(id, s);
			if (old != null) throw new IllegalArgumentException("Duplicate server with id " + id);
			
			ServerDef ss = s_aliasesMap.get(s.host);
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
		
		
		for(ServerDef s : s_serversMap.values())
		{
			String masterID = s.master;
			if (masterID != null)
			{
				if (isServer(masterID))
				{
					ServerDef master = getServer(masterID);
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
			logger.info("Added server : " + s);
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

	private ServerDef getServer(String id)
	{
		ServerDef server = s_serversMap.get(id);
		if (server == null)
			server = s_aliasesMap.get(id);
		
		if (server == null) throw new IllegalArgumentException("Server not found, id " + id + ", use --list to view available servers");
		return server;
	}
	
	private boolean isServer(String id)
	{
		ServerDef server = s_serversMap.get(id);
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
	
	void _lagStarted(ServerDef server, String message)
	{
		for(EventHandler handler : eventHandlers)
		{
			handler.lagStarted(server, message);
		}
	}
	
	void _lagEnded(ServerDef server, String message)
	{
		for(EventHandler handler : eventHandlers)
		{
			handler.lagEnded(server, message);
		}
	}
	
	void _error(ServerDef server, String message, Exception ex)
	{
		logger.info("Error in " + server.niceName()  + " : " + server);
		for(EventHandler handler : eventHandlers)
		{
			handler.error(server, message, ex);
		}	
	}
	
	void _clearError(ServerDef server, String message)
	{
		logger.info("Error cleared in " + server.niceName()  + " : " + server);
		for(EventHandler handler : eventHandlers)
		{
			handler.clearError(server, message);
		}	
	}

	public void stop() throws Exception
	{
		m_running = false;
		m_checkerThread.interrupt();
		m_masterUpdater.interrupt();
		
		try
		{
			m_checkerThread.join();
		}
		catch (InterruptedException e)
		{
		}
		
		try
		{
			m_masterUpdater.join();
		}
		catch (InterruptedException e)
		{
		}
		
		
		logger.info("Stopping jetty server");
	}
	
	private ExecutorService getThreadPool(int nt, final String namePrefix)
	{
		return Executors.newFixedThreadPool(nt, new ThreadFactory()
		{
			int id = 0;
			@Override
			public Thread newThread(final Runnable r)
			{
				return new Thread(namePrefix + (id++))
				{
					@Override
					public void run()
					{
						r.run();
					}
				};
			}
		});
	}
	
}
