package net.firefang.mysqlmonk;

import java.util.Arrays;

import org.tanukisoftware.wrapper.WrapperListener;
import org.tanukisoftware.wrapper.WrapperManager;

public class Service implements WrapperListener
{
	MysqlMonk m_monk;
	
	public Service(String[] args)
	{
		WrapperManager.start(this, args);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args)
	{
		new Service(args);
	}

	@Override
	public void controlEvent(int event)
	{
        if ((event == WrapperManager.WRAPPER_CTRL_LOGOFF_EVENT)
				&& (WrapperManager.isLaunchedAsService() || WrapperManager.isIgnoreUserLogoffs()))
		{
			// Ignore
		} 
        else
		{
			WrapperManager.stop(0);
			// Will not get here.
		}
	}

	@Override
	public Integer start(final String[] args)
	{
		MysqlMonk.logger.info("Service.start("+Arrays.asList(args)+")");
		try
		{
			m_monk = new MysqlMonk(args);
			MysqlMonk.logger.info("started");
			return null;
		}
		catch (Exception e)
		{
			MysqlMonk.logger.error("Exception starting mysql monk", e);
			return 1;
		}
	}

	@Override
	public int stop(int arg0)
	{
		System.out.println("Service.stop()");
		if (m_monk != null)
		{
			int t = 30*60*1000;
			System.out.println("WrapperManager.signalStopping("+t+")");
			WrapperManager.signalStopping(t);
			System.out.println("Service.stop()");
			int ret;
			try
			{
				m_monk.stop();
				ret = 0;
			}
			catch (Exception e)
			{
				MysqlMonk.logger.error("Error", e);
				ret = 1;
			}
			System.out.println("WrapperManager.signalStopped()");
			WrapperManager.signalStopped(ret);
			return ret;
		}
		return 0;
	}
}
