package net.firefang.mysqlmonk;

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
}
