package net.firefang.mysqlmonk;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
}
