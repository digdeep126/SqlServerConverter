package com.github.digdeep126;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * sql server数据库在向mysql等数据库迁移数据时，会遇到一个麻烦，sql server导出的datetime的结果是16进制形式的二进制结果，
 * 类似于 CAST(0x00009E0E0095524F AS DateTime)，这样的导出结果是无法直接向mysql数据库中导入的，所以需要对sql server
 * 导出的脚本中的所有的 datetime 字段类型进行转换，转换成mysql等数据库认可的结果：2010-10-13 09:03:39
 * 才能正确的完成sql server数据向mysql等数据库的迁移。
 * 注意本方法只能精确到秒级别，毫秒级别是不精确的。
 * 具体转换原理，参见：reference: http://stackoverflow.com/questions/12033598/cast-hex-as-datatime-how-to-get-date
 * @author digdeep@126.com
 */
public class SqlServerDateTimeUtils {
	private static final Calendar cal = Calendar.getInstance();
	
	static{
		cal.set(Calendar.YEAR, 1900);
		cal.set(Calendar.MONTH, Calendar.JANUARY);
		cal.set(Calendar.DATE, 1);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);	// 1900-01-01 00:00:00
	}

	/**
	 * 将sql server导出的16进制datetime字段： CAST(0x00009E0E0095524F AS DateTime) 形式转换成字符串形式：
	 * 2010-10-13 09:03:39 ，以利于 sql server 向 mysql, oracle 迁移数据 
	 * @param rawDateTime 
	 * @return 
	 * reference: http://stackoverflow.com/questions/12033598/cast-hex-as-datatime-how-to-get-date
	 */
	public static String convertRawDateTimeToStr(String rawDateTime){
		return convertRawToStr(rawDateTime, false);
	}
	

	/**
	 * 将sql server导出的datetime字段结果 CAST(0x00009E0E0095524F AS DateTime)，转换成
	 * 2010-10-13 09:03:39 或者 2010-10-13 09:03:39.394 注意毫秒部分不精确
	 * @param rawDateTime
	 * @param millisecondFlag 结果是否需要带毫秒部分 
	 * @return
	 * reference: http://stackoverflow.com/questions/12033598/cast-hex-as-datatime-how-to-get-date
	 */
	public static String convertRawToStr(String rawDateTime, boolean millisecondFlag) {
		if (rawDateTime == null || rawDateTime.trim().equals(""))
			return null;

		String rawData = rawDateTime.substring("CAST(".length(), rawDateTime.toUpperCase().indexOf(" AS DATETIME"));
		if (rawData == null || rawData.trim().equals(""))	
			return null;
		
		rawData = rawData.trim();
		String result = null;
		
		if(rawData.length() <= 10){		// rowData = "0x993a02CE"
			result = getDateTimeStr4Bytes(rawData);
		}
		if(rawData.length() > 10 && rawData.length() <= 18){	// rowData = "0x00009E0E0095524F"
			result = getDateTimeStr8Bytes(rawData, millisecondFlag);
		}
		return result;
	}
	
	/**
	 * sql server 利用 Small DateTime 4个字节  
	 * select CAST(0x993902CE as  SmallDateTime); 
	 * 2007-05-25 11:58:00 (只精确到分钟？？？)
	 *
	 * mysql:
	 * SELECT "0x993902CE" INTO @raw_data; 
	 * SELECT conv(substr(@raw_data, 3, 4), 16, 10) INTO @days; 
	 * SELECT conv(substr(@raw_data, 7, 4), 16, 10) INTO @minutes;
	 * SELECT "1900-01-01 00:00:00" INTO @start_date; 
	 * SELECT date_add(@start_date, interval @days DAY) INTO @date_plus_years; 
	 * SELECT date_add(@date_plus_years, interval @minutes MINUTE) INTO @final_date;
	 * select @final_date;
 	 * 2007-05-25 11:58:00
	 * @param rawData
	 * @return
	 */
	private static String getDateTimeStr4Bytes(String rawData){
		String day = rawData.substring(2, 2 + 4);	// rowData = "0x993a02CE"
		String minutes = rawData.substring(6, 6 + 4);

		Calendar calendar =  Calendar.getInstance();
		calendar.setTimeInMillis(cal.getTimeInMillis());	// 1900-01-01 00:00:00

		calendar.add(Calendar.DATE, Integer.parseInt(day, 16));
		calendar.add(Calendar.MINUTE, Integer.parseInt(minutes, 16));
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		return sdf.format(calendar.getTime());
	}
	
	/**
	 * sql server DateTime 利用 8 字节表示， 该转换精确到秒，无毫秒部分
	 * @param rawData
	 * @param millisecondFlag 结果是否需要带毫秒部分 
	 * @return 
	 * 
	 */
	private static String getDateTimeStr8Bytes(String rawData, boolean millisecondFlag){
		String day = rawData.substring(2, 2 + 8);			// 4字节表示距离1900年的天数
		String fraction = rawData.substring(10, 10 + 8);	// 4字节表示剩余的部分 faction

		int millis =  (int)(Integer.parseInt(fraction, 16) * 3.33333);	// faction*3.3333 之后表示的是毫秒数
		int seconds= millis / 1000;	// 得到秒数
		
		Calendar calendar =  Calendar.getInstance();
		calendar.setTimeInMillis(cal.getTimeInMillis());	// 1900-01-01 00:00:00

		calendar.add(Calendar.DATE, Integer.parseInt(day, 16));	// 加上天数
		calendar.add(Calendar.SECOND,  seconds);	// 加上秒数
//		calendar.add(Calendar.MILLISECOND,  millis); // 采用 Calendar.MILLISECOND 计算会导致在秒级别的出现误差
		
		SimpleDateFormat sdf = null;
		if(millisecondFlag)
			sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");	// 毫秒部分是不精确的，毫秒部分每次运行的结果不相同！	
		else
			sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");	// 不带毫秒部分，秒级别是精确的，每次运行的结果相同	
		
		return sdf.format(calendar.getTime());
	}
	
	public static void main(String[] args) {
		
		//sql server: select CAST(0x00009E0E0095524F AS DateTime) == 2010-10-13 09:03:39.783  正确的日期值
		String str = "CAST(0x00009E0E0095524F AS DateTime)";	
		System.out.println(convertRawToStr(str, true));		// 2010-10-13 09:03:39.374  毫秒部分不精确
		System.out.println(convertRawDateTimeToStr(str));	// 2010-10-13 09:03:39  秒级别是精确的
		
		try {
			// 这里的字符集一般是 StandardCharsets.UTF_16 或者 StandardCharsets.UTF_8，具体看你导出时采用的是那种字符集
			BufferedReader reader = Files.newBufferedReader(Paths.get("F:\\Members.sql"), StandardCharsets.UTF_16);
			BufferedWriter writer = Files.newBufferedWriter(Paths.get("F:\\Members_mysql.sql"), StandardCharsets.UTF_8);
			
			String line = null; 
			Matcher matcher = null; 
			String matcherStr = null;
			String reg = ".*((?i)CAST\\(0x[0-9-a-f-A-F]+ AS DateTime\\)).*"; // ( 为特殊字符，表示普通的( 需要用 \\( 来转义表示
			Pattern pattern = Pattern.compile(reg);

			while ((line = reader.readLine()) != null) {
				while (line.matches(reg)) {
					matcher = pattern.matcher(line);
					matcherStr = null;
					if (matcher.find()) {
						matcherStr = matcher.group(1);	// matcherStr = "CAST(0x00009E0E0095524F AS DateTime)"
						if (matcherStr != null) {
							String mysqlStr = convertRawDateTimeToStr(matcherStr);	
							if(mysqlStr != null)	
								line = line.replace(matcherStr, " '"+mysqlStr+"' "); // mysqlStr = '2010-10-13 09:03:39'
						}
					} else {
						break;	// break inner while loop
					}
				}
				
				writer.write(line);
				writer.newLine();
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println("done.");
	}
	
}
