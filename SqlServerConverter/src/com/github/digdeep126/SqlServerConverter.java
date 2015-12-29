package com.github.digdeep126;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * sql server数据库在向mysql等数据库迁移数据时，会遇到几个麻烦
 * 1) datetime 类型字段的导出结果需要转换；
 * 2) decimal 类型的字段的导出结果需要转换；
 * 3) insert 语句需要去掉 [和 ], 去掉多余的 insert, 最后变成  insert into user(id,name) values(1,'aa'), (2,'bb'); 的形式
 * @author digdeep@126.com
 */
public class SqlServerConverter {
	private static final int insertSQLcount = 10;
	private static String reg = ".*((?i)CAST\\(0x[0-9-a-f-A-F]+ AS DateTime\\)).*"; // CAST(0x00009E0E0095524F AS DateTime)
	private static Pattern pattern = Pattern.compile(reg);
	
	private static String sqlInertReg = ".*((?i)INSERT .* \\(.*\\) VALUES )\\(.*\\).*"; //insert member (id,name) values (xxx)
	private static Pattern sqlInertPattern = Pattern.compile(sqlInertReg);
	
	private static String decimalReg = ".*((?i)CAST\\(.* AS Decimal\\(.*\\)\\)).*";;	//CAST(5.00 AS Decimal(6, 2))
	private static Pattern decimalPattern = Pattern.compile(decimalReg); 
	
	/**
	 * 将每一条insert语句中的 "CAST(0x00009E0E0095524F AS DateTime)" 转换成对应的：'2010-10-13 09:03:39'
	 * @param line
	 * @return
	 */
	public static String dealWithDateTime(String line){
		Matcher matcher = null; 
		String matcherStr = null;
		
		while (line.matches(reg)) {
			matcher = pattern.matcher(line);
			matcherStr = null;
			if (matcher.find()) {
				matcherStr = matcher.group(1);	// matcherStr = "CAST(0x00009E0E0095524F AS DateTime)"
				if (matcherStr != null) {
					String mysqlStr = SqlServerDateTimeUtils.convertRawDateTimeToStr(matcherStr);	
					if(mysqlStr != null)	
						line = line.replace(matcherStr, " '"+mysqlStr+"' "); // mysqlStr = '2010-10-13 09:03:39'
				}
			} else {
				break;	// break inner while loop
			}
		}
		return line;
	}
	
	/**
	 * 将每一条insert语句中的 "CAST(5.00 AS Decimal(6, 2))" 转换成对应的：5.00
	 * @param line
	 * @return
	 */
	public static String dealWithDecimal(String line){
		Matcher matcher = null; 
		String matcherStr = null;
		
		while (line.matches(decimalReg)) {
			matcher = decimalPattern.matcher(line);
			matcherStr = null;
			if (matcher.find()) {
				matcherStr = matcher.group(1);	// matcherStr = CAST(5.00 AS Decimal(6, 2))
				if (matcherStr != null) {
					String result = matcherStr.substring("CAST(".length(), 
												matcherStr.toUpperCase().indexOf(" AS DECIMAL("));
					line = line.replace(matcherStr, result);
				}
			} else {
				break;	// break inner while loop
			}
		}
		return line;
	}
	
	/**
	 * @param sqlServerPath 输入文件
	 * @param sqlCharset 输入文件的编码
	 * @param mysqlPath 输出文件，以utf-8格式输出
	 * @return
	 */
	public static boolean converterSqlServerToMySQL(String sqlServerFile, Charset sqlServerCharset, String mysqlFile){
		Path inPath = Paths.get(sqlServerFile);
		Path outPath = Paths.get(mysqlFile);
		
		// writer 采用 StandardCharsets.UTF_8
		try(BufferedReader reader = Files.newBufferedReader(inPath, sqlServerCharset);
			 BufferedWriter writer = Files.newBufferedWriter(outPath, StandardCharsets.UTF_8);){
			
			String line = null; 
			Matcher matcher = null; 
			String matcherStr = null;
			
			int i = 0;
			while ((line = reader.readLine()) != null) {
				// 1. 转换 datetime: CAST(0x00009E0E0095524F AS DateTime) 转换成 '2010-10-13 09:03:39'
				line = dealWithDateTime(line);
				
				// 2. 转换decimal: CAST(5.00 AS Decimal(6, 2)) 转换成 5.00
				line = dealWithDecimal(line);
				
				// 3. 去掉 [ 和 ], 去掉多余的 insert, 最后变成  insert into user(id,name) values(1,'aa'), (2,'bb'); 的形式
				matcher = sqlInertPattern.matcher(line);
				if(matcher.find()){
					i++;
					matcherStr = matcher.group(1);	// matcherStr ==  insert [user]([id], [name]) values (1,'aa')
					
					if(i % insertSQLcount == 1){	// 每 insertSQLcount 条 insert 作为一组进行提交
						if(i > 1){
							writer.write(";");	// 加入 ; 进行提交
							writer.newLine();
						}
						line = line.replace(matcherStr, " ");	// line ==  (1,'aa')
						matcherStr = matcherStr.replace("[", "");
						matcherStr = matcherStr.replace("]", "");
						
						matcherStr = matcherStr.toLowerCase().replace("insert", "insert into ");
						line = matcherStr + "\n" + line;	// line == insert into user(id,name) \n values(1,'aa')
					}else{
						line = line.replace(matcherStr, ",");	// line == ,values(2,'aa')
					}
					writer.write(line);
					writer.newLine();
				}
			}
			
			if(i % insertSQLcount != 1 && i > 0) // 当末尾没有封号(;)， 时才添加一个封号(;)，避免在末尾出现两个封号(;;)
				writer.write(";");
			
			writer.newLine();
			writer.flush();		// 注意这里一定要 flush ，不然文档的最后面的最后一两条数据不会完整写到文件中的！！！
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		
		System.out.println("OK.");
		return true;
	}
	
	public static void main(String[] args) {
		// 这里的字符集一般是 StandardCharsets.UTF_16 或者 StandardCharsets.UTF_8，具体看你导出时采用的是那种字符集
		// 如果sql server导出时选择了 unicode，那么这里就应该使用 StandardCharsets.UTF_16
		SqlServerConverter.converterSqlServerToMySQL("F:\\Members.sql", StandardCharsets.UTF_16, 
													"F:\\Members_mysql.sql");
		
		System.out.println("-------------------------------------------------");
		
		SqlServerConverter.converterSqlServerToMySQL("F:\\model_choise.sql", StandardCharsets.UTF_16, 
														"F:\\model_choise_mysql.sql");
		System.out.println("done.");
	}
	
}
