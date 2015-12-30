# SqlServerConverter
sql server数据库在向mysql等数据库迁移数据时，会遇到一个麻烦，sql server导出的datetime和decimal的结果是16进制形式的二进制结果，
类似于 CAST(0x00009E0E0095524F AS DateTime)，这样的导出结果是无法直接向mysql数据库中导入的，所以需要对sql server
导出的脚本中的所有的 datetime和decimal 字段类型进行转换，转换成mysql等数据库认可的结果：2010-10-13 09:03:39
才能正确的完成sql server数据向mysql等数据库的迁移。
注意本方法只能精确到秒级别，毫秒级别是不精确的(sql server本身只能精确到3.3毫秒)。

具体可以参加博客：http://www.cnblogs.com/digdeep/p/4822499.html
