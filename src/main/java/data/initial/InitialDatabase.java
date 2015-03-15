package data.initial;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class InitialDatabase {
//进攻回合
	public static String driver = "com.mysql.jdbc.Driver";
	public static String url = "jdbc:mysql://127.0.0.1:3306/nba?rewriteBatchedStatements=true";
	public static String user = "root";
	public static String password = "123";
	public static void main(String[] args) {
		long time=System.currentTimeMillis();
		try {
			Class.forName(driver);
			Connection conn = DriverManager.getConnection(url, user, password);
			if(!conn.isClosed()){
				Statement statement = conn.createStatement();
				 conn.setAutoCommit(false);
				String sql="drop table matches";
				statement.addBatch(sql);
				sql="drop table playerdata";
				statement.addBatch(sql);
				sql="drop table playerinfo";
				statement.addBatch(sql);
				sql="drop table teaminfo";
				statement.addBatch(sql);
				sql="CREATE TABLE teaminfo (name varchar(255) ,abbr varchar(255) ,city varchar(255) ,`east/west` varchar(255) ,`partition` varchar(255) ,court varchar(255) ,year varchar(255))";
				statement.addBatch(sql);
				sql="CREATE TABLE matches (date varchar(255), `host/guest` varchar(1), name varchar(255),opponent varchar(255),`win/lose` varchar(1) , total int ,first int ,second int ,third int ,fourth int,primary key(date,name))";
				statement.addBatch(sql);
				sql="CREATE TABLE playerdata (`date`  varchar(255) ,team varchar(255) ,playername varchar(255) ,position varchar(255),minutes double ,fieldGoal int ,fieldGoalAttempts int ,`threepointFieldGoal` int ,`threepointFieldGoalAttempts` int ,freeThrow int ,freeThrowAttempts int ,offensiveRebound int ,defensiveRebound int ,backboard int ,assit int ,steal int ,block int ,turnOver int ,foul int ,scoring int,primary key(date,team,playername))";
				statement.addBatch(sql);
				sql="CREATE TABLE playerinfo (name varchar(255) ,number varchar(255) ,position varchar(255) ,height varchar(255) ,weight int ,birth varchar(255) ,age int ,exp varchar(255) ,school varchar(255) )";
				statement.addBatch(sql);
				statement.executeBatch();
				statement.clearBatch();
				conn.commit();  
				new InitialPlayerinfo(statement);
				conn.commit();  
				statement.clearBatch();
				new InitialTeaminfo(statement);
				conn.commit();  
				statement.clearBatch();
				new InitialMatches(conn); 
				statement.clearBatch();
				new InitialPlayerdata(conn); 
				conn.close(); 
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		time=System.currentTimeMillis()-time;
		System.out.println(time);
	}
}
