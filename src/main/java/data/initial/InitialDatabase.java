package data.initial;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class InitialDatabase {
//进攻回合
	public static String driver = "org.sqlite.JDBC";
	public static String url = "jdbc:sqlite:nba.db";

	public static void main(String[] args) {
		long time=System.currentTimeMillis();
		try {
			Class.forName(driver);
			Connection conn = DriverManager.getConnection(url);
			if(!conn.isClosed()){
				Statement statement = conn.createStatement();
				 conn.setAutoCommit(false);
				 String sql="DELETE FROM matches";
				statement.addBatch(sql);
				sql="DELETE FROM playerdata";
				statement.addBatch(sql);
				sql="DELETE FROM playerinfo";
				statement.addBatch(sql);
				sql="DELETE FROM teaminfo";
				statement.addBatch(sql);
				sql="DELETE FROM `playersum13-14`";
				statement.addBatch(sql);
				sql="DELETE FROM `teamsum13-14`";
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
				conn.commit();
				new InitialPlayersum(conn,statement);
				conn.commit();
				new InitialTeamsum(conn,statement);                  
				conn.close(); 
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		time=System.currentTimeMillis()-time;
		System.out.println(time);
	}
}
