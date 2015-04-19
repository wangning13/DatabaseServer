package data.initial;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class InitialDatabase {
	
	public static String driver = "org.sqlite.JDBC";
	public static String url = "jdbc:sqlite:nba.db";
	public static String initial_season;
	
	public static void main(String[] args) {
		long time=System.currentTimeMillis();
		File f=new File("data/matches");
		String[] filelist=f.list();
		initial_season = filelist[filelist.length/2];
		initial_season = initial_season.substring(0, initial_season.indexOf("_"));
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
				sql="DROP TABLE IF EXISTS `playersum"+initial_season+"`";
				statement.addBatch(sql);
				sql="DROP TABLE IF EXISTS `teamsum"+initial_season+"`";
				statement.addBatch(sql);
				sql="CREATE TABLE `playersum"+initial_season+"` (`playerName`	TEXT,`team`	TEXT,`appearance`	INTEGER,	`firstPlay`	INTEGER,`backboard`	INTEGER,	`assist`	INTEGER,	`minutes`	REAL,`fieldGoal`	INTEGER,`fieldGoalAttempts`	INTEGER,`threePointFieldGoal` INTEGER,`threePointFieldGoalAttempts` INTEGER,`freeThrow`	INTEGER,`freeThrowAttempts` INTEGER, `offensiveRebound` INTEGER, `defensiveRebound`	INTEGER,	`steal` INTEGER, `block`	INTEGER,	`turnOver` INTEGER, `foul` INTEGER, `scoring` INTEGER, `previousAverageScoring` INTEGER, `nearlyFiveAverageScoring` INTEGER,previousAverageBackboard INTEGER,nearlyFiveAverageBackboard INTEGER,previousAverageAssist INTEGER,nearlyFiveAverageAssist INTEGER,	`doubleDouble` INTEGER, PRIMARY KEY(playerName,team))";
				statement.addBatch(sql);
				sql="CREATE TABLE `teamsum"+initial_season+"` (`opponentFieldGoal`	INTEGER,`opponentFieldGoalAttempts` INTEGER,`opponentTurnOver` INTEGER,`opponentFreeThrowAttempts`	INTEGER,	`oppenentScoring`	INTEGER,	`teamName`	TEXT,`matches` INTEGER,`wins`	INTEGER,`fieldGoal`	INTEGER,	`fieldGoalAttempts` INTEGER,`threePointFieldGoal`	INTEGER,	`threePointFieldGoalAttempts`	INTEGER,	`freeThrow`	INTEGER,	`freeThrowAttempts`	INTEGER,	`offensiveRebound`	INTEGER,	`defensiveRebound`	INTEGER,	`opponentOffensiveRebound`	INTEGER,	`opponentDefensiveRebound` INTEGER,`backboard`	INTEGER,	`assist`	INTEGER,	`steal`	INTEGER,	`block`	INTEGER,	`turnOver` INTEGER,`foul` INTEGER,`scoring`	INTEGER,	`minutes`	REAL,`opponentBackBoard` INTEGER,`opponentThreePointFieldGoalAttempts`	INTEGER);";
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
