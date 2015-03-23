package data.getdata;

public class SqlStatement {
//order：DESC降序，ASC升序
	public static String countTeamMatches(String teamName){
		String r="SELECT COUNT(name) FROM matches WHERE name='"+teamName+"'";
		return r;
	}
	
	public static String countTeamWins(String teamName){
		String r="SELECT COUNT(name) FROM matches WHERE name='"+teamName+"' AND `win/lose`='w'";
		return r;
	}
	
	public static String countTeamSum(String teamName){
		String r="SELECT SUM(fieldGoal),SUM(fieldGoalAttempts),SUM(`threepointFieldGoal`),SUM(`threepointFieldGoalAttempts`),SUM(freeThrow),SUM(freeThrowAttempts),SUM(offensiveRebound),SUM(defensiveRebound),SUM(backboard),SUM(assit),SUM(steal),SUM(block),SUM(turnOver),SUM(foul),SUM(scoring),SUM(minutes) FROM playerdata WHERE team='"+teamName+"'";
		return r;
	}
	
	public static String getTeamName(){
		String r="SELECT abbr FROM teaminfo";
		return r;
	}
	
	public static String getTeamOpponent(String teamName){
		String r="SELECT date,opponent FROM matches WHERE name='"+teamName+"'";
		return r;
	}
	
	public static String getTeamOpponentSum(String date,String opponent){
		String r="SELECT SUM(fieldGoal),SUM(fieldGoalAttempts),SUM(freeThrowAttempts),SUM(offensiveRebound),SUM(defensiveRebound),SUM(turnOver),SUM(scoring),SUM(backBoard),SUM(threePointFieldGoalAttempts) FROM playerdata WHERE date='"+date+"' AND team='"+opponent+"'";
		return r;
	}
	
	public static String getTeaminfo(String teamName){
		String r="SELECT * FROM teaminfo WHERE abbr='"+teamName+"'";
		return r;
	}
	
	public static String countPlayerMatches(String playerName){
		String r="SELECT COUNT(playername) FROM playerdata WHERE playername='"+playerName+"'";
		return r;
	}
	
	public static String countPlayerSum(String playerName){
		String r="SELECT SUM(fieldGoal),SUM(fieldGoalAttempts),SUM(`threepointFieldGoal`),SUM(`threepointFieldGoalAttempts`),SUM(freeThrow),SUM(freeThrowAttempts),SUM(offensiveRebound),SUM(defensiveRebound),SUM(backboard),SUM(assit),SUM(minutes),SUM(steal),SUM(block),SUM(turnOver),SUM(foul),SUM(scoring) FROM playerdata WHERE playername='"+playerName+"'";
		return r;
	}
	public static String getPlayerinfo(String playerName){
		String r="SELECT * FROM playerinfo WHERE name='"+playerName+"'";
		return r;
	}
	
	public static String getPlayerFirstPlay(String playerName){
		String r="SELECT COUNT(*) FROM playerdata WHERE position !=''AND playername='"+playerName+"'";
		return r;
	}
	
	public static String getPlayerTeam(String playerName){
		String r="SELECT * FROM (SELECT date,team FROM playerdata WHERE playername='"+playerName+"' ORDER BY date DESC) AS temp GROUP BY team LIMIT 1";
		return r;
	}
	
	public static String countTeamSumForPlayer(String teamName){
		String r="SELECT SUM(fieldGoal),SUM(fieldGoalAttempts),SUM(freeThrow),SUM(offensiveRebound),SUM(defensiveRebound),SUM(minutes),SUM(freeThrowAttempts),SUM(backboard),SUM(turnOver) FROM playerdata WHERE team='"+teamName+"'";
		return r;
	}
	
	public static String getOpponentSumForPlayer(String date,String opponent){
		String r="SELECT SUM(fieldGoalAttempts),SUM(threePointFieldGoalAttempts),SUM(backBoard),SUM(offensiveRebound),SUM(defensiveRebound) FROM playerdata WHERE date='"+date+"' AND team='"+opponent+"'";
		return r;
	}
}
