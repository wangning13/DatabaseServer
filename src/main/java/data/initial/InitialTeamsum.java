package data.initial;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.ArrayList;

import data.getdata.SqlStatement;

public class InitialTeamsum {

	public InitialTeamsum(Connection conn,Statement statement) {
		System.out.println("初始化球队统计……");
		DecimalFormat df=new DecimalFormat("#.0");  
		String[] tempYear = InitialDatabase.initial_season.split("-");
		String season="date < '"+tempYear[1]+"-05' AND date > '"+tempYear[0]+"-09'";
		String insert = "INSERT INTO `teamsum"+InitialDatabase.initial_season+"`  values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		try {
			PreparedStatement ps=conn.prepareStatement(insert);
			ResultSet rs=statement.executeQuery(SqlStatement.getTeamName());
			ArrayList<String> team=new ArrayList<String>();
			while(rs.next())
				team.add(rs.getString(1));
			for (int j = 0; j < team.size(); j++) {
				String teamName=team.get(j);
				int opponentFieldGoal=0;//对手投篮命中数
				int opponentFieldGoalAttempts=0;//对手投篮出手次数
				int opponentTurnOver=0;//对手失误数
				int opponentFreeThrowAttempts=0;//对手罚球数
				int oppenentScoring=0;//对手得分
				int matches=0;//比赛场数
				int wins=0;//胜利场数
				int fieldGoal=0;//投篮命中数
				int fieldGoalAttempts=0;//投篮出手次数
				int threePointFieldGoal=0;//三分命中数
				int threePointFieldGoalAttempts=0;//三分出手数
				int freeThrow=0;//罚球命中数
				int freeThrowAttempts=0;//罚球出手数
				int offensiveRebound=0;//进攻篮板数
				int defensiveRebound=0;//防守篮板数
				int opponentOffensiveRebound=0;//对手进攻篮板数
				int opponentDefensiveRebound=0;//对手防守篮板数
				int backboard=0;//篮板数
				int assist=0;//助攻数
				int steal=0;//抢断数
				int block=0;//盖帽数
				int turnOver=0;//失误数
				int foul=0;//犯规数
				int scoring=0;//比赛得分
				double minutes=0;
				int opponentBackBoard=0;//对手总篮板
				int opponentThreePointFieldGoalAttempts=0;//对手三分出手数
			    rs=statement.executeQuery(SqlStatement.countTeamMatches(teamName,season));
			    while(rs.next())
			        matches=rs.getInt(1);
			    rs=statement.executeQuery(SqlStatement.countTeamSum(teamName,season));
			    while(rs.next()){
			    	    fieldGoal=rs.getInt(1);
			    	    fieldGoalAttempts=rs.getInt(2);
			    	    threePointFieldGoal=rs.getInt(3);
			    	    threePointFieldGoalAttempts=rs.getInt(4);
			    	    freeThrow=rs.getInt(5);
			    	    freeThrowAttempts=rs.getInt(6);
			    	    offensiveRebound=rs.getInt(7);
			    	    defensiveRebound=rs.getInt(8);
			    	    backboard=rs.getInt(9);
			    	    assist=rs.getInt(10);
			    	    steal=rs.getInt(11);
			    	    block=rs.getInt(12);
			    	    turnOver=rs.getInt(13);
			    	    foul=rs.getInt(14);
			    	    scoring=rs.getInt(15);
			    	    minutes=rs.getDouble(16);
			    	    minutes=Double.parseDouble(df.format(minutes));
			    }
			    rs=statement.executeQuery(SqlStatement.countTeamWins(teamName,season));
			    while(rs.next())
			    	wins=rs.getInt(1);
			    ArrayList<String> date=new ArrayList<String>();
			    ArrayList<String> opponent=new ArrayList<String>();
			    rs=statement.executeQuery(SqlStatement.getTeamOpponent(teamName,season));
			    while(rs.next()){
			    	date.add(rs.getString(1));
			    	opponent.add(rs.getString(2));
			    }
			    for (int i = 0; i < date.size(); i++) {
			    	rs=statement.executeQuery(SqlStatement.getTeamOpponentSum(date.get(i), opponent.get(i)));
			    	int temp1=0;
			    	int temp2=0;
			    	int temp3=0;
			    	int temp4=0;
			    	int temp5=0;
			    	int temp6=0;
			    	int temp7=0;
			    	int temp8=0;
			    	int temp9=0;
			    	while(rs.next()){
			    		temp1=rs.getInt(1);
			    		temp2=rs.getInt(2);
			    		temp3=rs.getInt(3);
			    		temp4=rs.getInt(4);
			    		temp5=rs.getInt(5);
			    		temp6=rs.getInt(6);
			    		temp7=rs.getInt(7);
			    		temp8=rs.getInt(8);
			    		temp9=rs.getInt(9);
			    	}
			    	opponentFieldGoal=opponentFieldGoal+temp1;
			    	opponentFieldGoalAttempts=opponentFieldGoalAttempts+temp2;
			    	opponentFreeThrowAttempts=opponentFreeThrowAttempts+temp3;
			    	opponentOffensiveRebound=opponentOffensiveRebound+temp4;
			    	opponentDefensiveRebound=opponentDefensiveRebound+temp5;
			    	opponentTurnOver=opponentTurnOver+temp6;
			    	oppenentScoring=oppenentScoring+temp7;
			    	opponentBackBoard=opponentBackBoard+temp8;
			    	opponentThreePointFieldGoalAttempts=opponentThreePointFieldGoalAttempts+temp9;
			    }
			    ps.setInt(1, opponentFieldGoal);
			    ps.setInt(2, opponentFieldGoalAttempts);
			    ps.setInt(3, opponentTurnOver);
			    ps.setInt(4, opponentFreeThrowAttempts);
			    ps.setInt(5, oppenentScoring);
			    ps.setString(6, teamName);
			    ps.setInt(7, matches);
			    ps.setInt(8, wins);
			    ps.setInt(9, fieldGoal);
			    ps.setInt(10, fieldGoalAttempts);
			    ps.setInt(11, threePointFieldGoal);
			    ps.setInt(12, threePointFieldGoalAttempts);
			    ps.setInt(13, freeThrow);
			    ps.setInt(14, freeThrowAttempts);
			    ps.setInt(15, offensiveRebound);
			    ps.setInt(16, defensiveRebound);
			    ps.setInt(17, opponentOffensiveRebound);
			    ps.setInt(18, opponentDefensiveRebound);
			    ps.setInt(19, backboard);
			    ps.setInt(20, assist);
			    ps.setInt(21, steal);
			    ps.setInt(22, block);
			    ps.setInt(23, turnOver);
			    ps.setInt(24, foul);
			    ps.setInt(25, scoring);
			    ps.setDouble(26, minutes);
			    ps.setInt(27, opponentBackBoard);
			    ps.setInt(28, opponentThreePointFieldGoalAttempts);
			    ps.addBatch();
			}
			ps.executeBatch();
			conn.commit(); 
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
}
