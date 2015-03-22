package data.initial;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.ArrayList;

import data.getdata.SqlStatement;

public class InitialPlayersum {


	public InitialPlayersum(Connection conn,Statement statement) {
		System.out.println("初始化球员统计……");
		File f=new File("data/players/info");
		String[] filelist=f.list();
		try {
		PreparedStatement ps=conn.prepareStatement("INSERT INTO playersum  values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
		for (int j = 0; j < filelist.length; j++) {
			String playerName=filelist[j];
			if(playerName.contains("'"))
				playerName=playerName.substring(0,playerName.indexOf("'"))+"\\"+playerName.substring(playerName.indexOf("'"), playerName.length());
			String team="";//球员队伍
			int appearance=0;//参赛场数
			int firstPlay=0;//先发场数
			int fieldGoal=0;//投篮命中数
			int fieldGoalAttempts=0;//投篮出手次数
			int threePointFieldGoal=0;//三分命中数
			int threePointFieldGoalAttempts=0;//三分出手数
			int freeThrow=0;//罚球命中数
			int freeThrowAttempts=0;//罚球出手数
			int offensiveRebound=0;//进攻数
			int defensiveRebound=0;//防守数
			int backboard=0;//篮板数
			int assist=0;//助攻数
			double minutes=0;//在场时间
			int steal=0;//抢断数
			int block=0;//盖帽数
			int turnOver=0;//失误数
			int foul=0;//犯规数 
			int scoring=0;//比赛得分
			int teamFieldGoal=0;
			int teamFieldGoalAttempts=0;
			int teamBackboard=0;//球队总篮板
			int teamFreeThrow=0;
			int teamOffensiveRebound=0;
			int teamDefensiveRebound=0;
			double teamMinutes=0;//球队上场总时间
			int teamFreeThrowAttempts=0;//球队罚球次数
			int teamTurnOver=0;//球队失误数
			int opponentBackBoard=0;//对手总篮板
			int opponentOffensiveRebound=0;//对手总进攻篮板
			int opponentDefensiveRebound=0;//对手总防守篮板
			int opponentFieldGoalAttempts=0;//对手投篮出手次数
			int opponentThreePointFieldGoalAttempts=0;//对手三分出手数
			
			double previousAverageScoring=0;//五场前的平均得分
			double nearlyFiveAverageScoring=0;//近五场的平均得分
			int doubleDouble=0;
			DecimalFormat df=new DecimalFormat("#.0");  
				ResultSet rs=statement.executeQuery(SqlStatement.getPlayerTeam(playerName));
				while(rs.next())
					team=rs.getString(2);
				rs=statement.executeQuery(SqlStatement.countPlayerMatches(playerName));
				while(rs.next())
					appearance=rs.getInt(1);
				rs=statement.executeQuery(SqlStatement.getPlayerFirstPlay(playerName));
				while(rs.next())
					firstPlay=rs.getInt(1);
				rs=statement.executeQuery(SqlStatement.countPlayerSum(playerName));
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
					minutes=Double.parseDouble(df.format(rs.getDouble(11)));
					steal=rs.getInt(12);
					block=rs.getInt(13);
					turnOver=rs.getInt(14);
					foul=rs.getInt(15);
					scoring=rs.getInt(16);
				}
				rs=statement.executeQuery(SqlStatement.countTeamSumForPlayer(team));
				while(rs.next()){
					teamFieldGoal=rs.getInt(1);
					teamFieldGoalAttempts=rs.getInt(2);
					teamFreeThrow=rs.getInt(3);
					teamOffensiveRebound=rs.getInt(4);
					teamDefensiveRebound=rs.getInt(5);
					teamMinutes=Double.parseDouble(df.format(rs.getDouble(6)));
					teamFreeThrowAttempts=rs.getInt(7);
					teamBackboard=rs.getInt(8);
					teamTurnOver=rs.getInt(9);
				}
				rs=statement.executeQuery(SqlStatement.getTeamOpponent(team));
				ArrayList<String> date=new ArrayList<String>();
				ArrayList<String> opponent=new ArrayList<String>();
				while(rs.next()){
					date.add(rs.getString(1));
					opponent.add(rs.getString(2));
				}
				for (int i = 0; i < date.size(); i++) {
					rs=statement.executeQuery(SqlStatement.getOpponentSumForPlayer(date.get(i), opponent.get(i)));
					int temp1=0;
					int temp2=0;
					int temp3=0;
					int temp4=0;
					int temp5=0;
					while(rs.next()){
						temp1=rs.getInt(1);
						temp2=rs.getInt(2);
						temp3=rs.getInt(3);
						temp4=rs.getInt(4);
						temp5=rs.getInt(5);
					}
					opponentFieldGoalAttempts=opponentFieldGoalAttempts+temp1;
					opponentThreePointFieldGoalAttempts=opponentThreePointFieldGoalAttempts+temp2;
					opponentBackBoard=opponentBackBoard+temp3;
					opponentOffensiveRebound=opponentOffensiveRebound+temp4;
					opponentDefensiveRebound=opponentDefensiveRebound+temp5;
				}
				String sql="SELECT scoring FROM playerdata WHERE playername='"+playerName+"' ORDER BY date DESC";
				ArrayList<Integer> allScoring=new ArrayList<Integer>();
				rs=statement.executeQuery(sql);
				while(rs.next())
					allScoring.add(rs.getInt(1));
				if(allScoring.size()>5){
				    for (int i = 0; i < 5; i++) {
				    	nearlyFiveAverageScoring=nearlyFiveAverageScoring+allScoring.get(i);
				    }
				    nearlyFiveAverageScoring=nearlyFiveAverageScoring/5;
				    for (int i = 5; i < allScoring.size(); i++) {
				    	previousAverageScoring=previousAverageScoring+allScoring.get(i);
				    }
				    previousAverageScoring=previousAverageScoring/(allScoring.size()-5);
				}
				sql="SELECT scoring,backboard,assit,steal,block FROM playerdata WHERE playername ='"+playerName+"'";
				rs=statement.executeQuery(sql);
				while(rs.next()){
					String temp=Integer.toString(rs.getInt(1))+Integer.toString(rs.getInt(2))+Integer.toString(rs.getInt(3))+Integer.toString(rs.getInt(4))+Integer.toString(rs.getInt(5));
					if(temp.length()>=7)
						doubleDouble++;
				}
				if (playerName.contains("'")) {
					playerName=playerName.substring(0,playerName.indexOf("\\"))+playerName.substring(playerName.indexOf("\\")+1, playerName.length());
				}
				ps.setString(1, playerName);
				ps.setString(2, team);
				ps.setInt(3, appearance);
				ps.setInt(4, firstPlay);
				ps.setInt(5, backboard);
				ps.setInt(6, assist);
				ps.setDouble(7, minutes);
				ps.setInt(8,fieldGoal);
				ps.setInt(9, fieldGoalAttempts);
				ps.setInt(10, threePointFieldGoal);
				ps.setInt(11, threePointFieldGoalAttempts);
				ps.setInt(12,freeThrow);
				ps.setInt(13,freeThrowAttempts);
				ps.setInt(14,offensiveRebound);
				ps.setInt(15,defensiveRebound);
				ps.setInt(16,steal);
				ps.setInt(17,block);
				ps.setInt(18,turnOver);
				ps.setInt(19,foul);
				ps.setInt(20,scoring);
				ps.setInt(21,teamFieldGoalAttempts);
				ps.setInt(22,teamBackboard);
				ps.setInt(23,teamFieldGoal);
				ps.setInt(24,teamFreeThrow);
				ps.setInt(25,teamOffensiveRebound);
				ps.setInt(26,teamDefensiveRebound);
				ps.setDouble(27,teamMinutes);
				ps.setInt(28,teamFreeThrowAttempts);
				ps.setInt(29,teamTurnOver);
				ps.setInt(30,opponentBackBoard);
				ps.setInt(31,opponentOffensiveRebound);
				ps.setInt(32,opponentDefensiveRebound);
				ps.setInt(33,opponentFieldGoalAttempts);
				ps.setInt(34,opponentThreePointFieldGoalAttempts);
				ps.setDouble(35,previousAverageScoring);
				ps.setDouble(36,nearlyFiveAverageScoring);
				ps.setInt(37,doubleDouble);
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
