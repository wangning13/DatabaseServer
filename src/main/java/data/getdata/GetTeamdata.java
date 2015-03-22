package data.getdata;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import po.TeamPO;
import po.TeaminfoPO;
import data.initial.InitialDatabase;
import dataservice.getdatadataservice.GetTeamdataDataService;

public class GetTeamdata extends UnicastRemoteObject implements GetTeamdataDataService{

	Statement statement;
	public GetTeamdata() throws RemoteException{
		try {
			Class.forName(InitialDatabase.driver);
			Connection conn = DriverManager.getConnection(InitialDatabase.url, InitialDatabase.user, InitialDatabase.password);
			statement = conn.createStatement();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	public TeaminfoPO getTeaminfo(String teamName){
		TeaminfoPO po=null;;
		try {
			ResultSet rs=statement.executeQuery(SqlStatement.getTeaminfo(teamName));
			while(rs.next())
				po=new TeaminfoPO(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getString(5), rs.getString(6), rs.getInt(7));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return po;
	}
	
	public ArrayList<String> getTeamPlayer(String teamName){
		ArrayList<String> teamPlayer=new ArrayList<String>();
		String sql="SELECT playername FROM(SELECT * FROM(SELECT DISTINCT team,playername FROM playerdata ORDER BY date DESC) AS a GROUP BY playername) AS b where team='"+teamName+"'";
		try {
			ResultSet rs=statement.executeQuery(sql);
			while(rs.next())
				teamPlayer.add(rs.getString(1));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return teamPlayer;
	}
	
	public TeamPO getTeamdata(String teamName){
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
		double fieldGoalPercentage=0;//投篮命中率
		double threePointShotPercentage=0;//三分命中率
		double freeThrowPercentage=0;//三分命中率
		double winningPercentage=0;//胜率
		double possessions=0;//进攻回合
		double offensiveEfficiency=0;//进攻效率
		double defensiveEfficiency=0;//防守效率
		double offensivebackboardEfficiency=0;//进攻篮板效率
		double defensivebackboardEfficiency=0;//防守篮板效率
		double stealEfficiency=0;//抢断效率
		double assitEfficiency=0;//助攻效率
		try {
			String sql="SELECT * FROM teamsum WHERE teamName='"+teamName+"'";
			ResultSet rs=statement.executeQuery(sql);
			while(rs.next()){
				opponentFieldGoal=rs.getInt(1);
				opponentFieldGoalAttempts=rs.getInt(2);
				opponentTurnOver=rs.getInt(3);
				opponentFreeThrowAttempts=rs.getInt(4);
				oppenentScoring=rs.getInt(5);
				matches=rs.getInt(7);
				wins=rs.getInt(8);
				fieldGoal=rs.getInt(9);
				fieldGoalAttempts=rs.getInt(10);
				threePointFieldGoal=rs.getInt(11);
				threePointFieldGoalAttempts=rs.getInt(12);
				freeThrow=rs.getInt(13);
				freeThrowAttempts=rs.getInt(14);
				offensiveRebound=rs.getInt(15);
				defensiveRebound=rs.getInt(16);
				opponentOffensiveRebound=rs.getInt(17);
				opponentDefensiveRebound=rs.getInt(18);
				backboard=rs.getInt(19);
				assist=rs.getInt(20);
				steal=rs.getInt(21);
				block=rs.getInt(22);
				turnOver=rs.getInt(23);
				foul=rs.getInt(24);
				scoring=rs.getInt(25);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		TeamPO po=new TeamPO(opponentFieldGoal, opponentFieldGoalAttempts, opponentTurnOver, opponentFreeThrowAttempts, oppenentScoring, teamName, matches, wins, fieldGoal, fieldGoalAttempts, threePointFieldGoal, threePointFieldGoalAttempts, freeThrow, freeThrowAttempts, offensiveRebound, defensiveRebound, opponentOffensiveRebound, opponentDefensiveRebound, backboard, assist, steal, block, turnOver, foul, scoring, fieldGoalPercentage, threePointShotPercentage, freeThrowPercentage, winningPercentage, possessions, offensiveEfficiency, defensiveEfficiency, offensivebackboardEfficiency, defensivebackboardEfficiency, stealEfficiency, assitEfficiency);
		return po;
	}
	
	public ArrayList<TeamPO> getAllTeamdata(String key,String order){
		String teamName="";
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
		double fieldGoalPercentage=0;//投篮命中率
		double threePointShotPercentage=0;//三分命中率
		double freeThrowPercentage=0;//三分命中率
		double winningPercentage=0;//胜率
		double possessions=0;//进攻回合
		double offensiveEfficiency=0;//进攻效率
		double defensiveEfficiency=0;//防守效率
		double offensivebackboardEfficiency=0;//进攻篮板效率
		double defensivebackboardEfficiency=0;//防守篮板效率
		double stealEfficiency=0;//抢断效率
		double assitEfficiency=0;//助攻效率
		ArrayList<TeamPO> po=new ArrayList<TeamPO>();
		try {
			String sql="SELECT * FROM teamsum ORDER BY "+key+" "+order;
			ResultSet rs=statement.executeQuery(sql);
			while(rs.next()){
				opponentFieldGoal=rs.getInt(1);
				opponentFieldGoalAttempts=rs.getInt(2);
				opponentTurnOver=rs.getInt(3);
				opponentFreeThrowAttempts=rs.getInt(4);
				oppenentScoring=rs.getInt(5);
				teamName=rs.getString(6);
				matches=rs.getInt(7);
				wins=rs.getInt(8);
				fieldGoal=rs.getInt(9);
				fieldGoalAttempts=rs.getInt(10);
				threePointFieldGoal=rs.getInt(11);
				threePointFieldGoalAttempts=rs.getInt(12);
				freeThrow=rs.getInt(13);
				freeThrowAttempts=rs.getInt(14);
				offensiveRebound=rs.getInt(15);
				defensiveRebound=rs.getInt(16);
				opponentOffensiveRebound=rs.getInt(17);
				opponentDefensiveRebound=rs.getInt(18);
				backboard=rs.getInt(19);
				assist=rs.getInt(20);
				steal=rs.getInt(21);
				block=rs.getInt(22);
				turnOver=rs.getInt(23);
				foul=rs.getInt(24);
				scoring=rs.getInt(25);
				TeamPO temp=new TeamPO(opponentFieldGoal, opponentFieldGoalAttempts, opponentTurnOver, opponentFreeThrowAttempts, oppenentScoring, teamName, matches, wins, fieldGoal, fieldGoalAttempts, threePointFieldGoal, threePointFieldGoalAttempts, freeThrow, freeThrowAttempts, offensiveRebound, defensiveRebound, opponentOffensiveRebound, opponentDefensiveRebound, backboard, assist, steal, block, turnOver, foul, scoring, fieldGoalPercentage, threePointShotPercentage, freeThrowPercentage, winningPercentage, possessions, offensiveEfficiency, defensiveEfficiency, offensivebackboardEfficiency, defensivebackboardEfficiency, stealEfficiency, assitEfficiency);
				po.add(temp);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return po;
	}
	
	public ArrayList<TeamPO> getSomeTeamdata(String condition,String key,String order){
		String teamName="";
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
		double fieldGoalPercentage=0;//投篮命中率
		double threePointShotPercentage=0;//三分命中率
		double freeThrowPercentage=0;//三分命中率
		double winningPercentage=0;//胜率
		double possessions=0;//进攻回合
		double offensiveEfficiency=0;//进攻效率
		double defensiveEfficiency=0;//防守效率
		double offensivebackboardEfficiency=0;//进攻篮板效率
		double defensivebackboardEfficiency=0;//防守篮板效率
		double stealEfficiency=0;//抢断效率
		double assitEfficiency=0;//助攻效率
		ArrayList<TeamPO> po=new ArrayList<TeamPO>();
		String sql="SELECT * FROM teamsum INNER JOIN(SELECT abbr FROM teaminfo WHERE "+condition+") AS a ON a.abbr=teamsum.teamName ORDER BY "+key+" "+order;
		try {
			ResultSet rs=statement.executeQuery(sql);
			while(rs.next()){
				opponentFieldGoal=rs.getInt(1);
				opponentFieldGoalAttempts=rs.getInt(2);
				opponentTurnOver=rs.getInt(3);
				opponentFreeThrowAttempts=rs.getInt(4);
				oppenentScoring=rs.getInt(5);
				teamName=rs.getString(6);
				matches=rs.getInt(7);
				wins=rs.getInt(8);
				fieldGoal=rs.getInt(9);
				fieldGoalAttempts=rs.getInt(10);
				threePointFieldGoal=rs.getInt(11);
				threePointFieldGoalAttempts=rs.getInt(12);
				freeThrow=rs.getInt(13);
				freeThrowAttempts=rs.getInt(14);
				offensiveRebound=rs.getInt(15);
				defensiveRebound=rs.getInt(16);
				opponentOffensiveRebound=rs.getInt(17);
				opponentDefensiveRebound=rs.getInt(18);
				backboard=rs.getInt(19);
				assist=rs.getInt(20);
				steal=rs.getInt(21);
				block=rs.getInt(22);
				turnOver=rs.getInt(23);
				foul=rs.getInt(24);
				scoring=rs.getInt(25);
				TeamPO temp=new TeamPO(opponentFieldGoal, opponentFieldGoalAttempts, opponentTurnOver, opponentFreeThrowAttempts, oppenentScoring, teamName, matches, wins, fieldGoal, fieldGoalAttempts, threePointFieldGoal, threePointFieldGoalAttempts, freeThrow, freeThrowAttempts, offensiveRebound, defensiveRebound, opponentOffensiveRebound, opponentDefensiveRebound, backboard, assist, steal, block, turnOver, foul, scoring, fieldGoalPercentage, threePointShotPercentage, freeThrowPercentage, winningPercentage, possessions, offensiveEfficiency, defensiveEfficiency, offensivebackboardEfficiency, defensivebackboardEfficiency, stealEfficiency, assitEfficiency);
				po.add(temp);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return po;
	}
	
	public ArrayList<TeamPO> getByEfficiency(ArrayList<TeamPO> po,String key,String order){
		ArrayList<TeamPO> r=new ArrayList<TeamPO>();
		String sql="CREATE TABLE temp (	opponentFieldGoal int,opponentFieldGoalAttempts int,opponentTurnOver int ,opponentFreeThrowAttempts int ,oppenentScoring int,teamName varchar(255), matches int,wins int, fieldGoal int, fieldGoalAttempts int, threePointFieldGoal int, threePointFieldGoalAttempts int, freeThrow int, freeThrowAttempts int, offensiveRebound int, defensiveRebound int,	opponentOffensiveRebound int ,opponentDefensiveRebound int , backboard int, assist int, steal int, block int, turnOver int, foul int, scoring int,fieldGoalPercentage double,threePointShotPercentage double,freeThrowPercentage double,winningPercentage double,possessions double,offensiveEfficiency double,defensiveEfficiency double,offensivebackboardEfficiency double,defensivebackboardEfficiency double,stealEfficiency double,assitEfficiency double)";
		try {
			statement.addBatch(sql);
			for (int i = 0; i < po.size(); i++) {
				TeamPO tt=po.get(i);
				sql="INSERT INTO temp values('"
						+ tt.getOpponentFieldGoal()
						+ "','"
						+ tt.getOpponentFieldGoalAttempts()
						+ "','"
						+ tt.getOpponentTurnOver()
						+ "','"
						+ tt.getOpponentFreeThrowAttempts()
						+ "','"
						+ tt.getOppenentScoring()
						+ "','"
						+ tt.getTeamName()
						+ "','"
						+ tt.getMatches()
						+ "','"
						+ tt.getWins()
						+ "','"
						+ tt.getFieldGoal()
						+ "','"
						+ tt.getFieldGoalAttempts()
						+ "','"
						+ tt.getThreePointFieldGoal()
						+ "','"
						+ tt.getThreePointFieldGoalAttempts()
						+ "','"
						+ tt.getFreeThrow()
						+ "','"
						+ tt.getFreeThrowAttempts()
						+ "','"
						+ tt.getOffensiveRebound()
						+ "','"
						+ tt.getDefensiveRebound()
						+ "','"
						+ tt.getOpponentOffensiveRebound()
						+ "','"
						+ tt.getOpponentDefensiveRebound()
						+ "','"
						+ tt.getBackboard()
						+ "','"
						+ tt.getAssist()
						+ "','"
						+ tt.getSteal()
						+ "','"
						+ tt.getBlock()
						+ "','"
						+ tt.getTurnOver()
						+ "','"
						+ tt.getFoul()
						+ "','"
						+ tt.getScoring() 
						+ "','"
						+ tt.getFieldGoalPercentage()
						+ "','"
						+tt.getThreePointShotPercentage()
						+ "','"
						+tt.getFieldGoalPercentage()
						+ "','"
						+tt.getWinningPercentage()
						+ "','"
						+tt.getPossessions()
						+ "','" 
						+tt.getOffensiveEfficiency()
						+ "','"
						+tt.getDefensiveEfficiency()
						+ "','"
						+tt.getOffensivebackboardEfficiency()
						+ "','"
						+tt.getDefensivebackboardEfficiency()
						+ "','"
						+tt.getStealEfficiency()
						+ "','"
						+tt.getAssitEfficiency()
						+ "')";
				statement.addBatch(sql);
			}
			statement.executeBatch();
			sql="SELECT * FROM temp ORDER BY `"+key+"`"+order;
			ResultSet rs=statement.executeQuery(sql);
			while(rs.next()){
				TeamPO tt=new TeamPO(rs.getInt(1), rs.getInt(2), rs.getInt(3), rs.getInt(4), rs.getInt(5), rs.getString(6), rs.getInt(7), rs.getInt(8), rs.getInt(9), rs.getInt(10), rs.getInt(11), rs.getInt(12), rs.getInt(13), rs.getInt(14), rs.getInt(15), rs.getInt(16), rs.getInt(17),rs.getInt(18), rs.getInt(19), rs.getInt(20), rs.getInt(21), rs.getInt(22),rs.getInt(23), rs.getInt(24), rs.getInt(25),rs.getDouble(26),rs.getDouble(27),rs.getDouble(28),rs.getDouble(29),rs.getDouble(30),rs.getDouble(31),rs.getDouble(32),rs.getDouble(33),rs.getDouble(34),rs.getDouble(35),rs.getDouble(36));
				r.add(tt);
			}
			sql="DROP TABLE temp";
			statement.execute(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return r;
	}
}