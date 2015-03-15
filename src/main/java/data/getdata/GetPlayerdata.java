package data.getdata;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.ArrayList;

import po.PlayerPO;
import po.PlayerinfoPO;
import data.initial.InitialDatabase;
import data.initial.InitialPlayerinfo;
import dataservice.getdatadataservice.GetPlayerdataDataService;

public class GetPlayerdata implements GetPlayerdataDataService{

	Statement statement;
	public GetPlayerdata() {
		try {
			Class.forName(InitialDatabase.driver);
			Connection conn = DriverManager.getConnection(InitialDatabase.url, InitialDatabase.user, InitialDatabase.password);
			statement = conn.createStatement();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	public PlayerPO getPlayerdata(String playerName){
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
		double minites=0;//在场时间
		int steal=0;//抢断数
		int block=0;//盖帽数
		int turnOver=0;//失误数
		int foul=0;//犯规数 
		int scoring=0;//比赛得分
		int teamFieldGoalAttempts=0;
		int teamBackboard=0;//球队总篮板
		double teamMinutes=0;//球队上场总时间
		int teamFreeThrowAttempts=0;//球队罚球次数
		int teamTurnOver=0;//球队失误数
		int opponentBackBoard=0;//对手总篮板
		int opponentOffensiveRebound=0;//对手总进攻篮板
		int opponentDefensiveRebound=0;//对手总防守篮板
		int opponentFieldGoalAttempts=0;//对手投篮出手次数
		int opponentThreePointFieldGoalAttempts=0;//对手三分出手数
		
		double threePointShotPercentage=0;//三分命中率
		double freeThrowPercentage=0;//罚球命中率
		double efficiency=0;//效率
		double GmScEfficiency=0;//GmSc效率
		double nearlyFivePercentage=0;//近五场提升率
		double trueShootingPercentage=0;//真实命中率
		double shootingEfficiency=0;//投篮效率
		double backboardPercentage=0;//篮板率
		double offensiveReboundPercentage=0;//进攻篮板率
		double defensiveReboundPercentage=0;//防守篮板率
		double assistPercentage=0;//助攻率
		double stealPercentage=0;//抢断率
		double blockPercentage=0;//盖帽率
		double turnOverPercentage=0;//失误率
		double usage=0;//使用率
		
		double previousAverageScoring=0;//五场前的平均得分
		double nearlyFiveAverageScoring=0;//近五场的平均得分
		DecimalFormat df=new DecimalFormat("#.0");  
		try {
			ResultSet rs=statement.executeQuery(SqlStatement.getPlayerTeam(playerName));
			team=rs.getString(2);
			rs=statement.executeQuery(SqlStatement.countPlayerMatches(playerName));
			appearance=rs.getInt(1);
			rs=statement.executeQuery(SqlStatement.getPlayerFirstPlay(playerName));
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
				minites=Double.parseDouble(df.format(rs.getDouble(11)));
				steal=rs.getInt(12);
				block=rs.getInt(13);
				turnOver=rs.getInt(14);
				foul=rs.getInt(15);
				scoring=rs.getInt(16);
			}
			rs=statement.executeQuery(SqlStatement.countTeamSumForPlayer(team));
			while(rs.next()){
				teamFieldGoalAttempts=rs.getInt(1);
				teamMinutes=Double.parseDouble(df.format(rs.getDouble(2)));
				teamFreeThrowAttempts=rs.getInt(3);
				teamBackboard=rs.getInt(4);
				teamTurnOver=rs.getInt(5);
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
				while(rs.next()){
					temp1=rs.getInt(1);
					temp2=rs.getInt(2);
				}
				opponentFieldGoalAttempts=opponentFieldGoalAttempts+temp1;
				opponentThreePointFieldGoalAttempts=opponentThreePointFieldGoalAttempts+temp2;
			}
			
 		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		PlayerPO po=new PlayerPO(playerName, team, appearance, firstPlay, backboard, assist, minites, opponentThreePointFieldGoalAttempts, fieldGoalAttempts, threePointFieldGoal, threePointFieldGoalAttempts, freeThrowAttempts, freeThrowAttempts, offensiveRebound, defensiveRebound, steal, block, turnOver, foul, scoring, teamFieldGoalAttempts, teamBackboard, teamFieldGoal, teamFreeThrow, teamOffensiveRebound, teamDefensiveRebound, teamMinutes, teamFreeThrowAttempts, teamTurnOver, opponentBackBoard, opponentOffensiveRebound, opponentDefensiveRebound, opponentFieldGoalAttempts, opponentThreePointFieldGoalAttempts, threePointShotPercentage, freeThrowPercentage, efficiency, GmScEfficiency, nearlyFivePercentage, trueShootingPercentage, shootingEfficiency, backboardPercentage, offensiveReboundPercentage, defensiveReboundPercentage, assistPercentage, stealPercentage, blockPercentage, turnOverPercentage, usage, previousAverageScoring, nearlyFiveAverageScoring);
		return po;
	}
	
	public PlayerinfoPO getPlayerinfo(String playerName){
		PlayerinfoPO po =null;
		try {
			ResultSet rs=statement.executeQuery(SqlStatement.getPlayerinfo(playerName));
			while(rs.next())
				po=new PlayerinfoPO(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getInt(5), rs.getString(6), rs.getInt(7), rs.getString(8), rs.getString(9));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return po;
	}
	
	public ArrayList<PlayerPO> getAllPlayerdata(String key,String order){
		ArrayList<PlayerPO> po=new ArrayList<PlayerPO>();
		ArrayList<PlayerPO> r=new ArrayList<PlayerPO>();
		for (int i = 0; i < InitialPlayerinfo.filelist.length; i++) {
			PlayerPO temp=getPlayerdata(InitialPlayerinfo.filelist[i]);
			po.add(temp);
		}
		r=getByOrder(po,key,order,true);
		return r;
	}
	//W/E
	public ArrayList<PlayerPO> getSomePlayerdata(String position,String partition,String key,String order){
		ArrayList<PlayerPO> po=new ArrayList<PlayerPO>();
		ArrayList<PlayerPO> r=new ArrayList<PlayerPO>();
		ArrayList<String> player=new ArrayList<String>();
		try {
			String sql="SELECT (name) FROM playerinfo";
			ResultSet rs=statement.executeQuery(sql);
			if(!position.equals("1")){
				sql="SELECT (name) FROM playerinfo WHERE position LIKE '%"+position+"%'";
				rs=statement.executeQuery(sql);
			}
			while(rs.next())
				player.add(rs.getString(1));
			if(partition.startsWith("league:")){
				partition=partition.substring(partition.indexOf(":")+1, partition.length());
				for (int i = 0; i < player.size(); i++) {
					String team="";
					rs=statement.executeQuery(SqlStatement.getPlayerTeam(player.get(i)));
					while(rs.next())
						team=rs.getString(1);
					rs=statement.executeQuery(SqlStatement.getTeaminfo(team));
					String temp="";
					while(rs.next())
						temp=rs.getString(4);
					if(!partition.equals(temp))
						player.remove(i);
				}
			}else if(partition.startsWith("partition:")){
				partition=partition.substring(partition.indexOf(":")+1, partition.length());
				for (int i = 0; i < player.size(); i++) {
					String team="";
					rs=statement.executeQuery(SqlStatement.getPlayerTeam(player.get(i)));
					while(rs.next())
						team=rs.getString(1);
					rs=statement.executeQuery(SqlStatement.getTeaminfo(team));
					String temp="";
					while(rs.next())
						temp=rs.getString(5);
					if(!partition.equals(temp))
						player.remove(i);
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for (int i = 0; i < player.size(); i++) {
			PlayerPO temp=getPlayerdata(player.get(i));
			po.add(temp);
		}
		r=getByOrder(po,key,order,false);
		return r;
	}

	private ArrayList<PlayerPO> getByOrder(ArrayList<PlayerPO> po,String key,String order,boolean isAll){
		ArrayList<PlayerPO> r=new ArrayList<PlayerPO>();
		String sql="CREATE TABLE temp (	 playerName String, team String,appearance int,firstPlay int,fieldGoal int,fieldGoalAttempts int,threePointFieldGoal int,threePointFieldGoalAttempts int,freeThrow int,freeThrowAttempts int,offensiveRebound int,defensiveRebound int,backboard int,assist int,minites double,steal int,block int,turnOver int, foul int,scoring int,teamFieldGoalAttempts int,teamBackboard int,teamMinutes double,teamFreeThrowAttempts int,teamTurnOver int,opponentFieldGoalAttempts int,opponentThreePointFieldGoalAttempts int)";
		try {
			statement.addBatch(sql);
			for (int i = 0; i < po.size(); i++) {
				PlayerPO pp=po.get(i);
				sql="INSERT INTO temp values('"
						+ pp.getPlayerName()
						+ "','"
						+ pp.getTeam()
						+ "','"
						+ pp.getAppearance()
						+ "','"
						+ pp.getFirstPlay()
						+ "','"
						+ pp.getFieldGoal()
						+ "','"
						+ pp.getFieldGoalAttempts()
						+ "','"
						+ pp.getThreePointFieldGoal()
						+ "','"
						+ pp.getThreePointFieldGoalAttempts()
						+ "','"
						+ pp.getFreeThrow()
						+ "','"
						+ pp.getFreeThrowAttempts()
						+ "','"
						+ pp.getOffensiveRebound()
						+ "','"
						+ pp.getDefensiveRebound()
						+ "','"
						+ pp.getBackboard()
						+ "','"
						+ pp.getAssist()
						+ "','"
						+ pp.getMinites()
						+ "','"
						+ pp.getSteal()
						+ "','"
						+ pp.getBlock()
						+ "','"
						+ pp.getTurnOver()
						+ "','"
						+ pp.getFoul()
						+ "','"
						+ pp.getScoring() 
						+ "','"
						+ pp.getTeamFieldGoalAttempts()
						+ "','"
						+ pp.getTeamBackboard()
						+ "','"
						+ pp.getTeamMinutes()
						+ "','"
						+ pp.getTeamFreeThrowAttempts()
						+ "','"
						+ pp.getTeamTurnOver()
						+ "','"
						+ pp.getOpponentFieldGoalAttempts()
						+ "','"
						+pp.getOpponentThreePointFieldGoalAttempts()
						+ "')";
				statement.addBatch(sql);
			}
			statement.executeBatch();
			if(isAll)
				sql="SELECT * FROM temp ORDER BY `"+key+"` "+order;
			else
				sql="SELECT * FROM temp ORDER BY `"+key+"` "+order+" LIMIT 50";
			ResultSet rs=statement.executeQuery(sql);
			while(rs.next()){
				PlayerPO pp=new PlayerPO(rs.getString(1), rs.getString(2), rs.getInt(3),rs.getInt(4) , rs.getInt(5), rs.getInt(6),rs.getInt(7),rs.getInt(8), rs.getInt(9), rs.getInt(10), rs.getInt(11), rs.getInt(12), rs.getInt(13), rs.getInt(14), rs.getDouble(15), rs.getInt(16), rs.getInt(17), rs.getInt(18), rs.getInt(19), rs.getInt(20), rs.getInt(21), rs.getInt(22), rs.getDouble(23), rs.getInt(24), rs.getInt(25), rs.getInt(26), rs.getInt(27));
				r.add(pp);
			}
			sql="DROP TABLE temp";
			statement.execute(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return r;
	}
	
	public ArrayList<PlayerPO> getByEfficiency(ArrayList<PlayerPO> po,String key,String order){
		ArrayList<PlayerPO> r=new ArrayList<PlayerPO>();
		
		return r;
	}
}
