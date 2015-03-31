package data.getdata;

import java.io.File;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.ArrayList;

import po.PlayerMatchPO;
import po.PlayerPO;
import po.PlayerinfoPO;
import data.initial.InitialDatabase;
import dataservice.getdatadataservice.GetPlayerdataDataService;

public class GetPlayerdata extends UnicastRemoteObject implements GetPlayerdataDataService{

	Statement statement;
	public GetPlayerdata() throws RemoteException{
		try {
			Class.forName(InitialDatabase.driver);
			Connection conn = DriverManager.getConnection(InitialDatabase.url);
			statement = conn.createStatement();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	public PlayerPO getPlayerdata(String playerName){
		if(playerName.contains("'"))
			playerName=playerName.substring(0,playerName.indexOf("'"))+"'"+playerName.substring(playerName.indexOf("'"), playerName.length());
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
		
		double fieldGoalShotPercentage=0;//投篮命中率
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
		int doubleDouble=0;
		String sql="SELECT * FROM playersum WHERE playerName='"+playerName+"'";
		try {
			ResultSet rs=statement.executeQuery(sql);
			while(rs.next()){
			    team=rs.getString(2);
			    appearance=rs.getInt(3);
			    firstPlay=rs.getInt(4);
			    backboard=rs.getInt(5);
			    assist=rs.getInt(6);
			    minutes=rs.getDouble(7);
			    fieldGoal=rs.getInt(8);
			    fieldGoalAttempts=rs.getInt(9);
			    threePointFieldGoal=rs.getInt(10);
			    threePointFieldGoalAttempts=rs.getInt(11);
			    freeThrow=rs.getInt(12);
			    freeThrowAttempts=rs.getInt(13);
			    offensiveRebound=rs.getInt(14);
			    defensiveRebound=rs.getInt(15);
			    steal=rs.getInt(16);
			    block=rs.getInt(17);
			    turnOver=rs.getInt(18);
			    foul=rs.getInt(19);
			    scoring=rs.getInt(20);
			    previousAverageScoring=rs.getDouble(21);
			    nearlyFiveAverageScoring=rs.getDouble(22);
			    doubleDouble=rs.getInt(23);
			}
			sql="SELECT fieldGoal,fieldGoalAttempts,backboard,freeThrow,offensiveRebound,defensiveRebound,minutes,freeThrowAttempts,turnOver,opponentBackBoard,opponentOffensiveRebound,opponentDefensiveRebound,opponentFieldGoalAttempts,opponentThreePointFieldGoalAttempts FROM teamsum WHERE teamName='"+team+"'";
			rs=statement.executeQuery(sql);
			while(rs.next()){
				teamFieldGoal=rs.getInt(1);
				teamFieldGoalAttempts=rs.getInt(2);
				teamBackboard=rs.getInt(3);
				teamFreeThrow=rs.getInt(4);
				teamOffensiveRebound=rs.getInt(5);
				teamDefensiveRebound=rs.getInt(6);
				teamMinutes=rs.getDouble(7);
				teamFreeThrowAttempts=rs.getInt(8);
				teamTurnOver=rs.getInt(9);
				opponentBackBoard=rs.getInt(10);
				opponentOffensiveRebound=rs.getInt(11);
				opponentDefensiveRebound=rs.getInt(12);
				opponentFieldGoalAttempts=rs.getInt(13);
				opponentThreePointFieldGoalAttempts=rs.getInt(14);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (playerName.contains("'")) {
			playerName=playerName.substring(0,playerName.indexOf("''"))+playerName.substring(playerName.indexOf("'")+1, playerName.length());
		}
		PlayerPO po=new PlayerPO(playerName,team,appearance, firstPlay,  backboard, assist, minutes, fieldGoal, fieldGoalAttempts, threePointFieldGoal,threePointFieldGoalAttempts,  freeThrow, freeThrowAttempts, offensiveRebound,defensiveRebound,steal,block,turnOver,foul,scoring, teamFieldGoalAttempts,teamBackboard,teamFieldGoal,teamFreeThrow,teamOffensiveRebound, teamDefensiveRebound,teamMinutes, teamFreeThrowAttempts, teamTurnOver,opponentBackBoard,opponentOffensiveRebound,opponentDefensiveRebound,opponentFieldGoalAttempts,opponentThreePointFieldGoalAttempts,fieldGoalShotPercentage,threePointShotPercentage,freeThrowPercentage,efficiency,GmScEfficiency,nearlyFivePercentage,trueShootingPercentage,shootingEfficiency,backboardPercentage,offensiveReboundPercentage,defensiveReboundPercentage,assistPercentage,stealPercentage,blockPercentage,turnOverPercentage,usage,previousAverageScoring,nearlyFiveAverageScoring,doubleDouble);
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
		String playerName="";
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
		
		double fieldGoalShotPercentage=0;//投篮命中率
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
		int doubleDouble=0;
		ArrayList<PlayerPO> po=new ArrayList<PlayerPO>();
		try {
			String sql="";
			ResultSet rs=null;
			if(order.equals("ASC")){
				sql="SELECT playerName FROM playersum WHERE team=''";
				rs=statement.executeQuery(sql);
				while(rs.next()){
					playerName=rs.getString(1);
					PlayerPO temp=new PlayerPO(playerName, team, appearance, firstPlay, backboard, assist, minutes, fieldGoal, fieldGoalAttempts, threePointFieldGoal, threePointFieldGoalAttempts, freeThrow, freeThrowAttempts, offensiveRebound, defensiveRebound, steal, block, turnOver, foul, scoring, teamFieldGoalAttempts, teamBackboard, teamFieldGoal, teamFreeThrow, teamOffensiveRebound, teamDefensiveRebound, teamMinutes, teamFreeThrowAttempts, teamTurnOver, opponentBackBoard, opponentOffensiveRebound, opponentDefensiveRebound, opponentFieldGoalAttempts, opponentThreePointFieldGoalAttempts, fieldGoalShotPercentage,threePointShotPercentage, freeThrowPercentage, efficiency, GmScEfficiency, nearlyFivePercentage, trueShootingPercentage, shootingEfficiency, backboardPercentage, offensiveReboundPercentage, defensiveReboundPercentage, assistPercentage, stealPercentage, blockPercentage, turnOverPercentage, usage, previousAverageScoring, nearlyFiveAverageScoring, doubleDouble);
					po.add(temp);
				}
			}
			sql="SELECT * FROM playersum,teamsum WHERE playersum.team=teamsum.teamName ORDER BY playersum."+key+" "+order;
			rs=statement.executeQuery(sql);
			while(rs.next()){
				playerName=rs.getString(1);
				team=rs.getString(2);
				appearance=rs.getInt(3);
				firstPlay=rs.getInt(4);
				backboard=rs.getInt(5);
				assist=rs.getInt(6);
				minutes=rs.getDouble(7);
				fieldGoal=rs.getInt(8);
				fieldGoalAttempts =rs.getInt(9);
				threePointFieldGoal =rs.getInt(10);
				threePointFieldGoalAttempts=rs.getInt(11);
				freeThrow =rs.getInt(12);
				freeThrowAttempts =rs.getInt(13);
				offensiveRebound =rs.getInt(14);
				defensiveRebound=rs.getInt(15);
				steal=rs.getInt(16);
				block=rs.getInt(17);
				turnOver =rs.getInt(18);
				foul=rs.getInt(19);
				scoring=rs.getInt(20);
				previousAverageScoring	=rs.getDouble(21);
				nearlyFiveAverageScoring =rs.getDouble(22);
				doubleDouble=rs.getInt(23);
				teamFieldGoal=rs.getInt(32);
				teamFieldGoalAttempts=rs.getInt(33);
				teamBackboard=rs.getInt(42);
				teamFreeThrow=rs.getInt(36);
				teamOffensiveRebound=rs.getInt(38);
				teamDefensiveRebound=rs.getInt(39);
				teamMinutes=rs.getDouble(49);
				teamFreeThrowAttempts=rs.getInt(37);
				teamTurnOver=rs.getInt(46);
				opponentBackBoard=rs.getInt(50);
				opponentOffensiveRebound=rs.getInt(40);
				opponentDefensiveRebound=rs.getInt(41);
				opponentFieldGoalAttempts=rs.getInt(25);
				opponentThreePointFieldGoalAttempts=rs.getInt(51);
				PlayerPO temp=new PlayerPO(playerName, team, appearance, firstPlay, backboard, assist, minutes, fieldGoal, fieldGoalAttempts, threePointFieldGoal, threePointFieldGoalAttempts, freeThrow, freeThrowAttempts, offensiveRebound, defensiveRebound, steal, block, turnOver, foul, scoring, teamFieldGoalAttempts, teamBackboard, teamFieldGoal, teamFreeThrow, teamOffensiveRebound, teamDefensiveRebound, teamMinutes, teamFreeThrowAttempts, teamTurnOver, opponentBackBoard, opponentOffensiveRebound, opponentDefensiveRebound, opponentFieldGoalAttempts, opponentThreePointFieldGoalAttempts, fieldGoalShotPercentage,threePointShotPercentage, freeThrowPercentage, efficiency, GmScEfficiency, nearlyFivePercentage, trueShootingPercentage, shootingEfficiency, backboardPercentage, offensiveReboundPercentage, defensiveReboundPercentage, assistPercentage, stealPercentage, blockPercentage, turnOverPercentage, usage, previousAverageScoring, nearlyFiveAverageScoring, doubleDouble);
				po.add(temp);
			}
			if(order.equals("DESC")){
				sql="SELECT playerName FROM playersum WHERE team=''";
				rs=statement.executeQuery(sql);
				while(rs.next()){
					playerName=rs.getString(1);
					PlayerPO temp=new PlayerPO(playerName, "", 0, 0, 0, 0, 0, 0, 0, 0, 0,0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
					po.add(temp);
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return po;
	}
	//W/E
	public ArrayList<PlayerPO> getSomePlayerdata(String position,String partition,String key,String order){
		String playerName="";
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
		
		double fieldGoalShotPercentage=0;//投篮命中率
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
		int doubleDouble=0;
		ArrayList<PlayerPO> po=new ArrayList<PlayerPO>();
		String sql="";
		ResultSet rs=null;
		try {
			if(partition.startsWith("league:")){
				partition =partition.substring(partition.indexOf(":")+1, partition.length());
				sql="SELECT * FROM playersum,teamsum,(SELECT playerName,team FROM playersum,teaminfo,playerinfo WHERE playersum.team=teaminfo.abbr AND playersum.playerName=playerinfo.name AND teaminfo.`east/west` = '"+partition+"' AND playerinfo.position LIKE '%"+position+"%') AS a WHERE playersum.playerName=a.playerName AND playersum.team=teamsum.teamName ORDER BY playersum."+key+" "+order+" LIMIT 50";
			}else{
				partition =partition.substring(partition.indexOf(":")+1, partition.length());
				sql="SELECT * FROM playersum,teamsum,(SELECT playerName,team FROM playersum,teaminfo,playerinfo WHERE playersum.team=teaminfo.abbr AND playersum.playerName=playerinfo.name AND teaminfo.partition = '"+partition+"' AND playerinfo.position LIKE '%"+position+"%') AS a WHERE playersum.playerName=a.playerName AND playersum.team=teamsum.teamName ORDER BY playersum."+key+" "+order+" LIMIT 50";
			}
		    rs=statement.executeQuery(sql);
		    while(rs.next()){
		    	playerName=rs.getString(1);
		    	team=rs.getString(2);
		    	appearance=rs.getInt(3);
		    	firstPlay=rs.getInt(4);
		    	backboard=rs.getInt(5);
		    	assist=rs.getInt(6);
		    	minutes=rs.getDouble(7);
		    	fieldGoal=rs.getInt(8);
		    	fieldGoalAttempts =rs.getInt(9);
		    	threePointFieldGoal =rs.getInt(10);
		    	threePointFieldGoalAttempts=rs.getInt(11);
		    	freeThrow =rs.getInt(12);
		    	freeThrowAttempts =rs.getInt(13);
		    	offensiveRebound =rs.getInt(14);
		    	defensiveRebound=rs.getInt(15);
		    	steal=rs.getInt(16);
		    	block=rs.getInt(17);
		    	turnOver =rs.getInt(18);
		    	foul=rs.getInt(19);
		    	scoring=rs.getInt(20);
		    	previousAverageScoring	=rs.getDouble(21);
		    	nearlyFiveAverageScoring =rs.getDouble(22);
		    	doubleDouble=rs.getInt(23);
		    	teamFieldGoal=rs.getInt(32);
		    	teamFieldGoalAttempts=rs.getInt(33);
		    	teamBackboard=rs.getInt(42);
		    	teamFreeThrow=rs.getInt(36);
		    	teamOffensiveRebound=rs.getInt(38);
		    	teamDefensiveRebound=rs.getInt(39);
		    	teamMinutes=rs.getDouble(49);
		    	teamFreeThrowAttempts=rs.getInt(37);
		    	teamTurnOver=rs.getInt(46);
		    	opponentBackBoard=rs.getInt(50);
		    	opponentOffensiveRebound=rs.getInt(40);
		    	opponentDefensiveRebound=rs.getInt(41);
		    	opponentFieldGoalAttempts=rs.getInt(25);
		    	opponentThreePointFieldGoalAttempts=rs.getInt(51);
		    	PlayerPO temp=new PlayerPO(playerName, team, appearance, firstPlay, backboard, assist, minutes, fieldGoal, fieldGoalAttempts, threePointFieldGoal, threePointFieldGoalAttempts, freeThrow, freeThrowAttempts, offensiveRebound, defensiveRebound, steal, block, turnOver, foul, scoring, teamFieldGoalAttempts, teamBackboard, teamFieldGoal, teamFreeThrow, teamOffensiveRebound, teamDefensiveRebound, teamMinutes, teamFreeThrowAttempts, teamTurnOver, opponentBackBoard, opponentOffensiveRebound, opponentDefensiveRebound, opponentFieldGoalAttempts, opponentThreePointFieldGoalAttempts, fieldGoalShotPercentage,threePointShotPercentage, freeThrowPercentage, efficiency, GmScEfficiency, nearlyFivePercentage, trueShootingPercentage, shootingEfficiency, backboardPercentage, offensiveReboundPercentage, defensiveReboundPercentage, assistPercentage, stealPercentage, blockPercentage, turnOverPercentage, usage, previousAverageScoring, nearlyFiveAverageScoring, doubleDouble);
		    	po.add(temp);
		    }
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return po;
	}
	
	public ArrayList<PlayerPO> getByEfficiency(ArrayList<PlayerPO> po,String key,String order){
		ArrayList<PlayerPO> r=new ArrayList<PlayerPO>();
		String sql="CREATE TABLE temp (	 playerName varchar(255), team varchar(255),appearance int,firstPlay int,backboard int,assist int,minites double,fieldGoal int,fieldGoalAttempts int,threePointFieldGoal int,threePointFieldGoalAttempts int,freeThrow int,freeThrowAttempts int,offensiveRebound int,defensiveRebound int,steal int,block int,turnOver int, foul int,scoring int,teamFieldGoalAttempts int,teamBackboard int,teamFieldGoal int,teamFreeThrow int, teamOffensiveRebound int, teamDefensiveRebound int,teamMinutes double,teamFreeThrowAttempts int,teamTurnOver int,opponentBackBoard int, opponentOffensiveRebound int, opponentDefensiveRebound int,opponentFieldGoalAttempts int,opponentThreePointFieldGoalAttempts int,fieldGoalShotPercentage double,threePointShotPercentage double, freeThrowPercentage double, efficiency double, GmScEfficiency double, nearlyFivePercentage double, trueShootingPercentage double, shootingEfficiency double, backboardPercentage double, offensiveReboundPercentage double, defensiveReboundPercentage double, assistPercentage double, stealPercentage double, blockPercentage double, turnOverPercentage double, `usage` double,	previousAverageScoring double, nearlyFiveAverageScoring double,doubleDouble int)";
		try {
			statement.addBatch(sql);
			for (int i = 0; i < po.size(); i++) {
				PlayerPO pp=po.get(i);
				if(pp.getPlayerName().contains("'"))
					pp.setPlayerName(pp.getPlayerName().substring(0,pp.getPlayerName().indexOf("'"))+"'"+pp.getPlayerName().substring(pp.getPlayerName().indexOf("'"), pp.getPlayerName().length()));
				sql="INSERT INTO temp values('"
						+ pp.getPlayerName()
						+ "','"
						+ pp.getTeam()
						+ "','"
						+ pp.getAppearance()
						+ "','"
						+ pp.getFirstPlay()
						+ "','"
						+ pp.getBackboard()
						+ "','"
						+ pp.getAssist()
						+ "','"
						+ pp.getMinites()
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
						+ pp.getTeamFieldGoal()
						+ "','"
						+pp.getTeamFreeThrow()
						+ "','"
						+pp.getTeamOffensiveRebound()
						+ "','"
						+pp.getTeamDefensiveRebound()
						+ "','"
						+ pp.getTeamMinutes()
						+ "','"
						+ pp.getTeamFreeThrowAttempts()
						+ "','"
						+ pp.getTeamTurnOver()
						+ "','"
						+pp.getOpponentBackBoard()
						+ "','"
						+pp.getOpponentOffensiveRebound()
						+ "','"
						+pp.getOpponentDefensiveRebound()
						+ "','"
						+ pp.getOpponentFieldGoalAttempts()
						+ "','"
						+pp.getOpponentThreePointFieldGoalAttempts()
						+ "','"
						+pp.getFieldGoalShotPercentage()
						+ "','"
						+pp.getThreePointShotPercentage()
						+ "','"
						+pp.getFreeThrowPercentage()
						+ "','"
						+pp.getEfficiency()
						+ "','"
						+pp.getGmScEfficiency()
						+ "','"
						+pp.getNearlyFivePercentage()
						+ "','"
						+pp.getTrueShootingPercentage()
						+ "','"
						+pp.getShootingEfficiency()
						+ "','"
						+pp.getBackboardPercentage()
						+ "','"
						+pp.getOffensiveReboundPercentage()
						+ "','"
						+pp.getDefensiveReboundPercentage()
						+ "','"
						+pp.getAssistPercentage()
						+ "','"
						+pp.getStealPercentage()
						+ "','"
						+pp.getBlockPercentage()
						+ "','"
						+pp.getTurnOverPercentage()
						+ "','"
						+pp.getUsage()
						+ "','"
						+pp.getPreviousAverageScoring()
						+ "','"
						+pp.getNearlyFiveAverageScoring()
						+ "','"
						+pp.getDoubleDouble()+"')";
				statement.addBatch(sql);
			}
			statement.executeBatch();
				sql="SELECT * FROM temp ORDER BY `"+key+"` "+order;
			ResultSet rs=statement.executeQuery(sql);
			while(rs.next()){
				PlayerPO pp=new PlayerPO(rs.getString(1), rs.getString(2), rs.getInt(3),rs.getInt(4) , rs.getInt(5), rs.getInt(6),rs.getDouble(7),rs.getInt(8), rs.getInt(9), rs.getInt(10), rs.getInt(11), rs.getInt(12), rs.getInt(13), rs.getInt(14), rs.getInt(15), rs.getInt(16), rs.getInt(17), rs.getInt(18), rs.getInt(19), rs.getInt(20), rs.getInt(21), rs.getInt(22),rs.getInt(23) ,rs.getInt(24),rs.getInt(25),rs.getInt(26),rs.getDouble(27), rs.getInt(28), rs.getInt(29), rs.getInt(30), rs.getInt(31), rs.getInt(32), rs.getInt(33), rs.getInt(34),rs.getDouble(35),rs.getDouble(36),rs.getDouble(37),rs.getDouble(38),rs.getDouble(39),rs.getDouble(40),rs.getDouble(41),rs.getDouble(42),rs.getDouble(43),rs.getDouble(44),rs.getDouble(45),rs.getDouble(46),rs.getDouble(47),rs.getDouble(48),rs.getDouble(49),rs.getDouble(50),rs.getDouble(51),rs.getDouble(52),rs.getInt(53));
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
	
	public ArrayList<PlayerMatchPO> getPlayerMonthMatch(String month,String player){
		ArrayList<PlayerMatchPO> po=new ArrayList<PlayerMatchPO>();
		String sql="SELECT * FROM playerdata WHERE date LIKE '"+month+"%' AND playername='"+player+"' ORDER BY date DESC";
		try {
			ResultSet rs=statement.executeQuery(sql);
			while(rs.next()){
				PlayerMatchPO temp=new PlayerMatchPO(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getDouble(5), rs.getInt(6),  rs.getInt(7),  rs.getInt(8),  rs.getInt(9),  rs.getInt(10),  rs.getInt(11),  rs.getInt(12),  rs.getInt(13),  rs.getInt(14),  rs.getInt(15),  rs.getInt(16),  rs.getInt(17),  rs.getInt(18),  rs.getInt(19),  rs.getInt(20));
				po.add(temp);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return po;
	}
	
	public ArrayList<PlayerMatchPO> getPlayerRecentFiveMatch(String player){
		ArrayList<PlayerMatchPO> po=new ArrayList<PlayerMatchPO>();
		String sql="SELECT * FROM playerdata WHERE playername='"+player+"' ORDER BY date DESC LIMIT 5";
		try {
			ResultSet rs=statement.executeQuery(sql);
			while(rs.next()){
				PlayerMatchPO temp=new PlayerMatchPO(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getDouble(5), rs.getInt(6),  rs.getInt(7),  rs.getInt(8),  rs.getInt(9),  rs.getInt(10),  rs.getInt(11),  rs.getInt(12),  rs.getInt(13),  rs.getInt(14),  rs.getInt(15),  rs.getInt(16),  rs.getInt(17),  rs.getInt(18),  rs.getInt(19),  rs.getInt(20));
				po.add(temp);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return po;
	}
	
	public ArrayList<PlayerMatchPO> getDayTop(String date,String condition){
		ArrayList<PlayerMatchPO> po=new ArrayList<PlayerMatchPO>();
		String sql="SELECT * FROM playerdata WHERE date='"+date+"' ORDER BY "+condition+" DESC LIMIT 5";
		try {
			ResultSet rs=statement.executeQuery(sql);
			while(rs.next()){
				PlayerMatchPO temp=new PlayerMatchPO(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getDouble(5), rs.getInt(6), rs.getInt(7), rs.getInt(8), rs.getInt(9), rs.getInt(10), rs.getInt(11), rs.getInt(12), rs.getInt(13), rs.getInt(14), rs.getInt(15), rs.getInt(16), rs.getInt(17), rs.getInt(18), rs.getInt(19), rs.getInt(20));
				po.add(temp);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return po;
	}
	
	public ArrayList<PlayerMatchPO> getSeasonTop(String season,String condition){
		ArrayList<PlayerMatchPO> po=new ArrayList<PlayerMatchPO>();
		String sql="SELECT * FROM playersum WHERE ORDER BY "+condition+" DESC LIMIT 5";
		try {
			ResultSet rs=statement.executeQuery(sql);
			while(rs.next()){
				PlayerMatchPO temp=new PlayerMatchPO(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getDouble(5), rs.getInt(6), rs.getInt(7), rs.getInt(8), rs.getInt(9), rs.getInt(10), rs.getInt(11), rs.getInt(12), rs.getInt(13), rs.getInt(14), rs.getInt(15), rs.getInt(16), rs.getInt(17), rs.getInt(18), rs.getInt(19), rs.getInt(20));
				po.add(temp);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return po;
	}


}