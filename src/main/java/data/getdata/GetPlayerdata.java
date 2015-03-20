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

import po.PlayerPO;
import po.PlayerinfoPO;
import data.initial.InitialDatabase;
import dataservice.getdatadataservice.GetPlayerdataDataService;

public class GetPlayerdata extends UnicastRemoteObject implements GetPlayerdataDataService{

	Statement statement;
	public GetPlayerdata() throws RemoteException{
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
		PlayerPO po=null;
		try {
			if(playerName.contains("'"))
				playerName=playerName.substring(0,playerName.indexOf("'"))+"\\"+playerName.substring(playerName.indexOf("'"), playerName.length());
			String sql="SELECT * FROM playersum WHERE playerName='"+playerName+"'";
			ResultSet rs=statement.executeQuery(sql);
			while(rs.next()){
			    if (playerName.contains("'")) {
			    	playerName=playerName.substring(0,playerName.indexOf("\\"))+playerName.substring(playerName.indexOf("\\")+1, playerName.length());
			    }
			    // po=new PlayerPO(playerName,rs.getString(1),rs.getInt(2), rs.getInt(2),  rs.getInt(2), rs.getInt(2), rs.getInt(2), rs.getInt(2), rs.getInt(2), rs.getInt(2),rs.getInt(2),  rs.getInt(2), rs.getInt(2), rs.getInt(2),defensiveRebound,steal,block,turnOver,foul,scoring, teamFieldGoalAttempts,teamBackboard,teamFieldGoal,teamFreeThrow,teamOffensiveRebound, teamDefensiveRebound,teamMinutes, teamFreeThrowAttempts, teamTurnOver,opponentBackBoard,opponentOffensiveRebound,opponentDefensiveRebound,opponentFieldGoalAttempts,opponentThreePointFieldGoalAttempts,threePointShotPercentage,freeThrowPercentage,efficiency,GmScEfficiency,nearlyFivePercentage,trueShootingPercentage,shootingEfficiency,backboardPercentage,offensiveReboundPercentage,defensiveReboundPercentage,assistPercentage,stealPercentage,blockPercentage,turnOverPercentage,usage,previousAverageScoring,nearlyFiveAverageScoring,doubleDouble);
			    }
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
		File f=new File("data/players/info");
		String[] filelist=f.list();
		for (int i = 0; i < filelist.length; i++) {
			PlayerPO temp=getPlayerdata(filelist[i]);
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
		String sql="CREATE TABLE temp (	 playerName varchar(255), team varchar(255),appearance int,firstPlay int,backboard int,assist int,minites double,fieldGoal int,fieldGoalAttempts int,threePointFieldGoal int,threePointFieldGoalAttempts int,freeThrow int,freeThrowAttempts int,offensiveRebound int,defensiveRebound int,steal int,block int,turnOver int, foul int,scoring int,teamFieldGoalAttempts int,teamBackboard int,teamFieldGoal int,teamFreeThrow int, teamOffensiveRebound int, teamDefensiveRebound int,teamMinutes double,teamFreeThrowAttempts int,teamTurnOver int,opponentBackBoard int, opponentOffensiveRebound int, opponentDefensiveRebound int,opponentFieldGoalAttempts int,opponentThreePointFieldGoalAttempts int,	previousAverageScoring double, nearlyFiveAverageScoring double,doubleDouble int)";
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
						+pp.getPreviousAverageScoring()
						+ "','"
						+pp.getNearlyFiveAverageScoring()
						+ "','"
						+pp.getDoubleDouble()+"')";
				statement.addBatch(sql);
			}
			statement.executeBatch();
			if(isAll)
				sql="SELECT * FROM temp ORDER BY `"+key+"` "+order;
			else
				sql="SELECT * FROM temp ORDER BY `"+key+"` "+order+" LIMIT 50";
			ResultSet rs=statement.executeQuery(sql);
			while(rs.next()){
				PlayerPO pp=new PlayerPO(rs.getString(1), rs.getString(2), rs.getInt(3),rs.getInt(4) , rs.getInt(5), rs.getInt(6),rs.getDouble(7),rs.getInt(8), rs.getInt(9), rs.getInt(10), rs.getInt(11), rs.getInt(12), rs.getInt(13), rs.getInt(14), rs.getInt(15), rs.getInt(16), rs.getInt(17), rs.getInt(18), rs.getInt(19), rs.getInt(20), rs.getInt(21), rs.getInt(22),rs.getInt(23) ,rs.getInt(24),rs.getInt(25),rs.getInt(26),rs.getDouble(27), rs.getInt(28), rs.getInt(29), rs.getInt(30), rs.getInt(31), rs.getInt(32), rs.getInt(33), rs.getInt(34),0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,rs.getDouble(35),rs.getDouble(36),rs.getInt(37));
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
		String sql="CREATE TABLE temp (	 playerName varchar(255), team varchar(255),appearance int,firstPlay int,backboard int,assist int,minites double,fieldGoal int,fieldGoalAttempts int,threePointFieldGoal int,threePointFieldGoalAttempts int,freeThrow int,freeThrowAttempts int,offensiveRebound int,defensiveRebound int,steal int,block int,turnOver int, foul int,scoring int,teamFieldGoalAttempts int,teamBackboard int,teamFieldGoal int,teamFreeThrow int, teamOffensiveRebound int, teamDefensiveRebound int,teamMinutes double,teamFreeThrowAttempts int,teamTurnOver int,opponentBackBoard int, opponentOffensiveRebound int, opponentDefensiveRebound int,opponentFieldGoalAttempts int,opponentThreePointFieldGoalAttempts int,threePointShotPercentage double, freeThrowPercentage double, efficiency double, GmScEfficiency double, nearlyFivePercentage double, trueShootingPercentage double, shootingEfficiency double, backboardPercentage double, offensiveReboundPercentage double, defensiveReboundPercentage double, assistPercentage double, stealPercentage double, blockPercentage double, turnOverPercentage double, usage double,	previousAverageScoring double, nearlyFiveAverageScoring double,doubleDouble int)";
		try {
			statement.addBatch(sql);
			for (int i = 0; i < po.size(); i++) {
				PlayerPO pp=po.get(i);
				if(pp.getPlayerName().contains("'"))
					pp.setPlayerName(pp.getPlayerName().substring(0,pp.getPlayerName().indexOf("'"))+"\\"+pp.getPlayerName().substring(pp.getPlayerName().indexOf("'"), pp.getPlayerName().length()));
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
				PlayerPO pp=new PlayerPO(rs.getString(1), rs.getString(2), rs.getInt(3),rs.getInt(4) , rs.getInt(5), rs.getInt(6),rs.getDouble(7),rs.getInt(8), rs.getInt(9), rs.getInt(10), rs.getInt(11), rs.getInt(12), rs.getInt(13), rs.getInt(14), rs.getInt(15), rs.getInt(16), rs.getInt(17), rs.getInt(18), rs.getInt(19), rs.getInt(20), rs.getInt(21), rs.getInt(22),rs.getInt(23) ,rs.getInt(24),rs.getInt(25),rs.getInt(26),rs.getDouble(27), rs.getInt(28), rs.getInt(29), rs.getInt(30), rs.getInt(31), rs.getInt(32), rs.getInt(33), rs.getInt(34),rs.getDouble(35),rs.getDouble(36),rs.getDouble(37),rs.getDouble(38),rs.getDouble(39),rs.getDouble(40),rs.getDouble(41),rs.getDouble(42),rs.getDouble(43),rs.getDouble(44),rs.getDouble(45),rs.getDouble(46),rs.getDouble(47),rs.getDouble(48),rs.getDouble(49),rs.getDouble(50),rs.getDouble(51),rs.getInt(52));
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
}