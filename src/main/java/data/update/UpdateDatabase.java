package data.update;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.TimerTask;

import data.initial.InitialDatabase;
import rmi.Server;

public class UpdateDatabase extends TimerTask{

	Connection conn=null;
	public void run(){
		File f=new File("data/matches");
		String[] matches=f.list();
		for (int i = 0; i < matches.length; i++) {
			String[] temp=matches[i].split("_");
			if(!Server.season.contains(temp[0])){
				createTable(temp[0]);
				Server.season.add(temp[0]);
			}
		}
		if(matches.length!=Server.matches.length){
			updateData(matches,Server.matches);
			Server.matches=matches;
		}
	}
	
	public void updateData(String[] newData,String[] oldData){
		try {
			Class.forName(InitialDatabase.driver);
			conn = DriverManager.getConnection(InitialDatabase.url);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} 
		if(newData.length>oldData.length){
			for (int i = 0; i < newData.length; i++) {
				boolean notExist=true;
				for (int j = 0; j < oldData.length; j++) {
					if(newData[i].equals(oldData[j])){
						notExist=false;
						break;
					}
				}
				if(notExist){
					//insert into playerdata
					String[] temp=newData[i].split("_");    //12-13
					String season=temp[0];
					String[] year=temp[0].split("-");
					String date=temp[1];
					String[] team=temp[2].split("-");
					if(date.startsWith("10-")||date.startsWith("11-")||date.startsWith("12-"))
							date=year[0]+"-"+date;
					else
						date=year[1]+"-"+date;                  //date  13-12-02
					try {
						FileReader fr=new FileReader("data/matches/"+newData[i]);
						BufferedReader br=new BufferedReader(fr);
						String line="";
						int count=0;
						String info="";
						while((line=br.readLine())!=null){
							if(!line.contains(";")){
								count++;
								continue;
							}
							if(line.charAt(0)>=48&&line.charAt(0)<=57){
								continue;
							}else{
								if(count==1)
									info=info+date+";"+team[0]+";"+line.substring(0,line.length()-1)+"%";
								else
									info=info+date+";"+team[1]+";"+line.substring(0,line.length()-1)+"%";
							}
						}
						PreparedStatement ps=conn.prepareStatement("INSERT INTO playerdata  values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
						String[] singleinfo=info.split("%");
						for (int j = 0; j < singleinfo.length; j++) {
							temp=singleinfo[j].split(";");
							if(temp[19].charAt(0)<48||temp[19].charAt(0)>57)
								temp[19]="0";
							double time=0;
							DecimalFormat df=new DecimalFormat("#.0");  
							if(temp[4].contains(":")){
								String[] temp1=temp[4].split(":");
								time=Double.parseDouble(temp1[0])+Double.parseDouble(df.format(Double.parseDouble(temp1[1])/60));
							}else if(temp[4].charAt(0)>=48&&temp[4].charAt(0)<=57){
								time=Double.parseDouble(df.format(Double.parseDouble(temp[4])/60));
							}
							ps.setString(1, temp[0]); 
							ps.setString(2, temp[1]); 
							ps.setString(3, temp[2]); 
							ps.setString(4, temp[3]); 
							ps.setDouble(5, time);
							ps.setInt(6, Integer.parseInt(temp[5]));
							ps.setInt(7, Integer.parseInt(temp[6]));
							ps.setInt(8, Integer.parseInt(temp[7]));
							ps.setInt(9, Integer.parseInt(temp[8]));
							ps.setInt(10, Integer.parseInt(temp[9]));
							ps.setInt(11, Integer.parseInt(temp[10]));
							ps.setInt(12, Integer.parseInt(temp[11]));
							ps.setInt(13, Integer.parseInt(temp[12]));
							ps.setInt(14, Integer.parseInt(temp[13]));
							ps.setInt(15, Integer.parseInt(temp[14]));
							ps.setInt(16, Integer.parseInt(temp[15]));
							ps.setInt(17, Integer.parseInt(temp[16]));
							ps.setInt(18, Integer.parseInt(temp[17]));
							ps.setInt(19, Integer.parseInt(temp[18]));
							ps.setInt(20, Integer.parseInt(temp[19]));
							ps.addBatch();
							String data="";
							for (int k = 0; k < 4; k++) {
								data=data+temp[k]+";";
							}
							data=data+time+";";
							for (int k = 5; k < 20; k++) {
								data=data+temp[k]+";";
							}
							updatePlayersum(season,data,conn);
						}
						ps.executeBatch();
						//insert into matches
						updataMatches(newData[i]);
						updateTeamsum(season,date,conn,team);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}else{
			
		}
	}
	
	public void createTable(String season){
		try {
			Class.forName(InitialDatabase.driver);
			conn = DriverManager.getConnection(InitialDatabase.url);
			Statement statement=conn.createStatement();
			String sql="CREATE TABLE IF NOT EXISTS `playersum"+season+"` (`playerName`	TEXT,`team`	TEXT,`appearance`	INTEGER,	`firstPlay`	INTEGER,`backboard`	INTEGER,	`assist`	INTEGER,	`minutes`	REAL,`fieldGoal`	INTEGER,`fieldGoalAttempts`	INTEGER,`threePointFieldGoal` INTEGER,`threePointFieldGoalAttempts` INTEGER,`freeThrow`	INTEGER,`freeThrowAttempts` INTEGER, `offensiveRebound` INTEGER, `defensiveRebound`	INTEGER,	`steal` INTEGER, `block`	INTEGER,	`turnOver` INTEGER, `foul` INTEGER, `scoring` INTEGER, `previousAverageScoring` INTEGER, `nearlyFiveAverageScoring` INTEGER,	`doubleDouble` INTEGER, PRIMARY KEY(playerName,team))";
			statement.addBatch(sql);
			sql="CREATE TABLE IF NOT EXISTS `teamsum"+season+"` (`opponentFieldGoal`	INTEGER,`opponentFieldGoalAttempts` INTEGER,`opponentTurnOver` INTEGER,`opponentFreeThrowAttempts`	INTEGER,	`oppenentScoring`	INTEGER,	`teamName`	TEXT,`matches` INTEGER,`wins`	INTEGER,`fieldGoal`	INTEGER,	`fieldGoalAttempts` INTEGER,`threePointFieldGoal`	INTEGER,	`threePointFieldGoalAttempts`	INTEGER,	`freeThrow`	INTEGER,	`freeThrowAttempts`	INTEGER,	`offensiveRebound`	INTEGER,	`defensiveRebound`	INTEGER,	`opponentOffensiveRebound`	INTEGER,	`opponentDefensiveRebound` INTEGER,`backboard`	INTEGER,	`assist`	INTEGER,	`steal`	INTEGER,	`block`	INTEGER,	`turnOver` INTEGER,`foul` INTEGER,`scoring`	INTEGER,	`minutes`	REAL,`opponentBackBoard` INTEGER,`opponentThreePointFieldGoalAttempts`	INTEGER);";
			statement.addBatch(sql);
			statement.executeBatch();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	public void updatePlayersum(String season,String data,Connection conn){
		try {
			Statement statement=conn.createStatement();
			String[] item=data.split(";");
			if(item[2].contains("'"))
				item[2]=item[2].substring(0,item[2].indexOf("'"))+"'"+item[2].substring(item[2].indexOf("'"), item[2].length());
			String playerName=item[2];
			String team=item[1];//球员队伍
			String position=item[3];
			double minutes=Double.parseDouble(item[4]);
			int fieldGoal=Integer.parseInt(item[5]);
			int fieldGoalAttempts=Integer.parseInt(item[6]);
			int threepointFieldGoal=Integer.parseInt(item[7]);
			int threepointFieldGoalAttempts=Integer.parseInt(item[8]);
			int freeThrow=Integer.parseInt(item[9]);
			int freeThrowAttempts=Integer.parseInt(item[10]);
			int offensiveRebound=Integer.parseInt(item[11]);
			int defensiveRebound=Integer.parseInt(item[12]);
			int backboard=Integer.parseInt(item[13]);
			int assist=Integer.parseInt(item[14]);
			int steal=Integer.parseInt(item[15]);
			int block=Integer.parseInt(item[16]);
			int turnOver=Integer.parseInt(item[17]);
			int foul=Integer.parseInt(item[18]);
			int scoring=Integer.parseInt(item[19]);
			int appearance=1;
			int firstPlay=0;
			double previousAverageScoring=0;//五场前的平均得分
			double nearlyFiveAverageScoring=0;//近五场的平均得分
			int doubleDouble=0;
			if(!position.equals(""))
				firstPlay++;
			//近五场问题
			String temp=Integer.toString(scoring)+Integer.toString(backboard)+Integer.toString(assist)+Integer.toString(steal)+Integer.toString(block);
			if(temp.length()>=7)
				doubleDouble++;
			String sql="SELECT * FROM `playersum"+season+"` WHERE playerName='"+item[2]+"'";
			ResultSet rs=statement.executeQuery(sql);
			while(rs.next()){
				appearance=appearance+rs.getInt(3);
				firstPlay=firstPlay+rs.getInt(4);
				backboard=backboard+rs.getInt(5);
				assist=assist+rs.getInt(6);
				minutes=minutes+rs.getInt(7);
				fieldGoal=fieldGoal+rs.getInt(8);
				fieldGoalAttempts=fieldGoalAttempts+rs.getInt(9);
				threepointFieldGoal=threepointFieldGoal+rs.getInt(10);
				threepointFieldGoalAttempts=threepointFieldGoalAttempts+rs.getInt(11);
				freeThrow=freeThrow+rs.getInt(12);
				freeThrowAttempts=freeThrowAttempts+rs.getInt(13);
				offensiveRebound=offensiveRebound+rs.getInt(14);
				defensiveRebound=defensiveRebound+rs.getInt(15);
				steal=steal+rs.getInt(16);
				block=block+rs.getInt(17);
				turnOver=turnOver+rs.getInt(18);
				foul=foul+rs.getInt(19);
				scoring=scoring+rs.getInt(20);
				previousAverageScoring=rs.getInt(21);
				nearlyFiveAverageScoring=rs.getInt(22);
				doubleDouble=doubleDouble+rs.getInt(23);
			}
			sql="DELETE FROM `playersum"+season+"` WHERE playerName='"+playerName+"'";
			if (playerName.contains("'")) {
				playerName=playerName.substring(0,playerName.indexOf("''"))+playerName.substring(playerName.indexOf("'")+1, playerName.length());
			}
			PreparedStatement ps=conn.prepareStatement("INSERT INTO `playersum"+season+"`  values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
			ps.setString(1, playerName);
			ps.setString(2, team);
			ps.setInt(3, appearance);
			ps.setInt(4, firstPlay);
			ps.setInt(5, backboard);
			ps.setInt(6, assist);
			ps.setDouble(7, minutes);
			ps.setInt(8, fieldGoal);
			ps.setInt(9, fieldGoalAttempts);
			ps.setInt(10, threepointFieldGoal);
			ps.setInt(11, threepointFieldGoalAttempts);
			ps.setInt(12, freeThrow);
			ps.setInt(13, freeThrowAttempts);
			ps.setInt(14, offensiveRebound);
			ps.setInt(15, defensiveRebound);
			ps.setInt(16, steal);
			ps.setInt(17, block);
			ps.setInt(18, turnOver);
			ps.setInt(19, foul);
			ps.setInt(20, scoring);
			ps.setDouble(21, previousAverageScoring);
			ps.setDouble(22, nearlyFiveAverageScoring);
			ps.setInt(23, doubleDouble);
			ps.addBatch();
			ps.executeBatch();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	public void updateTeamsum(String season,String date,Connection conn,String[] team){
		try {
			Statement statement=conn.createStatement();
			String sql="SELECT * FROM `playerdata"+season+"` WHERE date='"+date+"' AND team='"+team[0]+"'";
			ResultSet rs=statement.executeQuery(sql);
			while(rs.next()){
				
			}
			PreparedStatement ps=conn.prepareStatement("INSERT INTO `playersum"+season+"`  values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	public void updataMatches(String fileName){
		try {
			Class.forName(InitialDatabase.driver);
			conn = DriverManager.getConnection(InitialDatabase.url);
			String info="";
			FileReader fr=new FileReader("data/matches/"+fileName);
			BufferedReader br=new BufferedReader(fr);
			String line=br.readLine();
			String[] temp=line.split(";");
			String guest="";
			String[] item=fileName.split("_");
			String[] year=item[0].split("-");
			if(temp[0].startsWith("10-")||temp[0].startsWith("11-")||temp[0].startsWith("12-")){
				info=year[0]+"-"+temp[0]+";h;";
				guest=year[0]+"-"+temp[0]+";g;";
		    }else{
				info=year[1]+"-"+temp[0]+";h;";
				guest=year[1]+"-"+temp[0]+";g;";
		    }
			String[] temp1=temp[1].split("-");
			info=info+temp1[0]+";"+temp1[1]+";";
			guest=guest+temp1[1]+";"+temp1[0]+";";
			temp1=temp[2].split("-");
			if(Integer.parseInt(temp1[0])>Integer.parseInt(temp1[1])){
				info=info+"w;"+temp1[0]+";";
				guest=guest+"l;"+temp1[1]+";";
			}else{
				info=info+"l;"+temp1[0]+";";
				guest=guest+"w;"+temp1[1]+";";
			}
			line=br.readLine();
			temp=line.split(";");
			for (int j = 0; j < 4; j++) {
				temp1=temp[j].split("-");
				info=info+temp1[0]+";";
				guest=guest+temp1[1]+";";
			}
			info=info.substring(0, info.length()-1);
			PreparedStatement ps=conn.prepareStatement("INSERT INTO matches  values(?,?,?,?,?,?,?,?,?,?)");
			temp=info.split(";");
			ps.setString(1, temp[0]);
			ps.setString(2, temp[1]);
			ps.setString(3, temp[2]);
			ps.setString(4, temp[3]);
			ps.setString(5, temp[4]);
			ps.setInt(6, Integer.parseInt(temp[5]));
			ps.setInt(7, Integer.parseInt(temp[6]));
			ps.setInt(8, Integer.parseInt(temp[7]));
			ps.setInt(9, Integer.parseInt(temp[8]));
			ps.setInt(10, Integer.parseInt(temp[9]));
			ps.addBatch();
			temp=guest.split(";");
			ps.setString(1, temp[0]);
			ps.setString(2, temp[1]);
			ps.setString(3, temp[2]);
			ps.setString(4, temp[3]);
			ps.setString(5, temp[4]);
			ps.setInt(6, Integer.parseInt(temp[5]));
			ps.setInt(7, Integer.parseInt(temp[6]));
			ps.setInt(8, Integer.parseInt(temp[7]));
			ps.setInt(9, Integer.parseInt(temp[8]));
			ps.setInt(10, Integer.parseInt(temp[9]));
			ps.addBatch();
			ps.executeBatch();
			conn.commit(); 
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
