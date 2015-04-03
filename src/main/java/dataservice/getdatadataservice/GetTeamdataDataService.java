package dataservice.getdatadataservice;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;

import po.TeamMatchPO;
import po.TeamPO;
import po.TeaminfoPO;

public interface GetTeamdataDataService extends Remote {

	public TeamPO getTeamdata(String season,String teamName)throws RemoteException;
	
	public TeaminfoPO getTeaminfo(String teamName)throws RemoteException;
	
	public ArrayList<String> getTeamPlayer(String season,String teamName)throws RemoteException;
	
	public ArrayList<TeamPO> getAllTeamdata(String season,String key,String order)throws RemoteException;
	
	public ArrayList<TeamPO> getSomeTeamdata(String season,String condition,String key,String order)throws RemoteException;
	
	public ArrayList<TeamPO> getByEfficiency(ArrayList<TeamPO> po,String key,String order)throws RemoteException;
	
	public ArrayList<TeamMatchPO> getTeamMonthMatch(String month,String team)throws RemoteException;
	
	public TeamMatchPO getTeamMatch(String date,String team)throws RemoteException;
	
	public ArrayList<TeamMatchPO> getTeamRecentFiveMatch(String team)throws RemoteException;
	
	public ArrayList<TeamPO> getSeasonTop(String season,String condition)throws RemoteException;
	
}
