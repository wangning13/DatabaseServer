package dataservice.getdatadataservice;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;

import po.PlayerMatchPO;
import po.PlayerPO;
import po.PlayerinfoPO;

public interface GetPlayerdataDataService extends Remote{

	public PlayerPO getPlayerdata(String season,String playerName)throws RemoteException;
	
	public PlayerinfoPO getPlayerinfo(String playerName)throws RemoteException;
	
	public ArrayList<PlayerPO> getAllPlayerdata(String season,String key,String order)throws RemoteException;
	
	public ArrayList<PlayerPO> getSomePlayerdata(String season,String position,String partition,String key,String order)throws RemoteException;
	
	public ArrayList<PlayerPO> getByEfficiency(ArrayList<PlayerPO> po,String key,String order)throws RemoteException;
	//一场比赛一个球队所有球员数据
	public ArrayList<PlayerMatchPO> getPlayerMatchdata(String date,String team)throws RemoteException;
	
	public ArrayList<PlayerMatchPO> getPlayerMonthMatch(String month,String team)throws RemoteException;
	
	public ArrayList<PlayerMatchPO> getPlayerRecentFiveMatch(String player)throws RemoteException;
	
	public ArrayList<PlayerMatchPO> getDayTop(String date,String condition)throws RemoteException;
	//仅适用于场均
	public ArrayList<PlayerPO> getSeasonTop(String season,String condition)throws RemoteException;
}
