package dataservice.getdatadataservice;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;

import po.TeamPO;
import po.TeaminfoPO;

public interface GetTeamdataDataService extends Remote {

	public TeamPO getTeamdata(String teamName)throws RemoteException;
	
	public TeaminfoPO getTeaminfo(String teamName)throws RemoteException;
	
	public ArrayList<TeamPO> getAllTeamdata(String key,String order)throws RemoteException;
	
	public ArrayList<TeamPO> getSomeTeamdata(String condition,String key,String order)throws RemoteException;
	
	public ArrayList<TeamPO> getByEfficiency(ArrayList<TeamPO> po,String key,String order)throws RemoteException;
}
