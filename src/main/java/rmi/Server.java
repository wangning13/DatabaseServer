package rmi;

import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;

import data.getdata.GetPlayerdata;
import data.getdata.GetTeamdata;
import dataservice.getdatadataservice.GetPlayerdataDataService;
import dataservice.getdatadataservice.GetTeamdataDataService;

public class Server {

	public Server() {
		try {
			String rmi="127.0.0.1";
			
			LocateRegistry.createRegistry(2014);
			
			GetPlayerdataDataService gp=new GetPlayerdata();
			GetTeamdataDataService gt=new GetTeamdata();
			
			Naming.rebind("rmi://"+rmi+":2015/GetPlayerdata", gp);  
			Naming.rebind("rmi://"+rmi+":2015/GetTeamdata", gt); 
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String args[]) {  
		new Server();  
		} 
}
