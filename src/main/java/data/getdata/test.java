package data.getdata;

import java.rmi.RemoteException;
import java.util.ArrayList;

import po.TeamPO;

public class test {
	public static void main(String[] args) {
		long time=System.currentTimeMillis();
		GetTeamdata g=null;
		try {
			g = new GetTeamdata();
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		ArrayList<TeamPO> po=g.getSomeTeamdata("`east/west`='W'", "wins", "DESC");
		for (int i = 0; i < po.size(); i++) {
			System.out.println(po.get(i).getTeamName());
		}
		time=System.currentTimeMillis()-time;
		System.out.println(time);
	}
}
