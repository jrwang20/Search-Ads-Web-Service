package io.bit.ads;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;


public class AdsCampaignManager {
	private static AdsCampaignManager instance = null;
	private String mysql_host;
	private String mysql_db;
	private String mysql_user;
	private String mysql_pass;
	private static double minPriceThreshold = 0.0;
	protected AdsCampaignManager(String mysqlHost,String mysqlDb,String user,String pass)
	{
		mysql_host = mysqlHost;
		mysql_db = mysqlDb;	
		mysql_user = user;
		mysql_pass = pass;	
	}
	public static AdsCampaignManager getInstance(String mysqlHost,String mysqlDb,String user,String pass) {
	      if(instance == null) {
	         instance = new AdsCampaignManager(mysqlHost,mysqlDb,user,pass);
	      }
	      return instance;
	}
	//这个就是根据CampaignId进行dedupe的方法
	public  List<Ad> DedupeByCampaignId(List<Ad> adsCandidates)
	{
		List<Ad> dedupedAds = new ArrayList<Ad>();
		HashSet<Long> campaignIdSet = new HashSet<Long>();
		for(Ad ad : adsCandidates)
		{
			//利用一个HashSet，如果某一广告的campaignId没有出现的话，就把这个广告加进去
			//总之就是利用HashSet进行去重
			if(!campaignIdSet.contains(ad.campaignId))
			{
				dedupedAds.add(ad);
				campaignIdSet.add(ad.campaignId);
			}
			//这里的问题就在于，排在前面的广告总是容易被选出来
			//因为如果有相同的campaignId，即使是不同的广告，也会被过滤掉
			//所以排在前面的广告占优
			//但是由于这个adsCandidates的list是按照ranking score排好顺序的，所以前面的广告自然占优
		}
		return dedupedAds;
	}
	//这个方法是进行收钱，算budget
	//注意返回的是个list
	//这个意思就是说，我们投放广告，在这里算钱的同时，还会把人家付不起钱的那部分广告过滤除去，只会投放出人家付得起钱的广告
	public List<Ad> ApplyBudget(List<Ad> adsCandidates)
	{
		List<Ad> ads = new ArrayList<Ad>();
		try
		{
			//这里仍然要通过mysql，获取广告的各种信息
			//因为算钱要用到mysql数据里面的相关信息
			MySQLAccess mysql = new MySQLAccess(mysql_host, mysql_db, mysql_user, mysql_pass);
			for(int i = 0; i < adsCandidates.size()  - 1;i++)
			{
				//这里针对每一个Ads，把campaignId拿出来，然后根据campaignId计算现在的budget
				Ad ad = adsCandidates.get(i);
				Long campaignId = ad.campaignId;
				System.out.println("campaignId: " + campaignId);
				//计算budget的方法getBudget在mysql类里面
				Double budget = mysql.getBudget(campaignId);
				System.out.println("AdsCampaignManager ad.costPerClick= " + ad.costPerClick);
				System.out.println("AdsCampaignManager campaignId= " + campaignId);
				System.out.println("AdsCampaignManager budget left = " + budget);
				//然后根据budget再进行过滤
				//只有当前的costPerClick，也就是每点一次付的钱，小于当前的budget，并且大于最低要求，才可以算钱
				//否则如果超出了budget，就是说超出预算，人家付不起了，所以就不要再加入到广告列表里面
				//当然也不要去算钱了
				if(ad.costPerClick <= budget && ad.costPerClick >= minPriceThreshold)
				{
					//加入广告
					ads.add(ad);
					//从预算里面扣除已经支出的当前广告的钱
					budget = budget - ad.costPerClick;
					//扣除之后的预算，要重新在数据库里面进行更新
					//就是一个update的sql语句，在mysql类里面
					mysql.updateCampaignData(campaignId, budget);
				}
				//【注意】这里存在一个问题，虽然上面if()里面的逻辑是没有问题的
				//但是类似于银行取钱一样，同时可能会在算很多的budget - ad.costPerClick
				//这就是多线程，concurrency什么的
				//意思就是说，第68行取出来的budget = mysql.getBudget(campaignId)可能还是100块钱
				//进行到第81行的时候，由于其他的进程同时进行，实际上这里budget - ad.costPerClick的budget，在database里面可能就只剩下80块钱了
				//但是在这个code里面的budget，仍然是100块钱
				//意思就是说，这个code里面的budget没有同时响应其他进程里面的budget更新
				//所以budget总是减不下去，所以就会使得实际上的指出开销增加
				//也就是明明实际上budget在所有的进程中已经用完了，但是在这个code中还是没有用完
				//解决方法：应该利用数据库里面的额一个tranlaction来做
			}
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return ads;
	}
	
}
