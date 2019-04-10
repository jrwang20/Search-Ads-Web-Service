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
	//������Ǹ���CampaignId����dedupe�ķ���
	public  List<Ad> DedupeByCampaignId(List<Ad> adsCandidates)
	{
		List<Ad> dedupedAds = new ArrayList<Ad>();
		HashSet<Long> campaignIdSet = new HashSet<Long>();
		for(Ad ad : adsCandidates)
		{
			//����һ��HashSet�����ĳһ����campaignIdû�г��ֵĻ����Ͱ�������ӽ�ȥ
			//��֮��������HashSet����ȥ��
			if(!campaignIdSet.contains(ad.campaignId))
			{
				dedupedAds.add(ad);
				campaignIdSet.add(ad.campaignId);
			}
			//�������������ڣ�����ǰ��Ĺ���������ױ�ѡ����
			//��Ϊ�������ͬ��campaignId����ʹ�ǲ�ͬ�Ĺ�棬Ҳ�ᱻ���˵�
			//��������ǰ��Ĺ��ռ��
			//�����������adsCandidates��list�ǰ���ranking score�ź�˳��ģ�����ǰ��Ĺ����Ȼռ��
		}
		return dedupedAds;
	}
	//��������ǽ�����Ǯ����budget
	//ע�ⷵ�ص��Ǹ�list
	//�����˼����˵������Ͷ�Ź�棬��������Ǯ��ͬʱ��������˼Ҹ�����Ǯ���ǲ��ֹ����˳�ȥ��ֻ��Ͷ�ų��˼Ҹ�����Ǯ�Ĺ��
	public List<Ad> ApplyBudget(List<Ad> adsCandidates)
	{
		List<Ad> ads = new ArrayList<Ad>();
		try
		{
			//������ȻҪͨ��mysql����ȡ���ĸ�����Ϣ
			//��Ϊ��ǮҪ�õ�mysql��������������Ϣ
			MySQLAccess mysql = new MySQLAccess(mysql_host, mysql_db, mysql_user, mysql_pass);
			for(int i = 0; i < adsCandidates.size()  - 1;i++)
			{
				//�������ÿһ��Ads����campaignId�ó�����Ȼ�����campaignId�������ڵ�budget
				Ad ad = adsCandidates.get(i);
				Long campaignId = ad.campaignId;
				System.out.println("campaignId: " + campaignId);
				//����budget�ķ���getBudget��mysql������
				Double budget = mysql.getBudget(campaignId);
				System.out.println("AdsCampaignManager ad.costPerClick= " + ad.costPerClick);
				System.out.println("AdsCampaignManager campaignId= " + campaignId);
				System.out.println("AdsCampaignManager budget left = " + budget);
				//Ȼ�����budget�ٽ��й���
				//ֻ�е�ǰ��costPerClick��Ҳ����ÿ��һ�θ���Ǯ��С�ڵ�ǰ��budget�����Ҵ������Ҫ�󣬲ſ�����Ǯ
				//�������������budget������˵����Ԥ�㣬�˼Ҹ������ˣ����ԾͲ�Ҫ�ټ��뵽����б�����
				//��ȻҲ��Ҫȥ��Ǯ��
				if(ad.costPerClick <= budget && ad.costPerClick >= minPriceThreshold)
				{
					//������
					ads.add(ad);
					//��Ԥ������۳��Ѿ�֧���ĵ�ǰ����Ǯ
					budget = budget - ad.costPerClick;
					//�۳�֮���Ԥ�㣬Ҫ���������ݿ�������и���
					//����һ��update��sql��䣬��mysql������
					mysql.updateCampaignData(campaignId, budget);
				}
				//��ע�⡿�������һ�����⣬��Ȼ����if()������߼���û�������
				//��������������ȡǮһ����ͬʱ���ܻ�����ܶ��budget - ad.costPerClick
				//����Ƕ��̣߳�concurrencyʲô��
				//��˼����˵����68��ȡ������budget = mysql.getBudget(campaignId)���ܻ���100��Ǯ
				//���е���81�е�ʱ�����������Ľ���ͬʱ���У�ʵ��������budget - ad.costPerClick��budget����database������ܾ�ֻʣ��80��Ǯ��
				//���������code�����budget����Ȼ��100��Ǯ
				//��˼����˵�����code�����budgetû��ͬʱ��Ӧ�������������budget����
				//����budget���Ǽ�����ȥ�����Ծͻ�ʹ��ʵ���ϵ�ָ����������
				//Ҳ��������ʵ����budget�����еĽ������Ѿ������ˣ����������code�л���û������
				//���������Ӧ���������ݿ�����Ķ�һ��tranlaction����
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
