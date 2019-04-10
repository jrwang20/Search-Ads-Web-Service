package io.bit.ads;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.json.*;

public class AdsEngine {
	private String mAdsDataFilePath;
	private String mBudgetFilePath;
	private IndexBuilder indexBuilder;
	private String mMemcachedServer;
	private int mMemcachedPortal;
	private String mysql_host;
	private String mysql_db;
	private String mysql_user;
	private String mysql_pass;
	
	//����Ͱ���SearchAdsServer���潨��obj��ʱ�����Щ������������
	public AdsEngine(String adsDataFilePath, String budgetDataFilePath,String memcachedServer,int memcachedPortal,String mysqlHost,String mysqlDb,String user,String pass)
	{
		mAdsDataFilePath = adsDataFilePath;
		mBudgetFilePath = budgetDataFilePath;
		mMemcachedServer = memcachedServer;
		mMemcachedPortal = memcachedPortal;
		mysql_host = mysqlHost;
		mysql_db = mysqlDb;	
		mysql_user = user;
		mysql_pass = pass;	
		indexBuilder = new IndexBuilder(memcachedServer,memcachedPortal,mysql_host,mysql_db,mysql_user,mysql_pass);
	}
	
	//�������SearchAdsServer������õ�init��ʼ������
	//����������ǰѹ���json���ݸ�������Ȼ����н���
	public Boolean init()
	{
		//load ads data
		try (BufferedReader brAd = new BufferedReader(new FileReader(mAdsDataFilePath))) {
			String line;
			//�����mAdsDataFilePath��ʵ���Ͼ���֮ǰ������������Ads����
			//�������һ��whileѭ������Ads��json���ݸ�һ��һ�еĶ�
			while ((line = brAd.readLine()) != null) {
				//����json
				JSONObject adJson = new JSONObject(line);
				//����֮ǰ��ƺõ�һ��Ad���ad����
				Ad ad = new Ad(); 
				//adId��campaignId���Ƿǿյģ�����ǿյĻ�������ݾͲ�������
				if(adJson.isNull("adId") || adJson.isNull("campaignId")) {
					continue;
				}
				//����ǿգ��ǾͶ�����
				ad.adId = adJson.getLong("adId");
				ad.campaignId = adJson.getLong("campaignId");
				//�������Щ���ݣ�����Ϊ�գ��Ǿ͸�����һЩ�����ֵ
				ad.brand = adJson.isNull("brand") ? "" : adJson.getString("brand");
				ad.price = adJson.isNull("price") ? 100.0 : adJson.getDouble("price");
				ad.thumbnail = adJson.isNull("thumbnail") ? "" : adJson.getString("thumbnail");
				ad.title = adJson.isNull("title") ? "" : adJson.getString("title");
				ad.detail_url = adJson.isNull("detail_url") ? "" : adJson.getString("detail_url");						
				ad.bidPrice = adJson.isNull("bidPrice") ? 1.0 : adJson.getDouble("bidPrice");
				ad.pClick = adJson.isNull("pClick") ? 0.0 : adJson.getDouble("pClick");
				ad.category =  adJson.isNull("category") ? "" : adJson.getString("category");
				ad.description = adJson.isNull("description") ? "" : adJson.getString("description");
				//������Ǵ洢keywords��������΢��һ�㣬Ҫ��һ��arrayȥ��
				ad.keyWords = new ArrayList<String>();
				JSONArray keyWords = adJson.isNull("keyWords") ? null :  adJson.getJSONArray("keyWords");
				for(int j = 0; j < keyWords.length();j++)
				{
					ad.keyWords.add(keyWords.getString(j));
				}
				
				//�����ǹؼ���Ҫbuild inverted index and forward index
				//indexBuilder���������IndexBuilder����ഴ����
				//����ȥ�������IndexBuilder��
				if(!indexBuilder.buildInvertIndex(ad) || !indexBuilder.buildForwardIndex(ad))
				{
					//log				
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
		//���������Ѿ����ź��ˣ�index����������
		//��Ҫ��ʼ����load budget data
		try (BufferedReader brBudget = new BufferedReader(new FileReader(mBudgetFilePath))) {
			String line;
			//ͬ���ģ�������Ȼ��Ҫһ��һ�еض�
			while ((line = brBudget.readLine()) != null) {
				JSONObject campaignJson = new JSONObject(line);
				//��ȡ����json���������budget��campaignId
				Long campaignId = campaignJson.getLong("campaignId");
				double budget = campaignJson.getDouble("budget");
				//����һ��Campaign���󣬰�����ץȡ��campaignId��budget���洢��campaign��������
				Campaign camp = new Campaign();
				camp.campaignId = campaignId;
				camp.budget = budget;
				//Ȼ��ȥupdate Budget��ȥIndexBuilder����ȥ��
				if(!indexBuilder.updateBudget(camp))
				{
					//log
				}			
			}
		}catch (IOException e) {
			e.printStackTrace();
		}
		return true;
	}
	//������adsengine���Ѿ�init����
	
	
	//���ķ�����selectAds
	public List<Ad> selectAds(String query)
	{
		//query understanding
		//���ȣ�����lucene���library����NLP��query understanding�Ľ������һ��list of query term
		//�����֮ǰ����Ƶ���潲����ȥ����Ӧ��QueryParser����������صķ���
		//�����������õ���һ������õ�query
		List<String> queryTerms = QueryParser.getInstance().QueryUnderstand(query);
		//select ads candidates
		//Ȼ���������洦��õ�query����ΪselectAds�Ĳ�������ȥ�����ҳ�ads candidates
		//���ھ�������AdsSelector���getInstance������ȡһ��AdsSelector��obj��Ȼ�����selectAds����
		//ȥAdsSelector�����濴����ν��й��ĺ�ѡ
		List<Ad> adsCandidates = AdsSelector.getInstance(mMemcachedServer, mMemcachedPortal, mysql_host, mysql_db,mysql_user, mysql_pass).selectAds(queryTerms);		
		//L0 filter by pClick, relevance score
		//���ڣ���һ��filter������pClick��һ��˳�򣬳�������һ����Щcandidates
		//�����Ѿ���������˹��ĺ�ѡ������adsCandidates������Ҫ���н�һ������
		//ȥ��AdsFilter���level0��ɸѡ
		List<Ad> L0unfilteredAds = AdsFilter.getInstance().LevelZeroFilterAds(adsCandidates);
		System.out.println("L0unfilteredAds ads left = " + L0unfilteredAds.size());
		
		//rank
		//�������rank Ads
		//ȥ��AdsRanker
		List<Ad> rankedAds = AdsRanker.getInstance().rankAds(L0unfilteredAds);
		System.out.println("rankedAds ads left = " + rankedAds.size());

		//L1 filter by relevance score : select top K ads
		//Ȼ��ӹ��˺õ�candidates���棬ѡ��top K
		//������ǽ��е���level1��ɸѡ��ȥ��AdsFilter��level1ɸѡ
		int k = 20;
		List<Ad> unfilteredAds = AdsFilter.getInstance().LevelOneFilterAds(L0unfilteredAds,k);
		System.out.println("unfilteredAds ads left = " + unfilteredAds.size());

		//Dedupe ads per campaign
		//����Щ�������ȥ�أ�����dedup
		//ȥAdsCampaignManager�����Ҷ�Ӧ��DedupeByCampaignId����
		//������ʵ���ǽ���һЩ���ڵĴ���
		List<Ad> dedupedAds = AdsCampaignManager.getInstance(mysql_host, mysql_db,mysql_user, mysql_pass).DedupeByCampaignId(unfilteredAds);
		System.out.println("dedupedAds ads left = " + dedupedAds.size());

		//��������Ϻ󣬾Ϳ�ʼ��Ǯ��budget��
		//ȥ��AdsCampaignManager�����ApplyBudget����
		List<Ad> ads = AdsCampaignManager.getInstance(mysql_host, mysql_db,mysql_user, mysql_pass).ApplyBudget(dedupedAds);
		System.out.println("AdsCampaignManager ads left = " + ads.size());

		//allocation
		//���ʼ׼�����ù���λ����
		AdsAllocation.getInstance().AllocateAds(ads);
		return ads;
	}

}
