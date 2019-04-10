package io.bit.ads;

import java.util.ArrayList;
import java.util.List;

//���������������ɸѡ����
//�������м�����������Ӧ���ǲ�ͬ���β�ͬlevel��ɸѡ
public class AdsFilter {
	private static AdsFilter instance = null;
	private static double pClickThreshold = 0.0;
	private static double relevanceScoreThreshold = 0.01;
	private static int mimNumOfAds = 4;
	protected AdsFilter()
	{

	}
	public static AdsFilter getInstance() {
	      if(instance == null) {
	         instance = new AdsFilter();
	      }
	      return instance;
	}
	//��������������������Ѻ�ѡ������adsCandidates���й��˵ķ�����level0��ɸѡ
	//����������߼������Ǹ���ɸѡ���ı�׼
	public List<Ad> LevelZeroFilterAds(List<Ad> adsCandidates)
	{
		//���ȣ����ɸѡ����Ĺ�治��̫�٣�Ҫ�趨һ����͹������
		//����������Ĺ������������Ѿ�������������ˣ���ô��û�б�Ҫ��ɸѡ�ˣ������û�й����ʾ��
		if(adsCandidates.size() <= mimNumOfAds)
			return adsCandidates;
		//���濪ʼ��ʽɸѡ
		List<Ad> unfilteredAds = new ArrayList<Ad>();
		for(Ad ad : adsCandidates)
		{
			//����������pClick��relevanceScore����ѡ���
			//pClickThreshold��relevanceScoreThreshold�������úõ�����
			//ֻ�и������������ޣ������ܱ�ɸѡ�������ŵ�������Ǹ�ArrayList����
			if(ad.pClick >= pClickThreshold && ad.relevanceScore > relevanceScoreThreshold)
			{
				unfilteredAds.add(ad);
			}
		}
		return unfilteredAds;
	}
	//�����������������������level1��ɸѡ
	//��������Ǹ�ɸѡ��һ���ĵط����ǣ������level0ɸѡ��ֻҪ�ﵽĿ��ķ����Ϳ���ͨ��
	//����������Ǹ���ָ������������top k��ads��ɸѡ����
	public List<Ad> LevelOneFilterAds(List<Ad> adsCandidates,int k)
	{
		if(adsCandidates.size() <= mimNumOfAds)
			return adsCandidates;
		
		List<Ad> unfilteredAds = new ArrayList<Ad>();
		//�����loop��������ȡǰk�����
		for(int i = 0; i < Math.min(k, adsCandidates.size());i++)
		{
			unfilteredAds.add(adsCandidates.get(i));
		}
		return unfilteredAds;
	}
}
