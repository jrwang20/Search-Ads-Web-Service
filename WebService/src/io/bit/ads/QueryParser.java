package io.bit.ads;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import net.spy.memcached.MemcachedClient;

//�������ר����query understand����
//��һ����Ƶ�����н�������Ҫ����tokenize��NLP�ȵ�
//ע�⣬��������ܴ���spell check
//��lucene��spell check����������������û����
public class QueryParser {
	private static QueryParser instance = null;
//	//����query reweite����ؼ��������function
//	//index�ǵ�ǰquery�п��ĵڼ���term������nike running shoe��nike���ǵ�0��term��running���ǵ�1��term
//	//len������ش��б�ĳ��ȣ�����synonyms��500���ʺ�����Ӧ�Ľ���ʱ���ôlen�͵���500
//	//queryTermsTemp�Ǹ���ʱ�Ľ������Ϊ����query term��Ҫ����һ��array���棬�����nike running show��rewrite��nike running sneaker����������Ҫ����queryTermsTemp����
//	//allSynonymList�����Ǹ�synonyms�б��������е�term��term����ش��б�
//	//res�������յĽ��
//	private void QueryRewriteHelper(int index, int len, ArrayList<String> queryTermsTemp, List<List<String>> allSynonymList, List<List<String>> res) {
//		//��index����query���ȵ�ʱ�򣬾�˵��rewrite��ͷ�ˣ��Ѿ�rewrite������
//		if(index == len) {
//			res.add(queryTermsTemp);
//			return;
//		}
//		//���ݵ�ǰ��index���ѵ�ǰindex��Ӧ��word��synonyms���list�ҳ���
//		List<String> synonyms = allSynonymList.get(index);
//		for(int  i = 0; i < synonyms.size(); i++) {
//			//���������synonyms list�����ÿһ������ʣ���Ҫ�ó�����ӵ�queryTermsTemp����
//			//ע�⣬��ȻqueryTermsTemp�������洢��ʱ����ģ�������add(synonyms.get(i))֮ǰ������ȴ���ȸ��Ƴ�����һ��queryTerms
//			//Ȼ����queryTerms���������
//			//������Ϊ��������ϣ����ÿһ��queryTermsTemp���ŵ�res��������
//			//������Java���棬��passbyReference�ģ��������clone��ֱ�Ӵ������Ļ���ÿһ�ζ��ӵ���ͬһ��list���棬Ҳ����˵���еĽ�����ӵ���ͬһ��queryTermsTemp����
//			//��������Ҫ���Ƕ��queryTermsTemp�����res�����
//			ArrayList<String> queryTerms = (ArrayList<String>) queryTermsTemp.clone();
//			queryTerms.add(synonyms.get(i));
//			QueryRewriteHelper(index + 1, len, queryTerms, allSynonymList, res);
//		}
//	}
	
	protected QueryParser() {
		
	}
	public static QueryParser getInstance() {
	      if(instance == null) {
	         instance = new QueryParser();
	      }
	      return instance;
    }
	public List<String> QueryUnderstand(String query) {
		List<String> tokens = Utility.cleanedTokenize(query);
		return tokens;
	}
	
	public List<List<String>> QueryRewrite(String query, String memcachedServer,int memcachedPortal) {
		List<List<String>> res = new ArrayList<List<String>>();
		List<String> tokens = Utility.cleanedTokenize(query);
		String query_key = Utility.strJoin(tokens, "_");
		try {
			MemcachedClient cache = new MemcachedClient(new InetSocketAddress(memcachedServer, memcachedPortal));
			if(cache.get(query_key) instanceof List) {
				@SuppressWarnings("unchecked")
				List<String>  synonyms = (ArrayList<String>)cache.get(query_key);
				for(String synonym : synonyms) {
					List<String> token_list = new ArrayList<String>();
					String[] s = synonym.split("_");
					for(String w : s) {
						token_list.add(w);
					}
					res.add(token_list);
				}			
			}
			else {
				res.add(tokens);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	 
		return res;
	}
}
