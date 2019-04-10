package io.bittiger.ads;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import net.spy.memcached.MemcachedClient;

//这个就是专门做query understand的类
//这一步视频里面有讲过，主要就是tokenize，NLP等等
//注意，这里好像不能处理spell check
//在lucene的spell check函数，但是这里面没有用
public class QueryParser {
	private static QueryParser instance = null;
	//对于query reweite，最关键的是这个function
	//index是当前query中看的第几个term，比如nike running shoe，nike就是第0个term，running就是第1个term
	//len就是相关词列表的长度，比如synonyms有500个词和它对应的近义词表，那么len就等于500
	//queryTermsTemp是个临时的结果，因为最后的query term是要放在一个array里面，比如把nike running show给rewrite成nike running sneaker，这个结果就要放在queryTermsTemp里面
	//allSynonymList就是那个synonyms列表，储存所有的term和term的相关词列表
	//res就是最终的结果
	private void QueryRewriteHelper(int index, int len, ArrayList<String> queryTermsTemp, List<List<String>> allSynonymList, List<List<String>> res) {
		//当index等于query长度的时候，就说明rewrite到头了，已经rewrite结束了
		if(index == len) {
			res.add(queryTermsTemp);
			return;
		}
		//根据当前的index，把当前index对应的word的synonyms这个list找出来
		List<String> synonyms = allSynonymList.get(index);
		for(int  i = 0; i < synonyms.size(); i++) {
			//这里是针对synonyms list里面的每一个近义词，都要拿出来添加到queryTermsTemp里面
			//注意，虽然queryTermsTemp是用来存储临时结果的，但是在add(synonyms.get(i))之前，这里却是先复制出来了一个queryTerms
			//然后用queryTerms来进行添加
			//这是因为，最终是希望把每一个queryTermsTemp都放到res结果里面的
			//但是在Java里面，是passbyReference的，如果不做clone而直接传进来的话，每一次都加到了同一个list里面，也就是说所有的结果都加到了同一个queryTermsTemp里面
			//而我们想要的是多个queryTermsTemp存放在res里面的
			ArrayList<String> queryTerms = (ArrayList<String>) queryTermsTemp.clone();
			queryTerms.add(synonyms.get(i));
			QueryRewriteHelper(index + 1, len, queryTerms, allSynonymList, res);
		}s
	}
	
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
		List<List<String>> resTemp = new ArrayList<List<String>>();
		List<List<String>> allSynonymList = new ArrayList<List<String>>();
		List<String> tokens = Utility.cleanedTokenize(query);
		String query_key = Utility.strJoin(tokens, "_");
		try {
			MemcachedClient cache = new MemcachedClient(new InetSocketAddress(memcachedServer, memcachedPortal));
			for(String queryTerm : queryTerms){
				if(cache.get(queryTerm) instanceof List){
					List<String> synonymList = (List<String>)cache.get(queryTerm);
					allSynonymList.add(allSynonymList);
				}
			}
			int len = queryTerms.size();
			System.out.println("len of queryTerms = " + len);
			ArrayList<String> queryTermsTemp = new ArrayList<String>();
			QueryRewriteHelper(0, len, queryTermsTemp, allSynonymList, resTemp);

			//dedupe
			Set<String> uniqueQuery = new HashSet<>();
			for(int i = 0; i < resTemp.size(); i++){
				String hash = Utility.strJoin(resTemp.get(i), "_");
				if(uniqueQuery.contains(hash)){
					continue;
				}
				uniqueQuery.add(hash);
				Set<String> uniqueTerm = new HashSet<>();
				for(int j = 0; j < resTemp.get(i).size(); j++){
					String term = resTemp.get(i).get(j);
					if(uniqueTerm.contains(term)){
						break;
					}
					uniqueTerm.add(term);
				}
				if(uniqueTerm.size() == len){
					res.add(resTemp.get(i));
				}
			}

			//debug
			for(int i = 0; i < res.size(); i++){
				System.out.println("synonym");
				for(int j = 0; j < res.get(i).size(); j++){
					System.out.println("query term = " + res.get(i).get(j));
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	 
		return res;
	}
}
