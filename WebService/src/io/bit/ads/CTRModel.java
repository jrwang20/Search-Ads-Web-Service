package io.bit.ads;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.json.JSONArray;
import org.json.JSONObject;


public class CTRModel {
	private static CTRModel instance = null;
	private static ArrayList<Double> weights_logistic;
	private static Double bias_logistic;
	
	//这个支持两种model，logistic和GBDT
	//constructor
	protected CTRModel(String logistic_reg_model_file, String gbdt_model_path) {
		weights_logistic = new ArrayList<Double>();
		//首先读进来json，json文件里面是参数
		try (BufferedReader ctrLogisticReader = new BufferedReader(new FileReader(logistic_reg_model_file))) {
			String line ;
			//在这里一行一行的读
			while ((line = ctrLogisticReader.readLine()) != null) {
				//从每一行里面提取出Json对象
				JSONObject parameterJson = new JSONObject(line);
				//把Json里面的weight读出来，weights在这里面还是一个array的形式
				JSONArray weights = parameterJson.isNull("weights") ? null :  parameterJson.getJSONArray("weights");
				//对于weights里面的每一个元素，也就是每一个feature对应的weight读出来，然后存起来
				for(int j = 0; j < weights.length();j++)
				{
					weights_logistic.add(weights.getDouble(j));
					System.out.println("weights = " + weights.getDouble(j));		

				}
				//而bias放在这里
				bias_logistic= parameterJson.getDouble("bias");	
				System.out.println("bias_logistic = " + bias_logistic);		
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	public static CTRModel getInstance(String logistic_reg_model_file, String gbdt_model_path) {
		if (instance == null) {
			instance = new CTRModel(logistic_reg_model_file, gbdt_model_path);
		}
		return instance;
	}
	//在做完ads selection以后，就可以call这个function来predict这个CTR了
	//参数的这个feature就是feature，顺序和offline的不能乱
	//这个函数就是计算pClick的
	public double predictCTRWithLogisticRegression(ArrayList<Double> features) {
		double pClick = bias_logistic;
		//首先判断size是否是一一对应的
		if(features.size() != weights_logistic.size()) {
			System.out.println("ERROR : size of features doesn't equals to weights");
			return pClick;
		}
		//然后针对每一个feature都要把值拿出来，乘以相应的weight
		//这里的feature就是待训练数据中对应feature的数据
		for (int i = 0;i < features.size();i++) {
			pClick = pClick + weights_logistic.get(i) * features.get(i);
		}
		System.out.println("sigmoid input pClick = " + pClick);
		pClick = Utility.sigmoid(pClick);
		return pClick;	
	}
    
}
