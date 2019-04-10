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
	
	//���֧������model��logistic��GBDT
	//constructor
	protected CTRModel(String logistic_reg_model_file, String gbdt_model_path) {
		weights_logistic = new ArrayList<Double>();
		//���ȶ�����json��json�ļ������ǲ���
		try (BufferedReader ctrLogisticReader = new BufferedReader(new FileReader(logistic_reg_model_file))) {
			String line ;
			//������һ��һ�еĶ�
			while ((line = ctrLogisticReader.readLine()) != null) {
				//��ÿһ��������ȡ��Json����
				JSONObject parameterJson = new JSONObject(line);
				//��Json�����weight��������weights�������滹��һ��array����ʽ
				JSONArray weights = parameterJson.isNull("weights") ? null :  parameterJson.getJSONArray("weights");
				//����weights�����ÿһ��Ԫ�أ�Ҳ����ÿһ��feature��Ӧ��weight��������Ȼ�������
				for(int j = 0; j < weights.length();j++)
				{
					weights_logistic.add(weights.getDouble(j));
					System.out.println("weights = " + weights.getDouble(j));		

				}
				//��bias��������
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
	//������ads selection�Ժ󣬾Ϳ���call���function��predict���CTR��
	//���������feature����feature��˳���offline�Ĳ�����
	//����������Ǽ���pClick��
	public double predictCTRWithLogisticRegression(ArrayList<Double> features) {
		double pClick = bias_logistic;
		//�����ж�size�Ƿ���һһ��Ӧ��
		if(features.size() != weights_logistic.size()) {
			System.out.println("ERROR : size of features doesn't equals to weights");
			return pClick;
		}
		//Ȼ�����ÿһ��feature��Ҫ��ֵ�ó�����������Ӧ��weight
		//�����feature���Ǵ�ѵ�������ж�Ӧfeature������
		for (int i = 0;i < features.size();i++) {
			pClick = pClick + weights_logistic.get(i) * features.get(i);
		}
		System.out.println("sigmoid input pClick = " + pClick);
		pClick = Utility.sigmoid(pClick);
		return pClick;	
	}
    
}
