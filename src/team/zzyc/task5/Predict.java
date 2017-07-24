package src.team.zzyc.task5;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;

public class Predict {

	private String []filename={"08","09","10","11","12","13","14","15","16","17","18","19","20","21","22"};
	private double[] weight;
	private int [][][]data;
	private ArrayList<String> url;
	private int M,N;
	
	public Predict() throws FileNotFoundException{
		url=new ArrayList<>();
		readURL();
		
		M=24;
		N=url.size();
		
		data=new int[filename.length][M][N];
		readData();
		
		weight=new double[7];
		readWeight();
	}

	public void predict() throws FileNotFoundException{
		int RMSE=0;
		
		for(int i=0;i<M;i++){
			int temp=0;
			for(int j=0;j<N;j++){
				temp+=Math.pow(delta(i,j), 2);
			}
			temp/=N;
			RMSE+=Math.sqrt(temp);
		}
		RMSE/=M;
		
		System.out.println(RMSE);
	}
	
	private int delta(int time, int url){

		int avg=0;
		for(int i=7;i<filename.length-1;i++){
			avg+=data[i][time][url]*weight[i-7];
		}
		
		return avg-data[filename.length-1][time][url];
	}
	
	private void readWeight() throws FileNotFoundException{
		File file=new File("res/o5/weight.txt");
		Scanner in=new Scanner(file);
		for(int i=0;i<7;i++){
			weight[i]=in.nextDouble();
		}
		in.close();
	}
	
	private void readData() throws FileNotFoundException{
		for(int i=0;i<filename.length;i++){
			File file=new File("res/o5/"+filename[i]+".txt");
			Scanner in=new Scanner(file);
			in.nextLine();
			for(int j=0;j<M;j++){
				String []s=in.nextLine().split(" ");
				for(int k=1;k<s.length;k++){
					String u=s[k].split(":")[0];
					int n=Integer.parseInt(s[k].split(":")[1]);
					int index=url.indexOf(u);
					if(index>=0){
						data[i][j][index]=n;
					}
				}
			}
			in.close();
		}
	}
	
	private void readURL() throws FileNotFoundException{
		Scanner in=new Scanner(new File("res/url.txt"));
		while(in.hasNextLine()){
			url.add(in.nextLine());
		}
		in.close();
	}
	
}
