package src.team.zzyc;

import team.zzyc.task1.StatusCode;
import team.zzyc.task2.alternative.IP;
import team.zzyc.task3.URLInterface;
import team.zzyc.task4.AverageResponse;
import team.zzyc.task5.PredictProgram;

/**
 * Task1-Task4的入口函数
 * @author zhe
 */

public class Entry {

	public static void main(String[] args) throws Exception {
		if(args.length!=3){
			System.out.println("Usage: hadoop jar LogAnalysis.jar n inputPath outputPath");
			System.exit(0);
		}
		for(int i=0;i<args.length;i++){
			System.out.println(args[i]);
		}
		switch(args[0]){
		case "1":{
			StatusCode.run(args[1], args[2]);
			System.out.println("Task1 Complete!");
			break;
		}
		case "2":{
			IP.run(args[1], args[2]);
			System.out.println("Task2 Complete!");
			break;
		}
		case "3":{
			URLInterface.run(args[1], args[2]);
			System.out.println("Task3 Complete!");
			break;
		}
		case "4":{
			AverageResponse.run(args[1], args[2]);
			System.out.println("Task4 Complete!");
			break;
		}
		case "5":{
			PredictProgram.run(args[1], args[2]);
			System.out.println("Task5 Complete!");
			break;
		}
		default:{
			System.out.println("usage 1<=n<=5");
			System.exit(0);
		}
		}
	}

}
