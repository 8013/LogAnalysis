package team.zzyc;

import team.zzyc.task1.StatusCode;
import team.zzyc.task2.IPFrequence;
import team.zzyc.task2.alternative.IP;
import team.zzyc.task3.URLInterface;
import team.zzyc.task4.AverageResponse;

/**
 * Task1-Task4的入口函数
 * @author zhe
 */

public class Entry {

	public static void main(String[] args) throws Exception {
		if(args.length!=5){
			System.out.println("Usage: hadoop jar program.jar inputPath outputPath1 outputPath2 outputPath3 outputPath4");
			System.exit(0);
		}
		int i=0;
		StatusCode.run(args[0], args[1]);
		if(i==0)
			IP.run(args[0], args[2]);
		else
			IPFrequence.run(args[0], args[2]);
		URLInterface.run(args[0], args[3]);
		AverageResponse.run(args[0], args[4]);
	}

}
