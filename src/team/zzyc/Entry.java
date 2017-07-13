package team.zzyc;

import team.zzyc.task1.StatusCode;
import team.zzyc.task3.URLInterface;
import team.zzyc.task4.AverageResponse;

/**
 * Task1-Task4的入口函数
 * @author zhe
 */

public class Entry {

	public static void main(String[] args) throws Exception {
//		StatusCode.run(args[0], args[1]);
//		URLInterface.run(args[0], args[1]);
		AverageResponse.run(args[0], args[1]);
	}

}
