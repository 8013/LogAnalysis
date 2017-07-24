package src.team.zzyc.task5;

public class PredictProgram {

	public static void run(String input, String output) throws Exception {
		StatisticsUrlEveryDay.run(input, output);
		Predict p=new Predict();
		p.predict();
	}
	
}
