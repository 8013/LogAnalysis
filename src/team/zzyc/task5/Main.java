package team.zzyc.task5;

public class Main {

	public static void main(String[] args) throws Exception {
		StatisticsUrlEveryDay.run(args[0], args[1]);
		Predict p=new Predict();
		p.predict();
	}
	
}
