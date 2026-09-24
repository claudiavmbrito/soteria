public class HelloWorld {
	public static void main(String[] args) throws InterruptedException {
		long startTime = System.nanoTime();
		for (int i = 0; i < 20; i++) {
			System.out.println("Goodbye, cruel world!");
		}	
		long endTime = System.nanoTime();
		long timeElapsed = endTime - startTime;
		System.out.println("Execution time in nanoseconds  : " + timeElapsed);
	}
}
