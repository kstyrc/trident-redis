package storm.trident.redis;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.io.Files;

public class RedisServerRunner {
	
	private final String command;
	@SuppressWarnings("unused")
	private final String host;
	private final Integer port;
	
	private Process redisProcess;
	
	public RedisServerRunner(String commandFullPath, String host, Integer port) {
		this.command = commandFullPath;
		this.host = host;
		this.port = port;
	}
	
	public void start() throws IOException {
		ProcessBuilder pb = new ProcessBuilder(command, "--port", Integer.toString(port));
		File redisTmpDir = Files.createTempDir();
		redisTmpDir.deleteOnExit();
		pb.directory(redisTmpDir);
		
		File redisServer = new File(command);
		redisServer.setExecutable(true);
		redisProcess = pb.start();
		
		Pattern redisReadyPattern = Pattern.compile(".*The server is now ready to accept connections on port.*");
		BufferedReader reader = new BufferedReader(new InputStreamReader(redisProcess.getInputStream()));
		
		Matcher matcher = null;
		do {
			String outputLine = reader.readLine();
			matcher = redisReadyPattern.matcher(outputLine);
		} while (!matcher.matches());
	}
	
	public void stop() {
		redisProcess.destroy();
	}
}
