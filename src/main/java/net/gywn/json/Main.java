package net.gywn.json;

import java.io.File;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.gywn.json.beans.Binlog;
import net.gywn.json.binlog.MysqlBinlogServer;
import net.gywn.json.common.Config;
import picocli.CommandLine;
import picocli.CommandLine.Option;

public class Main implements Callable<Integer> {
	private static final Logger logger = LoggerFactory.getLogger(Main.class);

	@Option(names = { "--config-file" }, description = "Config file", required = true)
	private String configFile;

	private Config config;
	private MysqlBinlogServer mysqlBinlogServer;

	public static void main(String[] args) throws Exception {
		Main main = new Main();		
		Integer exitCode = new CommandLine(main).execute(args);
		if (exitCode == 0) {
			main.start();
		}
	}

	@Override
	public Integer call() throws Exception {

		// =============================
		// Unmarshal config file
		// =============================
		File file = new File(configFile);
		if (!file.exists()) {
			System.out.println("Config file not exists");
			return 1;
		}

		config = Config.unmarshal(file);
		System.out.println(config);
		mysqlBinlogServer = new MysqlBinlogServer(config);
		

		return 0;
	}

	private void start() {

		// ============================
		// load binlog position
		// ============================
		Binlog binlog = null;
		try {
			binlog = Binlog.read(config.getBinlogInfoFile());
		} catch (Exception e) {
		}
		binlog = new Binlog("binlog.000001", 155);
		mysqlBinlogServer.start(binlog);
	}
}
