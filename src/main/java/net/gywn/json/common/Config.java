package net.gywn.json.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import javax.sql.DataSource;

import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.commons.dbcp2.BasicDataSource;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Setter
@Getter
@ToString
public class Config {
	private int workerCount = 8;
	private int wokerQueueSize = 1000;
	private int exporterPort = 9104;

	private String binlogServer = "127.0.0.1";
	private int binlogServerPort = 3306;
	private int binlogServerID = 15;
	private String binlogServerUsername = "root";
	private String binlogServerPassword = "";
	private String binlogInfoFile;
	private DataSource binlogDS;
	private DataSource lookupDS;
	private DataSource targetDS;
	private BinlogPolicy[] binlogPolicies;
	private CaseInsensitiveMap<String, BinlogPolicy> binlogPolicyMap = new CaseInsensitiveMap<String, BinlogPolicy>();

	public static Config unmarshal(final File yamlFile) throws Exception {
		try (FileInputStream fileInputStream = new FileInputStream(yamlFile);
				InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream, "UTF-8")) {
			Config config = new Yaml(new Constructor(Config.class)).loadAs(inputStreamReader, Config.class);
			config.init();
			return config;
		}
	}

	public void init() throws Exception {

		for (BinlogPolicy binlogPolicy : binlogPolicies) {

			String name = binlogPolicy.getName();
			if (name == null) {
				throw new Exception("Policy name must be defined");
			}

			if (binlogPolicyMap.containsKey(name)) {
				throw new Exception(String.format("Duplicate binlog policy `%s`", name));
			}

			if (binlogPolicy.getTargetTable() == null) {
				throw new Exception(String.format("Target table not defined on policy `%s`", name));
			}

			if (binlogServerPassword == null) {
				binlogServerPassword = "";
			}
			
			BasicDataSource ds = new BasicDataSource();
			String jdbcUrl = String.format("jdbc:mysql://%s/%s?autoReconnect=true&useSSL=false&connectTimeout=3000",
					this.binlogServer, "information_schema");
			ds.setUrl(jdbcUrl);
			ds.setUsername(binlogServerUsername);
			ds.setPassword(binlogServerPassword);
			ds.setDefaultAutoCommit(true);
			ds.setInitialSize(1);
			ds.setMinIdle(1);
			ds.setMaxTotal(10);
			ds.setMaxWaitMillis(1000);
			ds.setTestOnBorrow(false);
			ds.setTestOnReturn(false);
			ds.setTestWhileIdle(true);
			ds.setNumTestsPerEvictionRun(3);
			ds.setTimeBetweenEvictionRunsMillis(60000);
			ds.setMinEvictableIdleTimeMillis(600000);
			ds.setValidationQuery("SELECT 1");
			ds.setValidationQueryTimeout(5);
			binlogDS = ds;

			binlogPolicyMap.put(name, binlogPolicy);
		}
	}
}
