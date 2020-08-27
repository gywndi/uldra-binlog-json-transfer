package net.gywn.json.binlog;

import static com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG_MICRO;
import static com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY;

import java.sql.Connection;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.BinaryLogClient.EventListener;
import com.github.shyiko.mysql.binlog.BinaryLogClient.LifecycleListener;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import lombok.Getter;
import net.gywn.json.beans.Binlog;
import net.gywn.json.beans.BinlogColumn;
import net.gywn.json.beans.BinlogTable;
import net.gywn.json.common.BinlogPolicy;
import net.gywn.json.common.Config;
import net.gywn.json.common.UldraUtil;

@Getter
public class MysqlBinlogServer {
	private static final Logger logger = LoggerFactory.getLogger(MysqlBinlogServer.class);
	private final Config config;
	private BinaryLogClient binaryLogClient = null;
	private MysqlBinlogWorker[] mysqlBinlogWorkers;
	private Binlog currentBinlog;
	private Map<Long, BinlogTable> binlogTableMap = new HashMap<Long, BinlogTable>();
	private Calendar time = Calendar.getInstance();
	private Thread thread;
	private Thread monThread;
	private boolean threadRunning = false;
	private Exception threadException;
	private MysqlBinlogServer mysqlBinlogServer;

	public MysqlBinlogServer(final Config config) {
		this.config = config;
		this.mysqlBinlogServer = this;
		if (config.getBinlogInfoFile() == null) {
			config.setBinlogInfoFile(String.format("binlog-pos-%s:%d:%d.info", config.getBinlogServer(),
					config.getBinlogServerPort(), config.getBinlogServerID()));
		}
	}

	public void start(final Binlog binlog) {
		try {
			binaryLogClient = new BinaryLogClient(config.getBinlogServer(), config.getBinlogServerPort(),
					config.getBinlogServerUsername(), config.getBinlogServerPassword());
			EventDeserializer eventDeserializer = new EventDeserializer();
			eventDeserializer.setCompatibilityMode(DATE_AND_TIME_AS_LONG_MICRO, CHAR_AND_BINARY_AS_BYTE_ARRAY);
			binaryLogClient.setEventDeserializer(eventDeserializer);
			binaryLogClient.setServerId(config.getBinlogServerID());
			binaryLogClient.registerEventListener(new EventListener() {
				public void onEvent(Event event) {
					EventType eventType = event.getHeader().getEventType();
					MysqlBinlogEventHandler.valuOf(eventType).receiveEvent(event, mysqlBinlogServer);
				}
			});

			binaryLogClient.registerLifecycleListener(new LifecycleListener() {

				public void onConnect(BinaryLogClient client) {
					if (currentBinlog == null) {
						currentBinlog = new Binlog(client.getBinlogFilename(), client.getBinlogPosition());
					}
					threadRunning = true;
				}

				public void onCommunicationFailure(BinaryLogClient client, Exception ex) {
					threadRunning = false;
				}

				public void onEventDeserializationFailure(BinaryLogClient client, Exception ex) {
					threadRunning = false;
				}

				public void onDisconnect(BinaryLogClient client) {
					threadRunning = false;
				}
			});

			// ============================
			// load binlog position
			// ============================
			if (binlog != null) {
				binaryLogClient.setBinlogFilename(binlog.getBinlogFile());
				binaryLogClient.setBinlogPosition(binlog.getBinlogPosition());
			}

			// =========================
			// Create transaction worker
			// =========================
			mysqlBinlogWorkers = new MysqlBinlogWorker[config.getWorkerCount()];
			for (int i = 0; i < mysqlBinlogWorkers.length; i++) {
				try {
					mysqlBinlogWorkers[i] = new MysqlBinlogWorker(config, i);
				} catch (Exception e) {
					System.out.println(e);
					System.exit(1);
				}
			}
			startMonThread();

			binaryLogClient.connect();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	// ========================================
	// binlog flush (every 500ms) & monitoring
	// ========================================
	private void startMonThread() {

		monThread = new Thread(new Runnable() {

			public void run() {
				while (true) {
					int jobCount = 0;
					List<Binlog> binlogList = new ArrayList<Binlog>();
					for (MysqlBinlogWorker mysqlBinlogWorker : mysqlBinlogWorkers) {
						Binlog binlog = mysqlBinlogWorker.getLastExecutedBinlog();
						if (binlog != null) {
							jobCount += mysqlBinlogWorker.getJobCount();
							binlogList.add(binlog);
						}
					}

					if (binlogList.size() > 0) {
						Binlog[] binlogArray = new Binlog[binlogList.size()];
						binlogList.toArray(binlogArray);
						Arrays.sort(binlogArray);
						Binlog binlog = jobCount > 0 ? binlogArray[0] : binlogArray[binlogArray.length - 1];
						Binlog.flush(binlog, config.getBinlogInfoFile());
					}

					UldraUtil.sleep(1000);
				}
			}

		});
		monThread.start();
	}

	public BinlogTable getBinlogTable(final TableMapEventData tableMapEventData) {
		// Binlog policy
		String database = tableMapEventData.getDatabase();
		String table = tableMapEventData.getTable();
		String name = String.format("%s.%s", database, table);
		BinlogPolicy binlogPolicy = config.getBinlogPolicyMap().get(name);

		Connection connection = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String query = null;
		while (true) {
			try {

				// Get connection
				connection = config.getBinlogDS().getConnection();

				// Get columns
				List<BinlogColumn> columns = new ArrayList<BinlogColumn>();
				query = " select";
				query += "  ordinal_position,";
				query += "  lower(column_name) column_name,";
				query += "  lower(character_set_name) character_set_name,";
				query += "  lower(data_type) data_type,";
				query += "  instr(column_type, 'unsigned') is_unsigned";
				query += " from information_schema.columns";
				query += " where table_schema = ?";
				query += " and table_name = ?";
				query += " order by ordinal_position";

				pstmt = connection.prepareStatement(query);
				pstmt.setString(1, database);
				pstmt.setString(2, table);
				rs = pstmt.executeQuery();
				while (rs.next()) {
					String columnName = rs.getString("column_name");
					String columnCharset = rs.getString("character_set_name");
					String dataType = rs.getString("data_type");
					boolean columnUnsigned = rs.getBoolean("is_unsigned");
					columns.add(new BinlogColumn(columnName, dataType, columnCharset, columnUnsigned));
				}
				rs.close();
				pstmt.close();
				
				if(columns.size() == 0) {
					throw new Exception("Target table not found");
				}

				// Get primary key & unique key
				List<BinlogColumn> rowKeys = new ArrayList<BinlogColumn>();
				query = " select distinct ";
				query += "  lower(column_name) column_name";
				query += " from information_schema.table_constraints a ";
				query += " inner join information_schema.statistics b on b.table_schema = a.table_schema ";
				query += "   and a.table_name = b.table_name ";
				query += "   and b.index_name = a.constraint_name ";
				query += " where lower(a.constraint_type) in ('primary key') ";
				query += " and a.table_schema = ? ";
				query += " and a.table_name = ? ";

				pstmt = connection.prepareStatement(query);
				pstmt.setString(1, database);
				pstmt.setString(2, table);
				rs = pstmt.executeQuery();
				while (rs.next()) {
					String columnName = rs.getString("column_name").toLowerCase();
					for (BinlogColumn column : columns) {
						if (column.getName().equals(columnName)) {
							column.setRowKey(true);
							rowKeys.add(column);
							break;
						}
					}
				}
				rs.close();
				pstmt.close();

				return new BinlogTable(name, columns, rowKeys, binlogPolicy);

			} catch (Exception e) {
				logger.error(e.getMessage());
				e.printStackTrace();
				UldraUtil.sleep(1000);
			} finally {
				try {
					connection.close();
				} catch (SQLException e) {
					logger.error(e.toString());
				}
			}
		}
	}
}