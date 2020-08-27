package net.gywn.json.binlog;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import javax.sql.DataSource;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import io.prometheus.client.Summary;
import lombok.Getter;
import net.gywn.json.beans.Binlog;
import net.gywn.json.beans.BinlogColumn;
import net.gywn.json.beans.BinlogOperation;
import net.gywn.json.common.Config;
import net.gywn.json.common.UldraUtil;

public class MysqlBinlogWorker {
	private final String threadName;
	private final BlockingQueue<BinlogOperation> queue;
	private final DataSource lookupDS;
	private final DataSource targetDS;
	private Thread thread;
	private boolean processing = false;
	private final static String BASE_LOOKUP_QUERY = "select * from %s where 1 = 1";
	private final static String BASE_INSERT_QUERY = "replace into %s (k, v) values (?, ?)";
	private final static String BASE_DELETE_QUERY = "delete from %s where k = ?";
	private final Gson gson = new GsonBuilder().serializeNulls().setFieldNamingStrategy(FieldNamingPolicy.IDENTITY)
			.create();

	@Getter
	private Binlog lastExecutedBinlog;

	public MysqlBinlogWorker(final Config config, final int threadNumber) throws SQLException {
		this.threadName = String.format("thread-%d", threadNumber);
		this.queue = new ArrayBlockingQueue<BinlogOperation>(config.getWokerQueueSize());
		this.lookupDS = config.getLookupDS();
		this.targetDS = config.getTargetDS();
		start();
	}

	public void enqueue(final BinlogOperation binlogOperation) {
		while (true) {
			try {
				queue.add(binlogOperation);
				break;
			} catch (Exception e) {
				System.out.println("[enqueue]" + e);
			}
		}
	}

	private void start() {

		thread = new Thread(new Runnable() {

			public void run() {
				StringBuffer sb = new StringBuffer();
				while (true) {
					try {
						// ========================
						// dequeue
						// ========================
						BinlogOperation binlogOperation = queue.take();

						Connection conn = null;

						// ========================
						// Data processing
						// ========================
						processing = true;
						while (true) {
							try {
								conn = lookupDS.getConnection();

								// ====================
								// lookup query
								// ====================
								String lookupQuery = String.format(BASE_LOOKUP_QUERY,
										binlogOperation.getBinlogTable().getName());
								for (BinlogColumn column : binlogOperation.getBinlogTable().getRowKeys()) {
									lookupQuery += String.format(" and %s = ?", column.getName());
								}

								PreparedStatement pstmtLookup = conn.prepareStatement(lookupQuery);
								for (int i = 0; i < binlogOperation.getBinlogTable().getRowKeys().size(); i++) {
									BinlogColumn column = binlogOperation.getBinlogTable().getRowKeys().get(i);
									pstmtLookup.setString(i + 1, binlogOperation.getData().get(column.getName()));
								}

								ResultSet rs = pstmtLookup.executeQuery();
								Map<String, String> map = null;
								if (rs.next()) {
									map = new HashMap<String, String>();
									ResultSetMetaData rsMeta = rs.getMetaData();
									for (int i = 1; i <= rsMeta.getColumnCount(); i++) {
										String key = rsMeta.getColumnLabel(i).toLowerCase();
										String val = null;
										switch (rsMeta.getColumnType(i)) {
										case java.sql.Types.TIME:
										case java.sql.Types.TIMESTAMP:
										case java.sql.Types.DATE:
											byte[] bytes = rs.getBytes(i);
											if (bytes != null) {
												for (byte c : bytes) {
													sb.append((char) c);
												}
												val = sb.toString();
												sb.setLength(0);
											}
											break;
										default:
											val = rs.getString(i);
										}
										map.put(key, val);
									}
								}
								pstmtLookup.close();
								conn.close();

								// ====================
								// target query
								// ====================
								conn = targetDS.getConnection();
								if (map != null) {
									String insertQuery = String.format(BASE_INSERT_QUERY,
											binlogOperation.getBinlogTable().getBinlogPolicy().getTargetTable());
									PreparedStatement pstmtInsert = conn.prepareStatement(insertQuery);
									pstmtInsert.setString(1, binlogOperation.getKey());
									pstmtInsert.setString(2, gson.toJson(map));
									pstmtInsert.executeUpdate();
									pstmtInsert.close();
								} else {
									String deleteQuery = String.format(BASE_DELETE_QUERY,
											binlogOperation.getBinlogTable().getBinlogPolicy().getTargetTable());
									PreparedStatement pstmtDelete = conn.prepareStatement(deleteQuery);
									pstmtDelete.setString(1, binlogOperation.getKey());
									pstmtDelete.executeUpdate();
								}
								conn.close();
								break;
							} catch (Exception e) {
								System.out.println(e);
								UldraUtil.sleep(1000);
							} finally {
								try {
									conn.close();
								} catch (Exception e) {
								}
								try {
									conn.close();
								} catch (Exception e) {
								}
							}
						}
						lastExecutedBinlog = new Binlog(binlogOperation.getBinlogPosition());
						processing = false;
					} catch (InterruptedException e) {
						System.out.println("[dequeue]" + e);
					}
				}
			}

		}, threadName);
		thread.start();
	}

	public int getJobCount() {
		return queue.size() + (processing ? 1 : 0);
	}
}
