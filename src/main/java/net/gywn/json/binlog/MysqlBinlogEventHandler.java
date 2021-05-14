package net.gywn.json.binlog;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.RotateEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;

import net.gywn.json.beans.BinlogOperation;
import net.gywn.json.beans.BinlogOperation.BinlogOperationType;
import net.gywn.json.beans.BinlogTable;

public enum MysqlBinlogEventHandler {

	WRITE_ROWS {

		@Override
		public void receiveEvent(final Event event, final MysqlBinlogServer mysqlBinlogServer) {

			WriteRowsEventData eventData = (WriteRowsEventData) event.getData();
			BinlogTable binlogTable = mysqlBinlogServer.getBinlogTableMap().get(eventData.getTableId());

			if (!binlogTable.isTarget()) {
				logger.debug(binlogTable.getName() + " is not target");
				return;
			}

			int slotSize = mysqlBinlogServer.getMysqlBinlogWorkers().length;
			for (Serializable[] row : eventData.getRows()) {

				// ============================
				// New image
				// ============================
				BinlogOperation op = new BinlogOperation(binlogTable, eventData.getIncludedColumns(), row,
						BinlogOperationType.WRITE, mysqlBinlogServer.getCurrentBinlog().toString());

				// ============================
				// Add new job
				// ============================
				mysqlBinlogServer.getMysqlBinlogWorkers()[op.getSlot(slotSize)].enqueue(op);

			}
		}
	},

	UPDATE_ROWS {

		@Override
		public void receiveEvent(final Event event, final MysqlBinlogServer mysqlBinlogServer) {

			UpdateRowsEventData eventData = (UpdateRowsEventData) event.getData();
			BinlogTable binlogTable = mysqlBinlogServer.getBinlogTableMap().get(eventData.getTableId());

			if (!binlogTable.isTarget()) {
				logger.debug(binlogTable.getName() + " is not target");
				return;
			}

			int slotSize = mysqlBinlogServer.getMysqlBinlogWorkers().length;
			for (Entry<Serializable[], Serializable[]> entry : eventData.getRows()) {

				// ============================
				// New image
				// ============================
				BinlogOperation opA = new BinlogOperation(binlogTable, eventData.getIncludedColumns(), entry.getValue(),
						BinlogOperationType.WRITE, mysqlBinlogServer.getCurrentBinlog().toString());

				// ============================
				// Before image
				// ============================
				BinlogOperation opB = new BinlogOperation(binlogTable, eventData.getIncludedColumnsBeforeUpdate(),
						entry.getKey(), BinlogOperationType.DELETE, mysqlBinlogServer.getCurrentBinlog().toString());

				// ============================
				// Merge primary key
				// ============================
				opA.mergeRowKey(opB);

				// ============================
				// Primary key has been changed
				// ============================
				if (opB.isRowKeyChanged(opA)) {
					mysqlBinlogServer.getMysqlBinlogWorkers()[opB.getSlot(slotSize)].enqueue(opB);
				}

				mysqlBinlogServer.getMysqlBinlogWorkers()[opA.getSlot(slotSize)].enqueue(opA);

			}
		}
	},
	DELETE_ROWS {
		@Override
		public void receiveEvent(final Event event, final MysqlBinlogServer mysqlBinlogServer) {
			DeleteRowsEventData eventData = (DeleteRowsEventData) event.getData();
			BinlogTable binlogTable = mysqlBinlogServer.getBinlogTableMap().get(eventData.getTableId());

			if (!binlogTable.isTarget()) {
				logger.debug(binlogTable.getName() + " is not target");
				return;
			}

			int slotSize = mysqlBinlogServer.getMysqlBinlogWorkers().length;
			for (Serializable[] row : eventData.getRows()) {

				// ============================
				// OLD image
				// ============================
				BinlogOperation op = new BinlogOperation(binlogTable, eventData.getIncludedColumns(), row,
						BinlogOperationType.DELETE, mysqlBinlogServer.getCurrentBinlog().toString());

				// ============================
				// Add new job
				// ============================
				mysqlBinlogServer.getMysqlBinlogWorkers()[op.getSlot(slotSize)].enqueue(op);

			}
		}
	},
	TABLE_MAP {
		@Override
		public void receiveEvent(final Event event, final MysqlBinlogServer mysqlBinlogServer) {

			TableMapEventData eventData = (TableMapEventData) event.getData();
			Map<Long, BinlogTable> binlogTableMap = mysqlBinlogServer.getBinlogTableMap();
			BinlogTable binlogTable = binlogTableMap.get(eventData.getTableId());
			if (binlogTable != null && binlogTable.equalsTable(eventData)) {
				logger.debug(eventData.getTableId() + " exists in cache");
				return;
			}

			// get table info from database
			logger.info(eventData.getTable() + ":" + eventData.getTableId() + " not in cache");
			binlogTable = mysqlBinlogServer.getBinlogTable(eventData);

			// remove cache same full name (db.tb)
			for (Entry<Long, BinlogTable> entry : binlogTableMap.entrySet()) {
				if (entry.getValue().getName().equals(binlogTable.getName())) {
					binlogTableMap.remove(entry.getKey());
					break;
				}
			}

			// add cache entry
			binlogTableMap.put(eventData.getTableId(), binlogTable);
		}
	},
	ROTATE {
		@Override
		public void receiveEvent(final Event event, final MysqlBinlogServer mysqlBinlogServer) {
			RotateEventData eventData = (RotateEventData) event.getData();
			mysqlBinlogServer.getCurrentBinlog().setBinlogFile(eventData.getBinlogFilename());
			mysqlBinlogServer.getCurrentBinlog().setBinlogPosition(eventData.getBinlogPosition());
		}
	},
	QUERY {
		@Override
		public void receiveEvent(final Event event, final MysqlBinlogServer mysqlBinlogServer) {
			EventHeaderV4 header = event.getHeader();
			mysqlBinlogServer.getCurrentBinlog().setBinlogPosition(header.getPosition());
		}
	},
	NOOP {
		@Override
		public void receiveEvent(final Event event, final MysqlBinlogServer mysqlBinlogServer) {
		}
	};

	private static final Map<EventType, MysqlBinlogEventHandler> map = new HashMap<EventType, MysqlBinlogEventHandler>();

	static {
		// ==========================
		// Initialize
		// ==========================
		for (EventType e : EventType.values()) {
			map.put(e, NOOP);
		}

		// ==========================
		// Set Event Type Map
		// ==========================
		map.put(EventType.PRE_GA_WRITE_ROWS, WRITE_ROWS);
		map.put(EventType.WRITE_ROWS, WRITE_ROWS);
		map.put(EventType.EXT_WRITE_ROWS, WRITE_ROWS);

		map.put(EventType.PRE_GA_UPDATE_ROWS, UPDATE_ROWS);
		map.put(EventType.UPDATE_ROWS, UPDATE_ROWS);
		map.put(EventType.EXT_UPDATE_ROWS, UPDATE_ROWS);

		map.put(EventType.PRE_GA_DELETE_ROWS, DELETE_ROWS);
		map.put(EventType.DELETE_ROWS, DELETE_ROWS);
		map.put(EventType.EXT_DELETE_ROWS, DELETE_ROWS);

		map.put(EventType.TABLE_MAP, TABLE_MAP);
		map.put(EventType.QUERY, QUERY);
		map.put(EventType.ROTATE, ROTATE);

	}

	public static MysqlBinlogEventHandler valuOf(EventType eventType) {
		return map.get(eventType);
	}

	public abstract void receiveEvent(final Event event, final MysqlBinlogServer mysqlBinlogServer);

	private static final Logger logger = LoggerFactory.getLogger(MysqlBinlogEventHandler.class);
}