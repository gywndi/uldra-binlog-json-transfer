package net.gywn.json.beans;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;

import lombok.Getter;
import lombok.ToString;
import net.gywn.json.binlog.MysqlBinlogEventHandler;
import net.gywn.json.common.BinlogPolicy;

@Getter
@ToString
public class BinlogTable {
	private static final Logger logger = LoggerFactory.getLogger(BinlogTable.class);
	
	private final String name;
	private final List<BinlogColumn> columns;
	private final List<BinlogColumn> rowKeys;
	private final BinlogPolicy binlogPolicy;

	public BinlogTable(final String name, List<BinlogColumn> columns, List<BinlogColumn> rowKeys,
			final BinlogPolicy binlogPolicy) {
		this.name = name;
		this.columns = columns;
		this.rowKeys = rowKeys;
		this.binlogPolicy = binlogPolicy;
	}

	public boolean isTarget() {
		return binlogPolicy != null;
	}

	public boolean equalsTable(final TableMapEventData tableMapEventData) {
		String eventTableName = getTableName(tableMapEventData.getDatabase(), tableMapEventData.getTable());
		if (eventTableName.equalsIgnoreCase(this.name)) {
			logger.info("`{}` is not same with `{}` in table map event", this.name, eventTableName);
			return false;
		}
		return true;
	}

	public static String getTableName(final String database, final String table) {
		return String.format("%s.%s", database, table);
	}

}
