package net.gywn.json.beans;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import net.gywn.json.common.BinlogPolicy;

@Getter
@ToString
public class BinlogTable {
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

}
