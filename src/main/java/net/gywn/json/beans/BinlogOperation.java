package net.gywn.json.beans;

import java.io.Serializable;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

import lombok.Getter;
import lombok.ToString;
import net.gywn.json.common.UldraUtil;

@Getter
@ToString
public class BinlogOperation {
	private final BinlogTable binlogTable;
	private final BinlogOperationType binlogOperationType;
	private final String binlogPosition;
	private final Map<String, String> data = new HashMap<String, String>();

	public BinlogOperation(final BinlogTable binlogTable, final BitSet bit, final Serializable[] row,
			final BinlogOperationType binlogOperationType, final String binlogPosition) {
		this.binlogTable = binlogTable;
		this.binlogOperationType = binlogOperationType;
		this.binlogPosition = binlogPosition;

		int seq = -1;
		for (Serializable serializable : row) {
			seq = bit.nextSetBit(seq + 1);
			BinlogColumn column = binlogTable.getColumns().get(seq);
			data.put(column.getName(), UldraUtil.toString(serializable, column));
		}
	}

	public String getKey() {
		return getKey(data);
	}

	public String getKey(Map<String, String> map) {
		String v = "";
		int i = 0;
		for (BinlogColumn column : binlogTable.getRowKeys()) {
			if (i++ > 0) {
				v += "_";
			}
			v += map.get(column.getName());
		}
		return v;
	}

	public long getCRC32Code() {
		String v = "";
		for (BinlogColumn column : binlogTable.getRowKeys()) {
			v += data.get(column.getName());
		}
		return UldraUtil.crc32(v);
	}

	public int getSlot(final int slotSize) {
		return (int) (getCRC32Code() % slotSize);
	}

	public void mergeRowKey(final BinlogOperation binlogOperation) {
		for (BinlogColumn column : binlogTable.getRowKeys()) {
			if (!data.containsKey(column.getName())) {
				data.put(column.getName(), binlogOperation.getData().get(column.getName()));
			}
		}
	}

	public boolean isRowKeyChanged(final BinlogOperation binlogOperation) {
		for (BinlogColumn column : binlogTable.getRowKeys()) {
			String key1 = data.get(column.getName());
			String key2 = binlogOperation.getData().get(column.getName());
			if (!key1.equals(key2)) {
				return true;
			}
		}
		return false;
	}

	public enum BinlogOperationType {
		WRITE, DELETE
	}

}
