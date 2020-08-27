package net.gywn.json.beans;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@ToString
public class BinlogColumn {
	private final String name;
	private final String charset;
	private final String type;
	private final boolean unsigned;
	
	@Setter
	private boolean rowKey = false;

	public BinlogColumn(final String name, final String type, final String charset, final boolean unsigned) {
		this.name = name;
		this.charset = charset;
		this.type = type;
		this.unsigned = unsigned;
	}
}
