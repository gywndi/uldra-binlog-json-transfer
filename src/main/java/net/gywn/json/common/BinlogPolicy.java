package net.gywn.json.common;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Setter
@Getter
@ToString
public class BinlogPolicy {
	private String name;
	private String targetTable;
}
