package hbase.advance;

import java.io.IOException;

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

public class MyRegionObserver extends BaseRegionObserver {

	static final byte[] CF = Bytes.toBytes("cf");
	static final byte[] Q_SRC = Bytes.toBytes("src");
	static final byte[] Q_CLONE = Bytes.toBytes("clone");

	@Override
	public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability)
			throws IOException {
		// Clone source column if clone column does not exist in the put.
		if (!put.has(CF, Q_CLONE)) {
			put.addColumn(CF, Q_CLONE, CellUtil.cloneValue(put.get(CF, Q_SRC).get(0)));
		}
	}

}