package dbJobs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class ContactInfo implements Writable, DBWritable {
	private static String EMPTY="";
	private String id;
	private String active;
	
	public ContactInfo() {
		this.id=EMPTY;
		this.active=EMPTY;
	}

	@Override
	public void write(PreparedStatement statement) throws SQLException {
		statement.setString(1,this.id);
		statement.setString(2,this.active);
	}

	@Override
	public void readFields(ResultSet resultSet) throws SQLException {
		this.id=resultSet.getString(1);
		this.active=resultSet.getString(2);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(id);
		out.writeUTF(active);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.id=in.readUTF();
		this.active=in.readUTF();
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getActive() {
		return active;
	}

	public void setActive(String active) {
		this.active = active;
	}

}
