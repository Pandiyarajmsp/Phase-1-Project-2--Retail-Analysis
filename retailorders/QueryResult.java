// ORM class for table 'null'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Fri Jun 09 01:08:46 IST 2017
// For connector: org.apache.sqoop.manager.DirectMySQLManager
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import com.cloudera.sqoop.lib.JdbcWritableBridge;
import com.cloudera.sqoop.lib.DelimiterSet;
import com.cloudera.sqoop.lib.FieldFormatter;
import com.cloudera.sqoop.lib.RecordParser;
import com.cloudera.sqoop.lib.BooleanParser;
import com.cloudera.sqoop.lib.BlobRef;
import com.cloudera.sqoop.lib.ClobRef;
import com.cloudera.sqoop.lib.LargeObjectLoader;
import com.cloudera.sqoop.lib.SqoopRecord;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class QueryResult extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  protected ResultSet __cur_result_set;
  private Integer customerNumber;
  public Integer get_customerNumber() {
    return customerNumber;
  }
  public void set_customerNumber(Integer customerNumber) {
    this.customerNumber = customerNumber;
  }
  public QueryResult with_customerNumber(Integer customerNumber) {
    this.customerNumber = customerNumber;
    return this;
  }
  private String upper_c_customerName_;
  public String get_upper_c_customerName_() {
    return upper_c_customerName_;
  }
  public void set_upper_c_customerName_(String upper_c_customerName_) {
    this.upper_c_customerName_ = upper_c_customerName_;
  }
  public QueryResult with_upper_c_customerName_(String upper_c_customerName_) {
    this.upper_c_customerName_ = upper_c_customerName_;
    return this;
  }
  private String contactFirstName;
  public String get_contactFirstName() {
    return contactFirstName;
  }
  public void set_contactFirstName(String contactFirstName) {
    this.contactFirstName = contactFirstName;
  }
  public QueryResult with_contactFirstName(String contactFirstName) {
    this.contactFirstName = contactFirstName;
    return this;
  }
  private String contactLastName;
  public String get_contactLastName() {
    return contactLastName;
  }
  public void set_contactLastName(String contactLastName) {
    this.contactLastName = contactLastName;
  }
  public QueryResult with_contactLastName(String contactLastName) {
    this.contactLastName = contactLastName;
    return this;
  }
  private String phone;
  public String get_phone() {
    return phone;
  }
  public void set_phone(String phone) {
    this.phone = phone;
  }
  public QueryResult with_phone(String phone) {
    this.phone = phone;
    return this;
  }
  private String addressLine1;
  public String get_addressLine1() {
    return addressLine1;
  }
  public void set_addressLine1(String addressLine1) {
    this.addressLine1 = addressLine1;
  }
  public QueryResult with_addressLine1(String addressLine1) {
    this.addressLine1 = addressLine1;
    return this;
  }
  private String city;
  public String get_city() {
    return city;
  }
  public void set_city(String city) {
    this.city = city;
  }
  public QueryResult with_city(String city) {
    this.city = city;
    return this;
  }
  private String state;
  public String get_state() {
    return state;
  }
  public void set_state(String state) {
    this.state = state;
  }
  public QueryResult with_state(String state) {
    this.state = state;
    return this;
  }
  private String postalCode;
  public String get_postalCode() {
    return postalCode;
  }
  public void set_postalCode(String postalCode) {
    this.postalCode = postalCode;
  }
  public QueryResult with_postalCode(String postalCode) {
    this.postalCode = postalCode;
    return this;
  }
  private String country;
  public String get_country() {
    return country;
  }
  public void set_country(String country) {
    this.country = country;
  }
  public QueryResult with_country(String country) {
    this.country = country;
    return this;
  }
  private Integer salesRepEmployeeNumber;
  public Integer get_salesRepEmployeeNumber() {
    return salesRepEmployeeNumber;
  }
  public void set_salesRepEmployeeNumber(Integer salesRepEmployeeNumber) {
    this.salesRepEmployeeNumber = salesRepEmployeeNumber;
  }
  public QueryResult with_salesRepEmployeeNumber(Integer salesRepEmployeeNumber) {
    this.salesRepEmployeeNumber = salesRepEmployeeNumber;
    return this;
  }
  private java.math.BigDecimal creditLimit;
  public java.math.BigDecimal get_creditLimit() {
    return creditLimit;
  }
  public void set_creditLimit(java.math.BigDecimal creditLimit) {
    this.creditLimit = creditLimit;
  }
  public QueryResult with_creditLimit(java.math.BigDecimal creditLimit) {
    this.creditLimit = creditLimit;
    return this;
  }
  private String checknumber;
  public String get_checknumber() {
    return checknumber;
  }
  public void set_checknumber(String checknumber) {
    this.checknumber = checknumber;
  }
  public QueryResult with_checknumber(String checknumber) {
    this.checknumber = checknumber;
    return this;
  }
  private java.sql.Date paymentdate;
  public java.sql.Date get_paymentdate() {
    return paymentdate;
  }
  public void set_paymentdate(java.sql.Date paymentdate) {
    this.paymentdate = paymentdate;
  }
  public QueryResult with_paymentdate(java.sql.Date paymentdate) {
    this.paymentdate = paymentdate;
    return this;
  }
  private java.math.BigDecimal amount;
  public java.math.BigDecimal get_amount() {
    return amount;
  }
  public void set_amount(java.math.BigDecimal amount) {
    this.amount = amount;
  }
  public QueryResult with_amount(java.math.BigDecimal amount) {
    this.amount = amount;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof QueryResult)) {
      return false;
    }
    QueryResult that = (QueryResult) o;
    boolean equal = true;
    equal = equal && (this.customerNumber == null ? that.customerNumber == null : this.customerNumber.equals(that.customerNumber));
    equal = equal && (this.upper_c_customerName_ == null ? that.upper_c_customerName_ == null : this.upper_c_customerName_.equals(that.upper_c_customerName_));
    equal = equal && (this.contactFirstName == null ? that.contactFirstName == null : this.contactFirstName.equals(that.contactFirstName));
    equal = equal && (this.contactLastName == null ? that.contactLastName == null : this.contactLastName.equals(that.contactLastName));
    equal = equal && (this.phone == null ? that.phone == null : this.phone.equals(that.phone));
    equal = equal && (this.addressLine1 == null ? that.addressLine1 == null : this.addressLine1.equals(that.addressLine1));
    equal = equal && (this.city == null ? that.city == null : this.city.equals(that.city));
    equal = equal && (this.state == null ? that.state == null : this.state.equals(that.state));
    equal = equal && (this.postalCode == null ? that.postalCode == null : this.postalCode.equals(that.postalCode));
    equal = equal && (this.country == null ? that.country == null : this.country.equals(that.country));
    equal = equal && (this.salesRepEmployeeNumber == null ? that.salesRepEmployeeNumber == null : this.salesRepEmployeeNumber.equals(that.salesRepEmployeeNumber));
    equal = equal && (this.creditLimit == null ? that.creditLimit == null : this.creditLimit.equals(that.creditLimit));
    equal = equal && (this.checknumber == null ? that.checknumber == null : this.checknumber.equals(that.checknumber));
    equal = equal && (this.paymentdate == null ? that.paymentdate == null : this.paymentdate.equals(that.paymentdate));
    equal = equal && (this.amount == null ? that.amount == null : this.amount.equals(that.amount));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof QueryResult)) {
      return false;
    }
    QueryResult that = (QueryResult) o;
    boolean equal = true;
    equal = equal && (this.customerNumber == null ? that.customerNumber == null : this.customerNumber.equals(that.customerNumber));
    equal = equal && (this.upper_c_customerName_ == null ? that.upper_c_customerName_ == null : this.upper_c_customerName_.equals(that.upper_c_customerName_));
    equal = equal && (this.contactFirstName == null ? that.contactFirstName == null : this.contactFirstName.equals(that.contactFirstName));
    equal = equal && (this.contactLastName == null ? that.contactLastName == null : this.contactLastName.equals(that.contactLastName));
    equal = equal && (this.phone == null ? that.phone == null : this.phone.equals(that.phone));
    equal = equal && (this.addressLine1 == null ? that.addressLine1 == null : this.addressLine1.equals(that.addressLine1));
    equal = equal && (this.city == null ? that.city == null : this.city.equals(that.city));
    equal = equal && (this.state == null ? that.state == null : this.state.equals(that.state));
    equal = equal && (this.postalCode == null ? that.postalCode == null : this.postalCode.equals(that.postalCode));
    equal = equal && (this.country == null ? that.country == null : this.country.equals(that.country));
    equal = equal && (this.salesRepEmployeeNumber == null ? that.salesRepEmployeeNumber == null : this.salesRepEmployeeNumber.equals(that.salesRepEmployeeNumber));
    equal = equal && (this.creditLimit == null ? that.creditLimit == null : this.creditLimit.equals(that.creditLimit));
    equal = equal && (this.checknumber == null ? that.checknumber == null : this.checknumber.equals(that.checknumber));
    equal = equal && (this.paymentdate == null ? that.paymentdate == null : this.paymentdate.equals(that.paymentdate));
    equal = equal && (this.amount == null ? that.amount == null : this.amount.equals(that.amount));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.customerNumber = JdbcWritableBridge.readInteger(1, __dbResults);
    this.upper_c_customerName_ = JdbcWritableBridge.readString(2, __dbResults);
    this.contactFirstName = JdbcWritableBridge.readString(3, __dbResults);
    this.contactLastName = JdbcWritableBridge.readString(4, __dbResults);
    this.phone = JdbcWritableBridge.readString(5, __dbResults);
    this.addressLine1 = JdbcWritableBridge.readString(6, __dbResults);
    this.city = JdbcWritableBridge.readString(7, __dbResults);
    this.state = JdbcWritableBridge.readString(8, __dbResults);
    this.postalCode = JdbcWritableBridge.readString(9, __dbResults);
    this.country = JdbcWritableBridge.readString(10, __dbResults);
    this.salesRepEmployeeNumber = JdbcWritableBridge.readInteger(11, __dbResults);
    this.creditLimit = JdbcWritableBridge.readBigDecimal(12, __dbResults);
    this.checknumber = JdbcWritableBridge.readString(13, __dbResults);
    this.paymentdate = JdbcWritableBridge.readDate(14, __dbResults);
    this.amount = JdbcWritableBridge.readBigDecimal(15, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.customerNumber = JdbcWritableBridge.readInteger(1, __dbResults);
    this.upper_c_customerName_ = JdbcWritableBridge.readString(2, __dbResults);
    this.contactFirstName = JdbcWritableBridge.readString(3, __dbResults);
    this.contactLastName = JdbcWritableBridge.readString(4, __dbResults);
    this.phone = JdbcWritableBridge.readString(5, __dbResults);
    this.addressLine1 = JdbcWritableBridge.readString(6, __dbResults);
    this.city = JdbcWritableBridge.readString(7, __dbResults);
    this.state = JdbcWritableBridge.readString(8, __dbResults);
    this.postalCode = JdbcWritableBridge.readString(9, __dbResults);
    this.country = JdbcWritableBridge.readString(10, __dbResults);
    this.salesRepEmployeeNumber = JdbcWritableBridge.readInteger(11, __dbResults);
    this.creditLimit = JdbcWritableBridge.readBigDecimal(12, __dbResults);
    this.checknumber = JdbcWritableBridge.readString(13, __dbResults);
    this.paymentdate = JdbcWritableBridge.readDate(14, __dbResults);
    this.amount = JdbcWritableBridge.readBigDecimal(15, __dbResults);
  }
  public void loadLargeObjects(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void loadLargeObjects0(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void write(PreparedStatement __dbStmt) throws SQLException {
    write(__dbStmt, 0);
  }

  public int write(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeInteger(customerNumber, 1 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeString(upper_c_customerName_, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(contactFirstName, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(contactLastName, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(phone, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(addressLine1, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(city, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(state, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(postalCode, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(country, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeInteger(salesRepEmployeeNumber, 11 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(creditLimit, 12 + __off, 3, __dbStmt);
    JdbcWritableBridge.writeString(checknumber, 13 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeDate(paymentdate, 14 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(amount, 15 + __off, 3, __dbStmt);
    return 15;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeInteger(customerNumber, 1 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeString(upper_c_customerName_, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(contactFirstName, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(contactLastName, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(phone, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(addressLine1, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(city, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(state, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(postalCode, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(country, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeInteger(salesRepEmployeeNumber, 11 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(creditLimit, 12 + __off, 3, __dbStmt);
    JdbcWritableBridge.writeString(checknumber, 13 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeDate(paymentdate, 14 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeBigDecimal(amount, 15 + __off, 3, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.customerNumber = null;
    } else {
    this.customerNumber = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.upper_c_customerName_ = null;
    } else {
    this.upper_c_customerName_ = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.contactFirstName = null;
    } else {
    this.contactFirstName = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.contactLastName = null;
    } else {
    this.contactLastName = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.phone = null;
    } else {
    this.phone = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.addressLine1 = null;
    } else {
    this.addressLine1 = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.city = null;
    } else {
    this.city = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.state = null;
    } else {
    this.state = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.postalCode = null;
    } else {
    this.postalCode = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.country = null;
    } else {
    this.country = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.salesRepEmployeeNumber = null;
    } else {
    this.salesRepEmployeeNumber = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.creditLimit = null;
    } else {
    this.creditLimit = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.checknumber = null;
    } else {
    this.checknumber = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.paymentdate = null;
    } else {
    this.paymentdate = new Date(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.amount = null;
    } else {
    this.amount = com.cloudera.sqoop.lib.BigDecimalSerializer.readFields(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.customerNumber) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.customerNumber);
    }
    if (null == this.upper_c_customerName_) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, upper_c_customerName_);
    }
    if (null == this.contactFirstName) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, contactFirstName);
    }
    if (null == this.contactLastName) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, contactLastName);
    }
    if (null == this.phone) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, phone);
    }
    if (null == this.addressLine1) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, addressLine1);
    }
    if (null == this.city) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, city);
    }
    if (null == this.state) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, state);
    }
    if (null == this.postalCode) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, postalCode);
    }
    if (null == this.country) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, country);
    }
    if (null == this.salesRepEmployeeNumber) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.salesRepEmployeeNumber);
    }
    if (null == this.creditLimit) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.creditLimit, __dataOut);
    }
    if (null == this.checknumber) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, checknumber);
    }
    if (null == this.paymentdate) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.paymentdate.getTime());
    }
    if (null == this.amount) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.amount, __dataOut);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.customerNumber) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.customerNumber);
    }
    if (null == this.upper_c_customerName_) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, upper_c_customerName_);
    }
    if (null == this.contactFirstName) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, contactFirstName);
    }
    if (null == this.contactLastName) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, contactLastName);
    }
    if (null == this.phone) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, phone);
    }
    if (null == this.addressLine1) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, addressLine1);
    }
    if (null == this.city) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, city);
    }
    if (null == this.state) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, state);
    }
    if (null == this.postalCode) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, postalCode);
    }
    if (null == this.country) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, country);
    }
    if (null == this.salesRepEmployeeNumber) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.salesRepEmployeeNumber);
    }
    if (null == this.creditLimit) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.creditLimit, __dataOut);
    }
    if (null == this.checknumber) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, checknumber);
    }
    if (null == this.paymentdate) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.paymentdate.getTime());
    }
    if (null == this.amount) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    com.cloudera.sqoop.lib.BigDecimalSerializer.write(this.amount, __dataOut);
    }
  }
  private static final DelimiterSet __outputDelimiters = new DelimiterSet((char) 126, (char) 10, (char) 0, (char) 0, false);
  public String toString() {
    return toString(__outputDelimiters, true);
  }
  public String toString(DelimiterSet delimiters) {
    return toString(delimiters, true);
  }
  public String toString(boolean useRecordDelim) {
    return toString(__outputDelimiters, useRecordDelim);
  }
  public String toString(DelimiterSet delimiters, boolean useRecordDelim) {
    StringBuilder __sb = new StringBuilder();
    char fieldDelim = delimiters.getFieldsTerminatedBy();
    __sb.append(FieldFormatter.escapeAndEnclose(customerNumber==null?"null":"" + customerNumber, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(upper_c_customerName_==null?"NA":upper_c_customerName_, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(contactFirstName==null?"NA":contactFirstName, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(contactLastName==null?"NA":contactLastName, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(phone==null?"NA":phone, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(addressLine1==null?"NA":addressLine1, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(city==null?"NA":city, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(state==null?"NA":state, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(postalCode==null?"NA":postalCode, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(country==null?"NA":country, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(salesRepEmployeeNumber==null?"null":"" + salesRepEmployeeNumber, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(creditLimit==null?"null":creditLimit.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(checknumber==null?"NA":checknumber, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(paymentdate==null?"null":"" + paymentdate, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(amount==null?"null":amount.toPlainString(), delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(customerNumber==null?"null":"" + customerNumber, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(upper_c_customerName_==null?"NA":upper_c_customerName_, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(contactFirstName==null?"NA":contactFirstName, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(contactLastName==null?"NA":contactLastName, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(phone==null?"NA":phone, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(addressLine1==null?"NA":addressLine1, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(city==null?"NA":city, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(state==null?"NA":state, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(postalCode==null?"NA":postalCode, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(country==null?"NA":country, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(salesRepEmployeeNumber==null?"null":"" + salesRepEmployeeNumber, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(creditLimit==null?"null":creditLimit.toPlainString(), delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(checknumber==null?"NA":checknumber, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(paymentdate==null?"null":"" + paymentdate, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(amount==null?"null":amount.toPlainString(), delimiters));
  }
  private static final DelimiterSet __inputDelimiters = new DelimiterSet((char) 126, (char) 10, (char) 0, (char) 0, false);
  private RecordParser __parser;
  public void parse(Text __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharSequence __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(byte [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(char [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(ByteBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  private void __loadFromFields(List<String> fields) {
    Iterator<String> __it = fields.listIterator();
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.customerNumber = null; } else {
      this.customerNumber = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.upper_c_customerName_ = null; } else {
      this.upper_c_customerName_ = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.contactFirstName = null; } else {
      this.contactFirstName = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.contactLastName = null; } else {
      this.contactLastName = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.phone = null; } else {
      this.phone = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.addressLine1 = null; } else {
      this.addressLine1 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.city = null; } else {
      this.city = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.state = null; } else {
      this.state = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.postalCode = null; } else {
      this.postalCode = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.country = null; } else {
      this.country = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.salesRepEmployeeNumber = null; } else {
      this.salesRepEmployeeNumber = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.creditLimit = null; } else {
      this.creditLimit = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.checknumber = null; } else {
      this.checknumber = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.paymentdate = null; } else {
      this.paymentdate = java.sql.Date.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.amount = null; } else {
      this.amount = new java.math.BigDecimal(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.customerNumber = null; } else {
      this.customerNumber = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.upper_c_customerName_ = null; } else {
      this.upper_c_customerName_ = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.contactFirstName = null; } else {
      this.contactFirstName = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.contactLastName = null; } else {
      this.contactLastName = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.phone = null; } else {
      this.phone = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.addressLine1 = null; } else {
      this.addressLine1 = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.city = null; } else {
      this.city = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.state = null; } else {
      this.state = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.postalCode = null; } else {
      this.postalCode = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.country = null; } else {
      this.country = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.salesRepEmployeeNumber = null; } else {
      this.salesRepEmployeeNumber = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.creditLimit = null; } else {
      this.creditLimit = new java.math.BigDecimal(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.checknumber = null; } else {
      this.checknumber = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.paymentdate = null; } else {
      this.paymentdate = java.sql.Date.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.amount = null; } else {
      this.amount = new java.math.BigDecimal(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    QueryResult o = (QueryResult) super.clone();
    o.paymentdate = (o.paymentdate != null) ? (java.sql.Date) o.paymentdate.clone() : null;
    return o;
  }

  public void clone0(QueryResult o) throws CloneNotSupportedException {
    o.paymentdate = (o.paymentdate != null) ? (java.sql.Date) o.paymentdate.clone() : null;
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new TreeMap<String, Object>();
    __sqoop$field_map.put("customerNumber", this.customerNumber);
    __sqoop$field_map.put("upper_c_customerName_", this.upper_c_customerName_);
    __sqoop$field_map.put("contactFirstName", this.contactFirstName);
    __sqoop$field_map.put("contactLastName", this.contactLastName);
    __sqoop$field_map.put("phone", this.phone);
    __sqoop$field_map.put("addressLine1", this.addressLine1);
    __sqoop$field_map.put("city", this.city);
    __sqoop$field_map.put("state", this.state);
    __sqoop$field_map.put("postalCode", this.postalCode);
    __sqoop$field_map.put("country", this.country);
    __sqoop$field_map.put("salesRepEmployeeNumber", this.salesRepEmployeeNumber);
    __sqoop$field_map.put("creditLimit", this.creditLimit);
    __sqoop$field_map.put("checknumber", this.checknumber);
    __sqoop$field_map.put("paymentdate", this.paymentdate);
    __sqoop$field_map.put("amount", this.amount);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("customerNumber", this.customerNumber);
    __sqoop$field_map.put("upper_c_customerName_", this.upper_c_customerName_);
    __sqoop$field_map.put("contactFirstName", this.contactFirstName);
    __sqoop$field_map.put("contactLastName", this.contactLastName);
    __sqoop$field_map.put("phone", this.phone);
    __sqoop$field_map.put("addressLine1", this.addressLine1);
    __sqoop$field_map.put("city", this.city);
    __sqoop$field_map.put("state", this.state);
    __sqoop$field_map.put("postalCode", this.postalCode);
    __sqoop$field_map.put("country", this.country);
    __sqoop$field_map.put("salesRepEmployeeNumber", this.salesRepEmployeeNumber);
    __sqoop$field_map.put("creditLimit", this.creditLimit);
    __sqoop$field_map.put("checknumber", this.checknumber);
    __sqoop$field_map.put("paymentdate", this.paymentdate);
    __sqoop$field_map.put("amount", this.amount);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if ("customerNumber".equals(__fieldName)) {
      this.customerNumber = (Integer) __fieldVal;
    }
    else    if ("upper_c_customerName_".equals(__fieldName)) {
      this.upper_c_customerName_ = (String) __fieldVal;
    }
    else    if ("contactFirstName".equals(__fieldName)) {
      this.contactFirstName = (String) __fieldVal;
    }
    else    if ("contactLastName".equals(__fieldName)) {
      this.contactLastName = (String) __fieldVal;
    }
    else    if ("phone".equals(__fieldName)) {
      this.phone = (String) __fieldVal;
    }
    else    if ("addressLine1".equals(__fieldName)) {
      this.addressLine1 = (String) __fieldVal;
    }
    else    if ("city".equals(__fieldName)) {
      this.city = (String) __fieldVal;
    }
    else    if ("state".equals(__fieldName)) {
      this.state = (String) __fieldVal;
    }
    else    if ("postalCode".equals(__fieldName)) {
      this.postalCode = (String) __fieldVal;
    }
    else    if ("country".equals(__fieldName)) {
      this.country = (String) __fieldVal;
    }
    else    if ("salesRepEmployeeNumber".equals(__fieldName)) {
      this.salesRepEmployeeNumber = (Integer) __fieldVal;
    }
    else    if ("creditLimit".equals(__fieldName)) {
      this.creditLimit = (java.math.BigDecimal) __fieldVal;
    }
    else    if ("checknumber".equals(__fieldName)) {
      this.checknumber = (String) __fieldVal;
    }
    else    if ("paymentdate".equals(__fieldName)) {
      this.paymentdate = (java.sql.Date) __fieldVal;
    }
    else    if ("amount".equals(__fieldName)) {
      this.amount = (java.math.BigDecimal) __fieldVal;
    }
    else {
      throw new RuntimeException("No such field: " + __fieldName);
    }
  }
  public boolean setField0(String __fieldName, Object __fieldVal) {
    if ("customerNumber".equals(__fieldName)) {
      this.customerNumber = (Integer) __fieldVal;
      return true;
    }
    else    if ("upper_c_customerName_".equals(__fieldName)) {
      this.upper_c_customerName_ = (String) __fieldVal;
      return true;
    }
    else    if ("contactFirstName".equals(__fieldName)) {
      this.contactFirstName = (String) __fieldVal;
      return true;
    }
    else    if ("contactLastName".equals(__fieldName)) {
      this.contactLastName = (String) __fieldVal;
      return true;
    }
    else    if ("phone".equals(__fieldName)) {
      this.phone = (String) __fieldVal;
      return true;
    }
    else    if ("addressLine1".equals(__fieldName)) {
      this.addressLine1 = (String) __fieldVal;
      return true;
    }
    else    if ("city".equals(__fieldName)) {
      this.city = (String) __fieldVal;
      return true;
    }
    else    if ("state".equals(__fieldName)) {
      this.state = (String) __fieldVal;
      return true;
    }
    else    if ("postalCode".equals(__fieldName)) {
      this.postalCode = (String) __fieldVal;
      return true;
    }
    else    if ("country".equals(__fieldName)) {
      this.country = (String) __fieldVal;
      return true;
    }
    else    if ("salesRepEmployeeNumber".equals(__fieldName)) {
      this.salesRepEmployeeNumber = (Integer) __fieldVal;
      return true;
    }
    else    if ("creditLimit".equals(__fieldName)) {
      this.creditLimit = (java.math.BigDecimal) __fieldVal;
      return true;
    }
    else    if ("checknumber".equals(__fieldName)) {
      this.checknumber = (String) __fieldVal;
      return true;
    }
    else    if ("paymentdate".equals(__fieldName)) {
      this.paymentdate = (java.sql.Date) __fieldVal;
      return true;
    }
    else    if ("amount".equals(__fieldName)) {
      this.amount = (java.math.BigDecimal) __fieldVal;
      return true;
    }
    else {
      return false;    }
  }
}
