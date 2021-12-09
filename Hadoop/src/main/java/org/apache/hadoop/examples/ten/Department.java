package org.apache.hadoop.examples.ten;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class  Department implements Writable {
    // 部门类
    // 10,accounting,new york
    private int deptno; // 部门号
    private String deptname; // 部门名称
    private String address; // 部门所在地点

    @Override
    public String toString(){

        return String.valueOf(deptno) + ',' + deptname + ',' + address;
    }

    //序列化方法：将java对象转化为可跨机器传输数据流（二进制串/字节）的一种技术，通俗理解为写操作
    public void write(DataOutput out) throws IOException{
        out.writeInt(this.deptno);
        out.writeUTF(this.deptname);
        out.writeUTF(this.address);
    }

    //反序列化方法：将可跨机器传输数据流（二进制串）转化为java对象的一种技术,通俗理解为读操作
    public void readFields(DataInput in) throws IOException{
        this.deptno = in.readInt();
        this.deptname = in.readUTF();
        this.address = in.readUTF();
    }

    public int getDeptno() {
        return deptno;
    }

    public void setDeptno(int deptno) {
        this.deptno = deptno;
    }

    public String getDeptname() {
        return deptname;
    }

    public void setDeptname(String deptname) {
        this.deptname = deptname;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }
}
