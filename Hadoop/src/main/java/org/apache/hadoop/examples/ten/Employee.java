package org.apache.hadoop.examples.ten;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class Employee implements Writable {
    // 数据样例：7369	SMITH	CLERK	7902	1980-12-17	800		20
    private int empno;
    private String ename;
    private String job;
    private int mgr;
    private String hiredate;
    private int sal;
    private int comm;//奖金
    private int deptno;


    @Override
    public String toString() {
        int total=comm+sal;
//		return "Employee [empno=" + empno + ", ename=" + ename + ", sal=" + sal + ", deptno=" + deptno + "]";
        return empno+","+ename+","+job+","+mgr+","+hiredate+","+sal+","+comm+","+deptno+","+total;
    }


    //序列化方法：将java对象转化为可跨机器传输数据流（二进制串/字节）的一种技术，通俗理解为写操作
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.empno);
        out.writeUTF(this.ename);
        out.writeUTF(this.job);
        out.writeInt(this.mgr);
        out.writeUTF(this.hiredate);
        out.writeInt(this.sal);
        out.writeInt(this.comm);
        out.writeInt(this.deptno);

    }
    //反序列化方法：将可跨机器传输数据流（二进制串）转化为java对象的一种技术,通俗理解为读操作
    public void readFields(DataInput in) throws IOException {
        this.empno = in.readInt();
        this.ename = in.readUTF();
        this.job = in.readUTF();
        this.mgr = in.readInt();
        this.hiredate = in.readUTF();
        this.sal = in.readInt();
        this.comm = in.readInt();
        this.deptno = in.readInt();
    }

    //其他类通过set/get方法操作变量：Source-->Generator Getters and Setters
    public int getEmpno() {
        return empno;
    }
    public void setEmpno(int empno) {
        this.empno = empno;
    }
    public String getEname() {
        return ename;
    }
    public void setEname(String ename) {
        this.ename = ename;
    }
    public String getJob() {
        return job;
    }
    public void setJob(String job) {
        this.job = job;
    }
    public int getMgr() {
        return mgr;
    }
    public void setMgr(int mgr) {
        this.mgr = mgr;
    }
    public String getHiredate() {
        return hiredate;
    }
    public void setHiredate(String hiredate) {
        this.hiredate = hiredate;
    }
    public int getSal() {
        return sal;
    }
    public void setSal(int sal) {
        this.sal = sal;
    }
    public int getComm() {
        return comm;
    }
    public void setComm(int comm) {
        this.comm = comm;
    }
    public int getDeptno() {
        return deptno;
    }
    public void setDeptno(int deptno) {
        this.deptno = deptno;
    }
}
