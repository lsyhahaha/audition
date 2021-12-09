package org.apache.hadoop.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Serializable {

    public static class SalaryTotalMapper extends Mapper<LongWritable, Text, IntWritable, Employee>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            // 数据样例：7369	SMITH	CLERK	7902	1980-12-17	800		20
            String data = value.toString();
            // 分词
            String[] words = data.split(",");
            // 创建员工对象
            Employee e = new Serializable.Employee();
            // 设置员工属性,依次为 员工编号，员工姓名，职位，领导的员工编号，雇佣日期，工资，奖金，部门编号
            e.setEmpno(Integer.parseInt(words[0]));
            e.setEname(words[1]);
            e.setJob(words[2]);

            try {
                e.setMgr(Integer.parseInt(words[3])); // 可能为空
            }catch (Exception ex){
                // 没有老板
                e.setMgr(-1);
            }
            e.setHiredate(words[4]);
            // 月薪
            e.setSal(Integer.parseInt(words[5]));
            try{
                e.setComm(Integer.parseInt(words[6]));
            }catch (Exception ex){
                // 没有奖金
                e.setComm(0);
            }
            e.setDeptno(Integer.parseInt(words[7]));

            context.write(new IntWritable(e.getDeptno()), e);
        }
    }

    public static class SalaryTotalReducer extends Reducer<IntWritable, Employee, IntWritable, Employee> {
        @Override
        protected void reduce(IntWritable k3, Iterable<Employee> v3, Context context) throws IOException, InterruptedException {
//            // 取出v3中的每个员工数据， 进行工资求和
//            int total = 0;
//            for (Employee e:v3){
//                total = total + e.getSal();
//            }
//
//            // 输出
            for (Employee e: v3){
                context.write(k3, e);
            }
            // 属性,依次为 员工编号，员工姓名，职位，领导的员工编号，雇佣日期，工资，奖金，部门编号
        }
    }

    public static class SalaryParitioner extends Partitioner<IntWritable, Employee>{
        @Override
        public int getPartition(IntWritable k2, Employee v2, int numPartition){
            // 如何建立分区
            if (v2.getSal() + v2.getComm() < 1500){
                // 低薪,放入1号分区
                return 1%numPartition;
            }else if (v2.getSal() + v2.getComm() >= 1500 && v2.getSal() + v2.getComm() < 3000){
                //中薪，放入2号分区
                return 2%numPartition;
            }else{
                // 高薪，放入0号分区
                return 3%numPartition;
            }

        }

    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf, "Serializable"); // 实例化一道作业
        job.setJarByClass(Serializable.class);

        // 指定job的mapper的输出的类型 k2 v2
        job.setMapperClass(Serializable.SalaryTotalMapper.class); // 设置Mapper类
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Employee.class);

        // !!!!!!!!!指定分区规则
        job.setPartitionerClass(Serializable.SalaryParitioner.class);
        // !!!!!!!!!指定建立几个分区
        job.setNumReduceTasks(3);

        // 指定job的reducer的输出的类型 k4 v4
        job.setReducerClass(Serializable.SalaryTotalReducer.class);// 设置ReducerClass类
        job.setOutputKeyClass(IntWritable.class);// 输出key的类型， 部门号
        job.setOutputValueClass(IntWritable.class);// 输出value的类型， 员工

        // 集群
//        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
//        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        // 本地测试
        FileInputFormat.setInputPaths(job, new Path("C:\\Users\\98708\\Desktop\\emp.csv"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\98708\\Desktop\\output"));

        // 执行任务
        job.waitForCompletion(true);

    }

    public static class Employee implements Writable {
        //字段名 EMPNO, ENAME,    JOB,   MGR,   HIREDATE,  SAL, COMM, DEPTNO
        //数据类型：Int，Char，          Char  ， Int，     Date  ，       Int   Int，  Int
        //数据: 7654, MARTIN, SALESMAN, 7698, 1981/9/28, 1250, 1400, 30
        //由以上定义变量
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
}