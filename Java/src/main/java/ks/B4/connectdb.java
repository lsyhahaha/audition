package ks.B4;

import java.sql.*;

class connectdb {
    public static void main(String[]args) {
        try {
            String url="jdbc:mysql://localhost:3306/depo?serverTimezone=UTC";
            String user="root";
            String pwd="123456";
            //加载驱动，这一句也可写为：Class.forName("com.mysql.jdbc.Driver");
            Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
            //建立到MySQL的连接
            Connection conn=DriverManager.getConnection(url,user,pwd);

            // 用于执行静态SQL语句并返回其生成的结果的对象。
            Statement stmt = conn.createStatement();

            // 调用executeQuery()方法，执行给定的SQL语句，这可能是 INSERT ， UPDATE ，或 DELETE声明，或者不返回任何内容，如SQL DDL语句的SQL语句。
            stmt.executeUpdate("INSERT INTO goods values ('1003', 'aaa', '200');");
            stmt.executeUpdate("DELETE FROM GOODS WHERE GNO = 1003");

            // 执行给定的SQL语句，返回一个 ResultSet对象。
            ResultSet rs = stmt.executeQuery("SELECT * FROM GOODS;");
            //处理结果集
            System.out.println("产品编号\t产品名称\t产品数量");
            while(rs.next()) {
                System.out.println(rs.getString("GNO") + '\t' + rs.getString("name") + "\t\t" + rs.getString("num"));
            }

            rs.close();//关闭数据库
            conn.close();
        }
        catch(Exception ex) {
            System.out.println("Error:" + ex.toString());}
    }
}
