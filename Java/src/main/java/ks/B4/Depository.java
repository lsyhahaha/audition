package ks.B4;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Scanner;

public class Depository {
    private int Dno;        // 仓库id
    private String Dname;   // 仓库名称
    private int Dnum;       // 仓库剩余货位

    public Depository(int dno, String dname, int dnum) {
        Dno = dno;
        Dname = dname;
        Dnum = dnum;
    }

    public static void addDepo(Connection conn) throws SQLException {
        // 键盘录入仓库对象所需要的数据，显示提示信息，提示要输出的何种信息
        Scanner sc = new Scanner(System.in);

        int id;

        while (true) {
            System.out.println("请输入仓库编号：");
            id = sc.nextInt();

            boolean flag = isUsed(conn, id);
            if (flag){
                System.out.println("你输入的仓库编号已经被占用，请重新输入");
            } else {
                break;
            }
        }

        System.out.println("请输入仓库名称：");
        String name = sc.next();

        System.out.println("请输入仓库的大小（剩余的货位）：");
        int num = sc.nextInt();

        // 创建仓库对象，把键盘录入的数据赋值给产品对象的成员变量
        Depository d = new Depository(id, name, num);


        // 将仓库对象添加到数据库中
        String sql = "INSERT INTO depo(Dno, Dname, Dnum) values (?, ?, ?);";
        PreparedStatement pstmt = conn.prepareStatement(sql);

        // 设置参数，参数索引从1开始
        pstmt.setInt(1, id);
        pstmt.setString(2, name);
        pstmt.setInt(3, num);

        //  执行 sql 语句
        int count = pstmt.executeUpdate();
        System.out.println("影响了" + count + "行");

        // 给出添加提示
        System.out.println("添加仓库成功");
    }

    private static boolean isUsed(Connection conn, int id) throws SQLException {
        // 如果集合中的某一个仓库的编号与其相同，返回true; 如果都不相同，返回false
        String sql = "SELECT * FROM DEPO WHERE DNO = ?";
        PreparedStatement pstmt = conn.prepareStatement(sql);
        pstmt.setInt(1, id);

        // 得到sql语句的结果
        ResultSet rs = pstmt.executeQuery();
        // 检索此 ResultSet对象的抓取大小。
        if (rs.getFetchSize() == 0) {
            return false;
        }

        return true;
    }

    public static void updateDepo(Connection conn) throws SQLException {
        // 键盘录入要修改仓库的仓库编号，显示提示信息
        Scanner sc = new Scanner(System.in);

        System.out.println("请输入你要修改的仓库的编号：");
        int id = sc.nextInt();

        // 键盘录入要修改的仓库信息
        System.out.println("请输入仓库的新名称：");
        String name = sc.next();

        System.out.println("请输入仓库的新的剩余货位数");
        int num = sc.nextInt();

        // 修改数据库中的信息
        String sql = "UPDATE DEPO SET DNO = ?, DNAME = ?, DNUM = ? WHERE DNO = ?";
        PreparedStatement pstmt = conn.prepareStatement(sql);
        // 参数设置
        pstmt.setInt(1, id);
        pstmt.setString(2, name);
        pstmt.setInt(3, num);
        pstmt.setInt(4, id);
        // 执行sql语句
        int count = pstmt.executeUpdate();
        System.out.println("影响了" + count + "行");

        if (count == 0){
            System.out.println("该仓库不存在，请重新输入");
        } else {
            // 给出修改成功提示
            System.out.println("修改仓库信息成功");
        }
    }

    public static void deleteDepo(Connection conn) throws SQLException {
        // 键盘输入要删除的仓库编号，显示提示信息
        Scanner sc = new Scanner(System.in);

        System.out.println("请输入你要删除的仓库编号");
        int id = sc.nextInt();

        // 在数据库中删除
        String sql = "DELETE FROM DEPO WHERE DNO = ?";
        PreparedStatement pstmt = conn.prepareStatement(sql);
        pstmt.setInt(1, id);
        //  执行 sql 语句
        int count = pstmt.executeUpdate();
        System.out.println("影响了" + count + "行");


        if (count == 0){
            System.out.println("该信息不存在， 请重新输入");
        } else {
            //给出删除成功的提示信息
            System.out.println("删除仓库信息成功");
        }
    }

    public static void findAllDepo(Connection conn) throws SQLException {
        Scanner sc = new Scanner(System.in);
        System.out.println("1.按编号查找\n2.按名称查找");
        int line = sc.nextInt();

        String sql;
        switch (line){
            case 1:
                System.out.println("请输入需要查找的编号：");
                int id = sc.nextInt();
                sql = "SELECT * FROM DEPO WHERE DNO = ?";
                PreparedStatement pstmt1 = conn.prepareStatement(sql);
                // 参数设置
                pstmt1.setInt(1, id);

                // 输出查询结果
                ResultSet rs1 = pstmt1.executeQuery();

                if (rs1.next()){
                    System.out.println("仓库编号\t仓库名称\t仓库剩余货位");
                    do {
                        System.out.println(rs1.getInt("DNO") + "\t\t" + rs1.getString("DNAME") + "\t\t" + rs1.getInt("DNUM"));
                    }while(rs1.next());
                } else {
                    System.out.println("该信息不存在， 请重新输入");
                    return;
                }
                break;
            case 2:
                System.out.println("请输入需要查找的名称：");
                String name = sc.next();
                sql = "SELECT * FROM DEPO WHERE DNAME = ?";
                PreparedStatement pstmt2 = conn.prepareStatement(sql);
                // 参数设置
                pstmt2.setString(1, name);

                // 输出查询结果
                ResultSet rs2 = pstmt2.executeQuery();

                if (rs2.next()){
                    System.out.println("仓库编号\t仓库名称\t仓库剩余货位");
                    do {
                        System.out.println(rs2.getInt("DNO") + "\t\t" + rs2.getString("DNAME") + "\t\t" + rs2.getInt("DNUM"));
                    }while(rs2.next());
                } else {
                    System.out.println("该信息不存在， 请重新输入");
                    return;
                }
                break;
            default:
                System.out.println("输入错误");
        }
    }
}
