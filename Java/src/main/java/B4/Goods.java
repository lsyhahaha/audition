package B4;

import java.sql.*;
import java.util.Scanner;

/*
    产品类：
       产品编号， 产品名称， 产品数量， 产品
*/
public class Goods {
    private int gno;        //产品编号
    private String name;    //产品名称
    private int num;        //产品库存数量

    public Goods(int gno, String name, int num) {
        this.gno = gno;
        this.name = name;
        this.num = num;
    }

    public static void addGoods(Connection conn) throws SQLException {
        /* 2．增加、修改、删除一个产品资料。 定义一个方法，用于添加产品信息*/

        // 键盘录入学生对象所需要的数据，显示提示信息，提示要输出何种信息
        Scanner sc = new Scanner(System.in);

        int id;

        while (true){
            System.out.println("请输入产品编号：");
            id = sc.nextInt();

            boolean flag = isUsed(conn, id);
            if (flag){
                System.out.println("你输入的产品编号已经别占用，请重新输入");
            } else {
                break;
            }
        }

        System.out.println("请输入产品名称：");
        String name = sc.next();

        System.out.println("请输入产品数量：");
        int num = sc.nextInt();

        // 将产品对象加到数据库中
        String sql = "INSERT INTO goods(gno, name, num) values (?, ?, ?);";
        PreparedStatement pstmt = conn.prepareStatement(sql);

        // 设置参数，参数索引从1开始
        pstmt.setInt(1, id);
        pstmt.setString(2, name);
        pstmt.setInt(3, num);

        //  执行 sql 语句
        int count = pstmt.executeUpdate();
        System.out.println("影响了" + count + "行");

        // 给出添加提示
        System.out.println("添加产品成功！");

    }

    private static boolean isUsed(Connection conn, int id) throws SQLException {
        /*定义一个方法，判断产品编号是否被使用*/

        // 如果集合中的某一个产品编号相同，返回true; 如果都不相同，返回false
        String sql = "SELECT * FROM GOODS WHERE GNO = ?";
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

    public static void findAllGoods(Connection conn) throws SQLException {
        /*     4．按条件显示产品资料（条件有按编号、名称等）。
                定义一个方法， 用于查看产品的信息
        */

        Scanner sc = new Scanner(System.in);
        System.out.println("1.按编号查找\n2.按名称查找");
        int line = sc.nextInt();

        String sql;
        switch (line){
            case 1:
                System.out.println("请输入需要查找的编号：");
                int id = sc.nextInt();
                sql = "SELECT * FROM GOODS WHERE GNO = ?";
                PreparedStatement pstmt1 = conn.prepareStatement(sql);
                // 参数设置
                pstmt1.setInt(1, id);

                // 输出查询结果
                ResultSet rs1 = pstmt1.executeQuery();

                if (rs1.next()){
                    System.out.println("产品编号\t产品名称\t产品库存");
                    do {
                        System.out.println(rs1.getInt("GNO") + "\t\t" + rs1.getString("NAME") + "\t\t" + rs1.getInt("NUM"));
                    }while(rs1.next());
                } else {
                    System.out.println("该信息不存在， 请重新输入");
                    return;
                }
                break;
            case 2:
                System.out.println("请输入需要查找的名称：");
                String name = sc.next();
                sql = "SELECT * FROM GOODS WHERE NAME = ?";
                PreparedStatement pstmt2 = conn.prepareStatement(sql);
                // 参数设置
                pstmt2.setString(1, name);

                // 输出查询结果
                ResultSet rs2 = pstmt2.executeQuery();

                if (rs2.next()){
                    System.out.println("产品编号\t产品名称\t产品库存");
                    do {
                        System.out.println(rs2.getInt("GNO") + "\t\t" + rs2.getString("NAME") + "\t\t" + rs2.getInt("NUM"));
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

    public static void updateGoods(Connection conn) throws SQLException {
        // 键盘录入要修改产品的产品编号，显示提示信息
        Scanner sc = new Scanner(System.in);

        System.out.println("请输入你要修改的产品的编号：");
        int id = sc.nextInt();


        // 键盘录入要修改的产品信息
        System.out.println("请输入产品的新名称：");
        String name = sc.next();

        System.out.println("请输入产品新的库存数量");
        int num = sc.nextInt();

        // 修改数据库中的信息
        String sql = "UPDATE GOODS SET GNO = ?, NAME = ?, NUM = ? WHERE GNO = ?";
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
            System.out.println("该产品不存在，请重新输入");
        } else {
            // 给出修改成功提示
            System.out.println("修改产品信息成功");
        }
    }

    public static void DeleteGoods(Connection conn) throws SQLException {
        // 键盘输入要删除的产品编号，显示提示信息
        Scanner sc = new Scanner(System.in);

        System.out.println("请输入你要删除的产品编号");
        int id = sc.nextInt();

        // 在数据库中删除
        String sql = "DELETE FROM GOODS WHERE GNO = ?";
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

    public static void getGoodNum(Connection conn) throws SQLException {
        // 定义一个方法，用于查找特定产品的数量（库存）
        Scanner sc = new Scanner(System.in);
        System.out.println("请输入产品名称：");
        String name = sc.next();

        // 去数据库中查找，获取这个产品的库存；没找到久给出提示信息
        String sql = "SELECT * FROM GOODS WHERE NAME = ?";
        PreparedStatement pstmt = conn.prepareStatement(sql);
        // 设置参数
        pstmt.setString(1, name);

        // 创建Resultset对象，输出查询结果
        ResultSet rs = pstmt.executeQuery();
        if (rs.next()){
            do {
                System.out.println("产品：" + rs.getString("name") + " " + "库存：" + rs.getString("num"));
            } while (rs.next());
        } else {
            System.out.println("该产品不存在，请重新输入");
        }
    }
}