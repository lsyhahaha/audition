package B4;

import java.sql.*;
import java.util.Scanner;


/*
    仓库类
*/
public class Manager {
    /*1. 输出语句完成主界面的编写；
    * 2. 用Scanner实现键盘盘录入数据
    * 3. 用switch语句完成操作的选择
    * 4. 用循环完成再次回到主界面
    * */

    public static void main(String[] args) throws ClassNotFoundException, InstantiationException, IllegalAccessException, SQLException {
        // 连接数据库
        String url="jdbc:mysql://localhost:3306/depo?serverTimezone=UTC";
        String user="root";
        String pwd="123456";
        //加载驱动，这一句也可写为：Class.forName("com.mysql.jdbc.Driver");
        Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
        //建立到MySQL的连接
        Connection conn= DriverManager.getConnection(url,user,pwd);
        System.out.println(conn.isClosed());

        // 用循环完成再次回到主界面
        while (true){
            // 用输出语句完成主界面的编写
            System.out.println("----------" +"简单得到仓库管理系统" + "----------" );
            System.out.println("1 增加一个仓库信息");
            System.out.println("2 修改一个仓库信息");
            System.out.println("3 删除一个仓库信息");
            System.out.println("4 增加一个产品信息");
            System.out.println("5 修改一个产品信息");
            System.out.println("6 删除一个产品信息");
            System.out.println("7 按条件显示仓库资料（条件有按编号或名称）");
            System.out.println("8 按条件显示产品资料（条件有按编号或名称）");
            System.out.println("9 查找指定产品的数量");
            System.out.println("10 退出");
            System.out.println("请输入你的选择：");

            // 用Scanner实现键盘录入数据
            Scanner sc = new Scanner(System.in);
            String line = sc.nextLine();

            // 用switch语句完成操作选择
            switch (line) {
                case "1":
                    Depository.addDepo(conn);
                    break;
                case "2":
                    Depository.updateDepo(conn);
                    break;
                case "3":
                    Depository.deleteDepo(conn);
                    break;
                case "4":
                    Goods.addGoods(conn);
                case "5":
                    Goods.updateGoods(conn);
                    break;
                case "6":
                    Goods.DeleteGoods(conn);
                    break;
                case "7":
                    Depository.findAllDepo(conn);
                    break;
                case "8":
                    Goods.findAllGoods(conn);
                    break;
                case "9":
                    Goods.getGoodNum(conn);
                    break;
                case "10":
                    System.out.println("谢谢使用！");
                    System.exit(0); // JVM退出
                default:
                    // 关闭数据库连接
                    conn.close();
                    System.out.println("输入错误，请重新输入！");
            }
        }
    }
}
