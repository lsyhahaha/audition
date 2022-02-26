package ks.A2;
import ks.A2.Contact;

import java.io.*;
import java.util.ArrayList;
import java.util.Scanner;

public class AddressList {
    //实现该类并包含添加、删除、修改、按姓名查等几个方法。提示：利用文件存储通讯录。（本题30分）
    private static void Menu() {
        System.out.println("1.添加联系人信息");
        System.out.println("2.删除联系人信息（按no）");
        System.out.println("3.修改联系人信息（按no）");
        System.out.println("4.查询联系人信息（按姓名）");
        System.out.println("5.显示所有联系人信息");
        System.out.println("6.保存所有联系人信息");
        System.out.println("7.退出系统");
    }

    public static void main(String[] args) throws IOException {
        // 将所有联系人的信息保存在集合中
        ArrayList<Contact> arrayList = new ArrayList<>();
        // 加载已有联系人
        load(arrayList);
        Scanner sc = new Scanner(System.in);
        int choice;
        do {
            Menu();
            System.out.print("请输入你的选择：");
            choice = sc.nextInt();
            switch (choice) {
                case 1 :
                    Input(arrayList);//添加
                    break;
                case 2 :
                    delete(arrayList);//删除
                    break;
                case 3 :
                    alter(arrayList);//修改
                    break;
                case 4 :
                    find(arrayList);//查询
                    break;
                case 5 :
                    show(arrayList);//显示
                    break;
                case 6 :
                    save(arrayList);//保存
                    break;
            }
        } while (choice != 7);
        save(arrayList);
        System.out.println("保存信息！");
    }

    private static void alter(ArrayList<Contact> arrayList) {
        int flag=0;
        System.out.print("请输入要修改的联系人编号：");
        Scanner sc=new Scanner(System.in);
        String no=sc.next();
        for (int i = 0; i <arrayList.size() ; i++) {
            if (arrayList.get(i).getNo().equals(no)) {
                System.out.println("你正在修改的信息是："+arrayList.get(i));
                arrayList.remove(i);
                System.out.println("请输入新信息：");
                Input(arrayList);
                flag=1;
                break;
            }
        }
        if(flag==0) System.out.println("系统中没有此联系人编号！");
    }

    //从文件读取联系人信息
    private static void load(ArrayList<Contact> arrayList) throws IOException {
        String no,name, sex, phoneNumber, address,email;
        BufferedReader br = new BufferedReader(new FileReader("src/main/java/ks.A2/phone.txt"));
        String line;
        while ((line = br.readLine())!=null) {
            Contact c=new Contact();
            String[] words = line.split("\\s+");
            no=words[0];
            name=words[1];
            sex=words[2];
            phoneNumber=words[3];
            address=words[4];
            email=words[5];
            c.setNo(no);
            c.setName(name);
            c.setSex(sex);
            c.setPhoneNumber(phoneNumber);
            c.setAddress(address);
            c.setEmail(email);
            arrayList.add(c);
        }
        br.close();
    }

    //保存联系人到文件
    private static void save(ArrayList<Contact> arrayList) throws IOException {
        BufferedWriter bw = new BufferedWriter(new FileWriter(".\\src\\main\\java\\ks.A2\\phone.txt"));
        for (Contact arr : arrayList) {
            bw.write(arr.getNo()+"\t\t");
            bw.write(arr.getName()+"\t\t");
            bw.write(arr.getSex()+"\t\t");
            bw.write(arr.getPhoneNumber()+"\t\t");
            bw.write(arr.getAddress()+"\t\t");
            bw.write(arr.getEmail());
            bw.newLine();
            bw.flush();
        }
        bw.close();
        System.out.println("信息已成功保存！");
    }

    //显示所有联系人信息
    private static void show(ArrayList<Contact> arrayList) {
        System.out.println("编号\t\t姓名\t\t性别\t\t电话\t\t\t\t地址\t\t\t邮箱" );
        for (Contact arr : arrayList) {
            System.out.println(arr);
        }
    }

    //删除联系人（按no）
    private static void delete(ArrayList<Contact> arrayList) {
        boolean flag=true;
        System.out.print("请输入要删除联系人的编号：");
        Scanner sc = new Scanner(System.in);
        String no = sc.next();
        for (int i = 0; i < arrayList.size(); i++) {
            if (arrayList.get(i).getNo().equals(no)) {
                flag=false;
                System.out.println("----已删除编号为" + no + "且姓名为"+arrayList.get(i).getName()+"的联系人的信息！-----");
                arrayList.remove(arrayList.get(i));
            }
        }
        if(flag){
            System.out.println("查无此人！");
        }
        else{
            System.out.println("-------删除完毕！----------");
        }

    }

    //按姓名查询联系人信息
    private static void find(ArrayList<Contact> arrayList) {
        boolean flag=true;
        System.out.print("请输入要查询联系人的姓名：");
        Scanner sc = new Scanner(System.in);
        String name = sc.next();
        for (Contact arr : arrayList) {
            if (arr.getName().equals(name)) {
                System.out.println(arr);
                flag=false;
            }
        }
        if(flag){
            System.out.println("查无此人！");
        }
        else{
            System.out.println("-------查找完毕！----------");
        }
    }

    //添加联系人信息
    private static void Input(ArrayList<Contact> arrayList) {
        Scanner sc = new Scanner(System.in);
        Contact c = new Contact();
        String no,name, sex, phoneNumber, address,email;
        System.out.println("请输入联系人信息：");
        System.out.print("编号：");
        no=sc.next();
        for (int i=0;i<arrayList.size();i++){
            if(arrayList.get(i).getNo().equals(no)) {
                System.out.print("编号重复,请重新输入:");
                no=sc.next();
                i=-1;
            }
        }
        System.out.print("姓名：");
        name = sc.next();
        System.out.print("性别：");
        sex = sc.next();
        System.out.print("电话：");
        phoneNumber = sc.next();
        System.out.print("地址：");
        address = sc.next();
        System.out.print("邮箱：");
        email=sc.next();
        c.setNo(no);
        c.setName(name);
        c.setSex(sex);
        c.setPhoneNumber(phoneNumber);
        c.setAddress(address);
        c.setEmail(email);
        arrayList.add(c);
        System.out.println("已成功录入信息！");
    }

}
