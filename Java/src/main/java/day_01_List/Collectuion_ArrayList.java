package day_01_List;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

class Clloction_ArrayList{
    public static void main(String[] args) {
        Collection<String> a = new ArrayList<>();

        //添加方法： boolean add()
        a.add("hello");
        a.add("world");
        a.add("你好");

        // 迭代器方式遍历
        Iterator<String> iterator = a.iterator();
        while (iterator.hasNext()) {
            String next = iterator.next();
            System.out.println(next);
        }

//        System.out.println(a.iterator());
        // 打印集合
        System.out.println(a);
    }

}
