package ks.A7;

public class Square extends Matrix{
    public Square(){
        super();
        System.out.println("调用Square类的无参构造方法（使用super）");
    }

    public Square(int n, int m){
        super(n , m);
        System.out.println("调用Square类的有参构造方法（使用super）");
    }

    public static void main(String[] args) {
        Matrix a = new Matrix();
        a.show();
        Square b = new Square();
        Square c = new Square(1, 2);
    }
}
