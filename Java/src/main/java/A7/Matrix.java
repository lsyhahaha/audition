package A7;

public class Matrix {
    private int[][] nums = new int[10][10];

    public Matrix() {
        System.out.println("调用Matrix类的无参构造方法");
        this.nums = new int[10][10];
    }

    public Matrix(int n, int m) {
        System.out.println("调用Matrix类的有参构造方法");
        this.nums = new int[n][m];

        for (int i = 0; i < n; i++){
            for (int j = 0; j < n; j++){
                // 获取随机数
                double d = Math.random();
                int a = (int)(d*100);
                nums[i][j] = a;
            }
        }
    }

    //输出Matrix类中数组的元素值
    public void output(){
        int m = this.nums.length;
        int n = this.nums[0].length;

        for (int i = 0; i < m; i++){
            for (int j = 0; j < n; j++){
                System.out.print(nums[i][j] + " ");
            }
            System.out.print('\n');
        }

    }

    //输出一个矩阵的转置矩阵
    public void transpose(){
        int m = this.nums.length;
        int n = this.nums[0].length;

        for (int i = 0; i < n; i++){
            for (int j = 0; j < m; j++){
                System.out.print(nums[j][i] + " ");
            }
            System.out.print('\n');
        }
    }

    public static void main(String[] args) {
        Matrix a = new Matrix(3, 3);
        System.out.println("原数组：");
        a.output();
        System.out.println("转置：");
        a.transpose();
    }

}
