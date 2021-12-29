package A2;
/*
2、个人通讯录系统
   建立一个通讯录，要求通讯录中必须含有编号、姓名，性别、电话、地址、Email等等。
   实现该类并包含添加、删除、修改、按姓名查等几个方法。提示：利用文件存储通讯录。（本题30分）
 */

public class Contact {
    private String no;
    private String name;
    private String sex;
    private String phoneNumber;
    private String address;
    private String email;

    public Contact() {
    }

    public String getNo() {
        return no;
    }

    public void setNo(String no) {
        this.no = no;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public String getPhoneNumber() {
        return phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

    @Override
    public String toString() {
        return  no + "\t\t" + name + "\t\t"  + sex + "\t\t"  + phoneNumber + "\t\t"  + address + "\t\t"  + email;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }
}
