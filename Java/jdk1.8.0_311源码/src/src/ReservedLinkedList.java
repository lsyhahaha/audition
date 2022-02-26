/*
* 定义一个单链表
*/
class ListNode {
    int val;
    ListNode next;
    ListNode () {}
    ListNode(int val) {this.val = val;}
    ListNode(int val, ListNode next) {this.val = val; this.next = next;}

    /*
    * 定义一个添加节点的方法*/
    public static ListNode add(int[] datas) {
        ListNode head = new ListNode(0);
        ListNode cur = head;

        for (int i = 0; i < datas.length; i++){
            ListNode next = new ListNode(datas[i]);
            cur.next = next;
            cur = next;
        }

        //信息提示
        System.out.println("当前链表：");
        ListNode.show(head.next);

        return head.next;
    }

    /*定义一个方法，打印链表*/
    public static void show(ListNode head) {
        while (head != null) {
            if (head.next == null){
                System.out.print(head.val);
                break;
            }
            System.out.print(head.val + "->");
            head = head.next;
        }
        System.out.println();
    }
}

public class ReservedLinkedList {
    // 反转链表
    public static ListNode reserved(ListNode head) {
        ListNode pre = null;

        while (head != null) {
            ListNode curNext = head.next;
            head.next = pre;
            pre = head;
            head = curNext;
        }
        return pre;
    }

    // 链表归并排序
    public static  ListNode sorted(ListNode head) {
        //,,
        if (head == null || head.next == null) return head;

        ListNode head1 = head;
        ListNode head2 = split(head);

        head1 = sorted(head1);
        head2 = sorted(head2);

        return merge(head1, head2);
    }

    // 合并连个排序链表
    private static ListNode merge(ListNode head1, ListNode head2) {
        ListNode dummy = new ListNode(0);
        ListNode p = dummy;

        while (head1 != null && head2 != null) {
            if (head1.val < head2.val) {
                p = p.next = head1;
                head1 = head1.next;
            } else {
                p = p.next = head2;
                head2 = head2.next;
            }
        }

        if (head1 != null) p.next = head1;
        if (head2 != null) p.next = head2;

        return dummy.next;
    }

    // 双指针找单链表中点模板
    private static ListNode split(ListNode head) {
        ListNode fast = head;
        ListNode slow = head;

        while (fast == null && fast.next.next == null) {
            slow = slow.next;
            fast = fast.next.next;
        }
        slow = slow.next;
        slow.next = null;

        return slow;
    }


    public static void main(String[] args) {
        ListNode a = ListNode.add(new int[]{2, 1, 3,5,6,3,5});

//        System.out.println("反转链表：");
//        a = ReservedLinkedList.reserved(a);
//        ListNode.show(a);

        System.out.println("归并排序：");
        a = ReservedLinkedList.sorted(a);
        ListNode.show(a);
    }
}
