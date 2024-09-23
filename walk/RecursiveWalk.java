package info.kgeorgiy.ja.denisov.walk;

public class RecursiveWalk {
    public static void main(String[] args) {
        try {
            new BaseWalk(Integer.MAX_VALUE, "RecursiveWalk").walk(args);
        } catch (WalkException e) {
            System.err.println(e.getMessage());
            if (e.getCause() != null) {
                System.err.println(e.getCause().getMessage());
            }
        }
    }
}
