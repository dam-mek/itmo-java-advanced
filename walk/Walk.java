package info.kgeorgiy.ja.denisov.walk;

public class Walk {
    public static void main(String[] args) {
        try {
            new BaseWalk(0, "Walk").walk(args);
        } catch (WalkException e) {
            System.err.println(e.getMessage());
        }
    }
}
