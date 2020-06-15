package mapReduce;

//non ero sicuro hadoop avrebbe accettato javafx.pair
public class Pair {
    private String x;
    private String y;

    public Pair(String x, String y) {
        this.x = x;
        this.y = y;
    }

    public String getX() {
        return x;
    }

    public void setX(String x) {
        this.x = x;
    }

    public String getY() {
        return y;
    }

    public void setY(String y) {
        this.y = y;
    }

    @Override
    public String toString() {
        return "Pair{" +
                "x='" + x + '\'' +
                ", y='" + y + '\'' +
                '}';
    }
}
