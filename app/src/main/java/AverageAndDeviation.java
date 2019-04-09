public class AverageAndDeviation {
    double average;
    double deviation;

    public AverageAndDeviation(double average, double deviation){
        this.average = average;
        this.deviation = deviation;
    }

    @Override
    public String toString() {
        return "AverageAndDeviation{" +
                "average=" + average +
                ", deviation=" + deviation +
                '}';
    }
}
