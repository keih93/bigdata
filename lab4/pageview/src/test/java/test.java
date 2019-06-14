
public class test {
    public static void main(String[] args){
        String word = "237.226.217.165 http://xrea.com/ 1478125715";
        String[] splittest = word.split("\\s");
        String small = splittest[0];
        System.out.println(splittest.length);
    }
}
