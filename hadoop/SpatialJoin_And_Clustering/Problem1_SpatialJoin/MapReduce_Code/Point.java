public class Point {
    int x;
    int y;
    public Point(int x, int y){
        this.x = x;
        this.y = y;
    }
    public Point(String point){
        this.x = Integer.parseInt(point.split(",")[0]);
        this.y = Integer.parseInt(point.split(",")[1]);
    }

    public int occupyBlock(){
        int xSize = 10000;
        int blockNum = 2000;
        int blockSize = xSize/blockNum; //5

        return (this.x/blockSize);
    }

    public String toString(){
        return x + "," + y;
    }
}
