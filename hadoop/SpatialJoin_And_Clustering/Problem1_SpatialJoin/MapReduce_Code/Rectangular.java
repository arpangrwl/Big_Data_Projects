public class Rectangular {

    int region;
    int bottomLeftX;
    int bottomLeftY;
    int topRightX;
    int topRightY;
    public Rectangular(int region, int bottomLeftX, int bottomLeftY, int topRightX, int topRightY) {
        this.region = region;
        this.bottomLeftX = bottomLeftX;
        this.bottomLeftY = bottomLeftY;
        this.topRightX = topRightX;
        this.topRightY = topRightY;
    }
    public Rectangular(String location) {
        String[] loc = location.split(",");
        if(loc.length == 5){
            this.region = Integer.parseInt(loc[0]);
            this.bottomLeftX = Integer.parseInt(loc[1]);
            this.bottomLeftY = Integer.parseInt(loc[2]);
            this.topRightX = Integer.parseInt(loc[3]);
            this.topRightY = Integer.parseInt(loc[4]);
        }else if(loc.length == 4){
            this.bottomLeftX = Integer.parseInt(loc[0]);
            this.bottomLeftY = Integer.parseInt(loc[1]);
            this.topRightX = Integer.parseInt(loc[2]);
            this.topRightY = Integer.parseInt(loc[3]);
        }

    }

    public int getWidth(){
        return (topRightX-bottomLeftX);
    }
    public int getHeight(){
        return (topRightY-bottomLeftY);
    }

    public String occupyBlock(){
        int xSize = 10000;
        int blockNum = 2000;
        int blockSize = xSize/blockNum; //5

        int recLeftBlock = this.bottomLeftX / blockSize; // x = 1    1/5 = 0
        int recRightBlock = this.topRightX / blockSize;

        StringBuilder sb = new StringBuilder();
        for(int i = recLeftBlock; i <= recRightBlock; i++){
            sb.append(i);
            if(i!=recRightBlock)    sb.append(",");
        }
        return sb.toString();
    }

    public int occupyBlockNum(){
        return this.occupyBlock().split(",").length;
    }

    public boolean pointInRec(int x, int y){
        if(x >= this.bottomLeftX && x <= this.bottomLeftY && y >= this.topRightX && y <= this.topRightY){
            return true;
        }else{
            return false;
        }
    }
    public boolean overlapWithAnotherRec(Rectangular rec2){
        boolean noOverlap = this.bottomLeftX > rec2.topRightX ||
                this.topRightX < rec2.bottomLeftX ||
                this.bottomLeftY > rec2.topRightY ||
                this.topRightY < rec2.bottomLeftY;
        return !noOverlap;
    }

    public boolean pointInRec(String point){
        int x = Integer.parseInt(point.split(",")[0]);
        int y = Integer.parseInt(point.split(",")[1]);
        if(x >= this.bottomLeftX && x <= this.topRightX && y >= this.bottomLeftY && y <= this.topRightY){
            return true;
        }else{
            return false;
        }
    }

}
