
sc

// Load one dataset
val pointsPath="hdfs:////user/ds503/project3/Points.csv"
val points=sc.textFile(pointsPath)

// Input point x, y
// Output cell ID
val getCellID: (Int, Int) => Int = {
    case (x, y) => {
        val cell_x = ((x-1) / 20) + 1
        val cell_y = ((y-1) / 20) + 1
        cell_x + (500 - cell_y) * 500

    }
}

// Input cell ID
// Output cell neighbour ID array
val getNeighbour: Int => Array[Int] = c =>  {

    val upLeft = c - 501
    val up = c - 500
    val upRight = c - 499

    val left = c - 1
    val right = c + 1

    val lowLeft = c + 499
    val low = c + 500
    val lowRight = c + 501

    if(c % 500 == 1) {
        Array(up, upRight, right, low, lowRight).filter(x => {x >= 1 & x <= 250000})
    } else if(c % 500 == 0){
        Array(upLeft, up, left, lowLeft, low).filter(x => {x >= 1 & x <= 250000})
    } else{
        Array(upLeft, up, upRight, left, right, lowLeft, low, lowRight).filter(x => {x >= 1 & x <= 250000})
    }

}

val pointsXY = points.map(x => {
    val field = x.split(",")
    (field(0).toInt, field(1).toInt)
})
pointsXY.take(2)

val cellPointsCount = pointsXY.map(x=>{
    (getCellID(x._1, x._2), 1)
}).reduceByKey((x, y) => x + y)
cellPointsCount.take(5)

val cellNeigCount = cellPointsCount.map(x => {
    val cellID = x._1
    val cellNeigCount = getNeighbour(cellID).length
    (cellID, cellNeigCount)
})
cellNeigCount.take(5)

val cellNeigFlatCount = cellPointsCount.flatMap(x=>{
    val cellID = x._1
    val cellNeigID = getNeighbour(cellID)
    for(oneNeig <- cellNeigID)
        yield (oneNeig, cellID)
}).leftOuterJoin(cellPointsCount)
cellNeigFlatCount.take(10)

// (cellID, neighbourPointCount)
val cellNeigPointCount = cellNeigFlatCount.map(x => {
    val cellID = x._2._1
//     val cellNeigPointCount = if(x._2._2 == None) 0 else x._2._2.get
    val cellNeigPointCount = if(x._2._2 == None) 0 else x._2._2.get
    (cellID, cellNeigPointCount)
}).reduceByKey((x, y) => (x + y))
cellNeigPointCount.take(5)

val cellNeigAvgCount = cellNeigPointCount.join(cellNeigCount).map(x => (x._1, (x._2._1.toDouble/x._2._2.toDouble)))
cellNeigAvgCount.take(5)


val cellDensity = cellPointsCount.join(cellNeigAvgCount).map(x => (x._1, (x._2._1.toDouble/x._2._2.toDouble))).sortBy(-_._2)


cellDensity.take(10)

val top10 = sc.parallelize(cellDensity.take(10))

top10.sortBy(-_._2).foreach(println)

val rdd = top10.flatMap(x => {
    val cellNeig = getNeighbour(x._1)
    for(oneNeig <- cellNeig)
        yield (oneNeig, (x._1, x._2, cellNeig.length))
}).leftOuterJoin(cellDensity).map{
    case (neigID, ((cellID, cellDensity, neigCount), neigDensity)) => {
        (cellID, cellDensity, neigCount, neigID, neigDensity.get)
    }
}.sortBy(_._1)

rdd.collect().foreach(println)
