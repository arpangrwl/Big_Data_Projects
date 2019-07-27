#!/usr/bin/env python
# coding: utf-8

# In[39]:

from pyspark import SparkContext
sc =SparkContext()


# ### 1. read points

# In[40]:


# Load one dataset
pointsPath="hdfs:////user/ds503/project3/Points.csv"
points=sc.textFile(pointsPath)


# ### 2. define two helper function

# In[41]:


# Input point x, y
# Output cell ID
def getCellID(x, y):
    cell_x = int((x-1) / 20) + 1
    cell_y = int((y-1) / 20) + 1
    return cell_x + (500 - cell_y) * 500


# In[42]:


# Input cell ID
# Output cell neighbour ID array
def getNeighbour(c):
    upLeft = c - 501
    up = c - 500
    upRight = c - 499

    left = c - 1
    right = c + 1

    lowLeft = c + 499
    low = c + 500
    lowRight = c + 501

    if(c % 500 == 1):
        res = [up, upRight, right, low, lowRight]
        res = list(filter(lambda x: (x >= 1) & (x <= 250000), res))
    elif(c % 500 == 0):
        res = [upLeft, up, left, lowLeft, low]
        res = list(filter(lambda x: (x >= 1) & (x <= 250000), res))
    else:
        res = [upLeft, up, upRight, left, right, lowLeft, low, lowRight]
        res = list(filter(lambda x: (x >= 1) & (x <= 250000), res))
    return res



# ### 3. parse point
# from string to tuple (x, y)

# In[43]:


pointsXY = points.map(lambda x: [int(x.split(",")[0]), int(x.split(",")[1])])
pointsXY.take(2)


# ### 4. compute cell points count
# (cellID, pointCount)

# In[44]:


cellPointsCount = pointsXY.map(lambda x: (getCellID(x[0], x[1]), 1)).reduceByKey(lambda x, y: x + y)
cellPointsCount.take(2)


# ### 5. compute cell neighbour count
# (cellID, neighbourCount)

# In[45]:


def mapFunc1(x):
    cellID = x[0]
    cellNeigCount = len(getNeighbour(cellID))
    return [cellID, cellNeigCount]

cellNeigCount = cellPointsCount.map(mapFunc1)
cellNeigCount.take(2)


# ### 6. compute cell neighbour point avg count
# (cellID, neighbourAvgPointCount)

# In[46]:


def mapFunc2(x):
    cellID = x[0]
    cellNeigID = getNeighbour(cellID)
    for oneNeig in cellNeigID:
        yield (oneNeig, cellID)

cellNeigFlatCount = cellPointsCount.flatMap(mapFunc2).leftOuterJoin(cellPointsCount)
cellNeigFlatCount.take(2)


# In[47]:


# (cellID, neighbourPointCount)
def mapFunc3(x):
    cellID = x[1][0]
    cellNeigPointCount = 0 if(x[1][1] == None) else x[1][1]
    return (cellID, cellNeigPointCount)
cellNeigPointCount = cellNeigFlatCount.map(mapFunc3).reduceByKey(lambda x, y: x + y)
cellNeigPointCount.take(2)


# In[48]:


cellNeigAvgCount = cellNeigPointCount.join(cellNeigCount).map(lambda x: (x[0], (x[1][0]/x[1][1])))
cellNeigAvgCount.take(2)


# ### 7. compute cell density

# In[49]:


cellDensity = cellPointsCount.join(cellNeigAvgCount).map(lambda x: (x[0], x[1][0]/ x[1][1])).sortBy(lambda x: -x[1])                     

# In[50]:


cellDensity.take(3)


# ### 8. Top10 Density

# In[51]:


top10 = sc.parallelize(cellDensity.take(10))


# In[52]:


top10.collect()


# ### 9. TOP k grid cells w.r.t their Relative-Density Scores
# (cellID, cellDensity, numOfNeighbours, neighbourID, neighbourDensity)

# In[53]:


def flatMapFunc4(x):
    cellNeig = getNeighbour(x[0])
    for oneNeig in cellNeig:
        yield (oneNeig, (x[0], x[1], len(cellNeig)))

def mapFunc5(x):
    neigID = x[0]
    cellID = x[1][0][0]
    cellDensity = x[1][0][1]
    neigCount = x[1][0][2]
    neigDensity = x[1][1]

    return cellID, cellDensity, neigCount, neigID, neigDensity

rdd = top10.flatMap(flatMapFunc4).leftOuterJoin(cellDensity).map(mapFunc5).sortBy(lambda x: x[0])


# In[54]:


rdd.collect()
