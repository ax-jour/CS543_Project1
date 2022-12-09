# CS543 Final Project: Flights Delay Prediction #

### Group 1 CS543 Fall2022 ###
Xiang Ao (Netid: xa17, Leader), Rishika Bhanushali (Netid: rb1182), Chao Zhao (Netid: cz333)

## Data Insights ##
We thought of doing EDA on our dataset to know answers of some fundamental questions to answer. We did this only for one chosen year as the format and features are exactly the same for all others.\
**Question 1:** Which Airports have maximum delay?
We see that the Wendover Airport in Utah has experienced
maximum Average delay in 2013.
![alt text](https://github.com/ax-jour/CS543_Project1/blob/master/assets/Airports_Max_delay.png?raw=true)
\
**Question 2:** How much delays do different Carriers experience
each month?
![alt text](https://github.com/ax-jour/CS543_Project1/blob/master/assets/EDA2.png?raw=true)
\
**Question 3:** Prime reasons for delays? Following are the flights cancelled due to the corresponding reasons.
![alt text](https://github.com/ax-jour/CS543_Project1/blob/master/assets/EDA3.png?raw=true)
<p>&nbsp;</p>

## Models and Methods ##
### Decision Tree ###
In this project, we build a decision tree model from scratch. The training process of the model is basically the process of constructing the tree. Since the size of the dataset is too large to fit in the memory, the training process can be divided into 2 parts, and the flow chart is as Figure below:\
![alt text](https://github.com/ax-jour/CS543_Project1/blob/master/assets/dt_flow_chart.png?raw=true)
\
**buildTree** with spark. In this approach, we implement a Breadth First Search process to build the tree, and utilize the **MapReduce** with PySpark to compute the Information Gain based on Entropy, which is used to find the best split.\ 

**buildTree** in memory. After several splits, the dataset associated with certain tree node is getting smaller and smaller. When it is small enough to fit into the memory, we utilize a Depth First Search process to continue build the tree in memory, since this is a lot time efficient compared to the MapReduce process.\

We set a few thresholds for the training process, including the max depth of the tree, and the minimum number of data points associated with a tree node. When the tree reaches these thresholds, it stop to grow.\

### Neural Network ###
In addition to our decision tree from scratch, we also developed a neural network model to classify the delay categories. There are several architectures we experimented with but got a very good result on the following architecture.\
![alt text](https://github.com/ax-jour/CS543_Project1/blob/master/assets/NN%20architecture.png?raw=true) \
![alt text](https://github.com/ax-jour/CS543_Project1/blob/master/assets/NN%20Loss.png?raw=true) \

### Other Regressions ###
#### Linear Regression ####
#### Boosted Linear Regression/AdaBoost ####
#### Decision Tree Regressor ####

## Result Obtained ##
### Decision Tree ###
![alt text](https://github.com/ax-jour/CS543_Project1/blob/master/assets/DTresult.png?raw=true)
<p>&nbsp;</p>

### Neural Network ###
| Type | Value |
| ----------- | ----------- |
| Layers | 5
| Activation| Softmax
| Optimizer | Adam
| loss | Categorical Crossentropy\

We used categorical Crossentropy as we had generalized this
problem to a classification of the Departure delay into ten
different classes. We categorized the delay into the following
classes:\
![alt text](https://github.com/ax-jour/CS543_Project1/blob/master/assets/categories.png?raw=true)\
We generally achieve greater accuracy on the same classifica-
tion problem than other approaches is because neural networks
go over the entire dataset multiple times but other methods go
over it only once. We could achieve a final accuracy of 65.43%

## Dataset Reference ##
[1] Y. 'W. Mu, “Airline delay and cancellation data, 2009 - 2018,” Kaggle, 11-Aug-2019. [Online]. Available:https://www.kaggle.com/datasets/yuanyuwendymu/airline-delay-and-cancellation-data-2009-2018. [Accessed: 06-Oct-2022]. <br />
[2] Menne, Matthew J., Imke Durre, Bryant Korzeniewski, Shelley McNeill, Kristy Thomas, Xungang Yin, Steven Anthony, Ron Ray, Russell S. Vose, Byron E.Gleason, and Tamara G. Houston (2012): Global Historical Climatology Network - Daily (GHCN-Daily), Version 3. [indicate subset used]. NOAA National Climatic Data Center. doi:10.7289/V5D21VHZ [Accessed: 06-Oct-2022]. <br />
[3] “Airports in the United States of America,” Humanitarian Data Exchange. [Online]. Available: https://data.humdata.org/dataset/ourairports-usa. [Accessed: 03-Nov-2022]. <br />

### Project Dataset shared folder ###
https://drive.google.com/drive/folders/1pBULKHVw1MY_UptIrXz_pPM7zCV93G3Y?usp=sharing
