## UIC CS441 - Engineering Distributed Objects for Cloud Computing

## Project 2: Hadoop Map Reduce on DBLP XML Data 

## Overview

In this homework, we aim to implement Hadoop map reduce model to dblp xml data to perform data analytics operations like finding top 10 authors at a particular venue.

## Instructions to run the model

### Prerequisites

- Your workstation should have [SBT](https://www.scala-sbt.org/) installed.

### Steps to run the project via SBT shell

- Clone this private repository.
- Open the sbt shell and navigate to the path as the local repository's path.

#### Compile and Test
Type the following command in the sbt shell in order to build the project and run unit tests.

```
sbt clean compile test
```

#### Creating JAR
Type the following command in the sbt shell in order to build the project and run unit tests.

```
sbt clean compile assembly
```

#### Run the Hadoop Map Reduce Model

Type the following command in the folder where jar is present.

```
hadoop jar ansul_goenka_hw2-assembly-0.1.jar <input directory> <output directory>
```
        
### Model

1. Parses the XML files with multiple tags to create shards by refering to Mahout's Documentation

2. There are six map reduce jobs:

    Job 1: Finding top ten published authors at each venue  
    
    Job 2: Finding the list of authors who published without interruption for N (N>10) years  
    
    Job 3: For each venue producing the list of publications that contains only one author  
    
    Job 4: The list of publications for each venue that contain the highest number of authors for each of these venues 
    
    Job 5: Top 100 authors in the descending order who publish with most co-authors  
    
    Job 6: List of 100 authors who publish without any co-authors  
    

3. Takes input from typesafe config files, uses slf4j for logging and Junit for testing

### Output
    
### Job 1: Finding top ten published authors at each venue  

![Job1](https://bitbucket.org/cs441-fall2020/ansul_goenka_hw2/raw/51647978f4e18fd2691034ec3b868e86644406a5/images/Job1.PNG)
    
### Job 2: Finding the list of authors who published without interruption for N (N>10) years  

![Job2](https://bitbucket.org/cs441-fall2020/ansul_goenka_hw2/raw/51647978f4e18fd2691034ec3b868e86644406a5/images/Job2.PNG)
    
### Job 3: For each venue producing the list of publications that contains only one author  

![Job3](https://bitbucket.org/cs441-fall2020/ansul_goenka_hw2/raw/51647978f4e18fd2691034ec3b868e86644406a5/images/Job3.PNG)

### Job 4: The list of publications for each venue that contain the highest number of authors for each of these venues   
 
![Job4](https://bitbucket.org/cs441-fall2020/ansul_goenka_hw2/raw/51647978f4e18fd2691034ec3b868e86644406a5/images/Job4.PNG)
    
### Job 5: Top 100 authors in the descending order who publish with most co-authors  

![Job5](https://bitbucket.org/cs441-fall2020/ansul_goenka_hw2/raw/2af5efa78ffd8c62bbcdc37f93a58d1afbe052be/images/Job5.png)
    
### Job 6: List of 100 authors who publish without any co-authors  

![Job6](https://bitbucket.org/cs441-fall2020/ansul_goenka_hw2/raw/2af5efa78ffd8c62bbcdc37f93a58d1afbe052be/images/Job6.png)
    

### EMR
[AWS EMR Execution Steps](https://youtu.be/cn2a_lSgGhU)


