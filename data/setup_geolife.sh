#!/bin/bash

#Assuming `Geolife_Trajectories_1.3.zip` exists in ~ 
cd ~
sudo apt-get install unzip -y
unzip Geolife_Trajectories_1.3.zip -d Geolife_Trajectories

hdfs dfs -mkdir /Geolife_Trajectories

hdfs dfs -put  ~/Geolife_Trajectories/Data /Geolife_Trajectories/
