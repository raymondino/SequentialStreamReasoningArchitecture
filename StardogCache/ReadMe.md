# StardogCache

This folder contains the run script to deploy the code into your folder. 
The architecture is built on the top of stardogs. 

# Prerequest:

1. install [stardog v4.2](http://docs.stardog.com/) (as of 2017/01/07)
2. require Java version 1.7 or 1.8
3. remind to set up the username and password as both "admin", or set up your own and change according the StardogCache/src/com/pnnl/shyre/main/Main.java's password and username. 
4. unzip StardogCache/files/LUBM45.nt.zip at the same directory. This unzips the streaming dataset "LUBM45.nt" which is 1GB in size. 
5. install ant

# Run:
1. cd into StardogCache folder
2. start stardog server
3. in your terminal, type ant, then the program will run
4. folders such as StardogCache/files/stardogFEFOdisk will contain all the benchmark files generated. 

# Disclaimer:
The code was originally implemented with stardog V4.0-rc3 and Java 7, and might malfunction when working with higher versions of both Java and stardog. The folder "stardog4.0-rc3 lib" contains the libary of stardog v4.0-rc3. The folder "lib" contains the libary of stardog v4.2. 
The default running library is v4.2