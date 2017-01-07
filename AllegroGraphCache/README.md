# AllegroGraphCache

This folder contains the run script to deploy the code into your folder. 
The architecture is built on the top of AllegroGraph. 

# Prerequest:

1. install [Allegrograph v6.1.4](http://franz.com/agraph/support/documentation/current/server-installation.html) (as of 2017/01/07)
2. require Java version 1.7 or 1.8
3. when installing allegrograph, it prompts to ask to set up a username and password, please set username and password both as "admin". If you set up a different username and password, please go to AllegroGraphCache/src/com/pnnl/shyure/main/Main.java and change variable password and username accordingly. 
4. after the initial configuration during the setup process, it will list the command line to start and stop AllegroGraph server. Make sure you save or remember it.
5. unzip AllegroGraph/files/LUBM45.nt.zip at the same directory. This unzips the streaming dataset "LUBM45.nt" which is 1GB in size. 
6. install ant

# Run:
1. cd into AllegroGraphCache folder
2. start AllegroGraph server
3. in your terminal, type ant, then the program will run
4. AllegroGraphCache/files/agFEFOdisk will contain all the benchmark files generated. 

# Disclaimer:
The code was originally implemented with AllegroGraph V5.0.2 and Java 7, and might malfunction when working with higher versions of both Java and AllegroGraph. The folder "agraph5.0.2 lib" contains the libary of AllegroGraph V5.0.2. The folder "lib" contains the libary of AllegroGraph V6.1.4. 
The default running library is v6.1.4