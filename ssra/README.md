# ssra

This folder contains an eclipse-friently source code view. You can open the whole project in eclipse by importing the pom.xml. 

# Pre-request
1. install AllegroGraph v6.1.4 and Stardog v4.2.2
2. start AllegroGraph and Stardog servers
3. configure AllegroGraph and Stardog's username and password. Please refer to AllegroGraphCache and StardogCache folder for details.
4. install maven
5. unzip resource/LUBM45.nt.zip in the same directory. This will generate the streaming dataset named "LUBM45.nt".
6. requires java v1.8

# Install
1. open your eclipse
2. import the project via provided pom.xml
3. in your buld path, please include all the libs in the lib folder
4. ssra/src/main/java/launcher/Launcher.java is the main class
5. in eclipse, configure your run argument to be either "stardog" or "agraph"
6. run code in eclipse.  

# Disclaimer
I tried to include AllegroGraph's java client libraries in the pom.xml. However, I didn't find any public maven repository that hosts the latest AllegroGraph java client. I have imported all the AllegroGraph's libs into my local maven repository manually, however, the code doesn't run by complaining the query parsing error. So I can only use "build path" to include all the allegrograph's lib in eclipse, which obviously prevents me from packaging the whole program for distribution.
