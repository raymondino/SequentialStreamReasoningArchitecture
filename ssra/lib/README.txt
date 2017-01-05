These are dependencies for AllegroGraph based triplestore. 

I thought I could add them manually into the maven repository, but it is too tedious..
I gave up by using Eclipse to add the lib into the build path. 
So I suggest you to add the all the jar files into build path if you want to compile it. 


------------------below is the legacy--------------------------------------

Since I didn't find a global maven repository that hosts allegrograph-java-client v6.1.4, I need to install them manually into the local maven repository.

What you need to do if the entire program doesn't run because of "missing allegrograph dependencies": 

get into ssr/lib/allegrograph-6.1.4 directory, then open your terminal (make sure you have maven installed and configured):

mvn install:install-file -Dfile=agraph-6.1.4.jar -DgroupId=com.franz -DartifactId=agraph-java-client -Dversion=6.1.4 -Dpackaging=jar

mvn install:install-file -Dfile=./jena/jena-core-2.11.1.jar -DgroupId=com.franz -DartifactId=jena-core -Dversion=2.11.1 -Dpackaging=jar

mvn install:install-file -Dfile=./jena/jena-arq-2.11.1.jar -DgroupId=com.franz -DartifactId=jena-arq -Dversion=2.11.1 -Dpackaging=jar


Then in your pom.xml add the following (which has already been already added by me if you downloaded this code): 

<dependencies>
   ...
    <dependency>
  	  <groupId>com.franz</groupId>
      <artifactId>agraph-java-client</artifactId>
      <version>6.1.4</version>
      <type>jar</type>
    </dependency>
    <dependency>
  	  <groupId>com.franz</groupId>
      <artifactId>jena-arq</artifactId>
      <version>2.11.1</version>
      <type>jar</type>
    </dependency>
    <dependency>
  	  <groupId>com.franz</groupId>
      <artifactId>jena-core</artifactId>
      <version>2.11.1</version>
      <type>jar</type>
    </dependency>
    ...
 </dependencies>