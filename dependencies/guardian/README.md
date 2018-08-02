Package dependencies to jar file
1. Create a maven project;
2. Override pom.xml of new project by this one;
3. Copy settings.xml to ~/.m2/;
4. Run 'mvn clean package';
5. Rename guardian-xxx-with-dependencies.jar to guardian-client-2.0-transwarp-5.2.0-SNAPSHOT.jar, and copy /usr/local/lib/.