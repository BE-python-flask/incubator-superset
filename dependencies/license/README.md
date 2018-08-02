Package dependencies to jar file
1. Copy setting.xml to ~/.m2/;
2. Deprecated test cases;
3. Run 'mvn clean package';
4. Rename 'pilot-license-1.0-transwarp-5.2.0-jar-with-dependencies.jar' to 'pilot-license-1.0-transwarp-5.2.0.jar';
5. Copy 'pilot-license-1.0-transwarp-5.2.0.jar' to /usr/local/lib/


When test, need to copy msl-site.xml to /etc/transwarp/conf/, and input valid ZK server.