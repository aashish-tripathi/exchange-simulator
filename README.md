## Welcome to GitHub Pages

#  Install third party dependency into your local repo

1. mvn install:install-file -Dfile=third-party-jars/jms-2.0.jar -DgroupId=tibco-ems -DartifactId=jms-2.0 -Dversion=2.0 -Dpackaging=jar -DgeneratePom=true
2. mvn install:install-file -Dfile=third-party-jars/tibemsd_sec.jar -DgroupId=tibco-ems -DartifactId=tibemsd_sec -Dversion=2.0 -Dpackaging=jar -DgeneratePom=true
3. mvn install:install-file -Dfile=third-party-jars/tibjms.jar -DgroupId=tibco-ems -DartifactId=tibjms -Dversion=2.0 -Dpackaging=jar -DgeneratePom=true
4. mvn install:install-file -Dfile=third-party-jars/tibjmsadmin.jar -DgroupId=tibco-ems -DartifactId=tibjmsadmin -Dversion=2.0 -Dpackaging=jar -DgeneratePom=true
5. mvn install:install-file -Dfile=third-party-jars/tibjmsapps.jar -DgroupId=tibco-ems -DartifactId=tibjmsapps -Dversion=2.0 -Dpackaging=jar -DgeneratePom=true
6. mvn install:install-file -Dfile=third-party-jars/tibrvjms.jar -DgroupId=tibco-ems -DartifactId=tibrvjms -Dversion=2.0 -Dpackaging=jar -DgeneratePom=true
7. mvn install:install-file -Dfile=third-party-jars/schema-1.0-SNAPSHOT.jar -DgroupId=tibco-ems -DartifactId=schema-1.0-SNAPSHOT -Dversion=2.0 -Dpackaging=jar -DgeneratePom=true


