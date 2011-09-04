
  ========== How to build ==========

1. Download and setup the excellent build tool SBT,
   version xsbt-launch-0.6.12.jar or later :
     http://code.google.com/p/simple-build-tool/wiki/Setup

2. Download or clone repository :
     git clone git://github.com/max-l/Squeryl.git

3. Open a shell in the project's root directory, and launch SBT,
   this will fetch the required version of Scala both for
   SBT itself and for Squeryl.

4. Type 'update' to have SBT fetch dependencies, and then
   the compile package commands are available.
   The test-run command will run the test suites against the
   minimalist but very complete H2 database.


  ========== Running in IDEA ========== 

 Import pom.xml.
