@echo off

REM built the docker image
docker build -f docker\linux\amd64\Dockerfile -t samplemonitormodule:0.0.1 src/

REM Make the build directories
mkdir tmp
mkdir build
cd tmp
mkdir samplemonitormoduleSoftwarePackage

REM Exporting the docker image
docker save --output samplemonitormoduleSoftwarePackage/samplemonitormodule.tar samplemonitormodule

REM Building the TAR.GZ File
tar -czvf samplemonitormodule.tar.gz samplemonitormoduleSoftwarePackage

REM Moving the prepared OTA package to the build folder
move samplemonitormodule.tar.gz ../build/samplemonitor.tar.gz

REM Delete the temp folder
cd ..
rmdir /S /Q tmp
