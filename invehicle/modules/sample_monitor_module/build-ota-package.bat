REM built the docker image
docker build -t samplemonitormodule:0.0.1 .

REM Exporting the docker image
docker save --output samplemonitormoduleSoftwarePackage/samplemonitormodule.tar samplemonitormodule

REM Building the TAR.GZ File
if not exists build\NUL mkdir build
tar -czvf tripandmaneuvermodule.tar.gz tripandmaneuverSoftwarePackage

pause