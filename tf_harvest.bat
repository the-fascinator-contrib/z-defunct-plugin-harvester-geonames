echo off

REM This script runs a harvest

REM Make sure you check to make sure tf_env.bat reflects
REM your config.

call "C:\Program Files\the-fascinator\code\tf_env.bat"

REM IF "%1"=="" goto USAGE

mvn -Dhttp.nonProxyHosts=localhost -DXmx1024m exec:java
goto :EOF

:USAGE
echo Usage: tf_harvest