cls 

@ECHO OFF

@REM Clean up .beam files
cd ebin

for %%i in (*.beam) do del %%i

cd..

@REM compile all files
@ECHO ON

for %%i in (src/*.erl) do erlc -o ebin/ "src/%%i"