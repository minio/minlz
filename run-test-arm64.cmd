@echo off
REM Build ARM64 test binary on host and run in Docker container
REM Usage: run-test-arm64.cmd [test flags]
REM Example: run-test-arm64.cmd -test.run=TestSrcMarginBoundary -test.v

echo Building ARM64 test binary...
set GOOS=linux
set GOARCH=arm64
go test -c -o minlz_arm64.test

if %ERRORLEVEL% neq 0 (
    echo Build failed!
    SET GOOS=windows
    SET GOARCH=amd64
    exit /b %ERRORLEVEL%
)

SET GOOS=windows
SET GOARCH=amd64

echo Running tests in ARM64 container...
docker run --rm --platform linux/arm64 -e GOGC=20 -e GOMEMLIMIT=2GiB -v "%cd%:/work" -w /work arm64v8/alpine ./minlz_arm64.test %*

