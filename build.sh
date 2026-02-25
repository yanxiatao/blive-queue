cd frontend
npm install
npm run build
cd ..
GOOS=windows GOARCH=amd64 CC=x86_64-w64-mingw32-gcc CGO_ENABLED=1 go build --ldflags="-s -w" .