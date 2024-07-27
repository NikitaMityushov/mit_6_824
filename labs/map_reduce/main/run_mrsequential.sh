go build -buildmode=plugin ../mrapps/wc.go
go run mrsequential.go wc.so pg*.txt
rm mr-out-*
echo done!