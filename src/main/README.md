
https://pdos.csail.mit.edu/6.824/labs/lab-mr.html


# Running MR Sequential

$ cd ~/6.5840
$ cd src/main
$ go build -buildmode=plugin ../mrapps/wc.go
$ rm mr-out*
$ go run mrsequential.go wc.so pg*.txt
$ more mr-out-0

