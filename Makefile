# test
export GOPATH:=$(shell readlink -f ../../../../)

test:
	echo $(GOPATH)
	go get github.com/smartystreets/goconvey/convey
	#	go install github.com/smartystreets/goconvey/convey
	go build github.com/h4ck3rm1k3/golearn
	go test github.com/h4ck3rm1k3/golearn/base
#-test.run 
