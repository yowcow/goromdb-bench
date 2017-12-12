all: dep goromdb-bench

dep:
	dep ensure

goromdb-bench: main.go
	go build
