.PHONY: dgemm run test release

dgemm:
	odin build . -debug

run:
	odin run . -debug

test:
	odin test . -thread-count:1

release:
	odin build . -o:speed
