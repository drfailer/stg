.PHONY: dgemm run test release

dgemm:
	odin build . -debug

run:
	odin run . -debug

test:
	odin test . -debug

release:
	odin build . -o:speed
