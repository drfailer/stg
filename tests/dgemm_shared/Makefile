.PHONY: dgemm run test release

dgemm:
	odin build . -debug -define:PROFILER_ENABLED=true

run:
	odin run . -debug -define:PROFILER_ENABLED=true

test:
	odin test . -thread-count:1 -define:PROFILER_ENABLED=false

release:
	odin build . -o:speed -define:PROFILER_ENABLED=true
