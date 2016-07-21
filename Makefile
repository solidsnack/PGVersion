all:
	swift build -Xcc -I/usr/local/include -Xlinker -L/usr/local/lib

clean:
	rm -rf .build/
