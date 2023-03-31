# Makefile for 3700kvstore

# Set the Go compiler and linker flags
GO := go
GOFLAGS := -ldflags "-s -w"

# Set the name of the executable file
EXECUTABLE := 3700kvstore

.PHONY: all clean

all: $(EXECUTABLE)

# Build the executable
$(EXECUTABLE): 3700kvstore.go
	$(GO) build $(GOFLAGS) -o $(EXECUTABLE) 3700kvstore.go

clean:
	rm -f $(EXECUTABLE)

