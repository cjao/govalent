all: govalent-server

govalent-server:
	go build -o $(BUILD_OUTPUT_DIR)/govalent-server ./server

clean:
	rm govalent-server
