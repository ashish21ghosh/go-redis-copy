# go-redis-copy
Copy redis data from one instance to another

## Usage
1. Clone this repository and go inside the folder.
2. Build with golang `go mod vendor && go build`
3. Run command `./go-redis-copy --src localhost:6379 --dest localhost:6389 --key hash_data`
