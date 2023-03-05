# CZ4031-Project-1

The aim of this project is to design and implement the storage and indexing components of a database management system which supports record search, insert, and delete operations

## Instructions to run
### Method 1
- Install Golang on your local machine, by following this installation: https://go.dev/doc/install
- Set up goroot and gopath properly. Make sure the project script is in the correct gopath.
 -- For eg: If your goroot is at ~C:\User\Desktop\go\src, make sure the project is in ~C:\User\Desktop\go\src\CZ4031-Project-1
- Run the command: go run main.go

### Method 2
- Download Docker from this link: https://www.docker.com/products/docker-desktop/
- Once your docker is set up, navigate to the root of the project.
- Build the docker image: “docker build -t cz4031_project1 .” Make sure the “.” is included.
- Run your built image: docker run cz4031_project1. The output will be displayed in your terminal.
- Take note that if you changed any part of the code, you will have to rebuild a new docker image. Else, docker will still continue running using the old image you have built.
