# Linked Data VocabTerms Lookup Service
Vocabserver is a tool to make it easier to use existing vocabularies within your organization. 

The aim of this project is to provide a service that allows 
 - to add selected existing vocabularies and glossaries by reference (ingest & sync) 
 - allow adding translations to the vocabularies where necessary
 - Access the harvested content to terms via a fast lookup API solution (full-text search on labels and filters on categories)
 - the service is integratable in various applications via a [reusable lookup widgets](https://github.com/vlizBE/vocabserver-webcomponent)


## Code
This repository hosts the code and configurations for the backend of vocabsearch.

Related repositories are:

- [vocabserver-frontend](https://github.com/vlizBE/vocabserver-frontend/) hosting the code of the admin frontend
- [vocabserver-webcomponent](https://github.com/vlizBE/vocabserver-webcomponent) hosting the code of a webcomponent that can connect with this backend

A demo is currently available on https://vocabsearch.redpencil.io/ . 


## Setting up the backend

### Prerequisites
The backend was tested on ubuntu 20.04 and requires both docker and docker-compose to be installed.

This can be done with the following script:
```sh
# update system
apt update > /dev/null &&  apt upgrade -y

# docker
apt install -y apt-transport-https ca-certificates curl software-properties-common
curl -fsSL https://download.docker.com/linux/ubuntu/gpg |  apt-key add -
add-apt-repository \
     "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
   $(lsb_release -cs) \
   stable"
apt update > /dev/null
apt -y install docker-ce
docker run --rm hello-world

# docker-compose
curl -L https://github.com/docker/compose/releases/download/1.28.2/docker-compose-$(uname -s)-$(uname -m) -o /usr/local/bin/docker-compose
curl -L https://raw.githubusercontent.com/docker/compose/1.28.2/contrib/completion/bash/docker-compose -o /etc/bash_completion.d/docker-compose
chmod +x /usr/local/bin/docker-compose

# dr and drc auto completion
echo "complete -F _docker_compose drc" >> /etc/bash_completion.d/docker-compose
echo "complete -F _docker dr" >> /usr/share/bash-completion/completions/docker
```

### installation of the backend
```sh
  # clone the repository
  git clone https://github.com/vlizBE/vocabserver-app.git
  # start the backend
  cd vocabsearch-app
  docker-compose up
```

### configuration and reverse proxy
By default the backend will expose no ports on the docker host, if you want you can add a port mapping to the identifer to expose the backend service and admin interface. See the [docker-compose.dev.yml](https://github.com/vlizBE/vocabserver-app/blob/415a7d4ded4b391f6fc50a07b24230b9b9a19f70/docker-compose.dev.yml#L6) file for an example on how to do this.

For production use it's recommend to place the backend behind a reverse proxy, so only the search API is exposed publicly.
Assuming the following port mapping has been added to the identifier:
```yml
ports:
 - 127.0.0.1:8888:80
```

You could use the following nginx config to serve as a reverse proxy:

```
server {
    server_name my.vocabsearch.domain {
      location /concepts/search {
        proxy_pass http://localhost:8888;  
      }
}
```

## Deployment

Deploying the backend can be done using docker-compose, to do so execute `docker-compose up -d` to deploy the backend deamonized.
When the backend starts up, it takes a few minutes to be responsive, so it's advised to wait a few when the frontend displays an empty page right after restarting or deploying.

## Which logs to monitor

### Vocab-fetch

This service is responsible for downloading a dataset and storing the metadata about it.
To see its logs use `docker-compose logs -f vocab-fetch` to see them updating live or `docker-compose logs vocab-fetch | less` to search in the logs until now.

### Content-unification

This service is responsible for running the configured unification.
To see its logs use `docker-compose logs -f content-unification` to see them updating live or `docker-compose logs content-unification | less` to search in the logs until now.
Note: running the unification might take a while, so don't worry if the job is busy for a long time.

### Ldes-consumer-manager

This service is responsible for creating the containers to download and update LDES datasources.
Its logs don't tell much, since it only creates other containers, these containers are linked to the `ldes-consumers` network.
To find all containers in that network `docker network inspect ldes-consumers` can be used.

