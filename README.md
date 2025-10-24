# wallaby2caom2
Application to generate a CAOM2 observation from CIRADA (https://cirada.ca/) Wallaby files.

# How To Run Wallaby Testing

In an empty directory:

1. This is the working directory, so it should probably have some space.

1. In the `main` branch of this repository, find the file `Dockerfile`. In the
`scripts` and `config` directories respectively, find the files
`docker-entrypoint.sh`, and `config.yml`. Copy these files to the working directory.

   ```
   wget https://raw.github.com/opencadc-metadata-curation/wallaby2caom2/main/Dockerfile
   wget https://raw.github.com/opencadc-metadata-curation/wallaby2caom2/main/scripts/docker-entrypoint.sh
   wget https://raw.github.com/opencadc-metadata-curation/wallaby2caom2/main/config/config.yml
   ```

1. Make `docker-entrypoint.sh` executable.

1. `config.yml` is configuration information for the ingestion. It will work with
the files named and described here. For a complete description of its
content, see
https://github.com/opencadc/collection2caom2/wiki/config.yml.

1. The ways to tell this tool the work to be done:

   1. provide a file containing the list of file ids to process, one file id
   per line, and the config.yml file containing the entries 'use_local_files'
   set to False, and 'task_types' set to -ingest -modify. The 'todo'
   file may provided in one of two ways:
      1. named 'todo.txt' in this directory, as specified in config.yml, or
      1. as the fully-qualified name with the --todo parameter

   1. provide the files to be processed in the working directory, and the
   config.yml file containing the entries 'use_local_files' set to True,
   and 'task_types' set to -store -ingest -modify.
      1. The store task does not have to be present, unless the files on disk
      are newer than the same files at CADC.

   1. provide the files to be processed in the working directory, and the
   config.yml file containing the entries 'use_local_files' set to True,
   and 'task_types' set to -scrape.
      1. This configuration will not attempt to write files or CAOM2 records
      to CADC. It is a good way to craft the content of the CAOM2 record without
      continually updating database content.

1. To build the container image, run this:

   ```
   docker build -f Dockerfile -t wallaby_run_cli ./
   ```

1. In the working directory, place a CADC proxy certificate. The Docker image can be used to create a
proxy certificate as follows. You will be prompted for the password for your CADC user:

   ```
   user@dockerhost:<cwd># docker run --rm -ti -v ${PWD}:/usr/src/app --user $(id -u):$(id -g) -e HOME=/usr/src/app --name wallably_run_cli wallaby_run cadc-get-cert --days-valid 10 --cert-filename /usr/src/app/cadcproxy.pem -u <your CADC username>
   ```

1. To run the application:

   ```
   user@dockerhost:<cwd># docker run --rm -ti -v <cwd>:/usr/src/app --user $(id -u):$(id -g) -e HOME=/usr/src/app --name wallaby_run_cli wallaby_run_cli wallaby_run
   ```

1. To edit and test the application from inside a container:

   ```
   user@dockerhost:<cwd># git clone https://github.com/opencadc-metadata-curation/wallaby2caom2.git
   user@dockerhost:<cwd># docker run --rm -ti -v <cwd>:/usr/src/app --user $(id -u):$(id -g) -e HOME=/usr/src/app --name wallaby_run_cli wallaby_run_cli /bin/bash
   root@53bef30d8af3:/usr/src/app# pip install -e ./wallaby2caom2
   root@53bef30d8af3:/usr/src/app# pip install mock pytest
   root@53bef30d8af3:/usr/src/app# cd wallaby2caom2/wallaby2caom2/tests
   root@53bef30d8af3:/usr/src/app# pytest
   ```

1. For some instructions that might be helpful on using containers, see:
https://github.com/opencadc/collection2caom2/wiki/Docker-and-Collections

