#!/bin/bash
cd "$( dirname "${BASH_SOURCE[0]}" )"/..
docker-compose exec temporal-admin-tools tctl $@
