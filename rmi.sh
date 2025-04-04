#! /bin/zsh
version=${1:-release}

docker rmi chromatices/scrapping_scheduler:${version}