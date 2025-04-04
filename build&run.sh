#! /bin/zsh

version=${1:-release}

docker build . -t chromatices/scrapping_scheduler:${version}
docker run --rm -itd --name scrapping_test chromatices/scrapping_scheduler:${version}
