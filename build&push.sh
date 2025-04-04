#!/bin/bash
version=${1:-release}

docker build . -t chromatices/scrapping_scheduler:${version}
docker push chromatices/scrapping_scheduler:${version}