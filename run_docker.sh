#!/usr/bin/env bash
docker build -t dirtrider/hh_parser .
docker run -d -t \
--name hh_parser \
--restart unless-stopped \
--volume ${PWD}/hh_parser/cfgs:/hh_parser_v2/hh_parser/cfgs \
--volume ${PWD}/hh_parser/logs:/hh_parser_v2/hh_parser/logs \
--volume ${PWD}/hh_parser/temp:/hh_parser_v2/hh_parser/temp \
dirtrider/hh_parser:latest