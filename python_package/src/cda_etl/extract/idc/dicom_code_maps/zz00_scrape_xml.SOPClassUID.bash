#!/usr/bin/env bash

url_file=SOPClassUID_source_xml.url

target_file=`cat $url_file | sed "s/^.*\///"`

curl -o $target_file `cat $url_file`


