#!/bin/bash

# Duplicates an existing CSV file and regenerates the first column from 1 to the number of lines
#
# Params:
#      $1   : CSV file to duplicate
#      $2   : (optional) output duplicated file, '_2x' is appended to file if missing
#
# Examples
#
#   $ dupcsv data/people_1K.csv data/people_2000.csv
#
#   $ dupcsv data/people_2K.csv
#   (generates data/people_2K.csv_2x)


file=${1:?}
target_file=${2:-"${file}_2x"}

header() {
    cat <<EOF
id,first_name,last_name,email,gender
EOF
}

duplicate_data() {
    awk -v FS="," -v OFS="," 'BEGIN {c=0; getline} {$1=++c; print $0; $1=++c; print $0}' < ${file}
}

header > ${target_file}
duplicate_data >> ${target_file}
