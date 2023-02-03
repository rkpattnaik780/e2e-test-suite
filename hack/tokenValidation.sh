#!/bin/bash

# script will try to use each offline token present in .json file, informing user about token validity and username each token is bind to
# arg: name of .json file which holds OFFLINE_TOKEN envs
# example run ./hack/tokenValidation.sh x.config.json


# function to prettily print mapping of token_env to username : e.g.: PRIMARY_OFFLINE_TOKEN: -> mk-test-e2e-primary
function prettifyOutput {
  local username_arg=$1
  local token_env_arg=$2

  tabs=$((7 - $(echo -n "$token_env_arg" | awk '{print int(length($0)/8)}')))
  line="$token_env_arg: $(printf '\t%.0s' $(seq 1 $tabs)) -> $username_arg"
  echo -e "$line"
}


# script will validate that provided offline tokens are not expired
if [ $# -eq 0 ]
then
  echo "Error: No file provided"
  exit 1
fi

file=$1
echo "Environment variables loaded from $file"

if [ ! -f "$file" ]
then
  echo "Error: File $file does not exist"
  exit 1
fi

# parse <something>.config.json file
json_file_input=$(cat "$file")

# extract all env. variables
keys=$(echo "$json_file_input" | jq -r 'keys[]')
# for each env variable
echo "TOKEN_ENV var that holds token, which belongs to user  -> with username"
for key in $keys
do
  # read value of given key (key,value)
  value=$(echo "$json_file_input" | jq -r ".$key")

  # if env variable does not ends with "_OFFLINE_TOKEN continue with next env var"
  if [[ ! "$key" == *"_OFFLINE_TOKEN" ]]
  then
    continue
  fi

  #    perform ocm
  ocm login --token "$value"
  if [ $? -ne 0 ]; then
    echo "Error: while performing ocm login ($key) return code $?"
    continue
  fi

  # get json response from whoami command containing username
  account_JSON=$(ocm whoami)
  username=$(echo "$account_JSON" | jq -r '.username')

  # prettier logging of mapped env_var_name to username
  prettifyOutput "$username" "$key"
done

