#!/usr/bin/env bash

# obtain token to communicte with prometheus server
# execute this script from /hack 
# firstly user need to have access (via oc) to the given cluster (in this case it is AWS stage data plane cluster).

# create kubernetes resources (RBACs, namespace) necessary to create token
oc apply -f stage-cluster-rbac-resources.yaml

# obtain secret
SECRET_NAME=$(oc get sa e2e-query-sa --namespace e2e-querry-ns -o json | jq -r '.secrets[0].name')
echo "obtained SECRET_NAME:"
echo ${SECRET_NAME}
echo "---"
echo

# # Extract the value of openshift.io/token-secret.name which holds name of actual secret token
TOKEN_SECRET_NAME=$(oc get secret ${SECRET_NAME} -n e2e-querry-ns -o json | jq -r '.metadata.annotations."openshift.io/token-secret.name"')
echo "obtained SECRET_NAME (token):"
echo ${TOKEN_SECRET_NAME}
echo "---"
echo

# obtain token data
TOKEN_DATA=$(oc get secret ${TOKEN_SECRET_NAME} --namespace e2e-querry-ns  -o jsonpath='{.data.token}')
echo "obtained TOKEN_DATA:"
echo "${TOKEN_DATA}"
echo "---"
echo

# decode token from base-64
TOKEN=$(echo "${TOKEN_DATA}" | base64 -d)
echo "obtained TOKEN (decoded) data:"
echo "${TOKEN}"
echo "---"
echo
