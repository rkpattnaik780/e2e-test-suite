#!/usr/bin/env bash

# obtain token to communicte with prometheus server
# execute this script from /hack 
# firstly user need to have access (via oc) to the given cluster (in this case it is stage). 

# create kubernetes resources (RBACs, namespace) necessary to create token
oc apply -f stage-cluster-rbac-resources.yaml

# obtain secret
SECRET_NAME=$(oc get sa e2e-query-sa --namespace e2e-querry-ns -o json | jq '.secrets[] | select(.name | contains("token")) | .name')
SECRET_NAME=$(sed -e 's/^"//' -e 's/"$//' <<< ${SECRET_NAME})
echo
echo "obtained SECRET_NAME:"
echo
echo ${SECRET_NAME}
echo "---"

# obtain token data
TOKEN_DATA=$(oc get secret ${SECRET_NAME} --namespace prom-query-ns  -o jsonpath='{.data.token}')
echo
echo "obtained TOKEN_DATA:"
echo
echo "${TOKEN_DATA}"
echo "---"

# decode token from base-64
TOKEN=$(echo "${TOKEN_DATA}" | base64 -d)
echo
echo "obtained TOKEN itself:"
echo
echo "${TOKEN}"
echo "---"
