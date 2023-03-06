#!/usr/bin/env bash

# Create the mk-e2e-tests namespace in the current log-in cluster (oc login ...)
# and the mk-e2e-tests-sa service account with admin privileges only in the mk-e2e-tests namespace.
#
# The mk-e2e-tests namespace and mk-e2e-tests-sa service account are used for the E2E tests that requires
# ane external OpenShift cluster, like the TestBindingOperator.
#
# The script could also be used to create a custom namespace for local test development
# NAMESPACE=my-e2e-tests ./hack/bootstrap-mk-e2e-tests-namespace.sh

# Inspired by https://gist.github.com/b1zzu/ccd9ef553d546a2009eca21ab45db97a
set -eEu -o pipefail
# shellcheck disable=SC2154
trap 's=$?; echo "$0: error on $0:$LINENO"; exit $s' ERR

NAMESPACE_NAME=${NAMESPACE:-"mk-e2e-tests"}
SERVICE_ACCOUNT_NAME=${NAMESPACE_NAME}-sa
SANDBOX=${SANDBOX:-"false"}

if [[ ${SANDBOX} != "true" ]]; then
  # create namespace and service account
  oc process -f hack/namespace-template.yaml -p NAME="${NAMESPACE_NAME}" |
    oc apply -f -
else

  # create the service account for the sandbox env
  oc process -f hack/sandbox-template.yaml -p NAME="${NAMESPACE_NAME}" |
    oc apply -f -
fi


SECRET_NAME=$(oc get serviceaccount "${SERVICE_ACCOUNT_NAME}" \
  --namespace "${NAMESPACE_NAME}" \
  -o json | jq -r '.secrets[0].name')

# Extract the value of openshift.io/token-secret.name which holds name of actual secret token
TOKEN_SECRET_NAME=$(oc get secret ${SECRET_NAME} -n ${NAMESPACE_NAME} -o json | jq -r '.metadata.annotations."openshift.io/token-secret.name"')

TOKEN_DATA=$(oc get secret "${TOKEN_SECRET_NAME}" \
  --namespace "${NAMESPACE_NAME}" \
  -o jsonpath='{.data.token}')

OPENSHIFT_API_URL=$(oc config view --minify -o jsonpath='{.clusters[*].cluster.server}')
TOKEN=$(echo "${TOKEN_DATA}" | base64 -d)

echo
echo "Token:"
echo
echo "${TOKEN}"
echo
echo "API Server:"
echo
echo "${OPENSHIFT_API_URL}"
echo
echo "Login:"
echo
echo "oc login --server '${OPENSHIFT_API_URL}' --token '${TOKEN}'"
echo
