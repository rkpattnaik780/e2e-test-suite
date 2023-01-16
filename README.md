# E2E Test Suite

TestNG based Java test suite focused on e2e testing application services running in the cloud.

## Requirements

* java jdk 11
* maven >= 3.3.1
* kcat (only to run Kcat tests)

## Build and check checkstyle

```
mvn install -DskipTests
```

## Executing tests

### Running full test suite

```
mvn test
```

### Running single test

```
mvn verify -Dit.test=ClassName
```

### Running subset tests defined by maven profile

```
mvn verify -Psmoke
```

## Writing tests

* Test method must be annotated with annotation `@Test`.
* Every new test class must extend `TestBase`
* Environment variables are stored in static `Environment` class
* Every new and variable used in testsuite must be added to table or env variables
* The test class must be included in one of the TestNG suites in `suites/`

## Environment Variables

This environment variables are used to configure tests behaviour, the ENVs marked with the double asterisk (**) are
required to run all tests. But not all tests require all ENVs, if you want to run a single test you can check ahead the
required variables in the test sourcecode javadoc.

Environment variables can also be configured in the [config.json](#config-file) file.

| Name                               | Description                                                                                                                                       | Default value                              |
|------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------|
| `CONFIG_FILE`                      | relative or absolute path to the [config.json](#config-file)                                                                                      | $(pwd)/config.json                         |
| `LOG_DIR`                          | path where the test suite will store additional collected logs and artifacts                                                                      | $(pwd)/target/logs                         |
| `PRIMARY_USERNAME`                 | ** https://sso.redhat.com username with quota to provision the tested service (See [Create test users](#create-test-users) if you don't have one) |                                            |
| `PRIMARY_PASSWORD`                 | ** primary user password                                                                                                                          |                                            |
| `SECONDARY_USERNAME`               | ** https://sso.redhat.com username in the same organization as the primary user                                                                   |                                            |
| `SECONDARY_PASSWORD`               | ** secondary user password                                                                                                                        |                                            |
| `ALIEN_USERNAME`                   | ** https://sso.redhat.com username in a different organization respect the primary user                                                           |                                            |
| `ALIEN_PASSWORD`                   | ** alien user password                                                                                                                            |                                            |
| `OPENSHIFT_API_URI`                | the base URI for the application services mgmt APIs (See [Test Environments](#test-environments))                                                 | `https://api.stage.openshift.com`          |
| `REDHAT_SSO_URI`                   | users authentication endpoint for application services mgmt APIs                                                                                  | `https://sso.redhat.com`                   |
| `REDHAT_SSO_LOGIN_FORM_ID`         | HTML `id` value of the login `<form>` the SSO application will present after redirect                                                             | `#rh-password-verification-form`           |
| `OPENSHIFT_IDENTITY_URI`           | users authentication endpoint for application services instances APIs (See [Test Environments](#test-environments))                               | `https://identity.api.stage.openshift.com` |
| `OPENSHIFT_IDENTITY_LOGIN_FORM_ID` | HTML `id` value of the login `<form>` the SSO application will present after redirect                                                             | `#rh-password-verification-form`           |
| `DEV_CLUSTER_SERVER`               | ** the API server URI of a OpenShift cluster with the binding operator installed                                                                  |                                            |
| `DEV_CLUSTER_TOKEN`                | ** the cluster user or service account token                                                                                                      |                                            |
| `DEV_CLUSTER_NAMESPACE`            | the namespace where to create test resources (See [Create test namespace on the dev cluster](#create-test-namespace-on-the-dev-cluster))          | `mk-e2e-tests`                             |
| `CLI_VERSION`                      | the CLI version to download from the app-services-cli repo                                                                                        | `latest`                                   |
| `CLI_PLATFORM`                     | windows/macOS/linux                                                                                                                               | `auto-detect`                              |
| `CLI_ARCH`                         | the CLI arch and os to download from the app-services-cli repo                                                                                    | `amd64`                                    |
| `CLI_EXCLUDE_VERSIONS`             | a regex that if match will exclude the the CLI versions matching it while looking for the latest CLI release                                      | `alpha`                                    |
| `GITHUB_TOKEN`                     | the github token used to download the CLI if needed                                                                                               |                                            |
| `LAUNCH_KEY`                       | A string key used to identify the current configuration and owner which is used to generate unique name and identify the launch                   | `change-me`                                |
| `SKIP_TEARDOWN`                    | Skip the whole test teardown in most tests, although some of them will need top re-enable it to succeed                                           | `false`                                    |
| `SKIP_KAFKA_TEARDOWN`              | Skip only the Kafka instance cleanup teardown in the tests that don't require a new instance for each run to speed the local development          | `false`                                    |
| `DEFAULT_KAFKA_REGION`             | Change the default region where kafka instances will be provisioned if the test suite doesn't decide otherwise                                    | `us-east-1`                                |
| `KAFKA_INSECURE_TLS`               | Boolean value to indicate whether the Kafka and Admin REST API TLS is insecure (for self-signed certificates)                                     | `false`                                    |
| `KAFKA_INSTANCE_API_TEMPLATE`      | URL template for the Kafka Admin REST API. May be used to specify plain-text HTTP or an alternate port                                            | `https://admin-server-%s/rest`             |

## Config File

All [Environment variables](#environment-variables) can also be configured in a JSON file, by default they are loaded
from the `config.json` file located in the project root, but an alternative JSON file can be configured used the the
`CONFIG_FILE` env.

Config file example:

```json
{
  "PRIMARY_USERNAME": "my-primary-user",
  "PRIMARY_PASSWORD": "password",
  "SECONDARY_USERNAME": "my-secondary-user",
  "SECONDARY_PASSWORD": "password",
  "ALIEN_USERNAME": "my-alien-user",
  "ALIEN_PASSWORD": "password",
  "DEV_CLUSTER_SERVER": "https://api.my.cluster:6443",
  "DEV_CLUSTER_TOKEN": "token",
  "DEV_CLUSTER_NAMESPACE": "a-custom-namespace-if-local",
  "LAUNCH_KEY": "my-username-if-local"
}
```

You can create multiple local `*.config.json` file that will be ignored by git (ex: `prod.config.json`) and you can
easily switch between them in IntelliJ using the [EnvFile](https://plugins.jetbrains.com/plugin/7861-envfile) plugin and
setting the `CONFIG_FILE` env in the `.env` file in the project root.

## Test Environments

The default targeted environment is the application services stage env.

#### Stage

| Env                      | Value                                      |
|--------------------------|--------------------------------------------|
| `OPENSHIFT_API_URI`      | `https://api.stage.openshift.com`          |
| `OPENSHIFT_IDENTITY_URI` | `https://identity.api.stage.openshift.com` |

#### Production

| Env                      | Value                                |
|--------------------------|--------------------------------------|
| `OPENSHIFT_API_URI`      | `https://api.openshift.com`          |
| `OPENSHIFT_IDENTITY_URI` | `https://identity.api.openshift.com` |

## Profiles

| Name        | Description                                                      |
|-------------|------------------------------------------------------------------|
| default     | run kafka, registry, devexp and quickstarts test suites          |
| sandbox     | run the sandbox test suite to test the openshift sandbox cluster |
| quickstarts | run the cucumber quickstarts test suite                          |

## Report to Prometheus

Tests can report metrics to Prometheus to analyze or monitor behaviours, like the number and frequency of failed API
requests that are retried automatically. A push gateway is required to send the metrics to prometheus, and it is
configured with the following ENVs:

| Name                      | Description                 | Default value         |
|---------------------------|-----------------------------|-----------------------|
| `PROMETHEUS_PUSH_GATEWAY` | Prometheus Push Gateway URL | `https://example.com` |

## Short guides

### Create test users

The test users needs to be manually created before running the test suites, but not all users are required for all
tests.

#### Organization

1. Go to https://cloud.redhat.com/ and click on **Create an account**
2. You can choose _Personal_ or _Corporate_
3. Fill all the required and optional data fields and create the account

> This will automatically create a new Organization where the user is the organization owner.

#### Primary user

1. Go to https://www.redhat.com/wapps/ugc/protected/usermgt/userList.html and login with the organization owner.
2. Click on **Add New User** and fill all the required and optional data fields for the primary user

> By default, the account will be able to create evaluation instances which will be enough to run single tests

#### Secondary user

2. Go to https://www.redhat.com/wapps/ugc/protected/usermgt/userList.html and login with the organization owner.
3. Click on **Add New User** and fill all the required and optional data fields for the secondary user

> The secondary user is exactly like the primary user, but by default it wouldn't have access to instances created by
> the primary user

#### Alien user

1. Go to https://cloud.redhat.com/ and click on **Create an account**
2. You can choose _Personal_ or _Corporate_
3. Fill all the required and optional data fields for the alien user and create the account

> This will create the Alien user in a new organization

#### Diff org user

1. Go to https://cloud.redhat.com/ and click on **Create an account**
2. You can choose _Personal_ or _Corporate_
3. Fill all the required and optional data fields for the alien user and create the account
4. Get the org id for the user created
   ````
    ocm get /api/accounts_mgmt/v1/organizations/$(ocm account orgs --columns ID | tail -n1) | jq -r .external_id
    ```
5. Create a MR on `ocm-resources` repo to add quota for that org [example MR](https://gitlab.cee.redhat.com/service/ocm-resources/-/merge_requests/2986)

> This will create the Diff org user in a new organization and add quota to create standard Kafka Instances

### Create test namespace on the dev cluster

1. Login to the dev cluster as kubeadmin
    ```
    oc login --token=KUBE_ADMIN_TOKEN --server=DEV_CLUSTER_SERVER
    ```
2. Execute the `./hack/bootstrap-mk-e2e-tests-namespace.sh` script
    ```
   ./hack/bootstrap-mk-e2e-tests-namespace.sh
    ```
3. Update
   the [staging Vault](https://vault.devshift.net/ui/vault/secrets/managed-services-ci/show/mk-e2e-test-suite/staging)
   with the new dev cluster token

### Recreate the service account for the sandbox cluster

1. Login to the sandbox cluster with the sandbox user
    ```
    oc login --token=KUBE_ADMIN_TOKEN --server=DEV_CLUSTER_SERVER
    ```
2. Execute the `./hack/bootstrap-mk-e2e-tests-namespace.sh` script
    ```
   SANDBOX=true NAMESPACE=THE_SANDBOX_NAMESPACE ./hack/bootstrap-mk-e2e-tests-namespace.sh
    ```
3. Update
   the [sandbox Vault](https://vault.devshift.net/ui/vault/secrets/managed-services-ci/show/mk-e2e-test-suite/sandbox)
   with the new dev cluster token

### Update the rhoas-model dependency

Pick the latest available version folder from:
https://github.com/redhat-developer/app-services-operator/tree/main/olm/olm-catalog/rhoas-operator

and update the property `rhoas-model.version` in `pom.xml`.

## Maintainers

* David Kornel <kornys@outlook.com>
* Henrich Zrncik <hzrncik@redhat.com>
* Tony Davidson <tdavidso@redhat.com>
