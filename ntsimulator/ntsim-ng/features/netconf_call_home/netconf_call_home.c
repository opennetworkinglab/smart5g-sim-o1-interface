/*************************************************************************
*
* Copyright 2020 highstreet technologies GmbH and others
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
***************************************************************************/

#define _GNU_SOURCE

#include "netconf_call_home.h"
#include "utils/log_utils.h"
#include "utils/sys_utils.h"
#include "utils/rand_utils.h"
#include "utils/http_client.h"
#include "utils/nts_utils.h"
#include <stdio.h>
#include <assert.h>

#include "core/session.h"
#include "core/framework.h"

#define NETCONF_CALLHOME_ENABLED_SCHEMA_PATH        "/nts-network-function:simulation/network-function/netconf/call-home"
#define NETCONF_CALLHOME_CURL_SEND_PAYLOAD_FORMAT   "{\"odl-netconf-callhome-server:device\":[{\"odl-netconf-callhome-server:unique-id\":\"%s\",\"odl-netconf-callhome-server:ssh-host-key\":\"%s\",\"odl-netconf-callhome-server:credentials\":{\"odl-netconf-callhome-server:username\":\"netconf\",\"odl-netconf-callhome-server:passwords\":[\"netconf\"]}}]}"
#define SDN_CONTROLLER_DETAILS_SCHEMA_PATH          "/nts-network-function:simulation/sdn-controller"

static int create_ssh_callhome_endpoint(sr_session_ctx_t *current_session, struct lyd_node *netconf_node);
static int create_tls_callhome_endpoint(sr_session_ctx_t *current_session, struct lyd_node *netconf_node);
static int send_odl_callhome_configuration(sr_session_ctx_t *current_session);

int netconf_call_home_feature_start(sr_session_ctx_t *current_session) {
    assert(current_session);
    assert_session();

    sr_val_t *value = 0;

    bool callhome_enabled = false;
    int rc = sr_get_item(current_session, NETCONF_CALLHOME_ENABLED_SCHEMA_PATH, 0, &value);
    if(rc == SR_ERR_OK) {
        callhome_enabled = value->data.bool_val;
        sr_free_val(value);
    }

    if(callhome_enabled == false) {
        log_message(2, "NETCONF CallHome is not enabled, not configuring NETCONF Server.\n");
        return NTS_ERR_OK;
    }

    struct lyd_node *netconf_node = 0;
    netconf_node = lyd_new_path(NULL, session_context, "/ietf-netconf-server:netconf-server", 0, 0, 0);
    if(netconf_node == 0) {
        log_error("could not create a new lyd_node");
        return NTS_ERR_FAILED;
    }

    rc = create_ssh_callhome_endpoint(current_session, netconf_node);
    if(rc != NTS_ERR_OK) {
        log_error("could not create SSH CallHome endpoint on the NETCONF Server");
        return NTS_ERR_FAILED;
    }

    rc = create_tls_callhome_endpoint(current_session, netconf_node);
    if(rc != NTS_ERR_OK) {
        log_error("could not create TLS CallHome endpoint on the NETCONF Server");
        return NTS_ERR_FAILED;
    }

    rc = sr_edit_batch(current_session, netconf_node, "merge");
    if(rc != SR_ERR_OK) {
        log_error("could not edit batch on datastore");
        return NTS_ERR_FAILED;
    }

    rc = sr_validate(current_session, "ietf-netconf-server", 0);
    if(rc != SR_ERR_OK) {
        struct ly_err_item *err = ly_err_first(session_context);
        log_error("sr_validate issues on STARTUP: %s", err->msg);
        return NTS_ERR_FAILED;
    }

    rc = sr_apply_changes(current_session, 0, 0);
    if(rc != SR_ERR_OK) {
        log_error("could not apply changes on datastore");
        return NTS_ERR_FAILED;
    }

    lyd_free_withsiblings(netconf_node);

    return NTS_ERR_OK;
}


static int create_ssh_callhome_endpoint(sr_session_ctx_t *current_session, struct lyd_node *netconf_node) {
    assert(current_session);
    assert(netconf_node);

    sr_val_t *value = 0;

    bool callhome_enabled = false;
    int rc = sr_get_item(current_session, NETCONF_CALLHOME_ENABLED_SCHEMA_PATH, 0, &value);
    if(rc == SR_ERR_OK) {
        callhome_enabled = value->data.bool_val;
        sr_free_val(value);
    }

    if(callhome_enabled == false) {
        log_message(2, "NETCONF CallHome is not enabled, not configuring NETCONF Server.\n");
        return NTS_ERR_OK;
    }

    controller_details_t *controller = controller_details_get(current_session);
    if(controller == 0) {
        log_error("controller_details_get failed");
        return NTS_ERR_FAILED;
    }

    char *controller_ip = strdup(controller->ip);
    uint16_t controller_callhome_port = controller->nc_callhome_port;
    controller_details_free(controller);

    if(controller_ip == 0) {
        log_error("strdup failed");
        return NTS_ERR_FAILED;
    }

    char xpath[500];
    sprintf(xpath, "/ietf-netconf-server:netconf-server/call-home/netconf-client[name='default-client']/endpoints/endpoint[name='callhome-ssh']/ssh/tcp-client-parameters/keepalives/idle-time");
    struct lyd_node *rcl = lyd_new_path(netconf_node, 0, xpath, "1", 0, LYD_PATH_OPT_NOPARENTRET);
    if(rcl == 0) {
        log_error("could not created yang path");
        free(controller_ip);
        return NTS_ERR_FAILED;
    }

    sprintf(xpath, "/ietf-netconf-server:netconf-server/call-home/netconf-client[name='default-client']/endpoints/endpoint[name='callhome-ssh']/ssh/tcp-client-parameters/keepalives/max-probes");
    rcl = lyd_new_path(netconf_node, 0, xpath, "10", 0, LYD_PATH_OPT_NOPARENTRET);
    if(rcl == 0) {
        log_error("could not created yang path");
        free(controller_ip);
        return NTS_ERR_FAILED;
    }

    sprintf(xpath, "/ietf-netconf-server:netconf-server/call-home/netconf-client[name='default-client']/endpoints/endpoint[name='callhome-ssh']/ssh/tcp-client-parameters/keepalives/probe-interval");
    rcl = lyd_new_path(netconf_node, 0, xpath, "5", 0, LYD_PATH_OPT_NOPARENTRET);
    if(rcl == 0) {
        log_error("could not created yang path");
        free(controller_ip);
        return NTS_ERR_FAILED;
    }

    sprintf(xpath, "/ietf-netconf-server:netconf-server/call-home/netconf-client[name='default-client']/endpoints/endpoint[name='callhome-ssh']/ssh/tcp-client-parameters/remote-address");
    rcl = lyd_new_path(netconf_node, 0, xpath, controller_ip, 0, LYD_PATH_OPT_NOPARENTRET);
    if(rcl == 0) {
        log_error("could not created yang path");
        free(controller_ip);
        return NTS_ERR_FAILED;
    }
    free(controller_ip);

    sprintf(xpath, "/ietf-netconf-server:netconf-server/call-home/netconf-client[name='default-client']/endpoints/endpoint[name='callhome-ssh']/ssh/tcp-client-parameters/remote-port");
    char port[20];
    sprintf(port, "%d", controller_callhome_port);
    rcl = lyd_new_path(netconf_node, 0, xpath, port, 0, LYD_PATH_OPT_NOPARENTRET);
    if(rcl == 0) {
        log_error("could not created yang path");
        return NTS_ERR_FAILED;
    }

    sprintf(xpath, "/ietf-netconf-server:netconf-server/call-home/netconf-client[name='default-client']/endpoints/endpoint[name='callhome-ssh']/ssh/ssh-server-parameters/server-identity/host-key[name='default-key']/public-key/keystore-reference");
    rcl = lyd_new_path(netconf_node, 0, xpath, KS_KEY_NAME, 0, LYD_PATH_OPT_NOPARENTRET);
    if(rcl == 0) {
        log_error("could not created yang path");
        return NTS_ERR_FAILED;
    }

    sprintf(xpath, "/ietf-netconf-server:netconf-server/call-home/netconf-client[name='default-client']/endpoints/endpoint[name='callhome-ssh']/ssh/ssh-server-parameters/client-authentication/supported-authentication-methods/publickey");
    rcl = lyd_new_path(netconf_node, 0, xpath, "", 0, LYD_PATH_OPT_NOPARENTRET);
    if(rcl == 0) {
        log_error("could not created yang path");
        return NTS_ERR_FAILED;
    }

    sprintf(xpath, "/ietf-netconf-server:netconf-server/call-home/netconf-client[name='default-client']/endpoints/endpoint[name='callhome-ssh']/ssh/ssh-server-parameters/client-authentication/supported-authentication-methods/passsword");
    rcl = lyd_new_path(netconf_node, 0, xpath, "", 0, LYD_PATH_OPT_NOPARENTRET);
    if(rcl == 0) {
        log_error("could not created yang path");
        return NTS_ERR_FAILED;
    }

    sprintf(xpath, "/ietf-netconf-server:netconf-server/call-home/netconf-client[name='default-client']/endpoints/endpoint[name='callhome-ssh']/ssh/ssh-server-parameters/client-authentication/supported-authentication-methods/other");
    rcl = lyd_new_path(netconf_node, 0, xpath, "interactive", 0, LYD_PATH_OPT_NOPARENTRET);
    if(rcl == 0) {
        log_error("could not created yang path");
        return NTS_ERR_FAILED;
    }

    sprintf(xpath, "/ietf-netconf-server:netconf-server/call-home/netconf-client[name='default-client']/endpoints/endpoint[name='callhome-ssh']/ssh/ssh-server-parameters/client-authentication/users");
    rcl = lyd_new_path(netconf_node, 0, xpath, "", 0, LYD_PATH_OPT_NOPARENTRET);
    if(rcl == 0) {
        log_error("could not created yang path");
        return NTS_ERR_FAILED;
    }

    sprintf(xpath, "/ietf-netconf-server:netconf-server/call-home/netconf-client[name='default-client']/connection-type/persistent");
    rcl = lyd_new_path(netconf_node, 0, xpath, "", 0, LYD_PATH_OPT_NOPARENTRET);
    if(rcl == 0) {
        log_error("could not created yang path");
        return NTS_ERR_FAILED;
    }

    rc = send_odl_callhome_configuration(current_session);
    if(rc != NTS_ERR_OK) {
        log_message(2, "could not send ODL Call Home configuration.\n");
    }

    return NTS_ERR_OK;
}

static int create_tls_callhome_endpoint(sr_session_ctx_t *current_session, struct lyd_node *netconf_node) {
    assert(current_session);
    assert(netconf_node);

    // checkAS future usage, TLS endpoint yet supported in ODL
    
    return NTS_ERR_OK;
}


static int send_odl_callhome_configuration(sr_session_ctx_t *current_session) {
    assert(current_session);

    char *public_ssh_key = read_key(SERVER_PUBLIC_SSH_KEY_PATH);
    if(public_ssh_key == 0) {
        log_error("could not read the public ssh key from file %s", SERVER_PUBLIC_SSH_KEY_PATH);
        return NTS_ERR_FAILED;
    }

    char *ssh_key_string;
    ssh_key_string = strtok(public_ssh_key, " ");
    ssh_key_string = strtok(NULL, " ");

    // checkAS we have hardcoded here the username and password of the NETCONF Server
    char *odl_callhome_payload = 0;
    asprintf(&odl_callhome_payload, NETCONF_CALLHOME_CURL_SEND_PAYLOAD_FORMAT, framework_environment.hostname, ssh_key_string);
    free(public_ssh_key);
    if(odl_callhome_payload == 0) {
        log_error("bad asprintf");
        return NTS_ERR_FAILED;
    }

    controller_details_t *controller = controller_details_get(current_session);
    if(controller == 0) {
        log_error("controller_details_get failed");
        return NTS_ERR_FAILED;
    }
    
    char *url = 0;
    asprintf(&url, "%s/rests/data/odl-netconf-callhome-server:netconf-callhome-server/allowed-devices/device=%s", controller->base_url, framework_environment.hostname);
    if(url == 0) {
        log_error("bad asprintf");
        controller_details_free(controller);
        return NTS_ERR_FAILED;
    }

    int rc = http_request(url, controller->username, controller->password, "PUT", odl_callhome_payload, 0, 0);
    if(rc != NTS_ERR_OK) {
        log_error("http_request failed");
    }
    
    free(url);
    controller_details_free(controller);
    free(odl_callhome_payload);

    return rc;
}