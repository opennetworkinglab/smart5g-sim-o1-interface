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

#include "test.h"
#include "utils/debug_utils.h"
#include "utils/log_utils.h"
#include "utils/rand_utils.h"
#include "utils/type_utils.h"
#include "utils/sys_utils.h"
#include <stdio.h>
#include <assert.h>
#include <pthread.h>

#include <libyang/libyang.h>
#include <sysrepo.h>
#include <sysrepo/values.h>
#include "core/app/nf_oran_du.h"
#include "core/session.h"
#include "core/framework.h"
#include "core/docker.h"
#include "core/xpath.h"

#include "core/datastore/schema.h"
#include "core/datastore/populate.h"
#include "core/datastore/operations.h"

static volatile int flag = 0;

typedef struct thread_t {
    int error;
    pthread_t thread;
} thread_t;

static void* apply_change_routine(void *arg) {

    thread_t *context = (thread_t*)arg;
    sr_session_ctx_t *sess;
    char xpath[256] = {0};
    int ret;

    ret = sr_session_start(session_connection, SR_DS_RUNNING, &sess);
    if(ret != SR_ERR_OK) {
        log_error("apply_change_routine() sr_session_start failed\n");
        context->error = NTS_ERR_FAILED;
    }

    sprintf(xpath, NTS_NF_ORAN_DU_CELL_ADMINISTRATIVE_STATE_PARAM_XPATH, "O-DU-1211", "13842601454c001");

    ret = sr_set_item_str(sess, xpath, "locked", NULL, 0);
    if(ret != SR_ERR_OK) {
        log_error("apply_change_routine() sr_set_item_str failed\n");
        context->error = NTS_ERR_FAILED;
    }

    /* wait for subscription before applying changes */
    while (flag == 0) {
        usleep(250);
    }

    /* perform change */
    log_add_verbose(1, "applying changes on %s...\n", xpath);
    ret = sr_apply_changes(sess, 0, 0);
    if(ret != SR_ERR_OK) {
        log_error("apply_change_routine() sr_apply_changes failed\n");
        context->error = NTS_ERR_FAILED;
    }

    sr_session_stop(sess);

    /* signal that we have finished applying changes */
    flag = 0;

    return 0;
}

static void* subscribe_change_routine(void *arg) {

    thread_t *context = (thread_t*)arg;
    sr_session_ctx_t *sess;
    sr_subscription_ctx_t *subscr = NULL;
    int ret;

    ret = sr_session_start(session_connection, SR_DS_RUNNING, &sess);
    if(ret != SR_ERR_OK) {
        log_error("subscribe_change_routine() sr_session_start failed\n");
        context->error = NTS_ERR_FAILED;
    }

    log_add_verbose(1, "subscribing to changes on %s...\n", NTS_NF_ORAN_DU_CELL_ADMINISTRATIVE_STATE_XPATH);
    ret = sr_module_change_subscribe(sess, NTS_NF_ORAN_DU_MODULE, NTS_NF_ORAN_DU_CELL_ADMINISTRATIVE_STATE_XPATH,
        administrative_state_change_cb, NULL, 0, SR_SUBSCR_CTX_REUSE, &subscr);
    if (ret != SR_ERR_OK) {
        log_error("subscribe_change_routine() sr_module_change_subscribe failed\n");
        context->error = NTS_ERR_FAILED;
    }

    /* signal that subscription was created */
    ++flag;

    /* wait for the other thread to apply changes */
    while (flag > 0) {
        usleep(250);
    }

    sr_unsubscribe(subscr);
    sr_session_stop(sess);

    return 0;
}

static int apply_xpath_change()
{
    thread_t change_thread = {.error = NTS_ERR_OK};
    thread_t subscribe_thread = {.error = NTS_ERR_OK};

    if(pthread_create(&change_thread.thread, 0, apply_change_routine, &change_thread)) {
        log_error("could not create change_thread\n");
        return NTS_ERR_FAILED;
    }

    if(pthread_create(&subscribe_thread.thread, 0, subscribe_change_routine, &subscribe_thread)) {
        log_error("could not create subscribe_thread\n");
        return NTS_ERR_FAILED;
    }

    pthread_join(change_thread.thread, NULL);
    pthread_join(subscribe_thread.thread, NULL);

    if (change_thread.error != NTS_ERR_OK || subscribe_thread.error != NTS_ERR_OK)
        return NTS_ERR_FAILED;

    return NTS_ERR_OK;
}

static int test_cell_off(void)
{
    sr_val_t *values = 0;
    size_t value_count = 0;
    sr_session_ctx_t *sess;

    int rc = sr_session_start(session_connection, SR_DS_RUNNING, &sess);
    if(rc != SR_ERR_OK) {
        log_error("sr_session_start failed\n");
        return NTS_ERR_FAILED;
    }

    rc = sr_get_items(sess, NTS_NF_ORAN_DU_CELL_ADMINISTRATIVE_STATE_XPATH, 0, 0, &values, &value_count);
    if(rc != SR_ERR_OK) {
        log_error("get items before change failed\n");
        return NTS_ERR_FAILED;
    }

    log_add_verbose(1, "Printing (%d) values\n", value_count);

    for(int i = 0; i < value_count; i++) {
        log_add_verbose(1, "xpath : %s\n", values[i].xpath);
        log_add_verbose(1, "type : %d\n", (int)values[i].type);
        log_add_verbose(1, "origin : %s\n", values[i].origin);
        log_add_verbose(1, "data : %s\n", values[i].data);

        if (strcmp(values[i].data.enum_val, "unlocked") != 0) {
            log_error("Expected 'unlocked' state\n");
            return NTS_ERR_FAILED;
        }
    }

    rc = apply_xpath_change();
    if(rc != NTS_ERR_OK) {
        log_error("apply_xpath_change failed\n");
        return NTS_ERR_FAILED;
    }

    rc = sr_get_items(sess, NTS_NF_ORAN_DU_CELL_ADMINISTRATIVE_STATE_XPATH, 0, 0, &values, &value_count);
    if(rc != SR_ERR_OK) {
        log_error("get items after change failed\n");
        return NTS_ERR_FAILED;
    }

    if (value_count != 2) {
        log_error("Expected (2) != Actual (%ld)\n", value_count);
        return NTS_ERR_FAILED;
    }

    if (strcmp(values[0].data.enum_val, "locked") != 0) {
        log_error("Expected 'locked' state\n");
        return NTS_ERR_FAILED;
    }

    if (strcmp(values[1].data.enum_val, "unlocked") != 0) {
        log_error("Expected 'unlocked' state\n");
        return NTS_ERR_FAILED;
    }

    sr_free_values(values, value_count);
    sr_session_stop(sess);

    return NTS_ERR_OK;
}

int exhaustive_test_run(void) {
    //first get all xpaths
    char **xpaths = 0;
    int xpaths_count = datastore_schema_get_xpaths(&xpaths);
    if(xpaths_count < 0) {
        log_error("datastore_schema_get_xpaths failed\n");
        return NTS_ERR_FAILED;
    }
    else {
        log_add_verbose(0, "datastore_schema_get_xpaths executed with "LOG_COLOR_BOLD_GREEN"success"LOG_COLOR_RESET" (%d)\n", xpaths_count);
    }

    //switching verbosity level to 0 so we don't see logs
    int old_verbosity_level = framework_arguments.verbosity_level;
    framework_arguments.verbosity_level = 0;

    //testing datastore_schema_print_xpath()
    for(int i = 0 ; i < xpaths_count; i++) {
        int rc = datastore_schema_print_xpath(xpaths[i]);
        if(rc != NTS_ERR_OK) {
            log_error("error in datastore_schema_print_xpath\n");
            return rc;
        }
    }

    log_add_verbose(0, "datastore_schema_print_xpath executed with "LOG_COLOR_BOLD_GREEN"success"LOG_COLOR_RESET" for all paths\n");

    //freeing paths
    for(int i = 0; i < xpaths_count; i++) {
        free(xpaths[i]);
    }
    free(xpaths);

    //testing schema_populate
    int rc = datastore_populate_all();
    if(rc != NTS_ERR_OK) {
        log_error("error in datastore_populate_all\n");
        return rc;
    }

    log_add_verbose(0, "datastore_populate_all executed with "LOG_COLOR_BOLD_GREEN"success"LOG_COLOR_RESET"\n");

    // testing cell_off
    rc = test_cell_off();
    if(rc != NTS_ERR_OK) {
        log_error("error in test_cell_off\n");
        return rc;
    }

    log_add_verbose(0, "test_cell_off executed with "LOG_COLOR_BOLD_GREEN"success"LOG_COLOR_RESET"\n");
    log_add_verbose(0, LOG_COLOR_BOLD_GREEN"ALL TESTS WENT GOOD!"LOG_COLOR_RESET"\n\n\n");

    //switching back verbosity level
    framework_arguments.verbosity_level = old_verbosity_level;

    return NTS_ERR_OK;
}

int test_mode_run(void) {
    assert_session();

   
    return NTS_ERR_OK;
}
