#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# This script loads aloha-env.sh if it exists, and ensures it is only loaded once.
# aloha-env.sh is loaded from ALOHA_CONF_DIR if set, or within the current directory's
# conf/ subdirectory.

# Figure out where Aloha is installed
if [[ -z "${ALOHA_HOME}" ]]; then
  export ALOHA_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

ALOHA_ENV_SH="aloha-env.sh"
if [[ -z "$ALOHA_ENV_LOADED" ]]; then
  export ALOHA_ENV_LOADED=1

  export ALOHA_CONF_DIR="${ALOHA_CONF_DIR:-"${ALOHA_HOME}"/conf}"

  ALOHA_ENV_SH="${ALOHA_CONF_DIR}/${ALOHA_ENV_SH}"
  if [[ -f "${ALOHA_ENV_SH}" ]]; then
    # Promote all variable declarations to environment (exported) variables
    set -a
    . ${ALOHA_ENV_SH}
    set +a
  fi
fi