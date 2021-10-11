#!/bin/bash

# Based on: http://www.richud.com/wiki/Ubuntu_Fluxbox_GUI_with_x11vnc_and_Xvfb

readonly G_LOG_I='[INFO]'
readonly G_LOG_W='[WARN]'
readonly G_LOG_E='[ERROR]'

main() {
    run_nginx
    run_uwsgi
    launch_xvfb
}

launch_xvfb() {
    # Set defaults if the user did not specify envs.
    export DISPLAY=${XVFB_DISPLAY:-:1}
    local screen=${XVFB_SCREEN:-0}
    local resolution=${XVFB_RESOLUTION:-1920x1080x24}
    local timeout=${XVFB_TIMEOUT:-5}
    # Start and wait for either Xvfb to be fully up or we hit the timeout.
    sudo /bin/sh -c "rm /tmp/.X0-lock"
    sudo /bin/sh -c "rm /tmp/.X1-lock"
    sudo Xvfb ${DISPLAY} -screen ${screen} ${resolution} &
    local loopCount=0
    until xdpyinfo -display ${DISPLAY} > /dev/null 2>&1
    do
        loopCount=$((loopCount+1))
        sleep 1
        if [ ${loopCount} -gt ${timeout} ]
        then
            echo "${G_LOG_E} xvfb failed to start."
            exit 1
        fi
    done
}

control_c() {
    echo ""
    exit
}

run_nginx() {
    sudo /usr/sbin/nginx -c /etc/nginx/nginx.conf
}

run_uwsgi(){
   uwsgi --ini /usr/src/uwsgi.ini
}

trap control_c SIGINT SIGTERM SIGHUP

main

exit