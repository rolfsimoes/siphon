#!/bin/bash

# Test script for siphon + liteq daemon
# This script makes it easy to test the daemon by handling database setup
# and running both daemon and publisher in sequence or in parallel.
#
# Usage:
#   ./inst/scripts/daemon.sh              # Interactive mode
#   ./inst/scripts/daemon.sh run          # Run daemon in foreground
#   ./inst/scripts/daemon.sh publish      # Publish test messages
#   ./inst/scripts/daemon.sh stop         # Send stop message
#   ./inst/scripts/daemon.sh clean        # Remove test database

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DB_PATH="/tmp/siphon-daemon-test.db"
QUEUE_NAME="jobs"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if R is available
check_r() {
    if ! command -v R &> /dev/null; then
        print_error "R is not installed or not in PATH"
        exit 1
    fi
}

# Run daemon
run_daemon() {
    print_info "Starting daemon..."
    print_info "Database: $DB_PATH"
    print_info "Queue: $QUEUE_NAME"
    print_info "Press Ctrl-C to stop"
    echo ""
    Rscript "$SCRIPT_DIR/daemon.R" "$DB_PATH"
}

# Publish test messages
publish_messages() {
    print_info "Publishing test messages..."
    Rscript "$SCRIPT_DIR/publisher.R" "$DB_PATH" --queue "$QUEUE_NAME"
}

# Send stop message only
send_stop() {
    print_info "Sending stop message..."
    Rscript -e "
library(liteq)
library(jsonlite)
q <- liteq::ensure_queue('$QUEUE_NAME', db = '$DB_PATH')
liteq::publish(q, jsonlite::toJSON(list(action = 'stop'), auto_unbox = TRUE))
message('Stop message sent')
"
}

# Clean up test database
clean_db() {
    if [ -f "$DB_PATH" ]; then
        print_info "Removing test database: $DB_PATH"
        rm "$DB_PATH"
        print_info "Database removed"
    else
        print_warn "Database not found: $DB_PATH"
    fi
}

# Show queue status
show_status() {
    print_info "Queue status:"
    Rscript -e "
library(liteq)
q <- liteq::ensure_queue('$QUEUE_NAME', db = '$DB_PATH')
n <- liteq::n_messages(q)
cat('  Queue: $QUEUE_NAME\n')
cat('  Database: $DB_PATH\n')
cat('  Messages in queue: ', n, '\n', sep = '')
"
}

# Interactive menu
interactive_mode() {
    while true; do
        echo ""
        echo "Siphon Daemon Test Menu"
        echo "======================="
        echo "1) Run daemon (foreground)"
        echo "2) Publish test messages"
        echo "3) Send stop message"
        echo "4) Show queue status"
        echo "5) Clean test database"
        echo "6) Exit"
        echo ""
        read -p "Select option [1-6]: " choice

        case $choice in
            1)
                run_daemon
                ;;
            2)
                publish_messages
                ;;
            3)
                send_stop
                ;;
            4)
                show_status
                ;;
            5)
                clean_db
                ;;
            6)
                print_info "Exiting..."
                exit 0
                ;;
            *)
                print_error "Invalid option"
                ;;
        esac
    done
}

# Main
check_r

case "${1:-}" in
    run)
        run_daemon
        ;;
    publish)
        publish_messages
        ;;
    stop)
        send_stop
        ;;
    clean)
        clean_db
        ;;
    status)
        show_status
        ;;
    "")
        interactive_mode
        ;;
    *)
        echo "Usage: $0 [run|publish|stop|clean|status]"
        echo ""
        echo "Commands:"
        echo "  run      - Run daemon in foreground"
        echo "  publish  - Publish test messages"
        echo "  stop     - Send stop message to daemon"
        echo "  clean    - Remove test database"
        echo "  status   - Show queue status"
        echo "  (none)   - Interactive menu"
        exit 1
        ;;
esac
